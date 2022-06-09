// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// mvccIncrementalIteratorMetamorphicTBI will randomly enable TBIs.
var mvccIncrementalIteratorMetamorphicTBI = util.ConstantWithMetamorphicTestBool(
	"mvcc-incremental-iter-tbi", true)

// MVCCIncrementalIterator iterates over the diff of the key range
// [startKey,endKey) and time range (startTime,endTime]. If a key was added or
// modified between startTime and endTime, the iterator will position at the
// most recent version (before or at endTime) of that key. If the key was most
// recently deleted, this is signaled with an empty value.
//
// Inline (unversioned) values are not supported, and may return an error or be
// omitted entirely. The iterator should not be used across such keys.
//
// Intents outside the time bounds are ignored. Intents inside the
// time bounds are handled according to the provided
// MVCCIncrementalIterIntentPolicy. By default, an error will be
// returned.
//
// Note: The endTime is inclusive to be consistent with the non-incremental
// iterator, where reads at a given timestamp return writes at that
// timestamp. The startTime is then made exclusive so that iterating time 1 to
// 2 and then 2 to 3 will only return values with time 2 once. An exclusive
// start time would normally make it difficult to scan timestamp 0, but
// CockroachDB uses that as a sentinel for key metadata anyway.
//
// Expected usage:
//    iter := NewMVCCIncrementalIterator(e, IterOptions{
//        StartTime:  startTime,
//        EndTime:    endTime,
//        UpperBound: endKey,
//    })
//    defer iter.Close()
//    for iter.SeekGE(startKey); ; iter.Next() {
//        ok, err := iter.Valid()
//        if !ok { ... }
//        [code using iter.Key() and iter.Value()]
//    }
//
// Note regarding the correctness of the time-bound iterator optimization:
//
// When using (t_s, t_e], say there is a version (committed or provisional)
// k@t where t is in that interval, that is visible to iter. All sstables
// containing k@t will be included in timeBoundIter. Note that there may be
// multiple sequence numbers for the key k@t at the storage layer, say k@t#n1,
// k@t#n2, where n1 > n2, some of which may be deleted, but the latest
// sequence number will be visible using iter (since not being visible would be
// a contradiction of the initial assumption that k@t is visible to iter).
// Since there is no delete across all sstables that deletes k@t#n1, there is
// no delete in the subset of sstables used by timeBoundIter that deletes
// k@t#n1, so the timeBoundIter will see k@t.
type MVCCIncrementalIterator struct {
	iter MVCCIterator

	// A time-bound iterator cannot be used by itself due to a bug in the time-
	// bound iterator (#28358). This was historically augmented with an iterator
	// without the time-bound optimization to act as a sanity iterator, but
	// issues remained (#43799), so now the iterator above is the main iterator
	// the timeBoundIter is used to check if any keys can be skipped by the main
	// iterator.
	timeBoundIter MVCCIterator

	startTime hlc.Timestamp
	endTime   hlc.Timestamp
	err       error
	valid     bool

	// For allocation avoidance, meta is used to store the timestamp of keys
	// regardless if they are metakeys.
	meta enginepb.MVCCMetadata

	// Configuration passed in MVCCIncrementalIterOptions.
	intentPolicy MVCCIncrementalIterIntentPolicy

	// Optional collection of intents created on demand when first intent encountered.
	intents []roachpb.Intent
}

var _ SimpleMVCCIterator = &MVCCIncrementalIterator{}

// MVCCIncrementalIterIntentPolicy controls how the
// MVCCIncrementalIterator will handle intents that it encounters
// when iterating.
type MVCCIncrementalIterIntentPolicy int

const (
	// MVCCIncrementalIterIntentPolicyError will immediately
	// return an error for any intent found inside the given time
	// range.
	MVCCIncrementalIterIntentPolicyError MVCCIncrementalIterIntentPolicy = iota
	// MVCCIncrementalIterIntentPolicyAggregate will not fail on
	// first encountered intent, but will proceed further. All
	// found intents will be aggregated into a single
	// WriteIntentError which would be updated during
	// iteration. Consumer would be free to decide if it wants to
	// keep collecting entries and intents or skip entries.
	MVCCIncrementalIterIntentPolicyAggregate
	// MVCCIncrementalIterIntentPolicyEmit will return intents to
	// the caller if they are inside the time range. Intents
	// outside of the time range will be filtered without error.
	//
	// TODO(ssd): If we relaxed the requirement that intents are
	// filtered by time range, we could avoid parsing intents
	// inside the iterator and leave it to the caller to deal
	// with.
	MVCCIncrementalIterIntentPolicyEmit
)

// MVCCIncrementalIterOptions bundles options for NewMVCCIncrementalIterator.
type MVCCIncrementalIterOptions struct {
	EndKey roachpb.Key

	// Only keys within (StartTime,EndTime] will be emitted. EndTime defaults to
	// hlc.MaxTimestamp. The time-bound iterator optimization will only be used if
	// StartTime is set, since we assume EndTime will be near the current time.
	StartTime hlc.Timestamp
	EndTime   hlc.Timestamp

	IntentPolicy MVCCIncrementalIterIntentPolicy
}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified reader and options. The timestamp hint range should not be more
// restrictive than the start and end time range.
func NewMVCCIncrementalIterator(
	reader Reader, opts MVCCIncrementalIterOptions,
) *MVCCIncrementalIterator {
	// Default to MaxTimestamp for EndTime, since the code assumes it is set.
	if opts.EndTime.IsEmpty() {
		opts.EndTime = hlc.MaxTimestamp
	}

	// We assume EndTime is near the current time, so there is little to gain from
	// using a TBI unless StartTime is set. However, we always vary it in
	// metamorphic test builds, for better test coverage of both paths.
	useTBI := opts.StartTime.IsSet()
	if util.IsMetamorphicBuild() { // NB: always randomize when metamorphic
		useTBI = mvccIncrementalIteratorMetamorphicTBI
	}

	var iter MVCCIterator
	var timeBoundIter MVCCIterator
	if useTBI {
		// An iterator without the timestamp hints is created to ensure that the
		// iterator visits every required version of every key that has changed.
		iter = reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			UpperBound: opts.EndKey,
		})
		// The timeBoundIter is only required to see versioned keys, since the
		// intents will be found by iter.
		timeBoundIter = reader.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
			UpperBound: opts.EndKey,
			// The call to startTime.Next() converts our exclusive start bound into
			// the inclusive start bound that MinTimestampHint expects.
			MinTimestampHint: opts.StartTime.Next(),
			MaxTimestampHint: opts.EndTime,
		})
	} else {
		iter = reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			UpperBound: opts.EndKey,
		})
	}

	return &MVCCIncrementalIterator{
		iter:          iter,
		startTime:     opts.StartTime,
		endTime:       opts.EndTime,
		timeBoundIter: timeBoundIter,
		intentPolicy:  opts.IntentPolicy,
	}
}

// SeekGE implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) SeekGE(startKey MVCCKey) {
	if i.timeBoundIter != nil {
		// Check which is the first key seen by the TBI.
		i.timeBoundIter.SeekGE(startKey)
		if ok, err := i.timeBoundIter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}
		tbiKey := i.timeBoundIter.Key().Key
		if tbiKey.Compare(startKey.Key) > 0 {
			// If the first key that the TBI sees is ahead of the given startKey, we
			// can seek directly to the first version of the key.
			startKey = MakeMVCCMetadataKey(tbiKey)
		}
	}
	i.iter.SeekGE(startKey)
	if !i.updateValid() {
		return
	}
	i.err = nil
	i.valid = true
	i.advance()
}

// Close implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) Close() {
	i.iter.Close()
	if i.timeBoundIter != nil {
		i.timeBoundIter.Close()
	}
}

// Next implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) Next() {
	i.iter.Next()
	if !i.updateValid() {
		return
	}
	i.advance()
}

// updateValid updates i.valid and i.err based on the underlying iterator, and
// returns true if valid.
// gcassert:inline
func (i *MVCCIncrementalIterator) updateValid() bool {
	if ok, err := i.iter.Valid(); !ok {
		i.err = err
		i.valid = false
		return false
	}
	return true
}

// NextKey implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) NextKey() {
	i.iter.NextKey()
	if !i.updateValid() {
		return
	}
	i.advance()
}

// maybeSkipKeys checks if any keys can be skipped by using a time-bound
// iterator. If keys can be skipped, it will update the main iterator to point
// to the earliest version of the next candidate key.
// It is expected (but not required) that TBI is at a key <= main iterator key
// when calling maybeSkipKeys().
func (i *MVCCIncrementalIterator) maybeSkipKeys() {
	if i.timeBoundIter == nil {
		// If there is no time bound iterator, we cannot skip any keys.
		return
	}
	tbiKey := i.timeBoundIter.UnsafeKey().Key
	iterKey := i.iter.UnsafeKey().Key
	if iterKey.Compare(tbiKey) > 0 {
		// If the iterKey got ahead of the TBI key, advance the TBI Key.
		//
		// We fast-path the case where the main iterator is referencing the next
		// key that would be visited by the TBI. In that case, after the following
		// NextKey call, we will have iterKey == tbiKey. This means that for the
		// incremental iterator to perform a Next or NextKey will require only 1
		// extra NextKey invocation while they remain in lockstep. This case will
		// be common if most keys are modified, or the modifications are clustered
		// in keyspace, which makes the incremental iterator optimization
		// ineffective. And so in this case we want to minimize the extra cost of
		// using the incremental iterator, by avoiding a SeekGE.
		i.timeBoundIter.NextKey()
		if ok, err := i.timeBoundIter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}
		tbiKey = i.timeBoundIter.UnsafeKey().Key

		cmp := iterKey.Compare(tbiKey)

		if cmp > 0 {
			// If the tbiKey is still behind the iterKey, the TBI key may be seeing
			// phantom MVCCKey.Keys. These keys may not be seen by the main iterator
			// due to aborted transactions and keys which have been subsumed due to
			// range tombstones. In this case we can SeekGE() the TBI to the main iterator.
			seekKey := MakeMVCCMetadataKey(iterKey)
			i.timeBoundIter.SeekGE(seekKey)
			if ok, err := i.timeBoundIter.Valid(); !ok {
				i.err = err
				i.valid = false
				return
			}
			tbiKey = i.timeBoundIter.UnsafeKey().Key
			cmp = iterKey.Compare(tbiKey)
		}

		if cmp < 0 {
			// In the case that the next MVCC key that the TBI observes is not the
			// same as the main iterator, we may be able to skip over a large group
			// of keys. The main iterator is seeked to the TBI in hopes that many
			// keys were skipped. Note that a Seek is an order of magnitude more
			// expensive than a Next call, but the engine has low-level
			// optimizations that attempt to make it cheaper if the seeked key is
			// "nearby" (within the same sstable block).
			seekKey := MakeMVCCMetadataKey(tbiKey)
			i.iter.SeekGE(seekKey)
			if !i.updateValid() {
				return
			}
		}
	}
}

// updateMeta initializes i.meta. It sets i.err and returns an error on any
// errors, e.g. if it encounters an intent in the time span (startTime, endTime]
// or an inline value.
func (i *MVCCIncrementalIterator) updateMeta() error {
	unsafeKey := i.iter.UnsafeKey()
	if unsafeKey.IsValue() {
		// The key is an MVCC value and not an intent or inline.
		i.meta.Reset()
		i.meta.Timestamp = unsafeKey.Timestamp.ToLegacyTimestamp()
		return nil
	}

	// The key is a metakey (an intent or inline meta). If an inline meta, we
	// will handle below. If an intent meta, then this is used later to see if
	// the timestamp of this intent is within the incremental iterator's time
	// bounds.
	if i.err = protoutil.Unmarshal(i.iter.UnsafeValue(), &i.meta); i.err != nil {
		i.valid = false
		return i.err
	}

	if i.meta.IsInline() {
		i.valid = false
		i.err = errors.Errorf("unexpected inline value found: %s", unsafeKey.Key)
		return i.err
	}

	if i.meta.Txn == nil {
		i.valid = false
		i.err = errors.Errorf("intent is missing a txn: %s", unsafeKey.Key)
	}

	metaTimestamp := i.meta.Timestamp.ToTimestamp()
	if i.startTime.Less(metaTimestamp) && metaTimestamp.LessEq(i.endTime) {
		switch i.intentPolicy {
		case MVCCIncrementalIterIntentPolicyError:
			i.err = &roachpb.WriteIntentError{
				Intents: []roachpb.Intent{
					roachpb.MakeIntent(i.meta.Txn, i.iter.Key().Key),
				},
			}
			i.valid = false
			return i.err
		case MVCCIncrementalIterIntentPolicyAggregate:
			// We are collecting intents, so we need to save it and advance to its proposed value.
			// Caller could then use a value key to update proposed row counters for the sake of bookkeeping
			// and advance more.
			i.intents = append(i.intents, roachpb.MakeIntent(i.meta.Txn, i.iter.Key().Key))
			return nil
		case MVCCIncrementalIterIntentPolicyEmit:
			// We will emit this intent to the caller.
			return nil
		default:
			return errors.AssertionFailedf("unknown intent policy: %d", i.intentPolicy)
		}
	}
	return nil
}

// advance advances the main iterator until it is referencing a key within
// (start_time, end_time].
//
// It populates i.err with an error if it encountered an inline value or an
// intent with a timestamp within the incremental iterator's bounds when the
// intent policy is MVCCIncrementalIterIntentPolicyError.
func (i *MVCCIncrementalIterator) advance() {
	for {
		i.maybeSkipKeys()
		if !i.valid {
			return
		}

		if err := i.updateMeta(); err != nil {
			return
		}

		// INVARIANT: we have an intent or an MVCC value.

		if i.meta.Txn != nil {
			switch i.intentPolicy {
			case MVCCIncrementalIterIntentPolicyEmit:
				// If our policy is emit, we may want this
				// intent. If it is outside our time bounds, it
				// will be filtered below.
			case MVCCIncrementalIterIntentPolicyError, MVCCIncrementalIterIntentPolicyAggregate:
				// We have encountered an intent but it must lie
				// outside the timestamp span (startTime,
				// endTime] or we have aggregated it. In either
				// case, we want to advance past it.
				i.iter.Next()
				if !i.updateValid() {
					return
				}
				continue
			}
		}

		// Note that MVCC keys are sorted by key, then by _descending_ timestamp
		// order with the exception of the metakey (timestamp 0) being sorted
		// first.
		metaTimestamp := i.meta.Timestamp.ToTimestamp()
		if i.endTime.Less(metaTimestamp) {
			i.iter.Next()
		} else if metaTimestamp.LessEq(i.startTime) {
			i.iter.NextKey()
		} else {
			// The current key is a valid user key and within the time bounds. We are
			// done.
			break
		}
		if !i.updateValid() {
			return
		}
	}
}

// Valid implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) Valid() (bool, error) {
	return i.valid, i.err
}

// UnsafeKey implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) UnsafeKey() MVCCKey {
	return i.iter.UnsafeKey()
}

// HasPointAndRange implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) HasPointAndRange() (bool, bool) {
	panic("not implemented")
}

// RangeBounds implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) RangeBounds() roachpb.Span {
	panic("not implemented")
}

// RangeKeys implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) RangeKeys() []MVCCRangeKeyValue {
	panic("not implemented")
}

// UnsafeValue implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) UnsafeValue() []byte {
	return i.iter.UnsafeValue()
}

// NextIgnoringTime returns the next key/value that would be encountered in a
// non-incremental iteration by moving the underlying non-TBI iterator forward.
// Intents in the time range (startTime,EndTime] are handled according to the
// iterator policy.
func (i *MVCCIncrementalIterator) NextIgnoringTime() {
	for {
		i.iter.Next()
		if !i.updateValid() {
			return
		}

		if err := i.updateMeta(); err != nil {
			return
		}

		// We have encountered an intent but it does not lie in the timestamp span
		// (startTime, endTime] so we do not throw an error, and attempt to move to
		// the next valid KV.
		if i.meta.Txn != nil && i.intentPolicy != MVCCIncrementalIterIntentPolicyEmit {
			continue
		}

		// We have a valid KV or an intent to emit.
		return
	}
}

// NumCollectedIntents returns number of intents encountered during iteration.
// This is only the case when intent aggregation is enabled, otherwise it is
// always 0.
func (i *MVCCIncrementalIterator) NumCollectedIntents() int {
	return len(i.intents)
}

// TryGetIntentError returns roachpb.WriteIntentError if intents were encountered
// during iteration and intent aggregation is enabled. Otherwise function
// returns nil. roachpb.WriteIntentError will contain all encountered intents.
func (i *MVCCIncrementalIterator) TryGetIntentError() error {
	if len(i.intents) == 0 {
		return nil
	}
	return &roachpb.WriteIntentError{
		Intents: i.intents,
	}
}
