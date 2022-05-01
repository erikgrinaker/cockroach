// Copyright 2014 The Cockroach Authors.
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
	"bytes"
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// DefaultStorageEngine represents the default storage engine to use.
var DefaultStorageEngine enginepb.EngineType

func init() {
	_ = DefaultStorageEngine.Set(envutil.EnvOrDefaultString("COCKROACH_STORAGE_ENGINE", "pebble"))
}

// SimpleMVCCIterator is an interface for iterating over key/value pairs in an
// engine. SimpleMVCCIterator implementations are thread safe unless otherwise
// noted. SimpleMVCCIterator is a subset of the functionality offered by
// MVCCIterator.
//
// The iterator exposes both point keys and range keys. Range keys are only
// emitted when enabled via IterOptions.KeyTypes. Currently, all range keys are
// MVCC range tombstones, and this is enforced during writes.
//
// Range keys and point keys exist separately in Pebble. A specific key position
// can have both a point key and multiple range keys overlapping it. Their
// properties are accessed via:
//
// HasPointAndRange(): Key types present at the current position.
// UnsafeKey():        Current position (point key if any).
// UnsafeValue():      Current point key value (if any).
// RangeBounds():      Start,end bounds of range keys at current position.
// RangeKeys():        All range keys/values overlapping current position.
//
// Consider the following point keys and range keys:
//
//     4: a4  b4
//     3: [-------)
//     2: [-------)
//     1:     b1  c1
//        a   b   c
//
// Range keys cover a span between two roachpb.Key bounds (start inclusive, end
// exclusive) and contain timestamp/value pairs. They overlap *all* point key
// versions within their key bounds regardless of timestamp. For example, when
// the iterator is positioned on b@4, it will also expose [a-c)@3 and [a-c)@2.
//
// During iteration with IterKeyTypePointsAndRanges, range keys are emitted at
// their start key and at every overlapping point key. For example, iterating
// across the above span would emit this sequence:
//
// UnsafeKey HasPointAndRange UnsafeValue RangeKeys
// a         false,true       -           [a-c)@3 [a-c)@2
// a@4       true,true        a4          [a-c)@3 [a-c)@2
// b@4       true,true        b4          [a-c)@3 [a-c)@2
// b@1       true,true        b1          [a-c)@3 [a-c)@2
// c@1       true,false       c1          -
//
// MVCCIterator reverse iteration yields the above sequence in reverse.
// Notably, bare range keys are still emitted at their start key (not end key),
// so they will be emitted last in this example.
//
// When using SeekGE within range key bounds, the iterator may land on the bare
// range key first, unless seeking exactly to an existing point key. E.g.:
//
// SeekGE UnsafeKey HasPointAndRange UnsafeValue RangeKeys
// b      b         false,true       -           [a-c)@3 [a-c)@2
// b@5    b@5       false,true       -           [a-c)@3 [a-c)@2
// b@4    b@4       true,true        b@4         [a-c)@3 [a-c)@2
// b@3    b@3       false,true       -           [a-c)@3 [a-c)@2
//
// Note that intents (with timestamp 0) encode to a bare roachpb.Key, so they
// will be colocated with a range key start bound. For example, if there was an
// intent on a in the above example, then both SeekGE(a) and forward iteration
// would land on a@0 and [a-c)@3,[a-c)@2 simultaneously, instead of the bare
// range keys first.
//
// Range keys do not have a stable, discrete identity, and should be
// considered a continuum: they may be merged or fragmented by other range key
// writes, split and merged along with CRDB ranges, partially removed by GC,
// and truncated by iterator bounds.
//
// Range keys are fragmented by Pebble such that all overlapping range keys
// between two keys form a stack of range key fragments at different timestamps.
// For example, writing [a-e)@1 and [c-g)@2 will yield this fragment structure:
//
//     2:     |---|---|
//     1: |---|---|
//        a   c   e   g
//
// Fragmentation makes all range key properties local, which avoids incurring
// unnecessary access costs across SSTs and CRDB ranges. It is deterministic
// on the current range key state, and does not depend on write history.
// Stacking allows easy access to all range keys overlapping a point key.
//
// TODO(erikgrinaker): Write a tech note on range keys and link it here.
type SimpleMVCCIterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// SeekGE advances the iterator to the first key in the engine which is >= the
	// provided key. This may be in the middle of a bare range key straddling the
	// seek key.
	SeekGE(key MVCCKey)
	// Valid must be called after any call to Seek(), Next(), Prev(), or
	// similar methods. It returns (true, nil) if the iterator points to
	// a valid key (it is undefined to call Key(), Value(), or similar
	// methods unless Valid() has returned (true, nil)). It returns
	// (false, nil) if the iterator has moved past the end of the valid
	// range, or (false, err) if an error has occurred. Valid() will
	// never return true with a non-nil error.
	Valid() (bool, error)
	// Next advances the iterator to the next key in the iteration. After this
	// call, Valid() will be true if the iterator was not positioned at the last
	// key.
	Next()
	// NextKey advances the iterator to the next MVCC key. This operation is
	// distinct from Next which advances to the next version of the current key
	// or the next key if the iterator is currently located at the last version
	// for a key. NextKey must not be used to switch iteration direction from
	// reverse iteration to forward iteration.
	//
	// If there is a point key and range key starting at the same key, e.g. a@3
	// and [a-c)@2, then these will be emitted separately with the bare range key
	// first -- unless the point key is an intent, which is colocated with the
	// start key and emitted together with it.
	NextKey()
	// UnsafeKey returns the same value as Key, but the memory is invalidated on
	// the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	UnsafeKey() MVCCKey
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	UnsafeValue() []byte
	// HasPointAndRange returns whether the current iterator position has a point
	// key and/or a range key. If Valid() returns true, one of these will be true,
	// otherwise they are both false. For details on range keys, see comment on
	// SimpleMVCCIterator.
	HasPointAndRange() (bool, bool)
	// RangeBounds returns the range bounds for the current range key, or an
	// empty span if there are none. The returned keys are only valid until the
	// next iterator call.
	RangeBounds() roachpb.Span
	// RangeKeys returns all range keys (with different timestamps) at the current
	// key position, or an empty list if there are none. When at a point key, it
	// will return all range keys overlapping that point key. The keys are only
	// valid until the next iterator operation. For details on range keys, see
	// comment on SimpleMVCCIterator.
	RangeKeys() []MVCCRangeKeyValue
}

// IteratorStats is returned from {MVCCIterator,EngineIterator}.Stats.
type IteratorStats struct {
	TimeBoundNumSSTs int

	// Iteration stats. We directly expose pebble.IteratorStats. Callers
	// may want to aggregate and interpret these in the following manner:
	// - Aggregate {Forward,Reverse}SeekCount, {Forward,Reverse}StepCount.
	// - Interpret the four aggregated stats as follows:
	//   - {SeekCount,StepCount}[InterfaceCall]: We can refer to these simply as
	//     {SeekCount,StepCount} in logs/metrics/traces. These represents
	//     explicit calls by the implementation of MVCCIterator, in response to
	//     the caller of MVCCIterator. A high count relative to the unique MVCC
	//     keys returned suggests there are many versions for the same key.
	//   - {SeekCount,StepCount}[InternalIterCall]: We can refer to these simply
	//     as {InternalSeekCount,InternalStepCount}. If these are significantly
	//     larger than the ones in the preceding bullet, it suggests that there
	//     are lots of uncompacted deletes or stale Pebble-versions (not to be
	//     confused with MVCC versions) that need to be compacted away. This
	//     should be very rare, but has been observed.
	Stats pebble.IteratorStats
}

// MVCCIterator is an interface for iterating over key/value pairs in an
// engine. It is used for iterating over the key space that can have multiple
// versions, and if often also used (due to historical reasons) for iterating
// over the key space that never has multiple versions (i.e.,
// MVCCKey.Timestamp.IsEmpty()).
//
// MVCCIterator implementations are thread safe unless otherwise noted.
//
// For details on range keys and iteration, see comment on SimpleMVCCIterator.
type MVCCIterator interface {
	SimpleMVCCIterator

	// SeekLT advances the iterator to the first key in the engine which is < the
	// provided key. Unlike SeekGE, when calling SeekLT within range key bounds
	// this will not land on the seek key, but rather on the closest point key
	// overlapping the range key or the range key's start bound.
	SeekLT(key MVCCKey)
	// Prev moves the iterator backward to the previous key in the iteration.
	// After this call, Valid() will be true if the iterator was not positioned at
	// the first key.
	Prev()

	// SeekIntentGE is a specialized version of SeekGE(MVCCKey{Key: key}), when
	// the caller expects to find an intent, and additionally has the txnUUID
	// for the intent it is looking for. When running with separated intents,
	// this can optimize the behavior of the underlying Engine for write heavy
	// keys by avoiding the need to iterate over many deleted intents.
	SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID)

	// Key returns the current key position. This may be a point key, or
	// the current position inside a range key (typically the start key
	// or the seek key when using SeekGE within its bounds).
	Key() MVCCKey
	// UnsafeRawKey returns the current raw key which could be an encoded
	// MVCCKey, or the more general EngineKey (for a lock table key).
	// This is a low-level and dangerous method since it will expose the
	// raw key of the lock table, i.e., the intentInterleavingIter will not
	// hide the difference between interleaved and separated intents.
	// Callers should be very careful when using this. This is currently
	// only used by callers who are iterating and deleting all data in a
	// range.
	UnsafeRawKey() []byte
	// UnsafeRawMVCCKey returns a serialized MVCCKey. The memory is invalidated
	// on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}. If the
	// iterator is currently positioned at a separated intent (when
	// intentInterleavingIter is used), it makes that intent look like an
	// interleaved intent key, i.e., an MVCCKey with an empty timestamp. This is
	// currently used by callers who pass around key information as a []byte --
	// this seems avoidable, and we should consider cleaning up the callers.
	UnsafeRawMVCCKey() []byte
	// Value returns the current point key value as a byte slice.
	Value() []byte
	// ValueProto unmarshals the value the iterator is currently
	// pointing to using a protobuf decoder.
	ValueProto(msg protoutil.Message) error
	// ComputeStats scans the underlying engine from start to end keys and
	// computes stats counters based on the values. This method is used after a
	// range is split to recompute stats for each subrange. The nowNanos arg
	// specifies the wall time in nanoseconds since the epoch and is used to
	// compute the total age of intents and garbage.
	//
	// To properly account for intents and range keys, the iterator must be
	// created with MVCCKeyAndIntentsIterKind and IterKeyTypePointsAndRanges,
	// and the LowerBound and UpperBound must be set equal to start and end
	// in order for range keys to be truncated to the bounds.
	//
	// TODO(erikgrinaker): This should be replaced by ComputeStatsForRange
	// instead, which should set up its own iterator with appropriate options.
	// This isn't currently done in order to do spanset assertions on it, but this
	// could be better solved by checking the iterator bounds in NewMVCCIterator
	// and requiring callers to set them appropriately.
	ComputeStats(start, end roachpb.Key, nowNanos int64) (enginepb.MVCCStats, error)
	// FindSplitKey finds a key from the given span such that the left side of
	// the split is roughly targetSize bytes. The returned key will never be
	// chosen from the key ranges listed in keys.NoSplitSpans and will always
	// sort equal to or after minSplitKey.
	//
	// DO NOT CALL directly (except in wrapper MVCCIterator implementations). Use the
	// package-level MVCCFindSplitKey instead. For correct operation, the caller
	// must set the upper bound on the iterator before calling this method.
	FindSplitKey(start, end, minSplitKey roachpb.Key, targetSize int64) (MVCCKey, error)
	// Stats returns statistics about the iterator.
	Stats() IteratorStats
	// SupportsPrev returns true if MVCCIterator implementation supports reverse
	// iteration with Prev() or SeekLT().
	SupportsPrev() bool
}

// EngineIterator is an iterator over key-value pairs where the key is
// an EngineKey.
type EngineIterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// SeekEngineKeyGE advances the iterator to the first key in the engine
	// which is >= the provided key.
	SeekEngineKeyGE(key EngineKey) (valid bool, err error)
	// SeekEngineKeyLT advances the iterator to the first key in the engine
	// which is < the provided key.
	SeekEngineKeyLT(key EngineKey) (valid bool, err error)
	// NextEngineKey advances the iterator to the next key/value in the
	// iteration. After this call, valid will be true if the iterator was not
	// originally positioned at the last key. Note that unlike
	// MVCCIterator.NextKey, this method does not skip other versions with the
	// same EngineKey.Key.
	// TODO(sumeer): change MVCCIterator.Next() to match the
	// return values, change all its callers, and rename this
	// to Next().
	NextEngineKey() (valid bool, err error)
	// PrevEngineKey moves the iterator backward to the previous key/value in
	// the iteration. After this call, valid will be true if the iterator was
	// not originally positioned at the first key.
	PrevEngineKey() (valid bool, err error)
	// UnsafeEngineKey returns the same value as EngineKey, but the memory is
	// invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	// REQUIRES: latest positioning function returned valid=true.
	UnsafeEngineKey() (EngineKey, error)
	// EngineKey returns the current key.
	// REQUIRES: latest positioning function returned valid=true.
	EngineKey() (EngineKey, error)
	// UnsafeRawEngineKey returns the current raw (encoded) key corresponding to
	// EngineKey. This is a low-level method and callers should avoid using
	// it. This is currently only used by intentInterleavingIter to implement
	// UnsafeRawKey.
	UnsafeRawEngineKey() []byte
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	// REQUIRES: latest positioning function returned valid=true.
	UnsafeValue() []byte
	// Value returns the current value as a byte slice.
	// REQUIRES: latest positioning function returned valid=true.
	Value() []byte
	// GetRawIter is a low-level method only for use in the storage package,
	// that returns the underlying pebble Iterator.
	GetRawIter() *pebble.Iterator
	// SeekEngineKeyGEWithLimit is similar to SeekEngineKeyGE, but takes an
	// additional exclusive upper limit parameter. The limit is semantically
	// best-effort, and is an optimization to avoid O(n^2) iteration behavior in
	// some pathological situations (uncompacted deleted locks).
	SeekEngineKeyGEWithLimit(key EngineKey, limit roachpb.Key) (state pebble.IterValidityState, err error)
	// SeekEngineKeyLTWithLimit is similar to SeekEngineKeyLT, but takes an
	// additional inclusive lower limit parameter. The limit is semantically
	// best-effort, and is an optimization to avoid O(n^2) iteration behavior in
	// some pathological situations (uncompacted deleted locks).
	SeekEngineKeyLTWithLimit(key EngineKey, limit roachpb.Key) (state pebble.IterValidityState, err error)
	// NextEngineKeyWithLimit is similar to NextEngineKey, but takes an
	// additional exclusive upper limit parameter. The limit is semantically
	// best-effort, and is an optimization to avoid O(n^2) iteration behavior in
	// some pathological situations (uncompacted deleted locks).
	NextEngineKeyWithLimit(limit roachpb.Key) (state pebble.IterValidityState, err error)
	// PrevEngineKeyWithLimit is similar to PrevEngineKey, but takes an
	// additional inclusive lower limit parameter. The limit is semantically
	// best-effort, and is an optimization to avoid O(n^2) iteration behavior in
	// some pathological situations (uncompacted deleted locks).
	PrevEngineKeyWithLimit(limit roachpb.Key) (state pebble.IterValidityState, err error)
	// Stats returns statistics about the iterator.
	Stats() IteratorStats
}

// IterOptions contains options used to create an {MVCC,Engine}Iterator.
//
// For performance, every {MVCC,Engine}Iterator must specify either Prefix or
// UpperBound.
type IterOptions struct {
	// If Prefix is true, Seek will use the user-key prefix of the supplied
	// {MVCC,Engine}Key (the Key field) to restrict which sstables are searched,
	// but iteration (using Next) over keys without the same user-key prefix
	// will not work correctly (keys may be skipped).
	Prefix bool
	// LowerBound gives this iterator an inclusive lower bound. Attempts to
	// SeekReverse or Prev to a key that is strictly less than the bound will
	// invalidate the iterator.
	LowerBound roachpb.Key
	// UpperBound gives this iterator an exclusive upper bound. Attempts to Seek
	// or Next to a key that is greater than or equal to the bound will invalidate
	// the iterator. UpperBound must be provided unless Prefix is true, in which
	// case the end of the prefix will be used as the upper bound.
	UpperBound roachpb.Key
	// If WithStats is true, the iterator accumulates performance
	// counters over its lifetime which can be queried via `Stats()`.
	WithStats bool
	// MinTimestampHint and MaxTimestampHint, if set, indicate that keys outside
	// of the time range formed by [MinTimestampHint, MaxTimestampHint] do not
	// need to be presented by the iterator. The underlying iterator may be able
	// to efficiently skip over keys outside of the hinted time range, e.g., when
	// an SST indicates that it contains no keys within the time range.
	//
	// Note that time bound hints are strictly a performance optimization, and
	// iterators with time bounds hints will frequently return keys outside of the
	// [start, end] time range. If you must guarantee that you never see a key
	// outside of the time bounds, perform your own filtering.
	//
	// These fields are only relevant for MVCCIterators. Additionally, an
	// MVCCIterator with timestamp hints will not see separated intents, and may
	// not see some interleaved intents. Currently, the only way to correctly
	// use such an iterator is to use it in concert with an iterator without
	// timestamp hints, as done by MVCCIncrementalIterator.
	MinTimestampHint, MaxTimestampHint hlc.Timestamp
	// KeyTypes specifies the types of keys to surface: point and/or range keys.
	// Use HasPointAndRange() to determine which key type is present at a given
	// iterator position, and RangeBounds() and RangeKeys() to access range keys.
	// Defaults to IterKeyTypePointsOnly. For more details on range keys, see
	// comment on SimpleMVCCIterator.
	//
	// NB: Range keys are only supported for use with MVCCIterators, but it is
	// legal to enable them for EngineIterators in order to derive cloned
	// MVCCIterators from them. Range key behavior for EngineIterators is
	// undefined, and we expect users who enable this to only iterate over parts
	// of the key space that can never contain range keys. This ensures that the
	// undefined behavior does not affect such users.
	KeyTypes IterKeyType
	// RangeKeyMaskingBelow enables masking (hiding) of point keys by range keys.
	// Any range key with a timestamp at or below RangeKeyMaskingBelow
	// will mask point keys below it, preventing them from being surfaced.
	// Consider the following example:
	//
	// 4          o---------------o    RangeKeyMaskingBelow=4 emits b3
	// 3      b3      d3               RangeKeyMaskingBelow=3 emits b3,d3,f2
	// 2  o---------------o   f2       RangeKeyMaskingBelow=2 emits b3,d3,f2
	// 1  a1  b1          o-------o    RangeKeyMaskingBelow=1 emits a1,b3,b1,d3,f2
	//    a   b   c   d   e   f   g
	//
	// Range keys themselves are not affected by the masking, and will be
	// emitted as normal.
	RangeKeyMaskingBelow hlc.Timestamp
	// useL6Filters allows the caller to opt into reading filter blocks for
	// L6 sstables. Only for use with Prefix = true. Helpful if a lot of prefix
	// Seeks are expected in quick succession, that are also likely to not
	// yield a single key. Filter blocks in L6 can be relatively large, often
	// larger than data blocks, so the benefit of loading them in the cache
	// is minimized if the probability of the key existing is not low or if
	// this is a one-time Seek (where loading the data block directly is better).
	useL6Filters bool
}

// IterKeyType configures which types of keys an iterator should surface.
//
// TODO(erikgrinaker): Combine this with MVCCIterKind somehow.
type IterKeyType = pebble.IterKeyType

const (
	// IterKeyTypePointsOnly iterates over point keys only.
	IterKeyTypePointsOnly = pebble.IterKeyTypePointsOnly
	// IterKeyTypePointsAndRanges iterates over both point and range keys.
	IterKeyTypePointsAndRanges = pebble.IterKeyTypePointsAndRanges
	// IterKeyTypeRangesOnly iterates over only range keys.
	IterKeyTypeRangesOnly = pebble.IterKeyTypeRangesOnly
)

// MVCCIterKind is used to inform Reader about the kind of iteration desired
// by the caller.
type MVCCIterKind int

// "Intent" refers to non-inline meta, that can be interleaved or separated.
const (
	// MVCCKeyAndIntentsIterKind specifies that intents must be seen, and appear
	// interleaved with keys, even if they are in a separated lock table.
	// Iterators of this kind are not allowed to span from local to global keys,
	// since the physical layout has the separated lock table in-between the
	// local and global keys. These iterators do strict error checking and panic
	// if the caller seems that to be trying to violate this constraint.
	// Specifically:
	// - If both bounds are set they must not span from local to global.
	// - Any bound (lower or upper), constrains the iterator for its lifetime to
	//   one of local or global keys. The iterator will not tolerate a seek that
	//   violates this constraint.
	// We could, with significant code complexity, not constrain an iterator for
	// its lifetime, and allow a seek that specifies a global (local) key to
	// change the constraint to global (local). This would allow reuse of the
	// same iterator with a large global upper-bound. But a Next call on the
	// highest local key (Prev on the lowest global key) would still not be able
	// to transparently skip over the intermediate lock table. We deem that
	// behavior to be more surprising and bug-prone (for the caller), than being
	// strict.
	MVCCKeyAndIntentsIterKind MVCCIterKind = iota
	// MVCCKeyIterKind specifies that the caller does not need to see intents.
	// Any interleaved intents may be seen, but no correctness properties are
	// derivable from such partial knowledge of intents. NB: this is a performance
	// optimization when iterating over (a) MVCC keys where the caller does
	// not need to see intents, (b) a key space that is known to not have multiple
	// versions (and therefore will never have intents), like the raft log.
	MVCCKeyIterKind
)

// ExportOptions contains options provided to export operation.
type ExportOptions struct {
	// StartKey determines start of the exported interval (inclusive).
	// StartKey.Timestamp is either empty which represent starting from a potential
	// intent and continuing to versions or non-empty, which represents starting
	// from a particular version.
	StartKey MVCCKey
	// EndKey determines the end of exported interval (exclusive).
	EndKey roachpb.Key
	// StartTS and EndTS determine exported time range as (startTS, endTS].
	StartTS, EndTS hlc.Timestamp
	// If ExportAllRevisions is true export every revision of a key for the interval,
	// otherwise only the latest value within the interval is exported.
	ExportAllRevisions bool
	// If TargetSize is positive, it indicates that the export should produce SSTs
	// which are roughly target size. Specifically, it will return an SST such that
	// the last key is responsible for meeting or exceeding the targetSize. If the
	// resumeKey is non-nil then the data size of the returned sst will be greater
	// than or equal to the targetSize.
	TargetSize uint64
	// If MaxSize is positive, it is an absolute maximum on byte size for the
	// returned sst. If it is the case that the versions of the last key will lead
	// to an SST that exceeds maxSize, an error will be returned. This parameter
	// exists to prevent creating SSTs which are too large to be used.
	MaxSize uint64
	// MaxIntents specifies the number of intents to collect and return in a
	// WriteIntentError. The caller will likely resolve the returned intents and
	// retry the call, which would be quadratic, so this significantly reduces the
	// overall number of scans. 0 disables batching and returns the first intent,
	// pass math.MaxUint64 to collect all.
	MaxIntents uint64
	// If StopMidKey is false, once function reaches targetSize it would continue
	// adding all versions until it reaches next key or end of range. If true, it
	// would stop immediately when targetSize is reached and return the next versions
	// timestamp in resumeTs so that subsequent operation can pass it to firstKeyTs.
	StopMidKey bool
	// ResourceLimiter limits how long iterator could run until it exhausts allocated
	// resources. Export queries limiter in its iteration loop to break out once
	// resources are exhausted.
	ResourceLimiter ResourceLimiter
	// If UseTBI is true, the backing MVCCIncrementalIterator will initialize a
	// time-bound iterator along with its regular iterator. The TBI will be used
	// as an optimization to skip over swaths of uninteresting keys i.e. keys
	// outside our time bounds, while locating the KVs to export.
	UseTBI bool
}

// Reader is the read interface to an engine's data. Certain implementations
// of Reader guarantee consistency of the underlying engine state across the
// different iterators created by NewMVCCIterator, NewEngineIterator:
// - pebbleSnapshot, because it uses an engine snapshot.
// - pebbleReadOnly, pebbleBatch: when the IterOptions do not specify a
//   timestamp hint (see IterOptions). Note that currently the engine state
//   visible here is not as of the time of the Reader creation. It is the time
//   when the first iterator is created, or earlier if
//   PinEngineStateForIterators is called.
// The ConsistentIterators method returns true when this consistency is
// guaranteed by the Reader.
// TODO(sumeer): this partial consistency can be a source of bugs if future
// code starts relying on it, but rarely uses a Reader that does not guarantee
// it. Can we enumerate the current cases where KV uses Engine as a Reader?
type Reader interface {
	// Close closes the reader, freeing up any outstanding resources. Note that
	// various implementations have slightly different behaviors. In particular,
	// Distinct() batches release their parent batch for future use while
	// Engines, Snapshots and Batches free the associated C++ resources.
	Close()
	// Closed returns true if the reader has been closed or is not usable.
	// Objects backed by this reader (e.g. Iterators) can check this to ensure
	// that they are not using a closed engine. Intended for use within package
	// engine; exported to enable wrappers to exist in other packages.
	Closed() bool
	// ExportMVCCToSst exports changes to the keyrange [StartKey, EndKey) over the
	// interval (StartTS, EndTS].
	// Deletions are included if all revisions are requested or if the StartTS
	// is non-zero.
	// This function looks at MVCC versions and intents, and returns an error if an
	// intent is found.
	// exportOptions determine ranges as well as additional export options. See
	// struct definition for details.
	//
	// Data is written to dest as it is collected. If error is returned content of
	// dest is undefined.
	//
	// Returns summary containing number of exported bytes, resumeKey and resumeTS
	// that allow resuming export if it was cut short because it reached limits or
	// an error if export failed for some reason.
	ExportMVCCToSst(
		ctx context.Context, exportOptions ExportOptions, dest io.Writer,
	) (_ roachpb.BulkOpSummary, resumeKey roachpb.Key, resumeTS hlc.Timestamp, _ error)
	// MVCCGet returns the value for the given key, nil otherwise. Semantically, it
	// behaves as if an iterator with MVCCKeyAndIntentsIterKind was used.
	//
	// Deprecated: use storage.MVCCGet instead.
	MVCCGet(key MVCCKey) ([]byte, error)
	// MVCCGetProto fetches the value at the specified key and unmarshals it
	// using a protobuf decoder. Returns true on success or false if the
	// key was not found. On success, returns the length in bytes of the
	// key and the value. Semantically, it behaves as if an iterator with
	// MVCCKeyAndIntentsIterKind was used.
	//
	// Deprecated: use MVCCIterator.ValueProto instead.
	MVCCGetProto(key MVCCKey, msg protoutil.Message) (ok bool, keyBytes, valBytes int64, err error)
	// MVCCIterate scans from the start key to the end key (exclusive), invoking the
	// function f on each key value pair. If f returns an error or if the scan
	// itself encounters an error, the iteration will stop and return the error.
	// If the first result of f is true, the iteration stops and returns a nil
	// error. Note that this method is not expected take into account the
	// timestamp of the end key; all MVCCKeys at end.Key are considered excluded
	// in the iteration.
	MVCCIterate(start, end roachpb.Key, iterKind MVCCIterKind, f func(MVCCKeyValue) error) error
	// NewMVCCIterator returns a new instance of an MVCCIterator over this engine.
	// The caller must invoke Close() on it when done to free resources.
	//
	// Write visibility semantics:
	//
	// 1. An iterator has a consistent view of the reader as of the time of its
	//    creation. Subsequent writes are never visible to it.
	//
	// 2. All iterators on readers with ConsistentIterators=true have a consistent
	//    view of the _engine_ (not reader) as of the time of the first iterator
	//    creation or PinEngineStateForIterators call: newer engine writes are
	//    never visible. The opposite holds for ConsistentIterators=false: new
	//    iterators see the most recent engine state at the time of their creation.
	//
	// 3. Iterators on unindexed batches never see batch writes, but satisfy
	//    ConsistentIterators for engine write visibility.
	//
	// 4. Iterators on indexed batches see all batch writes as of their creation
	//    time, but they satisfy ConsistentIterators for engine writes.
	NewMVCCIterator(iterKind MVCCIterKind, opts IterOptions) MVCCIterator
	// NewEngineIterator returns a new instance of an EngineIterator over this
	// engine. The caller must invoke EngineIterator.Close() when finished
	// with the iterator to free resources. The caller can change IterOptions
	// after this function returns. EngineIterators do not support range keys.
	NewEngineIterator(opts IterOptions) EngineIterator
	// ConsistentIterators returns true if the Reader implementation guarantees
	// that the different iterators constructed by this Reader will see the same
	// underlying Engine state. This is not true about Batch writes: new iterators
	// will see new writes made to the batch, existing iterators won't.
	ConsistentIterators() bool
	// SupportsRangeKeys returns true if the Reader implementation supports
	// range keys.
	//
	// TODO(erikgrinaker): Remove this after 22.2.
	SupportsRangeKeys() bool

	// PinEngineStateForIterators ensures that the state seen by iterators
	// without timestamp hints (see IterOptions) is pinned and will not see
	// future mutations. It can be called multiple times on a Reader in which
	// case the state seen will be either:
	// - As of the first call.
	// - For a Reader returned by Engine.NewSnapshot, the pinned state is as of
	//   the time the snapshot was taken.
	// So the semantics that are true for all Readers is that the pinned state
	// is somewhere in the time interval between the creation of the Reader and
	// the first call to PinEngineStateForIterators.
	// REQUIRES: ConsistentIterators returns true.
	PinEngineStateForIterators() error
}

// Writer is the write interface to an engine's data.
type Writer interface {
	// ApplyBatchRepr atomically applies a set of batched updates. Created by
	// calling Repr() on a batch. Using this method is equivalent to constructing
	// and committing a batch whose Repr() equals repr. If sync is true, the
	// batch is synchronously written to disk. It is an error to specify
	// sync=true if the Writer is a Batch.
	//
	// It is safe to modify the contents of the arguments after ApplyBatchRepr
	// returns.
	ApplyBatchRepr(repr []byte, sync bool) error

	// ClearMVCC removes the item from the db with the given MVCCKey. It
	// requires that the timestamp is non-empty (see
	// {ClearUnversioned,ClearIntent} if the timestamp is empty). Note that
	// clear actually removes entries from the storage engine, rather than
	// inserting MVCC tombstones.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearMVCC(key MVCCKey) error
	// ClearUnversioned removes an unversioned item from the db. It is for use
	// with inline metadata (not intents) and other unversioned keys (like
	// Range-ID local keys).
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearUnversioned(key roachpb.Key) error
	// ClearIntent removes an intent from the db. Unlike
	// {ClearMVCC,ClearUnversioned} this is a higher-level method that may make
	// changes in parts of the key space that are not only a function of the
	// input, and may choose to use a single-clear under the covers.
	// txnDidNotUpdateMeta allows for performance optimization when set to true,
	// and has semantics defined in MVCCMetadata.TxnDidNotUpdateMeta (it can
	// be conservatively set to false).
	//
	// It is safe to modify the contents of the arguments after it returns.
	//
	// TODO(sumeer): after the full transition to separated locks, measure the
	// cost of a PutIntent implementation, where there is an existing intent,
	// that does a <single-clear, put> pair. If there isn't a performance
	// decrease, we can stop tracking txnDidNotUpdateMeta and still optimize
	// ClearIntent by always doing single-clear.
	ClearIntent(key roachpb.Key, txnDidNotUpdateMeta bool, txnUUID uuid.UUID) error
	// ClearEngineKey removes the item from the db with the given EngineKey.
	// Note that clear actually removes entries from the storage engine. This is
	// a general-purpose and low-level method that should be used sparingly,
	// only when the other Clear* methods are not applicable.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearEngineKey(key EngineKey) error

	// ClearRawRange removes a set of entries, from start (inclusive) to end
	// (exclusive). It can be applied to a range consisting of MVCCKeys or the
	// more general EngineKeys -- it simply uses the roachpb.Key parameters as
	// the Key field of an EngineKey. Similar to the other Clear* methods,
	// this method actually removes entries from the storage engine.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearRawRange(start, end roachpb.Key) error
	// ClearMVCCRange removes MVCC keys from start (inclusive) to end (exclusive),
	// including intents. Similar to the other Clear* methods, this method
	// actually removes entries from the storage engine.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearMVCCRange(start, end roachpb.Key) error
	// ClearMVCCVersions removes MVCC key versions from start (inclusive) to end
	// (exclusive). It does not affect intents. It is meant for efficiently
	// clearing a subset of versions of a key, since the parameters are MVCCKeys
	// and not roachpb.Keys. Similar to the other Clear* methods, this method
	// actually removes entries from the storage engine.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearMVCCVersions(start, end MVCCKey) error
	// ClearMVCCIteratorRange removes all keys in the given span using an MVCC
	// iterator and clearing individual keys, including intents. Similar to the
	// other Clear* methods, this method actually removes entries from the storage
	// engine.
	ClearMVCCIteratorRange(start, end roachpb.Key) error

	// ExperimentalClearMVCCRangeKey deletes an MVCC range key from start
	// (inclusive) to end (exclusive) at the given timestamp. For any range key
	// that straddles the start and end boundaries, only the segments within the
	// boundaries will be cleared. Range keys at other timestamps are unaffected.
	// Clears are idempotent.
	//
	// This method is primarily intended for MVCC garbage collection and similar
	// internal use.
	//
	// This method is EXPERIMENTAL: range keys are under active development, and
	// have severe limitations including being ignored by all KV and MVCC APIs and
	// only being stored in memory.
	ExperimentalClearMVCCRangeKey(rangeKey MVCCRangeKey) error

	// ExperimentalClearAllMVCCRangeKeys deletes all MVCC range keys (i.e. all
	// versions) from start (inclusive) to end (exclusive). For any range key
	// that straddles the start and end boundaries, only the segments within the
	// boundaries will be cleared. Clears are idempotent.
	//
	// This method is primarily intended for MVCC garbage collection and similar
	// internal use.
	//
	// This method is EXPERIMENTAL: range keys are under active development, and
	// have severe limitations including being ignored by all KV and MVCC APIs and
	// only being stored in memory.
	ExperimentalClearAllMVCCRangeKeys(start, end roachpb.Key) error

	// ExperimentalPutMVCCRangeKey writes an MVCC range key. It will replace any
	// overlapping range keys at the given timestamp (even partial overlap). Only
	// MVCC range tombstones, i.e. an empty value, are currently allowed (other
	// kinds will need additional handling in MVCC APIs and elsewhere, e.g. stats
	// and GC).
	//
	// Range keys must be accessed using special iterator options and methods,
	// see SimpleMVCCIterator.RangeKeys() for details.
	//
	// TODO(erikgrinaker): Write a tech note on range keys and link it here.
	//
	// This method is EXPERIMENTAL: range keys are under active development, and
	// have severe limitations including being ignored by all KV and MVCC APIs and
	// only being stored in memory.
	ExperimentalPutMVCCRangeKey(MVCCRangeKey, MVCCValue) error

	// Merge is a high-performance write operation used for values which are
	// accumulated over several writes. Multiple values can be merged
	// sequentially into a single key; a subsequent read will return a "merged"
	// value which is computed from the original merged values. We only
	// support Merge for keys with no version.
	//
	// Merge currently provides specialized behavior for three data types:
	// integers, byte slices, and time series observations. Merged integers are
	// summed, acting as a high-performance accumulator.  Byte slices are simply
	// concatenated in the order they are merged. Time series observations
	// (stored as byte slices with a special tag on the roachpb.Value) are
	// combined with specialized logic beyond that of simple byte slices.
	//
	//
	// It is safe to modify the contents of the arguments after Merge returns.
	Merge(key MVCCKey, value []byte) error

	// PutMVCC sets the given key to the value provided. It requires that the
	// timestamp is non-empty (see {PutUnversioned,PutIntent} if the timestamp
	// is empty).
	//
	// It is safe to modify the contents of the arguments after PutMVCC returns.
	PutMVCC(key MVCCKey, value MVCCValue) error
	// PutRawMVCC is like PutMVCC, but it accepts an encoded MVCCValue. It
	// can be used to avoid decoding and immediately re-encoding an MVCCValue,
	// but should generally be avoided due to the lack of type safety.
	//
	// It is safe to modify the contents of the arguments after PutRawMVCC
	// returns.
	PutRawMVCC(key MVCCKey, value []byte) error
	// PutUnversioned sets the given key to the value provided. It is for use
	// with inline metadata (not intents) and other unversioned keys (like
	// Range-ID local keys).
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutUnversioned(key roachpb.Key, value []byte) error
	// PutIntent puts an intent at the given key to the value provided. This is
	// a higher-level method that may make changes in parts of the key space
	// that are not only a function of the input key, and may explicitly clear
	// the preceding intent. txnDidNotUpdateMeta defines what happened prior to
	// this put, and allows for performance optimization when set to true, and
	// has semantics defined in MVCCMetadata.TxnDidNotUpdateMeta (it can be
	// conservatively set to false).
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutIntent(ctx context.Context, key roachpb.Key, value []byte, txnUUID uuid.UUID) error
	// PutEngineKey sets the given key to the value provided. This is a
	// general-purpose and low-level method that should be used sparingly,
	// only when the other Put* methods are not applicable.
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutEngineKey(key EngineKey, value []byte) error

	// LogData adds the specified data to the RocksDB WAL. The data is
	// uninterpreted by RocksDB (i.e. not added to the memtable or sstables).
	//
	// It is safe to modify the contents of the arguments after LogData returns.
	LogData(data []byte) error
	// LogLogicalOp logs the specified logical mvcc operation with the provided
	// details to the writer, if it has logical op logging enabled. For most
	// Writer implementations, this is a no-op.
	LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails)

	// SingleClearEngineKey removes the most recent write to the item from the db
	// with the given key. Whether older writes of the item will come back
	// to life if not also removed with SingleClear is undefined. See the
	// following:
	//   https://github.com/facebook/rocksdb/wiki/Single-Delete
	// for details on the SingleDelete operation that this method invokes. Note
	// that clear actually removes entries from the storage engine, rather than
	// inserting MVCC tombstones. This is a low-level interface that must not be
	// called from outside the storage package. It is part of the interface
	// because there are structs that wrap Writer and implement the Writer
	// interface, that are not part of the storage package.
	//
	// It is safe to modify the contents of the arguments after it returns.
	SingleClearEngineKey(key EngineKey) error

	// ShouldWriteLocalTimestamps is only for internal use in the storage package.
	// This method is temporary, to handle the transition from clusters where not
	// all nodes understand local timestamps.
	ShouldWriteLocalTimestamps(ctx context.Context) bool
}

// ReadWriter is the read/write interface to an engine's data.
type ReadWriter interface {
	Reader
	Writer
}

// DurabilityRequirement is an advanced option. If in doubt, use
// StandardDurability.
//
// GuranteedDurability maps to pebble.IterOptions.OnlyReadGuaranteedDurable.
// This acknowledges the fact that we do not (without sacrificing correctness)
// sync the WAL for many writes, and there are some advanced cases
// (raftLogTruncator) that need visibility into what is guaranteed durable.
type DurabilityRequirement int8

const (
	// StandardDurability is what should normally be used.
	StandardDurability DurabilityRequirement = iota
	// GuaranteedDurability is an advanced option (only for raftLogTruncator).
	GuaranteedDurability
)

// Engine is the interface that wraps the core operations of a key/value store.
type Engine interface {
	ReadWriter
	// Attrs returns the engine/store attributes.
	Attrs() roachpb.Attributes
	// Capacity returns capacity details for the engine's available storage.
	Capacity() (roachpb.StoreCapacity, error)
	// Properties returns the low-level properties for the engine's underlying storage.
	Properties() roachpb.StoreProperties
	// Compact forces compaction over the entire database.
	Compact() error
	// Flush causes the engine to write all in-memory data to disk
	// immediately.
	Flush() error
	// GetMetrics retrieves metrics from the engine.
	GetMetrics() Metrics
	// GetEncryptionRegistries returns the file and key registries when encryption is enabled
	// on the store.
	GetEncryptionRegistries() (*EncryptionRegistries, error)
	// GetEnvStats retrieves stats about the engine's environment
	// For RocksDB, this includes details of at-rest encryption.
	GetEnvStats() (*EnvStats, error)
	// GetAuxiliaryDir returns a path under which files can be stored
	// persistently, and from which data can be ingested by the engine.
	//
	// Not thread safe.
	GetAuxiliaryDir() string
	// NewBatch returns a new instance of a batched engine which wraps
	// this engine. Batched engines accumulate all mutations and apply
	// them atomically on a call to Commit().
	NewBatch() Batch
	// NewReadOnly returns a new instance of a ReadWriter that wraps this
	// engine, and with the given durability requirement. This wrapper panics
	// when unexpected operations (e.g., write operations) are executed on it
	// and caches iterators to avoid the overhead of creating multiple iterators
	// for batched reads.
	//
	// All iterators created from a read-only engine are guaranteed to provide a
	// consistent snapshot of the underlying engine. See the comment on the
	// Reader interface and the Reader.ConsistentIterators method.
	NewReadOnly(durability DurabilityRequirement) ReadWriter
	// NewUnindexedBatch returns a new instance of a batched engine which wraps
	// this engine. It is unindexed, in that writes to the batch are not
	// visible to reads until after it commits. The batch accumulates all
	// mutations and applies them atomically on a call to Commit(). Read
	// operations return an error, unless writeOnly is set to false.
	//
	// When writeOnly is false, reads will be satisfied by reading from the
	// underlying engine, i.e., the caller does not see its own writes. This
	// setting should be used only when the caller is certain that this
	// optimization is correct, and beneficial. There are subtleties here -- see
	// the discussion on https://github.com/cockroachdb/cockroach/pull/57661 for
	// more details.
	//
	// TODO(sumeer): We should separate the writeOnly=true case into a
	// separate method, that returns a WriteBatch interface. Even better would
	// be not having an option to pass writeOnly=false, and have the caller
	// explicitly work with a separate WriteBatch and Reader.
	NewUnindexedBatch(writeOnly bool) Batch
	// NewSnapshot returns a new instance of a read-only snapshot
	// engine. Snapshots are instantaneous and, as long as they're
	// released relatively quickly, inexpensive. Snapshots are released
	// by invoking Close(). Note that snapshots must not be used after the
	// original engine has been stopped.
	NewSnapshot() Reader
	// Type returns engine type.
	Type() enginepb.EngineType
	// IngestExternalFiles atomically links a slice of files into the RocksDB
	// log-structured merge-tree.
	IngestExternalFiles(ctx context.Context, paths []string) error
	// PreIngestDelay offers an engine the chance to backpressure ingestions.
	// When called, it may choose to block if the engine determines that it is in
	// or approaching a state where further ingestions may risk its health.
	PreIngestDelay(ctx context.Context)
	// ApproximateDiskBytes returns an approximation of the on-disk size for the given key span.
	ApproximateDiskBytes(from, to roachpb.Key) (uint64, error)
	// CompactRange ensures that the specified range of key value pairs is
	// optimized for space efficiency.
	CompactRange(start, end roachpb.Key) error
	// InMem returns true if the receiver is an in-memory engine and false
	// otherwise.
	//
	// TODO(peter): This is a bit of a wart in the interface. It is used by
	// addSSTablePreApply to select alternate code paths, but really there should
	// be a unified code path there.
	InMem() bool
	// RegisterFlushCompletedCallback registers a callback that will be run for
	// every successful flush. Only one callback can be registered at a time, so
	// registering again replaces the previous callback. The callback must
	// return quickly and must not call any methods on the Engine in the context
	// of the callback since it could cause a deadlock (since the callback may
	// be invoked while holding mutexes).
	RegisterFlushCompletedCallback(cb func())
	// Filesystem functionality.
	fs.FS
	// ReadFile reads the content from the file with the given filename int this RocksDB's env.
	ReadFile(filename string) ([]byte, error)
	// WriteFile writes data to a file in this RocksDB's env.
	WriteFile(filename string, data []byte) error
	// CreateCheckpoint creates a checkpoint of the engine in the given directory,
	// which must not exist. The directory should be on the same file system so
	// that hard links can be used.
	CreateCheckpoint(dir string) error

	// SetMinVersion is used to signal to the engine the current minimum
	// version that it must maintain compatibility with.
	SetMinVersion(version roachpb.Version) error

	// MinVersionIsAtLeastTargetVersion returns whether the engine's recorded
	// storage min version is at least the target version.
	MinVersionIsAtLeastTargetVersion(target roachpb.Version) (bool, error)
}

// Batch is the interface for batch specific operations.
type Batch interface {
	// Iterators created on a batch can see some mutations performed after the
	// iterator creation. To guarantee that they see all the mutations, the
	// iterator has to be repositioned using a seek operation, after the
	// mutations were done.
	ReadWriter
	// Commit atomically applies any batched updates to the underlying
	// engine. This is a noop unless the batch was created via NewBatch(). If
	// sync is true, the batch is synchronously committed to disk.
	Commit(sync bool) error
	// Empty returns whether the batch has been written to or not.
	Empty() bool
	// Count returns the number of memtable-modifying operations in the batch.
	Count() uint32
	// Len returns the size of the underlying representation of the batch.
	// Because of the batch header, the size of the batch is never 0 and should
	// not be used interchangeably with Empty. The method avoids the memory copy
	// that Repr imposes, but it still may require flushing the batch's mutations.
	Len() int
	// Repr returns the underlying representation of the batch and can be used to
	// reconstitute the batch on a remote node using Writer.ApplyBatchRepr().
	Repr() []byte
}

// Metrics is a set of Engine metrics. Most are contained in the embedded
// *pebble.Metrics struct, which has its own documentation.
type Metrics struct {
	*pebble.Metrics
	// WriteStallCount counts the number of times Pebble intentionally delayed
	// incoming writes. Currently, the only two reasons for this to happen are:
	// - "memtable count limit reached"
	// - "L0 file count limit exceeded"
	//
	// We do not split this metric across these two reasons, but they can be
	// distinguished in the pebble logs.
	WriteStallCount int64
	// DiskSlowCount counts the number of times Pebble records disk slowness.
	DiskSlowCount int64
	// DiskStallCount counts the number of times Pebble observes slow writes
	// on disk lasting longer than MaxSyncDuration (`storage.max_sync_duration`).
	DiskStallCount int64
}

// NumSSTables returns the total number of SSTables in the LSM, aggregated
// across levels.
func (m *Metrics) NumSSTables() int64 {
	var num int64
	for _, lm := range m.Metrics.Levels {
		num += lm.NumFiles
	}
	return num
}

// IngestedBytes returns the sum of all ingested tables, aggregated across all
// levels of the LSM.
func (m *Metrics) IngestedBytes() uint64 {
	var ingestedBytes uint64
	for _, lm := range m.Metrics.Levels {
		ingestedBytes += lm.BytesIngested
	}
	return ingestedBytes
}

// CompactedBytes returns the sum of bytes read and written during
// compactions across all levels of the LSM.
func (m *Metrics) CompactedBytes() (read, written uint64) {
	for _, lm := range m.Metrics.Levels {
		read += lm.BytesRead
		written += lm.BytesCompacted
	}
	return read, written
}

// EnvStats is a set of RocksDB env stats, including encryption status.
type EnvStats struct {
	// TotalFiles is the total number of files reported by rocksdb.
	TotalFiles uint64
	// TotalBytes is the total size of files reported by rocksdb.
	TotalBytes uint64
	// ActiveKeyFiles is the number of files using the active data key.
	ActiveKeyFiles uint64
	// ActiveKeyBytes is the size of files using the active data key.
	ActiveKeyBytes uint64
	// EncryptionType is an enum describing the active encryption algorithm.
	// See: ccl/storageccl/engineccl/enginepbccl/key_registry.proto
	EncryptionType int32
	// EncryptionStatus is a serialized enginepbccl/stats.proto::EncryptionStatus protobuf.
	EncryptionStatus []byte
}

// EncryptionRegistries contains the encryption-related registries:
// Both are serialized protobufs.
type EncryptionRegistries struct {
	// FileRegistry is the list of files with encryption status.
	// serialized storage/engine/enginepb/file_registry.proto::FileRegistry
	FileRegistry []byte
	// KeyRegistry is the list of keys, scrubbed of actual key data.
	// serialized ccl/storageccl/engineccl/enginepbccl/key_registry.proto::DataKeysRegistry
	KeyRegistry []byte
}

// Scan returns up to max key/value objects starting from start (inclusive)
// and ending at end (non-inclusive). Specify max=0 for unbounded scans. Since
// this code may use an intentInterleavingIter, the caller should not attempt
// a single scan to span local and global keys. See the comment in the
// declaration of intentInterleavingIter for details.
func Scan(reader Reader, start, end roachpb.Key, max int64) ([]MVCCKeyValue, error) {
	var kvs []MVCCKeyValue
	err := reader.MVCCIterate(start, end, MVCCKeyAndIntentsIterKind, func(kv MVCCKeyValue) error {
		if max != 0 && int64(len(kvs)) >= max {
			return iterutil.StopIteration()
		}
		kvs = append(kvs, kv)
		return nil
	})
	return kvs, err
}

// ScanIntents scans intents using only the separated intents lock table. It
// does not take interleaved intents into account at all.
func ScanIntents(
	ctx context.Context, reader Reader, start, end roachpb.Key, maxIntents int64, targetBytes int64,
) ([]roachpb.Intent, error) {
	intents := []roachpb.Intent{}

	if bytes.Compare(start, end) >= 0 {
		return intents, nil
	}

	ltStart, _ := keys.LockTableSingleKey(start, nil)
	ltEnd, _ := keys.LockTableSingleKey(end, nil)
	iter := reader.NewEngineIterator(IterOptions{LowerBound: ltStart, UpperBound: ltEnd})
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	var intentBytes int64
	var ok bool
	var err error
	for ok, err = iter.SeekEngineKeyGE(EngineKey{Key: ltStart}); ok; ok, err = iter.NextEngineKey() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if maxIntents != 0 && int64(len(intents)) >= maxIntents {
			break
		}
		if targetBytes != 0 && intentBytes >= targetBytes {
			break
		}
		key, err := iter.EngineKey()
		if err != nil {
			return nil, err
		}
		lockedKey, err := keys.DecodeLockTableSingleKey(key.Key)
		if err != nil {
			return nil, err
		}
		if err = protoutil.Unmarshal(iter.UnsafeValue(), &meta); err != nil {
			return nil, err
		}
		intents = append(intents, roachpb.MakeIntent(meta.Txn, lockedKey))
		intentBytes += int64(len(lockedKey)) + int64(len(iter.Value()))
	}
	if err != nil {
		return nil, err
	}
	return intents, nil
}

// WriteSyncNoop carries out a synchronous no-op write to the engine.
func WriteSyncNoop(eng Engine) error {
	batch := eng.NewBatch()
	defer batch.Close()

	if err := batch.LogData(nil); err != nil {
		return err
	}

	if err := batch.Commit(true /* sync */); err != nil {
		return err
	}
	return nil
}

// ClearRangeWithHeuristic clears the keys from start (inclusive) to end
// (exclusive). Depending on the number of keys, it will either use ClearRawRange
// or clear individual keys. It works with EngineKeys, so don't expect it to
// find and clear separated intents if [start, end) refers to MVCC key space.
func ClearRangeWithHeuristic(reader Reader, writer Writer, start, end roachpb.Key) error {
	iter := reader.NewEngineIterator(IterOptions{UpperBound: end})
	defer iter.Close()

	// It is expensive for there to be many range deletion tombstones in the same
	// sstable because all of the tombstones in an sstable are loaded whenever the
	// sstable is accessed. So we avoid using range deletion unless there is some
	// minimum number of keys. The value here was pulled out of thin air. It might
	// be better to make this dependent on the size of the data being deleted. Or
	// perhaps we should fix Pebble to handle large numbers of tombstones in an
	// sstable better. Note that we are referring to storage-level tombstones here,
	// and not MVCC tombstones.
	const clearRangeMinKeys = 64
	// Peek into the range to see whether it's large enough to justify
	// ClearRawRange. Note that the work done here is bounded by
	// clearRangeMinKeys, so it will be fairly cheap even for large
	// ranges.
	//
	// TODO(sumeer): Could add the iterated keys to the batch, so we don't have
	// to do the scan again. If there are too many keys, this will mean a mix of
	// point tombstones and range tombstone.
	count := 0
	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: start})
	for valid {
		count++
		if count > clearRangeMinKeys {
			break
		}
		valid, err = iter.NextEngineKey()
	}
	if err != nil {
		return err
	}
	if count > clearRangeMinKeys {
		return writer.ClearRawRange(start, end)
	}
	valid, err = iter.SeekEngineKeyGE(EngineKey{Key: start})
	for valid {
		var k EngineKey
		if k, err = iter.UnsafeEngineKey(); err != nil {
			break
		}
		if err = writer.ClearEngineKey(k); err != nil {
			break
		}
		valid, err = iter.NextEngineKey()
	}
	return err
}

var ingestDelayL0Threshold = settings.RegisterIntSetting(
	settings.TenantWritable,
	"rocksdb.ingest_backpressure.l0_file_count_threshold",
	"number of L0 files after which to backpressure SST ingestions",
	20,
)

var ingestDelayTime = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"rocksdb.ingest_backpressure.max_delay",
	"maximum amount of time to backpressure a single SST ingestion",
	time.Second*5,
)

// PreIngestDelay may choose to block for some duration if L0 has an excessive
// number of files in it or if PendingCompactionBytesEstimate is elevated. This
// it is intended to be called before ingesting a new SST, since we'd rather
// backpressure the bulk operation adding SSTs than slow down the whole RocksDB
// instance and impact all foreground traffic by adding too many files to it.
// After the number of L0 files exceeds the configured limit, it gradually
// begins delaying more for each additional file in L0 over the limit until
// hitting its configured (via settings) maximum delay. If the pending
// compaction limit is exceeded, it waits for the maximum delay.
func preIngestDelay(ctx context.Context, eng Engine, settings *cluster.Settings) {
	if settings == nil {
		return
	}
	metrics := eng.GetMetrics()
	targetDelay := calculatePreIngestDelay(settings, metrics.Metrics)

	if targetDelay == 0 {
		return
	}
	log.VEventf(ctx, 2, "delaying SST ingestion %s. %d L0 files, %d L0 Sublevels",
		targetDelay, metrics.Levels[0].NumFiles, metrics.Levels[0].Sublevels)

	select {
	case <-time.After(targetDelay):
	case <-ctx.Done():
	}
}

func calculatePreIngestDelay(settings *cluster.Settings, metrics *pebble.Metrics) time.Duration {
	maxDelay := ingestDelayTime.Get(&settings.SV)
	l0ReadAmpLimit := ingestDelayL0Threshold.Get(&settings.SV)

	const ramp = 10
	l0ReadAmp := metrics.Levels[0].NumFiles
	if metrics.Levels[0].Sublevels >= 0 {
		l0ReadAmp = int64(metrics.Levels[0].Sublevels)
	}

	if l0ReadAmp > l0ReadAmpLimit {
		delayPerFile := maxDelay / time.Duration(ramp)
		targetDelay := time.Duration(l0ReadAmp-l0ReadAmpLimit) * delayPerFile
		if targetDelay > maxDelay {
			return maxDelay
		}
		return targetDelay
	}
	return 0
}

// Helper function to implement Reader.MVCCIterate().
func iterateOnReader(
	reader Reader, start, end roachpb.Key, iterKind MVCCIterKind, f func(MVCCKeyValue) error,
) error {
	if reader.Closed() {
		return errors.New("cannot call MVCCIterate on a closed batch")
	}
	if start.Compare(end) >= 0 {
		return nil
	}

	it := reader.NewMVCCIterator(iterKind, IterOptions{UpperBound: end})
	defer it.Close()

	it.SeekGE(MakeMVCCMetadataKey(start))
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}
		if err := f(MVCCKeyValue{Key: it.Key(), Value: it.Value()}); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}
