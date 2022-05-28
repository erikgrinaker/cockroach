// Copyright 2022 The Cockroach Authors.
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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// pointSynthesizingIter wraps an MVCCIterator, and synthesizes MVCC point keys
// for MVCC range keys above/below existing point keys, and at the start of
// range keys (truncated to iterator bounds). If emitOnSeekGE is set, it will
// also unconditionally synthesize point keys around a SeekGE seek key.
//
// It does not emit MVCC range keys at all, since these would appear to conflict
// with the synthesized point keys.
//
// During iteration, any range keys overlapping the current iterator position
// are kept in rangeKeys. When atPoint is true, the iterator is positioned on a
// real point key in the underlying iterator. Otherwise, it is positioned on a
// synthetic point key given by rangeKeysPos and rangeKeys[rangeKeysIdx].
//
// The relative positioning of pointSynthesizingIter and the underlying iterator
// is as follows in the forward direction:
//
// - atPoint=true: rangeKeysIdx points to a range key following the point key,
//   or beyond the slice bounds when there are no further range keys at this
//   key position.
//
// - atPoint=false: the underlying iterator is on a following key or exhausted.
//   This can either be a different version of the current key or a different
//   point/range key.
//
// This positioning is mirrored in the reverse direction. For example, when
// atPoint=true and rangeKeys are exhausted, rangeKeysIdx will be len(rangeKeys)
// in the forward direction and -1 in the reverse direction. Similarly, the
// underlying iterator is always >= rangeKeysPos in the forward direction and <=
// in reverse.
//
// See also assertInvariants() which asserts positioning invariants.
type pointSynthesizingIter struct {
	iter MVCCIterator

	// rangeKeys contains any range keys that overlap the current key position,
	// for which points will be synthesized.
	rangeKeys []MVCCRangeKeyValue

	// rangeKeysPos is the current key (along the rangeKeys span) that points will
	// be synthesized for. It is only set if rangeKeys is non-empty, and may
	// differ from the underlying iterator position.
	rangeKeysPos roachpb.Key

	// rangeKeysIdx is the rangeKeys index of the current/pending range key
	// to synthesize a point for. See struct comment for details.
	rangeKeysIdx int

	// rangeKeysStart contains the start key of the current rangeKeys stack. It is
	// only used to memoize rangeKeys for adjacent keys.
	rangeKeysStart roachpb.Key

	// atPoint is true if the synthesizing iterator is positioned on a real point
	// key in the underlying iterator. See struct comment for details.
	atPoint bool

	// reverse is true when the current iterator direction is in reverse, i.e.
	// following a SeekLT or Prev call.
	reverse bool

	// emitOnSeekGE will synthesize point keys for the SeekGE seek key if it
	// overlaps with a range key even if no point key exists. The primary use-case
	// is to synthesize point keys for e.g. an MVCCGet that does not match a point
	// key but overlaps a range key, which is necessary for conflict checks.
	//
	// This is optional, because e.g. pebbleMVCCScanner often uses seeks as an
	// optimization to skip over old versions of a key, and we don't want to keep
	// synthesizing point keys every time it skips ahead.
	//
	// TODO(erikgrinaker): This could instead check for prefix iterators, or a
	// separate SeekPrefixGE() method, but we don't currently have APIs for it.
	emitOnSeekGE bool
}

var _ MVCCIterator = new(pointSynthesizingIter)

// newPointSynthesizingIter creates a new pointSynthesizingIter.
func newPointSynthesizingIter(iter MVCCIterator, emitOnSeekGE bool) *pointSynthesizingIter {
	return &pointSynthesizingIter{
		iter:         iter,
		emitOnSeekGE: emitOnSeekGE,
	}
}

// updateRangeKeys updates i.rangeKeys and related fields with range keys from
// the underlying iterator. rangeKeysIdx is reset to the first/last range key.
func (i *pointSynthesizingIter) updateRangeKeys() {
	if _, hasRange := i.iter.HasPointAndRange(); hasRange {
		i.rangeKeysPos = append(i.rangeKeysPos[:0], i.iter.UnsafeKey().Key...)
		if rangeStart := i.iter.RangeBounds().Key; !rangeStart.Equal(i.rangeKeysStart) {
			i.rangeKeys = i.rangeKeys[:0]
			for _, rk := range i.iter.RangeKeys() {
				// TODO(erikgrinaker): We should optimize the clone cost.
				i.rangeKeys = append(i.rangeKeys, rk.Clone())
			}
			i.rangeKeysStart = append(i.rangeKeysStart[:0], rangeStart...)
		}
	} else if len(i.rangeKeys) != 0 {
		i.rangeKeys = i.rangeKeys[:0]
		i.rangeKeysPos = i.rangeKeysPos[:0]
		i.rangeKeysStart = i.rangeKeysStart[:0]
	}
	if !i.reverse {
		i.rangeKeysIdx = 0
	} else {
		i.rangeKeysIdx = len(i.rangeKeys) - 1 // NB: -1 is correct with no range keys
	}
}

// updateAtPoint updates i.atPoint according to whether the synthesizing
// iterator is positioned on the real point key in the underlying iterator.
// Requires i.rangeKeys to have been positioned first.
func (i *pointSynthesizingIter) updateAtPoint() {
	if hasPoint, _ := i.iter.HasPointAndRange(); !hasPoint {
		i.atPoint = false
	} else if len(i.rangeKeys) == 0 {
		i.atPoint = true
	} else if point := i.iter.UnsafeKey(); !point.Key.Equal(i.rangeKeysPos) {
		i.atPoint = false
	} else if !i.reverse {
		i.atPoint = i.rangeKeysIdx >= len(i.rangeKeys) ||
			point.Timestamp.IsEmpty() ||
			i.rangeKeys[i.rangeKeysIdx].RangeKey.Timestamp.LessEq(point.Timestamp)
	} else {
		i.atPoint = i.rangeKeysIdx < 0 || (point.Timestamp.IsSet() &&
			point.Timestamp.LessEq(i.rangeKeys[i.rangeKeysIdx].RangeKey.Timestamp))
	}
}

// updatePosition updates the synthesizing iterator with the position of the
// underlying iterator. This may step the underlying iterator to position it
// correctly relative to bare range keys.
func (i *pointSynthesizingIter) updatePosition() {
	if !i.reverse {
		i.updateRangeKeys()
		// If we're on a bare range key in the forward direction, we populate the
		// range keys but then step iter ahead before updating the point position.
		// The next position may be a point key colocated with the current range key
		// position, which must be interleaved with the synthetic points.
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
			i.iter.Next()
		}
		i.updateAtPoint()
	} else {
		// If we're on a bare range key in the reverse direction, and we've already
		// emitted synthetic points for this key (as evidenced by rangeKeysPos),
		// then we skip over the bare range key to avoid duplicates.
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
			if i.iter.UnsafeKey().Key.Equal(i.rangeKeysPos) {
				i.iter.Prev()
			}
		}
		i.updateRangeKeys()
		i.updateAtPoint()
	}
}

// SeekGE implements MVCCIterator.
func (i *pointSynthesizingIter) SeekGE(key MVCCKey) {
	i.reverse = false
	i.iter.SeekGE(key)

	// If we land in the middle of a bare range key and emitOnSeekGE is disabled,
	// then skip over it to the next point/range key. However, if we're seeking to
	// a specific version and don't find an older point key at the seek key, then
	// we also need to peek backwards for an existing point key above us, which
	// would mandate that we synthesize point keys here after all.
	//
	// TODO(erikgrinaker): It might be faster to first do an unversioned seek to
	// look for previous points and then a versioned seek.
	var positioned bool
	if !i.emitOnSeekGE {
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
			if !i.iter.RangeBounds().Key.Equal(i.iter.UnsafeKey().Key) {
				i.iter.Next()

				if key.Timestamp.IsSet() {
					ok, err := i.iter.Valid()
					if err == nil && (!ok || !key.Key.Equal(i.iter.UnsafeKey().Key)) {
						i.iter.Prev()
						if hasP, _ := i.iter.HasPointAndRange(); hasP && key.Key.Equal(i.iter.UnsafeKey().Key) {
							i.updateRangeKeys()
							positioned = true
						}
						i.iter.Next()
					}
				}
			}
		}
	}

	if !positioned {
		i.updateRangeKeys()

		// If we're now at a bare range key, it must either be at the start of it,
		// or in the middle with emitOnSeekGE enabled. In either case, we want to
		// move the iterator ahead to look for a point key that may be colocated
		// with the start/seek key in order to interleave it.
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
			i.iter.Next()
		}
	}

	// If we're seeking to a specific version, skip newer range keys.
	if len(i.rangeKeys) > 0 && key.Timestamp.IsSet() && key.Key.Equal(i.rangeKeysPos) {
		i.rangeKeysIdx = sort.Search(len(i.rangeKeys), func(idx int) bool {
			return i.rangeKeys[idx].RangeKey.Timestamp.LessEq(key.Timestamp)
		})
	}

	i.updateAtPoint()

	// It's possible that we seeked past all of the range key versions. In this
	// case, we have to reposition on the next key (current iter key).
	if !i.atPoint && i.rangeKeysIdx >= len(i.rangeKeys) {
		i.updatePosition()
	}
}

// SeekIntentGE implements MVCCIterator.
func (i *pointSynthesizingIter) SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID) {
	i.reverse = false
	i.iter.SeekIntentGE(key, txnUUID)

	// If we land in the middle of a bare range key and emitOnSeekGE is disabled,
	// then skip over it to the next point/range key.
	if !i.emitOnSeekGE {
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
			if !i.iter.RangeBounds().Key.Equal(i.iter.UnsafeKey().Key) {
				i.iter.Next()
			}
		}
	}

	i.updatePosition()
}

// Next implements MVCCIterator.
func (i *pointSynthesizingIter) Next() {
	// When changing direction, flip the relative positioning with iter.
	if i.reverse {
		i.reverse = false
		if !i.atPoint && len(i.rangeKeys) == 0 { // iterator was exhausted
			i.iter.Next()
			i.updatePosition()
			return
		} else if i.atPoint {
			i.rangeKeysIdx++
		} else {
			i.iter.Next()
		}
	}

	// Step off the current point, either real or synthetic.
	if i.atPoint {
		i.iter.Next()
	} else {
		i.rangeKeysIdx++
	}
	i.updateAtPoint()

	// If we've exhausted the current range keys, update with the underlying
	// iterator position (which must now be at a later key).
	if !i.atPoint && i.rangeKeysIdx >= len(i.rangeKeys) {
		i.updatePosition()
	}
}

// NextKey implements MVCCIterator.
func (i *pointSynthesizingIter) NextKey() {
	// When changing direction, flip the relative positioning with iter.
	if i.reverse {
		i.reverse = false
		if !i.atPoint {
			i.iter.Next()
		}
	}
	// Don't call NextKey() if the underlying iterator is already on the next key.
	if i.atPoint || i.rangeKeysPos.Equal(i.iter.UnsafeKey().Key) {
		i.iter.NextKey()
	}
	i.updatePosition()
}

// SeekLT implements MVCCIterator.
func (i *pointSynthesizingIter) SeekLT(key MVCCKey) {
	i.reverse = true
	i.iter.SeekLT(key)

	// If we did a versioned seek and find a range key that overlaps the seek key,
	// we may have skipped over existing point key versions of the seek key. These
	// would mandate that we synthesize point keys for the seek key after all, so
	// we peek ahead to check for them.
	//
	// TODO(erikgrinaker): It might be faster to do an unversioned seek from the
	// next key first to look for points.
	var positioned bool
	if key.Timestamp.IsSet() {
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange {
			if !hasPoint || !i.iter.UnsafeKey().Key.Equal(key.Key) {
				if key.Key.Compare(i.iter.RangeBounds().EndKey) < 0 {
					i.iter.Next()
					if hasP, _ := i.iter.HasPointAndRange(); hasP && i.iter.UnsafeKey().Key.Equal(key.Key) {
						i.updateRangeKeys()
						positioned = true
					}
					i.iter.Prev()
				}
			}
		}
	}

	if !positioned {
		i.updateRangeKeys()
	}

	// If we're seeking to a specific version, skip over older range keys.
	if key.Timestamp.IsSet() && key.Key.Equal(i.rangeKeysPos) {
		i.rangeKeysIdx = sort.Search(len(i.rangeKeys), func(idx int) bool {
			return i.rangeKeys[idx].RangeKey.Timestamp.LessEq(key.Timestamp)
		}) - 1
	}

	i.updateAtPoint()

	// It's possible that we seeked past all of the range key versions. In this
	// case, we have to reposition on the previous key (current iter key).
	if !i.atPoint && i.rangeKeysIdx < 0 {
		i.updatePosition()
	}
}

// Prev implements MVCCIterator.
func (i *pointSynthesizingIter) Prev() {
	// When changing direction, flip the relative positioning with iter.
	if !i.reverse {
		i.reverse = true
		if !i.atPoint && len(i.rangeKeys) == 0 { // iterator was exhausted
			i.iter.Prev()
			i.updatePosition()
			return
		} else if i.atPoint {
			i.rangeKeysIdx--
		} else {
			i.iter.Prev()
		}
	}

	// Step off the current point key (real or synthetic).
	if i.atPoint {
		i.iter.Prev()
	} else {
		i.rangeKeysIdx--
	}
	i.updateAtPoint()

	// If we've exhausted the current range keys, and we're not positioned on a
	// point key at the current range key position, then update with the
	// underlying iter position (which must be before the current key).
	if i.rangeKeysIdx < 0 && (!i.atPoint || !i.rangeKeysPos.Equal(i.iter.UnsafeKey().Key)) {
		i.updatePosition()
	}
}

// Valid implements MVCCIterator.
func (i *pointSynthesizingIter) Valid() (bool, error) {
	if util.RaceEnabled {
		if err := i.assertInvariants(); err != nil {
			panic(err)
		}
	}
	if !i.atPoint && i.rangeKeysIdx >= 0 && i.rangeKeysIdx < len(i.rangeKeys) {
		return true, nil // on synthetic point key
	}
	return i.iter.Valid()
}

// Key implements MVCCIterator.
func (i *pointSynthesizingIter) Key() MVCCKey {
	return i.UnsafeKey().Clone()
}

// UnsafeKey implements MVCCIterator.
func (i *pointSynthesizingIter) UnsafeKey() MVCCKey {
	if i.atPoint {
		return i.iter.UnsafeKey()
	}
	if i.rangeKeysIdx >= len(i.rangeKeys) || i.rangeKeysIdx < 0 {
		return MVCCKey{}
	}
	return MVCCKey{
		Key:       i.rangeKeysPos,
		Timestamp: i.rangeKeys[i.rangeKeysIdx].RangeKey.Timestamp,
	}
}

// UnsafeRawKey implements MVCCIterator.
func (i *pointSynthesizingIter) UnsafeRawKey() []byte {
	if i.atPoint {
		return i.iter.UnsafeRawKey()
	}
	return EncodeMVCCKeyPrefix(i.rangeKeysPos)
}

// UnsafeRawMVCCKey implements MVCCIterator.
func (i *pointSynthesizingIter) UnsafeRawMVCCKey() []byte {
	if i.atPoint {
		return i.iter.UnsafeRawMVCCKey()
	}
	return EncodeMVCCKey(i.UnsafeKey())
}

// Value implements MVCCIterator.
func (i *pointSynthesizingIter) Value() []byte {
	if v := i.UnsafeValue(); v != nil {
		return append([]byte{}, v...)
	}
	return nil
}

// UnsafeValue implements MVCCIterator.
func (i *pointSynthesizingIter) UnsafeValue() []byte {
	if i.atPoint {
		return i.iter.UnsafeValue()
	}
	if i.rangeKeysIdx >= len(i.rangeKeys) || i.rangeKeysIdx < 0 {
		return nil
	}
	return i.rangeKeys[i.rangeKeysIdx].Value
}

// ValueProto implements MVCCIterator.
func (i *pointSynthesizingIter) ValueProto(msg protoutil.Message) error {
	return protoutil.Unmarshal(i.UnsafeValue(), msg)
}

// HasPointAndRange implements MVCCIterator.
func (i *pointSynthesizingIter) HasPointAndRange() (bool, bool) {
	ok, err := i.Valid()
	return ok && err == nil, false
}

// RangeBounds implements MVCCIterator.
func (i *pointSynthesizingIter) RangeBounds() roachpb.Span {
	return roachpb.Span{}
}

// RangeKeys implements MVCCIterator.
func (i *pointSynthesizingIter) RangeKeys() []MVCCRangeKeyValue {
	return []MVCCRangeKeyValue{}
}

// Close implements MVCCIterator.
func (i *pointSynthesizingIter) Close() {
	i.iter.Close()
}

// ComputeStats implements MVCCIterator.
func (i *pointSynthesizingIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return i.iter.ComputeStats(start, end, nowNanos)
}

// FindSplitKey implements MVCCIterator.
func (i *pointSynthesizingIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	return i.iter.FindSplitKey(start, end, minSplitKey, targetSize)
}

// Stats implements MVCCIterator.
func (i *pointSynthesizingIter) Stats() IteratorStats {
	return i.iter.Stats()
}

// SupportsPrev implements MVCCIterator.
func (i *pointSynthesizingIter) SupportsPrev() bool {
	return i.iter.SupportsPrev()
}

// assertInvariants asserts iterator invariants.
func (i *pointSynthesizingIter) assertInvariants() error {
	// If the underlying iterator has errored, make sure we're not positioned on a
	// synthetic point such that Valid() will surface the error.
	if _, err := i.iter.Valid(); err != nil {
		if !i.atPoint && i.rangeKeysIdx >= 0 && i.rangeKeysIdx < len(i.rangeKeys) {
			return errors.NewAssertionErrorWithWrappedErrf(err, "iterator error with synthetic point %s",
				i.rangeKeysPos)
		}
		return nil
	}

	// When atPoint is true, the underlying iterator must be valid and on a point.
	if i.atPoint {
		if ok, _ := i.iter.Valid(); !ok {
			return errors.AssertionFailedf("atPoint with invalid iter")
		}
		if hasPoint, _ := i.iter.HasPointAndRange(); !hasPoint {
			return errors.AssertionFailedf("atPoint at non-point position %s", i.iter.UnsafeKey())
		}
	}

	// rangeKeysIdx is never more than 1 outside of the slice bounds, and the
	// excess depends on the direction: len(rangeKeys) in the forward direction,
	// -1 in the reverse.
	if i.rangeKeysIdx < 0 || i.rangeKeysIdx >= len(i.rangeKeys) {
		if (!i.reverse && i.rangeKeysIdx != len(i.rangeKeys)) || (i.reverse && i.rangeKeysIdx != -1) {
			return errors.AssertionFailedf("invalid rangeKeysIdx %d with length %d and reverse=%t",
				i.rangeKeysIdx, len(i.rangeKeys), i.reverse)
		}
	}

	// If rangeKeys is empty, atPoint is true unless exhausted and other state is
	// cleared. In this case, there's nothing more to check.
	if len(i.rangeKeys) == 0 {
		if ok, _ := i.iter.Valid(); ok && !i.atPoint {
			return errors.AssertionFailedf("no rangeKeys nor atPoint")
		}
		if len(i.rangeKeysPos) > 0 {
			return errors.AssertionFailedf("no rangeKeys but rangeKeysPos %s", i.rangeKeysPos)
		}
		if len(i.rangeKeysStart) > 0 {
			return errors.AssertionFailedf("no rangeKeys but rangeKeysStart %s", i.rangeKeysStart)
		}
		return nil
	}

	// rangeKeysStart must be set, and rangeKeysPos must be at or after it. This
	// implies that rangeKeysPos must also be set.
	if len(i.rangeKeysStart) == 0 {
		return errors.AssertionFailedf("no rangeKeysStart at %s", i.iter.UnsafeKey())
	}
	if i.rangeKeysPos.Compare(i.rangeKeysStart) < 0 {
		return errors.AssertionFailedf("rangeKeysPos %s not after rangeKeysStart %s",
			i.rangeKeysPos, i.rangeKeysStart)
	}

	// rangeKeysIdx must be valid if we're not on a point.
	if !i.atPoint && (i.rangeKeysIdx < 0 || i.rangeKeysIdx >= len(i.rangeKeys)) {
		return errors.AssertionFailedf("not atPoint with invalid rangeKeysIdx %d at %s",
			i.rangeKeysIdx, i.rangeKeysPos)
	}

	// If the underlying iterator is exhausted, then there's nothing more to
	// check. We must either be on a synthetic point key or exhausted iterator.
	if ok, _ := i.iter.Valid(); !ok {
		return nil
	}

	// We now have range keys and a non-exhausted iterator. Check their relative
	// positioning as minimum and maximum iter keys (in MVCC order). We can assume
	// that overlapping range keys and point keys don't have the same timestamp,
	// since this is enforced by MVCC mutations.
	var minKey, maxKey MVCCKey

	// The iterator should never lag behind the range key position.
	if !i.reverse {
		minKey = MVCCKey{Key: i.rangeKeysPos}
	} else {
		maxKey = MVCCKey{Key: i.rangeKeysPos, Timestamp: hlc.MinTimestamp}
	}

	// If we're not at a real point, then the iterator must be ahead of the
	// current synthesized point. If we are on a point, then it must lie between
	// the surrounding range keys (if they exist).
	minIdx, maxIdx := -1, -1
	if !i.atPoint {
		if !i.reverse {
			minIdx = i.rangeKeysIdx
		} else {
			maxIdx = i.rangeKeysIdx
		}
	} else if !i.reverse {
		minIdx = i.rangeKeysIdx - 1
		maxIdx = i.rangeKeysIdx
	} else {
		minIdx = i.rangeKeysIdx
		maxIdx = i.rangeKeysIdx + 1
	}
	if minIdx >= 0 && minIdx < len(i.rangeKeys) {
		minKey = MVCCKey{Key: i.rangeKeysPos, Timestamp: i.rangeKeys[minIdx].RangeKey.Timestamp}
	}
	if maxIdx >= 0 && maxIdx < len(i.rangeKeys) {
		maxKey = MVCCKey{Key: i.rangeKeysPos, Timestamp: i.rangeKeys[maxIdx].RangeKey.Timestamp}
	}

	iterKey := i.iter.Key()
	if minKey.Key != nil && iterKey.Compare(minKey) < 0 {
		return errors.AssertionFailedf("iter %s below minimum key %s", iterKey, minKey)
	}
	if maxKey.Key != nil && iterKey.Compare(maxKey) > 0 {
		return errors.AssertionFailedf("iter %s above maximum key %s", iterKey, maxKey)
	}

	return nil
}
