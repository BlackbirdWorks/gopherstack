package kinesis

import "time"

// streamDeadlinePassed reports whether stream's ReadyAt deadline has passed
// as of now.
func streamDeadlinePassed(stream *Stream, now time.Time) bool {
	return !stream.ReadyAt.IsZero() && !now.Before(stream.ReadyAt)
}

// effectiveStreamStatus returns stream's status as of now, resolving a due
// CREATING/UPDATING->ACTIVE deadline without mutating stream. Callers that
// only need to know the current status (not report or advance it, e.g. the
// PutRecord hot path) can call this under whatever lock they already hold.
// DELETING is never resolved here -- removing a stream from the backend
// requires b.mu for writing, which callers of this helper may not hold; use
// resolveStreamTransitionLocked for that.
func effectiveStreamStatus(stream *Stream, now time.Time) string {
	if (stream.Status == streamStatusCreating || stream.Status == streamStatusUpdating) &&
		streamDeadlinePassed(stream, now) {
		return streamStatusActive
	}

	return stream.Status
}

// streamEffectivelyGone reports whether stream should be treated as already
// removed: DELETING with its removal deadline passed. Pure/non-mutating, for
// hot-path readers (GetRecords, GetShardIterator) that only hold b.mu for
// reading and so cannot perform the physical removal themselves -- see
// resolveStreamTransitionLocked, which does that lazily elsewhere (any
// DescribeStream/ListStreams/mutation call).
func streamEffectivelyGone(stream *Stream, now time.Time) bool {
	return stream.Status == streamStatusDeleting && streamDeadlinePassed(stream, now)
}

// resolveStreamTransitionLocked returns the current stream for region/name
// after resolving any lazy CREATING/UPDATING->ACTIVE transition whose
// deadline has passed, or reports ErrStreamNotFound if a DELETING deadline
// has passed (physically removing the stream). Callers must hold b.mu for
// writing -- see services/dsql's resolveClusterLocked and services/dax's
// sweepClusterTransitionsLocked (commit b42c0fe60) for the same lazy-deadline
// pattern.
func (b *InMemoryBackend) resolveStreamTransitionLocked(region, name string) (*Stream, error) {
	key := streamKey(region, name)

	stream, ok := b.streams.Get(key)
	if !ok {
		return nil, ErrStreamNotFound
	}

	stream.mu.Lock("resolveStreamTransition.stream")
	now := b.nowFunc()

	switch {
	case stream.Status == streamStatusDeleting && streamDeadlinePassed(stream, now):
		stream.mu.Unlock()
		b.finishStreamDeletionLocked(region, name, stream)

		return nil, ErrStreamNotFound
	case (stream.Status == streamStatusCreating || stream.Status == streamStatusUpdating) &&
		streamDeadlinePassed(stream, now):
		stream.Status = streamStatusActive
		stream.ReadyAt = time.Time{}
	}
	stream.mu.Unlock()

	return stream, nil
}

// resolveRegionStreamsLocked returns every live stream in region after
// resolving each one's due lazy transition (see
// resolveStreamTransitionLocked), pruning any whose DELETING deadline has
// passed. Callers must hold b.mu for writing.
func (b *InMemoryBackend) resolveRegionStreamsLocked(region string) []*Stream {
	names := make([]string, 0, len(b.streamsByRegion.Get(region)))
	for _, s := range b.streamsByRegion.Get(region) {
		names = append(names, s.Name)
	}

	live := make([]*Stream, 0, len(names))
	for _, name := range names {
		s, err := b.resolveStreamTransitionLocked(region, name)
		if err != nil {
			continue
		}
		live = append(live, s)
	}

	return live
}

// finishStreamDeletionLocked physically removes stream -- already past its
// DELETING deadline -- from the backend. Callers must hold b.mu for writing;
// stream.mu must NOT be held.
func (b *InMemoryBackend) finishStreamDeletionLocked(region, name string, stream *Stream) {
	stream.mu.Lock("finishStreamDeletion.stream")
	if stream.Tags != nil {
		stream.Tags.Close()
	}
	stream.mu.Unlock()

	b.streams.Delete(streamKey(region, name))

	b.faultsMu.Lock("finishStreamDeletion.faults")
	delete(b.faultsStore(region), name)
	b.faultsMu.Unlock()

	delete(b.policiesStore(region), stream.ARN)

	stream.mu.Close()

	if b.OnStreamPurged != nil {
		b.OnStreamPurged(name)
	}
}
