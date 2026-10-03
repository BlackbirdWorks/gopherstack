package dsql

import (
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateStream creates a Kinesis change-data-capture stream on a cluster.
// New streams start CREATING and lazily transition to ACTIVE on the next
// read once streamActivationDelay elapses.
func (b *InMemoryBackend) CreateStream(clusterIdentifier string, in CreateStreamInput) (*Stream, error) {
	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}

	if in.Target == nil || in.Target.RoleArn == "" || in.Target.StreamArn == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateStream")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(clusterIdentifier)
	if err != nil {
		return nil, err
	}

	b.sweepDeletedStreamsLocked(time.Now())

	if b.countStreamsLocked(clusterIdentifier) >= maxStreamsPerCluster {
		return nil, ErrStreamQuotaExceeded
	}

	streamIdentifier := newIdentifier()
	now := time.Now().UTC()

	tags := make(map[string]string, len(in.Tags))
	maps.Copy(tags, in.Tags)

	target := *in.Target

	s := &Stream{
		ClusterIdentifier: clusterIdentifier,
		StreamIdentifier:  streamIdentifier,
		ARN:               streamARN(c.Region, c.AccountID, clusterIdentifier, streamIdentifier),
		Status:            streamStatusCreating,
		Format:            in.Format,
		Ordering:          in.Ordering,
		CreationTime:      now,
		PendingUntil:      now.Add(streamActivationDelay),
		Target:            &target,
		Tags:              tags,
	}

	b.streams.Put(s)

	return s.clone(), nil
}

func (b *InMemoryBackend) sweepDeletedStreamsLocked(now time.Time) {
	for _, s := range b.streams.All() {
		if s.deletionDue(now) {
			b.streams.Delete(streamKey(s.ClusterIdentifier, s.StreamIdentifier))
		}
	}
}

func (b *InMemoryBackend) countStreamsLocked(clusterIdentifier string) int {
	n := 0

	for _, s := range b.streams.All() {
		if s.ClusterIdentifier == clusterIdentifier {
			n++
		}
	}

	return n
}

// GetStream returns the current information about a stream.
func (b *InMemoryBackend) GetStream(clusterIdentifier, streamIdentifier string) (*Stream, error) {
	b.mu.Lock("GetStream")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked(clusterIdentifier, streamIdentifier)
	if err != nil {
		return nil, err
	}

	return s.clone(), nil
}

// DeleteStream marks a stream DELETING; it is lazily removed streamDeletionDelay
// later, on the next read.
func (b *InMemoryBackend) DeleteStream(clusterIdentifier, streamIdentifier string) (*Stream, error) {
	b.mu.Lock("DeleteStream")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked(clusterIdentifier, streamIdentifier)
	if err != nil {
		return nil, err
	}

	if s.Status != streamStatusDeleting {
		s.Status = streamStatusDeleting
		s.PendingUntil = time.Now().UTC().Add(streamDeletionDelay)
	}

	return s.clone(), nil
}

// ListStreams returns a cluster's streams ordered by stream identifier, paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListStreams(clusterIdentifier, nextToken string, maxResults int) ([]*Stream, string, error) {
	b.mu.Lock("ListStreams")
	defer b.mu.Unlock()

	if _, err := b.resolveClusterLocked(clusterIdentifier); err != nil {
		return nil, "", err
	}

	matched := make([]*Stream, 0)

	for _, s := range b.streams.All() {
		if s.ClusterIdentifier != clusterIdentifier {
			continue
		}

		b.advanceStreamLocked(s)

		if s.deletionDue(time.Now()) {
			b.streams.Delete(streamKey(clusterIdentifier, s.StreamIdentifier))

			continue
		}

		matched = append(matched, s.clone())
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].StreamIdentifier < matched[j].StreamIdentifier })

	p := page.New(matched, nextToken, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}
