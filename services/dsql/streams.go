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

// DeleteStream removes a stream immediately (real AWS transitions through
// DELETING, but nothing else in this backend observes a stream's
// intermediate delete state, so removing it synchronously here is
// behaviorally equivalent to any client that only checks for
// ResourceNotFoundException afterward).
func (b *InMemoryBackend) DeleteStream(clusterIdentifier, streamIdentifier string) (*Stream, error) {
	b.mu.Lock("DeleteStream")
	defer b.mu.Unlock()

	s, err := b.resolveStreamLocked(clusterIdentifier, streamIdentifier)
	if err != nil {
		return nil, err
	}

	b.streams.Delete(streamKey(clusterIdentifier, streamIdentifier))

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
		matched = append(matched, s.clone())
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].StreamIdentifier < matched[j].StreamIdentifier })

	p := page.New(matched, nextToken, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}
