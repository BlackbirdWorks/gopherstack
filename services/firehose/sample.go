package firehose

import (
	"context"
	"fmt"
	"slices"
)

const (
	maxRecentSamples     = 10
	maxSampleRecordBytes = 64 * 1024
)

// noteSample keeps the newest maxRecentSamples small records for schema discovery.
func (s *DeliveryStream) noteSample(rec []byte) {
	if len(rec) > maxSampleRecordBytes {
		return
	}

	if len(s.recentSamples) >= maxRecentSamples {
		s.recentSamples = s.recentSamples[1:]
	}

	s.recentSamples = append(s.recentSamples, slices.Clone(rec))
}

// SampleRecords returns up to limit of the most recently ingested records (oldest first).
func (b *InMemoryBackend) SampleRecords(ctx context.Context, streamName string, limit int) ([][]byte, error) {
	b.mu.RLock("SampleRecords")
	defer b.mu.RUnlock()

	s, ok := b.streams.Get(regionKey(getRegionFromContext(ctx, b), streamName))
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	samples := s.recentSamples
	if limit > 0 && len(samples) > limit {
		samples = samples[len(samples)-limit:]
	}

	out := make([][]byte, len(samples))
	for i, rec := range samples {
		out[i] = slices.Clone(rec)
	}

	return out, nil
}
