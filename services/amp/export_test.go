package amp

import (
	"time"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SampleCount returns the stored samples of a workspace across all series.
func (b *InMemoryBackend) SampleCount(wsID string) int {
	b.mu.RLock("SampleCount")
	defer b.mu.RUnlock()

	n := 0
	for _, s := range b.seriesByWS.Get(wsID) {
		n += len(s.Samples)
	}

	return n
}

// WorkspaceCount returns the stored workspaces, deleting ones included.
func (b *InMemoryBackend) WorkspaceCount() int {
	b.mu.RLock("WorkspaceCount")
	defer b.mu.RUnlock()

	return b.workspaces.Len()
}

// WriteSeries is the exported test alias of a remote-write series.
type WriteSeries = writeSeries

// EncodeWriteRequest builds a snappy remote-write body.
func EncodeWriteRequest(series []WriteSeries) []byte { return encodeWriteRequest(series) }

// DecodeWriteRequest parses a snappy remote-write body.
func DecodeWriteRequest(body []byte) ([]WriteSeries, error) { return decodeWriteRequest(body) }

// NewSeries builds a WriteSeries for tests.
func NewSeries(labels map[string]string, samples ...promql.Sample) WriteSeries {
	return WriteSeries{Labels: labels, Samples: samples}
}
