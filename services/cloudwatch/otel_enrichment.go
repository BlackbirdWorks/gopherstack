package cloudwatch

import (
	"fmt"
	"slices"
	"time"
)

const maxOTelEnrichmentFilters = 100

// otelEnrichmentStatusLocked defaults to Stopped, the state before StartOTelEnrichment.
func (b *InMemoryBackend) otelEnrichmentStatusLocked() string {
	if s, ok := b.otelEnrichment.Get(otelEnrichmentSingletonKey); ok && s.Status != "" {
		return s.Status
	}

	return otelEnrichmentStatusStopped
}

// GetOTelEnrichment returns the current account-level status of vended
// metric OTel/PromQL enrichment.
func (b *InMemoryBackend) GetOTelEnrichment() (string, error) {
	b.mu.RLock("GetOTelEnrichment")
	defer b.mu.RUnlock()

	return b.otelEnrichmentStatusLocked(), nil
}

// OTelEnrichmentDetails returns a copy of the account enrichment state (status, timestamps, filters).
func (b *InMemoryBackend) OTelEnrichmentDetails() OTelEnrichmentState {
	b.mu.RLock("OTelEnrichmentDetails")
	defer b.mu.RUnlock()

	st := OTelEnrichmentState{Key: otelEnrichmentSingletonKey, Status: otelEnrichmentStatusStopped}
	if got, ok := b.otelEnrichment.Get(otelEnrichmentSingletonKey); ok {
		st = cloneOTelState(got)
	}

	return st
}

func cloneOTelState(s *OTelEnrichmentState) OTelEnrichmentState {
	out := *s
	out.IncludeFilters = cloneOTelSelectors(s.IncludeFilters)
	out.ExcludeFilters = cloneOTelSelectors(s.ExcludeFilters)

	return out
}

func cloneOTelSelectors(in []OTelEnrichmentSelector) []OTelEnrichmentSelector {
	if len(in) == 0 {
		return nil
	}

	out := make([]OTelEnrichmentSelector, len(in))
	for i, sel := range in {
		out[i] = OTelEnrichmentSelector{Namespace: sel.Namespace, MetricNames: slices.Clone(sel.MetricNames)}
	}

	return out
}

// StartOTelEnrichment enables enrichment; starting while running keeps the stored filters and timestamps.
func (b *InMemoryBackend) StartOTelEnrichment() error {
	b.mu.Lock("StartOTelEnrichment")
	defer b.mu.Unlock()

	if got, ok := b.otelEnrichment.Get(otelEnrichmentSingletonKey); ok && got.Status == otelEnrichmentStatusRunning {
		return nil
	}

	now := time.Now().UTC()
	b.otelEnrichment.Put(&OTelEnrichmentState{
		Key:       otelEnrichmentSingletonKey,
		Status:    otelEnrichmentStatusRunning,
		CreatedAt: now,
		UpdatedAt: now,
	})

	return nil
}

// StopOTelEnrichment disables enrichment and discards the stored filters.
func (b *InMemoryBackend) StopOTelEnrichment() error {
	b.mu.Lock("StopOTelEnrichment")
	defer b.mu.Unlock()

	b.otelEnrichment.Put(&OTelEnrichmentState{
		Key:    otelEnrichmentSingletonKey,
		Status: otelEnrichmentStatusStopped,
	})

	return nil
}

// UpdateOTelEnrichment replaces the include/exclude filter pair; enrichment must be running.
func (b *InMemoryBackend) UpdateOTelEnrichment(include, exclude []OTelEnrichmentSelector) (OTelEnrichmentState, error) {
	if len(include)+len(exclude) > maxOTelEnrichmentFilters {
		return OTelEnrichmentState{}, fmt.Errorf(
			"%w: at most %d filters are allowed across IncludeFilters and ExcludeFilters",
			ErrValidation, maxOTelEnrichmentFilters,
		)
	}

	for _, sel := range slices.Concat(include, exclude) {
		if sel.Namespace == "" {
			return OTelEnrichmentState{}, fmt.Errorf("%w: filter Namespace is required", ErrValidation)
		}

		if len(sel.MetricNames) > maxOTelEnrichmentFilters {
			return OTelEnrichmentState{}, fmt.Errorf(
				"%w: at most %d MetricNames are allowed per filter", ErrValidation, maxOTelEnrichmentFilters,
			)
		}
	}

	b.mu.Lock("UpdateOTelEnrichment")
	defer b.mu.Unlock()

	cur, ok := b.otelEnrichment.Get(otelEnrichmentSingletonKey)
	if !ok || cur.Status != otelEnrichmentStatusRunning {
		return OTelEnrichmentState{}, fmt.Errorf(
			"%w: OTel enrichment is not running for this account",
			ErrOTelEnrichmentNotRunning,
		)
	}

	next := &OTelEnrichmentState{
		Key:            otelEnrichmentSingletonKey,
		Status:         otelEnrichmentStatusRunning,
		CreatedAt:      cur.CreatedAt,
		UpdatedAt:      time.Now().UTC(),
		IncludeFilters: cloneOTelSelectors(include),
		ExcludeFilters: cloneOTelSelectors(exclude),
	}
	b.otelEnrichment.Put(next)

	return cloneOTelState(next), nil
}
