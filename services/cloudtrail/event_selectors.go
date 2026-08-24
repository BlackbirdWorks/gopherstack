package cloudtrail

import (
	"fmt"
	"time"
)

// PutEventSelectors sets event selectors for a trail. Basic and advanced selectors
// are mutually exclusive: providing AdvancedEventSelectors clears EventSelectors and vice versa.
func (b *InMemoryBackend) PutEventSelectors(
	nameOrARN string,
	selectors []EventSelector,
	advancedSelectors []AdvancedEventSelector,
) (*Trail, error) {
	b.mu.Lock("PutEventSelectors")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}
	if len(advancedSelectors) > 0 {
		// Advanced selectors replace basic selectors.
		t.AdvancedEventSelectors = copyAdvancedEventSelectors(advancedSelectors)
		t.EventSelectors = nil
		t.HasCustomEventSelectors = true
	} else {
		// Basic selectors replace advanced selectors.
		t.EventSelectors = selectors
		t.AdvancedEventSelectors = nil
		t.HasCustomEventSelectors = len(selectors) > 0
	}
	cp := *t
	cp.EventSelectors = copyEventSelectors(t.EventSelectors)
	cp.AdvancedEventSelectors = copyAdvancedEventSelectors(t.AdvancedEventSelectors)

	return &cp, nil
}

// GetEventSelectors returns both basic and advanced event selectors for a trail.
func (b *InMemoryBackend) GetEventSelectors(
	nameOrARN string,
) (string, []EventSelector, []AdvancedEventSelector, error) {
	b.mu.RLock("GetEventSelectors")
	defer b.mu.RUnlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return "", nil, nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}

	return t.TrailARN, copyEventSelectors(t.EventSelectors), copyAdvancedEventSelectors(t.AdvancedEventSelectors), nil
}

// PutInsightSelectors sets insight selectors for a trail, updating HasInsightSelectors.
func (b *InMemoryBackend) PutInsightSelectors(trailNameOrARN string, selectors []InsightSelector) (*Trail, error) {
	b.mu.Lock("PutInsightSelectors")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(trailNameOrARN)
	if t == nil {
		return nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, trailNameOrARN)
	}
	t.InsightSelectors = make([]InsightSelector, len(selectors))
	copy(t.InsightSelectors, selectors)
	t.HasInsightSelectors = len(selectors) > 0

	cp := *t
	cp.InsightSelectors = make([]InsightSelector, len(t.InsightSelectors))
	copy(cp.InsightSelectors, t.InsightSelectors)
	cp.EventSelectors = copyEventSelectors(t.EventSelectors)
	cp.AdvancedEventSelectors = copyAdvancedEventSelectors(t.AdvancedEventSelectors)

	return &cp, nil
}

// GetInsightSelectors returns insight selectors for a trail.
// AWS returns InsightNotEnabledException when no insight selectors are configured.
func (b *InMemoryBackend) GetInsightSelectors(trailNameOrARN string) (string, []InsightSelector, error) {
	b.mu.RLock("GetInsightSelectors")
	defer b.mu.RUnlock()

	t := b.findByNameOrARNLocked(trailNameOrARN)
	if t == nil {
		return "", nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, trailNameOrARN)
	}
	if len(t.InsightSelectors) == 0 {
		return "", nil, fmt.Errorf("%w: trail %s does not have Insights enabled", ErrInsightNotEnabled, trailNameOrARN)
	}
	cp := make([]InsightSelector, len(t.InsightSelectors))
	copy(cp, t.InsightSelectors)

	return t.TrailARN, cp, nil
}

// GetEDSInsightSelectors returns insight selectors for an event data store.
// AWS returns InsightNotEnabledException when no insight selectors are configured.
func (b *InMemoryBackend) GetEDSInsightSelectors(edsIDOrARN string) (string, []InsightSelector, error) {
	b.mu.RLock("GetEDSInsightSelectors")
	defer b.mu.RUnlock()

	eds := b.findEventDataStoreLocked(edsIDOrARN)
	if eds == nil {
		return "", nil, fmt.Errorf("%w: event data store %s not found", ErrNotFound, edsIDOrARN)
	}
	if len(eds.InsightSelectors) == 0 {
		return "", nil, fmt.Errorf(
			"%w: event data store %s does not have Insights enabled", ErrInsightNotEnabled, edsIDOrARN,
		)
	}
	cp := make([]InsightSelector, len(eds.InsightSelectors))
	copy(cp, eds.InsightSelectors)

	return eds.EventDataStoreARN, cp, nil
}

// GetEventConfiguration returns the event configuration for a trail or event
// data store ARN. AWS returns an empty configuration when none has been set.
func (b *InMemoryBackend) GetEventConfiguration(resourceARN string) *EventConfiguration {
	b.mu.RLock("GetEventConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.eventConfigs[resourceARN]
	if !ok {
		return &EventConfiguration{}
	}
	cp := *cfg

	return &cp
}

// PutEventConfiguration sets the event configuration for a trail or event
// data store ARN.
func (b *InMemoryBackend) PutEventConfiguration(
	resourceARN string,
	aggregationConfigurations, contextKeySelectors []map[string]any,
	maxEventSize string,
) *EventConfiguration {
	b.mu.Lock("PutEventConfiguration")
	defer b.mu.Unlock()

	cfg := &EventConfiguration{
		AggregationConfigurations: aggregationConfigurations,
		ContextKeySelectors:       contextKeySelectors,
		MaxEventSize:              maxEventSize,
	}
	b.eventConfigs[resourceARN] = cfg
	cp := *cfg

	return &cp
}

// ListInsightsData returns empty insights data (stub).
func (b *InMemoryBackend) ListInsightsData() []map[string]any {
	return []map[string]any{}
}

// ListInsightsMetricData returns empty insights metric data (stub). The real
// ListInsightsMetricDataOutput.Values field is []float64, not a list of
// records.
func (b *InMemoryBackend) ListInsightsMetricData() []float64 {
	return []float64{}
}

// PutEDSInsightSelectors sets insight selectors for an event data store.
func (b *InMemoryBackend) PutEDSInsightSelectors(
	edsIDOrARN string,
	selectors []InsightSelector,
) (*EventDataStore, error) {
	b.mu.Lock("PutEDSInsightSelectors")
	defer b.mu.Unlock()

	eds := b.findEventDataStoreLocked(edsIDOrARN)
	if eds == nil {
		return nil, fmt.Errorf("%w: event data store %s not found", ErrNotFound, edsIDOrARN)
	}
	eds.InsightSelectors = make([]InsightSelector, len(selectors))
	copy(eds.InsightSelectors, selectors)
	eds.UpdatedTimestamp = time.Now().UTC()
	cp := *eds
	cp.AdvancedEventSelectors = copyAdvancedEventSelectors(eds.AdvancedEventSelectors)
	cp.InsightSelectors = make([]InsightSelector, len(eds.InsightSelectors))
	copy(cp.InsightSelectors, eds.InsightSelectors)

	return &cp, nil
}
