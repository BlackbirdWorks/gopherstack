package awsconfig

import (
	"context"
	"encoding/json"
)

type backendSnapshot struct {
	Recorders           map[string]*ConfigurationRecorder       `json:"recorders"`
	Channels            map[string]*DeliveryChannel             `json:"channels"`
	AggregationAuths    map[string]*AggregationAuthorization    `json:"aggregationAuths,omitempty"`
	ConfigRules         map[string]*ConfigRule                  `json:"configRules,omitempty"`
	Aggregators         map[string]*ConfigurationAggregator     `json:"aggregators,omitempty"`
	ConformancePacks    map[string]*ConformancePack             `json:"conformancePacks,omitempty"`
	OrgConfigRules      map[string]*OrganizationConfigRule      `json:"orgConfigRules,omitempty"`
	OrgConformancePacks map[string]*OrganizationConformancePack `json:"orgConformancePacks,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Recorders:           b.recorders,
		Channels:            b.channels,
		AggregationAuths:    b.aggregationAuths,
		ConfigRules:         b.configRules,
		Aggregators:         b.aggregators,
		ConformancePacks:    b.conformancePacks,
		OrgConfigRules:      b.orgConfigRules,
		OrgConformancePacks: b.orgConformancePacks,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Recorders == nil {
		snap.Recorders = make(map[string]*ConfigurationRecorder)
	}

	if snap.Channels == nil {
		snap.Channels = make(map[string]*DeliveryChannel)
	}

	if snap.AggregationAuths == nil {
		snap.AggregationAuths = make(map[string]*AggregationAuthorization)
	}

	if snap.ConfigRules == nil {
		snap.ConfigRules = make(map[string]*ConfigRule)
	}

	if snap.Aggregators == nil {
		snap.Aggregators = make(map[string]*ConfigurationAggregator)
	}

	if snap.ConformancePacks == nil {
		snap.ConformancePacks = make(map[string]*ConformancePack)
	}

	if snap.OrgConfigRules == nil {
		snap.OrgConfigRules = make(map[string]*OrganizationConfigRule)
	}

	if snap.OrgConformancePacks == nil {
		snap.OrgConformancePacks = make(map[string]*OrganizationConformancePack)
	}

	b.recorders = snap.Recorders
	b.channels = snap.Channels
	b.aggregationAuths = snap.AggregationAuths
	b.configRules = snap.ConfigRules
	b.aggregators = snap.Aggregators
	b.conformancePacks = snap.ConformancePacks
	b.orgConfigRules = snap.OrgConfigRules
	b.orgConformancePacks = snap.OrgConformancePacks

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
