package timestreamquery

import (
	"encoding/json"
	"log/slog"
	"maps"
)

// backendSnapshot is the serialisable form of InMemoryBackend state.
type backendSnapshot struct {
	ScheduledQueries map[string]map[string]*ScheduledQuery `json:"scheduled_queries"` // region → name → SQ
	ArnIndex         map[string]map[string]string          `json:"arn_index"`         // region → ARN → name
	AccountSettings  map[string]accountSettingsSnapshot    `json:"account_settings"`  // region → settings
}

// accountSettingsSnapshot is the serialisable form of AccountSettings.
type accountSettingsSnapshot struct {
	MaxQueryTCU       *int32 `json:"max_query_tcu,omitempty"`
	QueryPricingModel string `json:"query_pricing_model"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Deep-copy scheduled queries across all regions.
	sqCopy := make(map[string]map[string]*ScheduledQuery, len(b.scheduledQueries))
	for region, regionMap := range b.scheduledQueries {
		inner := make(map[string]*ScheduledQuery, len(regionMap))
		for name, sq := range regionMap {
			inner[name] = cloneScheduledQuery(sq)
		}
		sqCopy[region] = inner
	}

	// Deep-copy ARN index across all regions.
	arnCopy := make(map[string]map[string]string, len(b.arnIndex))
	for region, regionMap := range b.arnIndex {
		arnCopy[region] = maps.Clone(regionMap)
	}

	// Snapshot account settings across all regions.
	settingsSnap := make(map[string]accountSettingsSnapshot, len(b.accountSettings))
	for region, s := range b.accountSettings {
		snap := accountSettingsSnapshot{QueryPricingModel: s.QueryPricingModel}
		if s.MaxQueryTCU != nil {
			v := *s.MaxQueryTCU
			snap.MaxQueryTCU = &v
		}
		settingsSnap[region] = snap
	}

	data, err := json.Marshal(backendSnapshot{
		ScheduledQueries: sqCopy,
		ArnIndex:         arnCopy,
		AccountSettings:  settingsSnap,
	})
	if err != nil {
		slog.Default().Warn("timestreamquery: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.scheduledQueries = snap.ScheduledQueries
	b.arnIndex = snap.ArnIndex

	// Reconstruct per-region account settings.
	b.accountSettings = make(map[string]AccountSettings, len(snap.AccountSettings))
	for region, s := range snap.AccountSettings {
		b.accountSettings[region] = AccountSettings{
			QueryPricingModel: s.QueryPricingModel,
			MaxQueryTCU:       s.MaxQueryTCU,
		}
	}

	ensureNonNilMaps(b)

	return nil
}

// ensureNonNilMaps initialises any nil maps in the backend.
// Must be called with the write lock held.
func ensureNonNilMaps(b *InMemoryBackend) {
	if b.scheduledQueries == nil {
		b.scheduledQueries = make(map[string]map[string]*ScheduledQuery)
	}

	if b.arnIndex == nil {
		b.arnIndex = make(map[string]map[string]string)
	}

	if b.queries == nil {
		b.queries = make(map[string]*QueryResult)
	}

	if b.accountSettings == nil {
		b.accountSettings = make(map[string]AccountSettings)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(data)
	}

	return nil
}
