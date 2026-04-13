package timestreamquery

import (
	"encoding/json"
	"log/slog"
	"maps"
)

// backendSnapshot is the serialisable form of InMemoryBackend state.
type backendSnapshot struct {
	ScheduledQueries map[string]*ScheduledQuery `json:"scheduled_queries"`
	ArnIndex         map[string]string          `json:"arn_index"`
	AccountSettings  accountSettingsSnapshot    `json:"account_settings"`
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

	sqCopy := make(map[string]*ScheduledQuery, len(b.scheduledQueries))
	for k, v := range b.scheduledQueries {
		sqCopy[k] = cloneScheduledQuery(v)
	}

	snap := backendSnapshot{
		ScheduledQueries: sqCopy,
		ArnIndex:         maps.Clone(b.arnIndex),
		AccountSettings: accountSettingsSnapshot{
			QueryPricingModel: b.accountSettings.QueryPricingModel,
		},
	}

	if b.accountSettings.MaxQueryTCU != nil {
		v := *b.accountSettings.MaxQueryTCU
		snap.AccountSettings.MaxQueryTCU = &v
	}

	data, err := json.Marshal(snap)
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
	b.accountSettings = AccountSettings{
		QueryPricingModel: snap.AccountSettings.QueryPricingModel,
		MaxQueryTCU:       snap.AccountSettings.MaxQueryTCU,
	}

	ensureNonNilMaps(b)

	return nil
}

// ensureNonNilMaps initialises any nil maps in the backend.
// Must be called with the write lock held.
func ensureNonNilMaps(b *InMemoryBackend) {
	if b.scheduledQueries == nil {
		b.scheduledQueries = make(map[string]*ScheduledQuery)
	}

	if b.arnIndex == nil {
		b.arnIndex = make(map[string]string)
	}

	if b.queries == nil {
		b.queries = make(map[string]*QueryResult)
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
