package mwaa

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Environments map[string]*Environment  `json:"environments"`
	ARNIndex     map[string]string        `json:"arnIndex"`
	Metrics      map[string][]MetricDatum `json:"metrics"`
	AccountID    string                   `json:"accountID"`
	Region       string                   `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Environments: b.environments,
		ARNIndex:     b.arnIndex,
		Metrics:      b.metrics,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("mwaa: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)
	fixNilEnvTags(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.environments = snap.Environments
	b.arnIndex = snap.ARNIndex
	b.metrics = snap.Metrics
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Environments == nil {
		snap.Environments = make(map[string]*Environment)
	}

	if snap.ARNIndex == nil {
		snap.ARNIndex = make(map[string]string)
	}

	if snap.Metrics == nil {
		snap.Metrics = make(map[string][]MetricDatum)
	}
}

// fixNilEnvTags ensures restored environments have non-nil tag maps.
func fixNilEnvTags(snap *backendSnapshot) {
	for _, env := range snap.Environments {
		if env.Tags == nil {
			env.Tags = make(map[string]string)
		}
	}
}
