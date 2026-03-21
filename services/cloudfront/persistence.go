package cloudfront

import "encoding/json"

type backendSnapshot struct {
	Distributions map[string]*Distribution         `json:"distributions"`
	OAIs          map[string]*OriginAccessIdentity `json:"oais"`
	Invalidations map[string][]*Invalidation       `json:"invalidations,omitempty"`
	AccountID     string                           `json:"accountId"`
	Region        string                           `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Distributions: b.distributions,
		OAIs:          b.oais,
		Invalidations: b.invalidations,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, _ := json.Marshal(snap)

	return data
}

// Restore loads backend state from a JSON snapshot and rebuilds the ARN index.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Distributions == nil {
		snap.Distributions = make(map[string]*Distribution)
	}

	if snap.OAIs == nil {
		snap.OAIs = make(map[string]*OriginAccessIdentity)
	}

	if snap.Invalidations == nil {
		snap.Invalidations = make(map[string][]*Invalidation)
	}

	// Rebuild the ARN-to-ID index after restore so O(1) tag operations remain correct.
	arnIndex := make(map[string]string, len(snap.Distributions))
	for id, d := range snap.Distributions {
		arnIndex[d.ARN] = id
	}

	b.distributions = snap.Distributions
	b.oais = snap.OAIs
	b.invalidations = snap.Invalidations
	b.distributionARNs = arnIndex
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
