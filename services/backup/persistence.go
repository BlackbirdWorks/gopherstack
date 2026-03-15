package backup

import (
	"encoding/json"
)

type backendSnapshot struct {
	Vaults    map[string]*Vault `json:"vaults"`
	Plans     map[string]*Plan  `json:"plans"`
	Jobs      map[string]*Job   `json:"jobs"`
	AccountID string            `json:"accountID"`
	Region    string            `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Vaults:    b.vaults,
		Plans:     b.plans,
		Jobs:      b.jobs,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
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

	if snap.Vaults == nil {
		snap.Vaults = make(map[string]*Vault)
	}
	if snap.Plans == nil {
		snap.Plans = make(map[string]*Plan)
	}
	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*Job)
	}

	b.vaults = snap.Vaults
	b.plans = snap.Plans
	b.jobs = snap.Jobs
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild ARN indexes from restored state.
	b.vaultARNIndex = make(map[string]string, len(b.vaults))
	for name, v := range b.vaults {
		b.vaultARNIndex[v.BackupVaultArn] = name
	}

	b.planARNIndex = make(map[string]string, len(b.plans))
	b.planIDIndex = make(map[string]string, len(b.plans))
	for name, p := range b.plans {
		b.planARNIndex[p.BackupPlanArn] = name
		b.planIDIndex[p.BackupPlanID] = name
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
