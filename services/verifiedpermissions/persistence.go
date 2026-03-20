package verifiedpermissions

import "encoding/json"

type backendSnapshot struct {
	PolicyStores    map[string]*PolicyStore               `json:"policyStores"`
	Policies        map[string]map[string]*Policy         `json:"policies"`
	PolicyTemplates map[string]map[string]*PolicyTemplate `json:"policyTemplates"`
	AccountID       string                                `json:"accountID"`
	Region          string                                `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		PolicyStores:    b.policyStores,
		Policies:        b.policies,
		PolicyTemplates: b.policyTemplates,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, _ := json.Marshal(snap)

	if data == nil {
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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.PolicyStores == nil {
		snap.PolicyStores = make(map[string]*PolicyStore)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]map[string]*Policy)
	}

	if snap.PolicyTemplates == nil {
		snap.PolicyTemplates = make(map[string]map[string]*PolicyTemplate)
	}

	b.policyStores = snap.PolicyStores
	b.policies = snap.Policies
	b.policyTemplates = snap.PolicyTemplates
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
