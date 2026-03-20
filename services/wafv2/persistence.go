package wafv2

import "encoding/json"

type backendSnapshot struct {
	WebACLs   map[string]*WebACL `json:"webACLs"`
	IPSets    map[string]*IPSet  `json:"ipSets"`
	AccountID string             `json:"accountID"`
	Region    string             `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		WebACLs:   b.webACLs,
		IPSets:    b.ipSets,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, _ := json.Marshal(snap)

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

	if snap.WebACLs == nil {
		snap.WebACLs = make(map[string]*WebACL)
	}

	if snap.IPSets == nil {
		snap.IPSets = make(map[string]*IPSet)
	}

	b.webACLs = snap.WebACLs
	b.ipSets = snap.IPSets
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.webACLByARN = make(map[string]string, len(snap.WebACLs))
	b.ipSetByARN = make(map[string]string, len(snap.IPSets))

	for _, w := range snap.WebACLs {
		b.webACLByARN[b.WebACLARN(w.Name, w.ID, w.Scope)] = w.ID
	}

	for _, s := range snap.IPSets {
		b.ipSetByARN[b.IPSetARN(s.Name, s.ID, s.Scope)] = s.ID
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
