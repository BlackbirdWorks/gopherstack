package wafv2

import "encoding/json"

type backendSnapshot struct {
	WebACLs      map[string]*WebACL `json:"webACLs"`
	IPSets       map[string]*IPSet  `json:"ipSets"`
	Associations map[string]string  `json:"associations,omitempty"`
	AccountID    string             `json:"accountID"`
	Region       string             `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		WebACLs:      b.webACLs,
		IPSets:       b.ipSets,
		Associations: b.associations,
		AccountID:    b.accountID,
		Region:       b.region,
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

	if snap.WebACLs == nil {
		snap.WebACLs = make(map[string]*WebACL)
	}

	if snap.IPSets == nil {
		snap.IPSets = make(map[string]*IPSet)
	}

	if snap.Associations == nil {
		snap.Associations = make(map[string]string)
	}

	b.webACLs = snap.WebACLs
	b.ipSets = snap.IPSets
	b.associations = snap.Associations
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.webACLByARN = make(map[string]string, len(snap.WebACLs))
	b.ipSetByARN = make(map[string]string, len(snap.IPSets))
	b.webACLByNameScope = make(map[string]string, len(snap.WebACLs))
	b.ipSetByNameScope = make(map[string]string, len(snap.IPSets))

	for _, w := range snap.WebACLs {
		b.webACLByARN[b.WebACLARN(w.Name, w.ID, w.Scope)] = w.ID
		b.webACLByNameScope[nameScope(w.Name, w.Scope)] = w.ID
	}

	for _, s := range snap.IPSets {
		b.ipSetByARN[b.IPSetARN(s.Name, s.ID, s.Scope)] = s.ID
		b.ipSetByNameScope[nameScope(s.Name, s.Scope)] = s.ID
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
