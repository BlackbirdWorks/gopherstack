package identitystore

import "encoding/json"

type backendSnapshot struct {
	Users       map[string]*User            `json:"users"`
	Groups      map[string]*Group           `json:"groups"`
	Memberships map[string]*GroupMembership `json:"memberships"`
	AccountID   string                      `json:"accountID"`
	Region      string                      `json:"region"`
	Counter     int                         `json:"counter"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Users:       b.users,
		Groups:      b.groups,
		Memberships: b.memberships,
		AccountID:   b.accountID,
		Region:      b.region,
		Counter:     b.counter,
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

	if snap.Users == nil {
		snap.Users = make(map[string]*User)
	}

	if snap.Groups == nil {
		snap.Groups = make(map[string]*Group)
	}

	if snap.Memberships == nil {
		snap.Memberships = make(map[string]*GroupMembership)
	}

	b.users = snap.Users
	b.groups = snap.Groups
	b.memberships = snap.Memberships
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.counter = snap.Counter
	b.rebuildIndexes()

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
