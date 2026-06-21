package identitystore

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Users       map[string]map[string]*User            `json:"users"`
	Groups      map[string]map[string]*Group           `json:"groups"`
	Memberships map[string]map[string]*GroupMembership `json:"memberships"`
	AccountID   string                                 `json:"accountID"`
	Region      string                                 `json:"region"`
	Counter     int                                    `json:"counter"`
}

func (s *backendSnapshot) ensureNonNil() {
	if s.Users == nil {
		s.Users = make(map[string]map[string]*User)
	}

	if s.Groups == nil {
		s.Groups = make(map[string]map[string]*Group)
	}

	if s.Memberships == nil {
		s.Memberships = make(map[string]map[string]*GroupMembership)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
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

	return persistence.MarshalSnapshot(ctx, "identitystore", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "identitystore", data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

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
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
