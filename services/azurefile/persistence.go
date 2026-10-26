package azurefile

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const azureFileSnapshotVersion = 1

type backendSnapshot struct {
	Accounts map[string]*account `json:"accounts"`
	Version  int                 `json:"version"`
}

// Snapshot serialises the backend state. It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "azurefile", backendSnapshot{
		Version: azureFileSnapshotVersion, Accounts: b.accounts,
	})
}

// Restore loads backend state from a snapshot. It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azurefile", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != azureFileSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azurefile: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", azureFileSnapshotVersion)

		b.accounts = make(map[string]*account)

		return nil
	}

	if snap.Accounts == nil {
		snap.Accounts = make(map[string]*account)
	}

	for name, a := range snap.Accounts {
		if err := a.validate(); err != nil {
			return fmt.Errorf("%w: account %q: %w", ErrSnapshotNull, name, err)
		}
	}

	b.accounts = snap.Accounts

	return nil
}

func (a *account) validate() error {
	if a == nil {
		return errNullEntry("account")
	}

	if a.Shares == nil {
		a.Shares = make(map[string]*share)
	}

	for n, s := range a.Shares {
		if s == nil || s.Root == nil {
			return errNullEntry("share " + n)
		}

		for id, sn := range s.Snapshots {
			if sn == nil || sn.Root == nil {
				return errNullEntry("snapshot " + id)
			}
		}
	}

	for _, d := range a.Deleted {
		if d == nil || d.Root == nil {
			return errNullEntry("deleted share")
		}
	}

	return nil
}

type nullEntryError string

func (e nullEntryError) Error() string { return "null " + string(e) }

func errNullEntry(what string) error { return nullEntryError(what) }

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("azurefile: restore snapshot: %w", err)
	}

	return nil
}
