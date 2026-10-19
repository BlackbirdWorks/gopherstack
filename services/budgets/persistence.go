package budgets

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "budgets: snapshot marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{Version: snapshotVersion, Tables: tables, AccountID: b.accountID, Region: b.region}

	return persistence.MarshalSnapshot(ctx, serviceName, &snap)
}

// Restore loads a snapshot; an incompatible version is discarded.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, serviceName, data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "budgets: discarding incompatible snapshot",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("budgets: restore snapshot tables: %w", err)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.actions.Range(func(r *ActionRecord) bool {
		r.Action.Status = settleInterrupted(r.Action.Status)

		return true
	})

	return nil
}

// settleInterrupted turns an in-flight status into the matching failure: the run died with the process.
func settleInterrupted(status string) string {
	switch status {
	case statusExecInProgress:
		return statusExecFailure
	case statusReverseInProgress:
		return statusReverseFailure
	case statusResetInProgress:
		return statusResetFailure
	default:
		return status
	}
}

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
