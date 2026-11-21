package gcpbigquery

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to Dataset, Table or Job would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Datasets     map[string]*Dataset `json:"datasets"`
	Jobs         map[string]*Job     `json:"jobs"`
	Reservations *reservationState   `json:"reservations,omitempty"`
	Seq          int64               `json:"seq"`
	Version      int                 `json:"version"`
}

// Snapshot serialises the backend state.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "gcpbigquery",
		backendSnapshot{Version: snapshotVersion, Datasets: b.datasets, Jobs: b.jobs, Seq: b.seq, Reservations: b.res})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpbigquery", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpbigquery: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.datasets, b.jobs, b.seq, b.res = map[string]*Dataset{}, map[string]*Job{}, 0, newReservationState()

		return nil
	}

	for key, d := range snap.Datasets {
		if d == nil {
			return fmt.Errorf("%w: dataset %q", ErrSnapshotNull, key)
		}

		normalizeDataset(d)
	}

	if snap.Datasets == nil {
		snap.Datasets = map[string]*Dataset{}
	}

	if snap.Jobs == nil {
		snap.Jobs = map[string]*Job{}
	}

	if snap.Reservations == nil {
		snap.Reservations = newReservationState()
	}

	snap.Reservations.normalize()
	b.datasets, b.jobs, b.seq, b.res = snap.Datasets, snap.Jobs, snap.Seq, snap.Reservations
	b.markInterrupted()

	return nil
}

func normalizeDataset(d *Dataset) {
	if d.Meta == nil {
		d.Meta = map[string]any{}
	}

	if d.Tables == nil {
		d.Tables = map[string]*Table{}
	}

	if d.Routines == nil {
		d.Routines = map[string]*Routine{}
	}

	for id, r := range d.Routines {
		if r == nil || r.Resource == nil {
			delete(d.Routines, id)
		}
	}

	for id, t := range d.Tables {
		if t == nil {
			delete(d.Tables, id)

			continue
		}

		if t.Meta == nil {
			t.Meta = map[string]any{}
		}

		if t.Rows == nil {
			t.Rows = [][]any{}
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpbigquery: restore snapshot: %w", err)
	}

	return nil
}
