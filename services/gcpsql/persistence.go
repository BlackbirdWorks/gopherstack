package gcpsql

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to instRec or Operation would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Instances map[string]*instRec    `json:"instances"`
	Backups   map[string]*projBackup `json:"projectBackups,omitempty"`
	Ops       map[string]*Operation  `json:"ops"`
	Seq       int64                  `json:"seq"`
	Version   int                    `json:"version"`
}

// Snapshot serialises the backend state. Engine data is not part of it. Backup dumps are immutable once stored, so
// they are encoded after the backend lock is released.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")

	instances, ierr := json.Marshal(b.instances, jsonx.V1())
	ops, oerr := json.Marshal(b.ops, jsonx.V1())
	seq := b.seq
	backups := make(map[string]projBackup, len(b.projBackups))

	for k, p := range b.projBackups {
		backups[k] = *p
	}

	hook := b.snapshotHook
	b.mu.RUnlock()

	if hook != nil {
		hook()
	}

	rawBackups, berr := json.Marshal(backups, jsonx.V1())
	if err := errors.Join(ierr, oerr, berr); err != nil {
		logger.Load(ctx).WarnContext(ctx, "gcpsql: snapshot marshal failed", "error", err)

		return nil
	}

	return persistence.MarshalSnapshot(ctx, "gcpsql", encodedSnapshot{
		Instances: instances, Backups: rawBackups, Ops: ops, Seq: seq, Version: snapshotVersion,
	})
}

type encodedSnapshot struct {
	Instances jsontext.Value `json:"instances"`
	Backups   jsontext.Value `json:"projectBackups,omitempty"`
	Ops       jsontext.Value `json:"ops"`
	Seq       int64          `json:"seq"`
	Version   int            `json:"version"`
}

// Restore loads backend state from a snapshot and relaunches the engines of its instances.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpsql", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpsql: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		return nil
	}

	for key, rec := range snap.Instances {
		if rec == nil {
			return fmt.Errorf("%w: instance %q", ErrSnapshotNull, key)
		}

		normalizeRec(rec)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.instances, b.ops, b.seq, b.projBackups = snap.Instances, snap.Ops, snap.Seq, snap.Backups

	if b.projBackups == nil {
		b.projBackups = map[string]*projBackup{}
	}

	b.failRunningBackupsLocked()

	if b.ops == nil {
		b.ops = map[string]*Operation{}
	}

	b.interruptOpsLocked()
	b.relaunchLocked()

	return nil
}

func (b *InMemoryBackend) failRunningBackupsLocked() {
	for k, p := range b.projBackups {
		switch {
		case p == nil:
			delete(b.projBackups, k)
		case p.State == backupStateRunning:
			p.State, p.EndTime, p.Dump, p.Sum = backupStateFailed, stamp(b.now()), nil, ""
			p.Error = &OperationError{Kind: errKind, Code: opErrorDefault, Message: errInterrupted.Error()}
		}
	}
}

func normalizeRec(rec *instRec) {
	if rec.Databases == nil {
		rec.Databases = map[string]*Database{}
	}

	if rec.Users == nil {
		rec.Users = map[string]*userRec{}
	}

	if rec.Backups == nil {
		rec.Backups = map[string]*backupRec{}
	}

	if rec.Certs == nil {
		rec.Certs = map[string]*certRec{}
	}

	for name, d := range rec.Databases {
		d.System = isSystemDatabase(rec.Inst.DatabaseVersion, name)
	}
}

func isSystemDatabase(version, name string) bool {
	if isPG(version) {
		return name == pgAdminUser
	}

	return slices.Contains(mysqlSystemDBs(), name)
}

func (b *InMemoryBackend) interruptOpsLocked() {
	for _, op := range b.ops {
		if op.Status != opDone {
			op.Status, op.EndTime = opDone, stamp(b.now())
			op.Error = opError(errInterrupted)
		}
	}
}

// relaunchLocked starts a fresh engine for every restored instance; engine data is not persisted.
func (b *InMemoryBackend) relaunchLocked() {
	for key, rec := range b.instances {
		if rec.Inst.State == stateDeleting {
			delete(b.instances, key)

			continue
		}

		rec.Inst.State, rec.Inst.IPAddresses, rec.Inst.GopherstackEndpoint = statePending, nil, ""

		b.enqueueLocked(rec, task{fn: func(ctx context.Context) error { return b.bringUp(ctx, rec) }})
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpsql: restore snapshot: %w", err)
	}

	return nil
}
