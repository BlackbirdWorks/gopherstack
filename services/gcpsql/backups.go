package gcpsql

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strconv"
)

// RestoreBackupInput is the restoreBackupContext of instances.restoreBackup.
type RestoreBackupInput struct {
	BackupRunID json.Number `json:"backupRunId"`
	InstanceID  string      `json:"instanceId"`
	Project     string      `json:"project"`
}

func (b *InMemoryBackend) nextBackupID() string {
	b.seq++

	return strconv.FormatInt(b.now().UnixMilli()*1000+b.seq%1000, 10)
}

func newBackupRun(rec *instRec, id, description string, now string) *backupRec {
	in := &rec.Inst

	return &backupRec{Run: BackupRun{
		Kind: kindBackupRun, ID: id, Status: "RUNNING", Type: "ON_DEMAND", BackupKind: "SNAPSHOT",
		Description: description, EnqueuedTime: now, StartTime: now, WindowStartTime: now, Instance: in.Name,
		SelfLink: instanceLink(in.Project, in.Name) + "/backupRuns/" + id, Location: "us",
		DatabaseVersion: in.DatabaseVersion, TimeZone: "UTC",
	}}
}

// InsertBackupRun takes an on-demand logical backup of every user database.
func (b *InMemoryBackend) InsertBackupRun(project, instance, description string) (*Operation, error) {
	b.mu.Lock("InsertBackupRun")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	if rec.Inst.State == stateSuspended {
		return nil, errNotRunnable()
	}

	id := b.nextBackupID()
	run := newBackupRun(rec, id, description, stamp(b.now()))
	rec.Backups[id] = run

	meta := opMeta{backup: map[string]any{"backupId": id, keyKind: "sql#backupContext"}}

	return b.submitLocked(rec, project, "BACKUP_VOLUME", meta, func(ctx context.Context) error {
		dump, derr := b.dumpAll(ctx, rec)

		b.mu.Lock("InsertBackupRun.task")
		defer b.mu.Unlock()

		run.Run.EndTime = stamp(b.now())

		if derr != nil {
			run.Run.Status, run.Run.Error = "FAILED", &OperationError{
				Kind:    errKind,
				Code:    opErrorDefault,
				Message: derr.Error(),
			}

			return derr
		}

		run.Run.Status, run.Dump = "SUCCESSFUL", dump

		return nil
	}), nil
}

// dumpAll dumps every user database of rec; it is empty without an engine.
func (b *InMemoryBackend) dumpAll(ctx context.Context, rec *instRec) ([]byte, error) {
	eng := b.engineOf(rec)
	if eng == nil {
		return nil, nil
	}

	return eng.Dump(ctx, b.exportableDBs(rec, nil), DumpOptions{Markers: true})
}

func (b *InMemoryBackend) exportableDBs(rec *instRec, only []string) []string {
	b.mu.RLock("exportableDBs")
	defer b.mu.RUnlock()

	var out []string

	for name, d := range rec.Databases {
		if d.System && !isPG(rec.Inst.DatabaseVersion) {
			continue
		}

		if len(only) == 0 || contains(only, name) {
			out = append(out, name)
		}
	}

	sort.Strings(out)

	return out
}

func contains(list []string, v string) bool {
	return slices.Contains(list, v)
}

// GetBackupRun returns a backup run.
func (b *InMemoryBackend) GetBackupRun(project, instance, id string) (*BackupRun, error) {
	b.mu.RLock("GetBackupRun")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	r, ok := rec.Backups[id]
	if !ok {
		return nil, errNotFound("The backup run does not exist.")
	}

	cp := r.Run

	return &cp, nil
}

// ListBackupRuns lists backup runs, newest first.
func (b *InMemoryBackend) ListBackupRuns(project, instance string) ([]*BackupRun, error) {
	b.mu.RLock("ListBackupRuns")
	defer b.mu.RUnlock()

	rec, err := b.lookupLocked(project, instance)
	if err != nil {
		return nil, err
	}

	out := make([]*BackupRun, 0, len(rec.Backups))

	for _, r := range rec.Backups {
		cp := r.Run
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })

	return out, nil
}

// DeleteBackupRun removes a backup run.
func (b *InMemoryBackend) DeleteBackupRun(project, instance, id string) (*Operation, error) {
	b.mu.Lock("DeleteBackupRun")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	if _, ok := rec.Backups[id]; !ok {
		return nil, errNotFound("The backup run does not exist.")
	}

	delete(rec.Backups, id)

	return b.submitLocked(rec, project, "DELETE_BACKUP", opMeta{}, func(context.Context) error { return nil }), nil
}

// RestoreBackup replaces the databases of the target instance with a backup of the same engine family.
func (b *InMemoryBackend) RestoreBackup(project, instance string, in RestoreBackupInput) (*Operation, error) {
	b.mu.Lock("RestoreBackup")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	if in.Project != "" && in.Project != project {
		return nil, errInvalid("Restoring a backup from another project is not supported by the emulator.")
	}

	src, err := b.lookupLocked(project, orDefault(in.InstanceID, instance))
	if err != nil {
		return nil, err
	}

	run, ok := src.Backups[in.BackupRunID.String()]
	if !ok || run.Run.Status != "SUCCESSFUL" {
		return nil, errNotFound("The backup run does not exist.")
	}

	if isPG(src.Inst.DatabaseVersion) != isPG(rec.Inst.DatabaseVersion) {
		return nil, errInvalid("A backup can only be restored into an instance of the same database engine.")
	}

	dump := run.Dump

	return b.submitLocked(rec, project, "RESTORE_VOLUME", opMeta{}, func(ctx context.Context) error {
		eng := b.engineOf(rec)
		if eng == nil || len(dump) == 0 {
			return nil
		}

		if rerr := eng.Restore(ctx, "", dump); rerr != nil {
			return rerr
		}

		return b.reconcile(ctx, rec, eng)
	}), nil
}

// reconcile adds records for databases a restore or import created.
func (b *InMemoryBackend) reconcile(ctx context.Context, rec *instRec, eng Engine) error {
	names, err := eng.ListDatabases(ctx)
	if err != nil {
		return err
	}

	b.mu.Lock("reconcile")
	defer b.mu.Unlock()

	for _, n := range names {
		if _, ok := rec.Databases[n]; !ok && !rec.gone {
			rec.Databases[n] = newDatabase(rec, n, "", "", false)
		}
	}

	return nil
}
