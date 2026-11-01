package ocidatabase

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	longTermMinDays = 90
	hoursPerDay     = 24
)

type backupWire struct {
	TimeStarted          time.Time  `json:"timeStarted"`
	TimeEnded            *time.Time `json:"timeEnded,omitempty"`
	TimeAvailableTill    *time.Time `json:"timeAvailableTill,omitempty"`
	ID                   string     `json:"id"`
	CompartmentID        string     `json:"compartmentId"`
	AutonomousDatabaseID string     `json:"autonomousDatabaseId"`
	DisplayName          string     `json:"displayName"`
	Type                 string     `json:"type"`
	LifecycleState       string     `json:"lifecycleState"`
	LifecycleDetails     string     `json:"lifecycleDetails,omitempty"`
	DBVersion            string     `json:"dbVersion"`
	RetentionDays        int        `json:"retentionPeriodInDays,omitempty"`
	IsAutomatic          bool       `json:"isAutomatic"`
	IsRestorable         bool       `json:"isRestorable"`
}

func (b *InMemoryBackend) backupView(bk *Backup) backupWire {
	return backupWire{
		ID: bk.ID, CompartmentID: bk.CompartmentID, AutonomousDatabaseID: bk.DBID, DisplayName: bk.DisplayName,
		Type: bk.Type, LifecycleState: bk.State, LifecycleDetails: bk.Details, DBVersion: bk.DBVersion,
		TimeStarted: bk.Started, TimeEnded: bk.Ended, TimeAvailableTill: bk.AvailableTill,
		RetentionDays: bk.RetentionDays, IsAutomatic: bk.IsAutomatic, IsRestorable: bk.State == backupActive,
	}
}

// addAutomaticBackup records the first automatic backup of a database. Callers hold the write lock.
func (b *InMemoryBackend) addAutomaticBackup(db *ADB) {
	now := b.nowUTC()
	till := now.Add(time.Duration(db.BackupDays) * hoursPerDay * time.Hour)

	bk := &Backup{
		ID: newOCID("autonomousdatabasebackup"), CompartmentID: db.CompartmentID, DBID: db.ID,
		DisplayName: "Automatic Incremental Backup", Type: backupIncremental, State: backupActive,
		Started: now, Ended: &now, AvailableTill: &till, DBVersion: db.DBVersion, RetentionDays: db.BackupDays,
		IsAutomatic: true, Seq: b.nextSeq(),
	}
	b.backups[bk.ID] = bk
}

// CreateBackup starts a manual backup. Only the lifecycle is emulated: the backup holds no data.
func (b *InMemoryBackend) CreateBackup(in backupIn) (backupWire, string, string, error) {
	if in.DBID == nil || *in.DBID == "" {
		return backupWire{}, "", "", errMissing("autonomousDatabaseId is required")
	}

	long := deref(in.LongTerm, false)
	if long && (in.RetentionDays == nil || *in.RetentionDays < longTermMinDays) {
		return backupWire{}, "", "", errParam("retentionPeriodInDays of at least 90 is required for a long-term backup")
	}

	b.mu.Lock("CreateBackup")
	defer b.mu.Unlock()

	db, aerr := b.guard(*in.DBID, "", stateAvailable)
	if aerr != nil {
		return backupWire{}, "", "", aerr
	}

	bk := b.newBackup(db, in, long)

	wr := b.begin(db, step{
		typ: "CREATE_AUTONOMOUS_DATABASE_BACKUP", action: "CREATED", state: stateBackingUp, target: stateAvailable,
	})
	wr.ResourceID, wr.ResourceType = bk.ID, "autonomousdatabasebackup"
	bk.TransitionAt = db.TransitionAt

	if bk.TransitionAt == nil {
		now := b.nowUTC()
		bk.State, bk.Ended = backupActive, &now
	}

	return b.backupView(bk), wr.ID, backupEtag(bk), nil
}

func (b *InMemoryBackend) newBackup(db *ADB, in backupIn, long bool) *Backup {
	typ, days := backupIncremental, db.BackupDays
	name := "Manual backup " + b.nowUTC().Format("20060102T150405Z")

	if long {
		typ, days = backupLongTerm, *in.RetentionDays
	}

	if in.RetentionDays != nil && !long {
		days = *in.RetentionDays
	}

	if in.DisplayName != nil && *in.DisplayName != "" {
		name = *in.DisplayName
	}

	now := b.nowUTC()
	till := now.Add(time.Duration(days) * hoursPerDay * time.Hour)
	bk := &Backup{
		ID: newOCID("autonomousdatabasebackup"), CompartmentID: db.CompartmentID, DBID: db.ID, DisplayName: name,
		Type: typ, State: backupCreating, Started: now, AvailableTill: &till, DBVersion: db.DBVersion,
		RetentionDays: days, Seq: b.nextSeq(),
	}
	b.backups[bk.ID] = bk

	return bk
}

// GetBackup returns one backup.
func (b *InMemoryBackend) GetBackup(id string) (backupWire, error) {
	b.mu.RLock("GetBackup")
	defer b.mu.RUnlock()

	bk, ok := b.backups[id]
	if !ok {
		return backupWire{}, errNotFound(id)
	}

	return b.backupView(bk), nil
}

// BackupFilter selects backups in a list.
type BackupFilter struct {
	DBID        string
	Compartment string
	State       string
	DisplayName string
	Type        string
}

// ListBackups lists backups by database or compartment.
func (b *InMemoryBackend) ListBackups(f BackupFilter, o ListOpts) (page.Page[backupWire], error) {
	if f.DBID == "" && f.Compartment == "" {
		return page.Page[backupWire]{}, errMissing("autonomousDatabaseId or compartmentId is required")
	}

	b.mu.RLock("ListBackups")
	defer b.mu.RUnlock()

	if f.Compartment != "" {
		if aerr := b.checkCompartment(f.Compartment); aerr != nil {
			return page.Page[backupWire]{}, aerr
		}
	}

	var all []*Backup

	for _, bk := range b.backups {
		if bk.State != backupDeleted && f.matches(bk) {
			all = append(all, bk)
		}
	}

	pg := paginate(all, o,
		func(x *Backup) string { return x.DisplayName },
		func(x *Backup) (time.Time, int64) { return x.Started, x.Seq },
	)

	out := make([]backupWire, 0, len(pg.Data))
	for _, bk := range pg.Data {
		out = append(out, b.backupView(bk))
	}

	return page.Page[backupWire]{Data: out, Next: pg.Next}, nil
}

func (f BackupFilter) matches(bk *Backup) bool {
	switch {
	case f.DBID != "" && bk.DBID != f.DBID:
		return false
	case f.Compartment != "" && bk.CompartmentID != f.Compartment:
		return false
	case f.State != "" && bk.State != f.State:
		return false
	case f.DisplayName != "" && !strs.Equal(bk.DisplayName, f.DisplayName):
		return false
	case f.Type != "" && bk.Type != f.Type:
		return false
	}

	return true
}

// DeleteBackup deletes a manual backup.
func (b *InMemoryBackend) DeleteBackup(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteBackup")
	defer b.mu.Unlock()

	bk, ok := b.backups[id]
	if !ok || bk.State == backupDeleted {
		return "", errNotFound(id)
	}

	if bk.IsAutomatic {
		return "", errState("Automatic backups cannot be deleted; they expire with their retention period")
	}

	if bk.State != backupActive {
		return "", errState("The backup " + id + " is in state " + bk.State + " and cannot be deleted")
	}

	if ifMatch != "" && ifMatch != backupEtag(bk) {
		return "", errEtag()
	}

	b.dropBackup(bk)

	wr := b.newWR(bk.CompartmentID, bk.ID, "autonomousdatabasebackup", "DELETE_AUTONOMOUS_DATABASE_BACKUP", "DELETED")
	wr.FinishAt = bk.TransitionAt

	return wr.ID, nil
}

func backupEtag(bk *Backup) string { return bk.ID }
