package ocimysql

import (
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	creationManual   = "MANUAL"
	creationAuto     = "AUTOMATIC"
	backupFull       = "FULL"
	backupIncr       = "INCREMENTAL"
	maxBackupRetain  = 365
	bytesPerGB       = 1 << 30
	backupMinSizeGBs = 1
)

func cloneSystem(s *DBSystem) *DBSystem {
	c := *s
	c.Maintenance, c.Extra = maps.Clone(s.Maintenance), maps.Clone(s.Extra)
	c.TransitionAt = nil

	return &c
}

func sizeGBs(n int) int { return max(backupMinSizeGBs, (n+bytesPerGB-1)/bytesPerGB) }

// newBackupRecord stores a backup of s. Without a dump (state-only mode, or a dump still being taken) it settles on
// the clock; with one it is ACTIVE at once.
func (b *InMemoryBackend) newBackupRecord(s *DBSystem, creation, name string, retention int, dump []byte) *Backup {
	now := b.nowUTC()
	bk := &Backup{
		ID:            newOCID("mysqlbackup"),
		CompartmentID: s.CompartmentID,
		DBSystemID:    s.ID,
		Name:          name,
		State:         stateCreating,
		BackupType:    backupFull,
		CreationType:  creation,
		SoftDelete:    disabled,
		Retention:     retention,
		SizeGB:        backupMinSizeGBs,
		Etag:          newEtag(),
		TimeCreated:   now,
		TimeUpdated:   now,
		Snapshot:      cloneSystem(s),
		Seq:           b.nextSeq(),
	}
	b.backups[bk.ID] = bk

	switch {
	case dump != nil:
		bk.State, bk.Dump, bk.SizeGB = stateActive, dump, sizeGBs(len(dump))
	case b.eng == nil:
		bk.TransitionAt = b.transitionAt()
	}

	return bk
}

func (b *InMemoryBackend) dropAutomaticBackups(sysID string) {
	for _, bk := range b.backups {
		if bk.DBSystemID == sysID && bk.CreationType == creationAuto && bk.State != stateDeleted {
			bk.State, bk.Dump, bk.TransitionAt = stateDeleted, nil, nil
		}
	}
}

// startBackup records a backup of s and, with the engine, dumps the server in the background.
func (b *InMemoryBackend) startBackup(
	s *DBSystem,
	creation, name string,
	retention int,
	op string,
) (*Backup, string, *APIError) {
	if b.eng != nil && b.eng.live[s.ID] == nil {
		return nil, "", errState("DB system " + s.ID + " has no running server to back up")
	}

	bk := b.newBackupRecord(s, creation, name, retention, nil)

	var wr string
	if op != "" {
		wr = b.newWR(op, bk.CompartmentID, entityBackup, bk.ID, actionCreated, bk.TransitionAt)
	}

	if b.eng != nil {
		b.spawnBackup(bk.ID, wr, s.ID)
	}

	return bk, wr, nil
}

func (b *InMemoryBackend) spawnBackup(bkID, wrID, sysID string) {
	ctx := b.runCtx

	b.wg.Go(func() {
		var (
			dump []byte
			err  error
		)

		if inst := b.instance(sysID); inst == nil {
			err = errNoServer
		} else {
			dump, err = inst.Dump(ctx)
		}

		b.mu.Lock("spawnBackup")
		defer b.mu.Unlock()

		bk, ok := b.backups[bkID]
		if !ok || ctx.Err() != nil {
			return
		}

		bk.TimeUpdated, bk.Etag = b.nowUTC(), newEtag()

		if err != nil {
			bk.State, bk.Details = stateFailed, "Backup failed: "+err.Error()
			b.finishWR(wrID, bk.Details)

			return
		}

		if dump == nil {
			dump = []byte{}
		}

		bk.State, bk.Dump, bk.SizeGB = stateActive, dump, sizeGBs(len(dump))
		b.finishWR(wrID, "")
	})
}

// autoBackups takes the daily backup of every ACTIVE DB system once its backup window has opened.
func (b *InMemoryBackend) autoBackups() {
	now := b.nowUTC()

	for _, s := range b.systems {
		if s.State != stateActive || !s.BackupPolicy.IsEnabled {
			continue
		}

		t, err := time.Parse("15:04", s.BackupPolicy.WindowStartTime)
		if err != nil {
			continue
		}

		window := time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC)
		if window.After(now) {
			window = window.AddDate(0, 0, -1)
		}

		if !s.LastAutoBackup.Before(window) {
			continue
		}

		s.LastAutoBackup = now
		_, _, _ = b.startBackup(
			s,
			creationAuto,
			"Automatic backup "+now.Format("2006-01-02"),
			s.BackupPolicy.RetentionInDays,
			"",
		)
	}
}

// expireBackups deletes backups older than their retention.
func (b *InMemoryBackend) expireBackups() {
	now := b.nowUTC()

	for _, bk := range b.backups {
		if bk.State != stateActive {
			continue
		}

		if bk.TimeCreated.Add(time.Duration(bk.Retention) * hoursPerDay * time.Hour).Before(now) {
			bk.State, bk.Dump = stateDeleted, nil
		}
	}
}

// CreateBackupIn is the CreateBackup body.
type CreateBackupIn struct {
	Tags
	Retention   *int   `json:"retentionInDays"`
	DBSystemID  string `json:"dbSystemId"`
	Name        string `json:"displayName"`
	Description string `json:"description"`
	SoftDelete  string `json:"softDelete"`
	BackupType  string `json:"backupType"`
}

// CreateBackup starts a manual backup of an ACTIVE DB system.
func (b *InMemoryBackend) CreateBackup(in CreateBackupIn) (Doc, string, string, error) {
	b.mu.Lock("CreateBackup")
	defer b.mu.Unlock()

	if in.DBSystemID == "" {
		return nil, "", "", errMissing("dbSystemId is required")
	}

	if aerr := firstErr(
		validEnum("backupType", in.BackupType, backupFull, backupIncr),
		validEnum("softDelete", in.SoftDelete, enabled, disabled),
		validRetention(in.Retention),
	); aerr != nil {
		return nil, "", "", aerr
	}

	b.settleID(in.DBSystemID)

	s, ok := b.systems[in.DBSystemID]
	if !ok || s.State == stateDeleted {
		return nil, "", "", errNotFound(in.DBSystemID)
	}

	if s.State != stateActive {
		return nil, "", "", errState("DB system " + s.ID + " is in state " + s.State + "; backups require ACTIVE")
	}

	name := orStr(in.Name, "MySQLBackup"+b.nowUTC().Format("20060102150405"))
	if aerr := validateName(name); aerr != nil {
		return nil, "", "", aerr
	}

	retention := s.BackupPolicy.RetentionInDays
	if in.Retention != nil {
		retention = *in.Retention
	}

	bk, wr, aerr := b.startBackup(s, creationManual, name, retention, wrCreateBackup)
	if aerr != nil {
		return nil, "", "", aerr
	}

	bk.Description, bk.Tags = in.Description, in.Tags
	setNonEmpty(&bk.BackupType, strings.ToUpper(in.BackupType))
	setNonEmpty(&bk.SoftDelete, strings.ToUpper(in.SoftDelete))

	return b.backupDoc(bk, true), bk.Etag, wr, nil
}

func validRetention(r *int) *APIError {
	if r != nil && (*r < minRetentionDays || *r > maxBackupRetain) {
		return errParam("retentionInDays must be between 1 and 365")
	}

	return nil
}

func (b *InMemoryBackend) backupDoc(bk *Backup, full bool) Doc {
	d := Doc{
		"id": bk.ID, keyCompartment: bk.CompartmentID, keyTimeCreated: bk.TimeCreated, keyLifecycle: bk.State,
		"backupType": bk.BackupType, "creationType": bk.CreationType, paramDBSystem: bk.DBSystemID,
		keyDisplayName: bk.Name, "softDelete": bk.SoftDelete, "backupSizeInGBs": bk.SizeGB,
		"retentionInDays": bk.Retention, "lifecycleDetails": bk.Details,
	}
	putTags(d, bk.Tags)

	if bk.Description != "" {
		d["description"] = bk.Description
	}

	if sn := bk.Snapshot; sn != nil {
		d["dataStorageSizeInGBs"], d["mysqlVersion"], d["shapeName"] = sn.DataGB, sn.Version, sn.Shape
	}

	if full {
		d[keyTimeUpdated] = bk.TimeUpdated

		if sn := bk.Snapshot; sn != nil {
			snap := b.systemDoc(sn)
			snap["adminUsername"], snap["region"] = sn.AdminUser, Region
			delete(snap, keyLifecycle)
			d["dbSystemSnapshot"] = snap
		}
	}

	return d
}

// GetBackup returns one backup.
func (b *InMemoryBackend) GetBackup(id string) (Doc, string, error) {
	b.mu.Lock("GetBackup")
	defer b.mu.Unlock()

	b.settleID(id)

	bk, ok := b.backups[id]
	if !ok {
		return nil, "", errNotFound(id)
	}

	return b.backupDoc(bk, true), bk.Etag, nil
}

// BackupListIn are the ListBackups filters.
type BackupListIn struct {
	Compartment  string
	ID           string
	DBSystemID   string
	Name         string
	State        string
	CreationType string
	ListOpts
}

func (in BackupListIn) matches(bk *Backup) bool {
	if bk.CompartmentID != in.Compartment || (in.ID != "" && bk.ID != in.ID) ||
		(in.DBSystemID != "" && bk.DBSystemID != in.DBSystemID) || (in.Name != "" && bk.Name != in.Name) ||
		(in.CreationType != "" && !strings.EqualFold(bk.CreationType, in.CreationType)) {
		return false
	}

	return stateMatches(bk.State, in.State)
}

// ListBackups lists the backups of a compartment.
func (b *InMemoryBackend) ListBackups(in BackupListIn) (page.Page[Doc], error) {
	if aerr := firstErr(
		b.checkCompartmentLocked(in.Compartment),
		validListOpts(in.ListOpts, keyDisplayName, keyTimeCreated, keyTimeUpdated),
		validEnum("creationType", in.CreationType, creationManual, creationAuto, "OPERATOR"),
	); aerr != nil {
		return page.Page[Doc]{}, aerr
	}

	b.mu.Lock("ListBackups")
	defer b.mu.Unlock()

	b.settleAll()

	var all []*Backup

	for _, bk := range b.backups {
		if in.matches(bk) {
			all = append(all, bk)
		}
	}

	pg := paginate(all, in.ListOpts, func(bk *Backup) sortKeys {
		return sortKeys{created: bk.TimeCreated, updated: bk.TimeUpdated, name: bk.Name, seq: bk.Seq}
	})

	out := make([]Doc, 0, len(pg.Data))
	for _, bk := range pg.Data {
		out = append(out, b.backupDoc(bk, false))
	}

	return page.Page[Doc]{Data: out, Next: pg.Next}, nil
}

// UpdateBackupIn is the UpdateBackup body.
type UpdateBackupIn struct {
	Tags
	Name        *string `json:"displayName"`
	Description *string `json:"description"`
	Retention   *int    `json:"retentionInDays"`
	SoftDelete  string  `json:"softDelete"`
}

// UpdateBackup updates a backup in place.
func (b *InMemoryBackend) UpdateBackup(id, ifMatch string, in UpdateBackupIn) (Doc, string, error) {
	b.mu.Lock("UpdateBackup")
	defer b.mu.Unlock()

	b.settleID(id)

	bk, ok := b.backups[id]
	if !ok || bk.State == stateDeleted {
		return nil, "", errNotFound(id)
	}

	if aerr := firstErr(checkEtag(bk.Etag, ifMatch), validEnum("softDelete", in.SoftDelete, enabled, disabled),
		validRetention(in.Retention)); aerr != nil {
		return nil, "", aerr
	}

	if bk.State != stateActive {
		return nil, "", errState("backup " + id + " is in state " + bk.State + "; updates require ACTIVE")
	}

	if in.Name != nil {
		if aerr := validateName(*in.Name); aerr != nil {
			return nil, "", aerr
		}

		bk.Name = *in.Name
	}

	if in.Description != nil {
		bk.Description = *in.Description
	}

	if in.Retention != nil {
		bk.Retention = *in.Retention
	}

	setNonEmpty(&bk.SoftDelete, strings.ToUpper(in.SoftDelete))
	bk.Tags.merge(in.Tags)
	bk.TimeUpdated, bk.Etag = b.nowUTC(), newEtag()

	return b.backupDoc(bk, true), bk.Etag, nil
}

// DeleteBackup deletes a backup and returns its work request.
func (b *InMemoryBackend) DeleteBackup(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteBackup")
	defer b.mu.Unlock()

	b.settleID(id)

	bk, ok := b.backups[id]
	if !ok || bk.State == stateDeleted {
		return "", errNotFound(id)
	}

	if aerr := checkEtag(bk.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if bk.State != stateActive && bk.State != stateFailed {
		return "", errState("backup " + id + " is in state " + bk.State + "; deletion requires ACTIVE or FAILED")
	}

	bk.State, bk.TimeUpdated, bk.Etag, bk.TransitionAt = stateDeleting, b.nowUTC(), newEtag(), b.transitionAt()

	return b.newWR(wrDeleteBackup, bk.CompartmentID, entityBackup, id, actionDeleted, bk.TransitionAt), nil
}

// ChangeBackupCompartment moves a backup to another compartment.
func (b *InMemoryBackend) ChangeBackupCompartment(id, ifMatch, compartment string) error {
	b.mu.Lock("ChangeBackupCompartment")
	defer b.mu.Unlock()

	b.settleID(id)

	bk, ok := b.backups[id]
	if !ok || bk.State == stateDeleted {
		return errNotFound(id)
	}

	if aerr := firstErr(checkEtag(bk.Etag, ifMatch), b.checkCompartment(compartment)); aerr != nil {
		return aerr
	}

	bk.CompartmentID, bk.TimeUpdated, bk.Etag = compartment, b.nowUTC(), newEtag()

	return nil
}

func (h *Handler) createBackup(q *req) error {
	var in CreateBackupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	d, etag, wr, err := h.Backend.CreateBackup(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}

func (h *Handler) getBackup(q *req) error {
	d, etag, err := h.Backend.GetBackup(q.param("backupId"))

	return reply(q, d, etag, err)
}

func (h *Handler) listBackups(q *req) error {
	pg, err := h.Backend.ListBackups(BackupListIn{
		Compartment: q.query(keyCompartment), ID: q.query("backupId"), DBSystemID: q.query(paramDBSystem),
		Name: q.query(keyDisplayName), State: q.query(keyLifecycle), CreationType: q.query("creationType"),
		ListOpts: q.listOpts(),
	})

	return collectionReply(q, pg, err)
}

func (h *Handler) updateBackup(q *req) error {
	var in UpdateBackupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	d, etag, err := h.Backend.UpdateBackup(q.param("backupId"), q.ifMatch(), in)

	return reply(q, d, etag, err)
}

func (h *Handler) deleteBackup(q *req) error {
	wr, err := h.Backend.DeleteBackup(q.param("backupId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeBackupCompartment(q *req) error {
	var in struct {
		CompartmentID string `json:"compartmentId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.ChangeBackupCompartment(q.param("backupId"), q.ifMatch(), in.CompartmentID); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusOK)

	return nil
}
