package ocimysql

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
)

const (
	wrCopyBackup      = "COPY_BACKUP"
	validNot          = "NOT_VALIDATED"
	validOK           = "VALIDATED"
	validRetry        = "NEEDS_ATTENTION"
	validFailed       = "FAILED"
	preparedYes       = "PREPARED"
	preparedNo        = "NOT_PREPARED"
	maxAutoBackupDays = 35
)

// BackupValidation is the outcome of the last ValidateBackup.
type BackupValidation struct {
	TimeLast   *time.Time `json:"timeLastValidated,omitempty"`
	PreparedAt *time.Time `json:"timePrepared,omitempty"`
	Status     string     `json:"validationStatus"`
	Duration   string     `json:"estimatedRestoreDuration,omitempty"`
	Error      string     `json:"errorMessage,omitempty"`
	Prepared   bool       `json:"prepared"`
}

func (bk *Backup) validationDoc() Doc {
	v := bk.Validation
	if v == nil {
		return Doc{"validationStatus": validNot, "backupPreparationStatus": preparedNo}
	}

	d := Doc{"validationStatus": v.Status, "backupPreparationStatus": preparedNo}

	if v.TimeLast != nil {
		d["timeLastValidated"] = *v.TimeLast
	}

	if v.Duration != "" {
		d["estimatedRestoreDuration"] = v.Duration
	}

	if v.Error != "" {
		d["errorMessage"] = v.Error
	}

	if v.Prepared {
		d["backupPreparationStatus"] = preparedYes
		d["preparedBackupDetails"] = Doc{"timePrepared": v.PreparedAt}
	}

	return d
}

// ValidateBackupIn is the ValidateBackup body.
type ValidateBackupIn struct {
	IsPreparedBackupRequired *bool `json:"isPreparedBackupRequired"`
}

// ValidateBackup restores a backup onto a throwaway server to check that it can be restored.
func (b *InMemoryBackend) ValidateBackup(id, ifMatch, token string, in ValidateBackupIn) (Doc, string, string, error) {
	b.mu.Lock("ValidateBackup")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("ValidateBackup/"+id, token, b.nowUTC()); ok {
		bk := b.backups[id]

		return b.backupDoc(bk, true), bk.Etag, v, nil
	}

	b.settleID(id)

	bk, ok := b.backups[id]
	if !ok || bk.State == stateDeleted {
		return nil, "", "", errNotFound(id)
	}

	if in.IsPreparedBackupRequired == nil {
		return nil, "", "", errMissing("isPreparedBackupRequired is required")
	}

	if aerr := checkEtag(bk.Etag, ifMatch); aerr != nil {
		return nil, "", "", aerr
	}

	if bk.State != stateActive {
		return nil, "", "", errState("backup " + id + " is in state " + bk.State + "; validation requires ACTIVE")
	}

	now := b.nowUTC()
	bk.Etag, bk.TimeUpdated = newEtag(), now
	wr := b.newWR(wrUpdateBackup, bk.CompartmentID, entityBackup, id, actionUpdated, nil)
	b.retry.Remember("ValidateBackup/"+id, token, wr, now)

	prepare := *in.IsPreparedBackupRequired

	if b.eng == nil || bk.Dump == nil {
		reason := "the backup holds no data: it was taken without a database engine"
		if b.eng == nil {
			reason = "no database engine is available to restore the backup"
		}

		b.settleValidation(bk, BackupValidation{Status: validRetry, Error: reason}, false, wr)

		return b.backupDoc(bk, true), bk.Etag, wr, nil
	}

	dump, image := bk.Dump, b.eng.imageFor(bk.snapshotVersion())
	ctx := b.runCtx
	doc := b.backupDoc(bk, true)
	etag := bk.Etag

	b.wg.Go(func() {
		res := restoreCheck(ctx, b.eng.launcher, image, dump)

		b.mu.Lock("ValidateBackup")
		defer b.mu.Unlock()

		if cur, found := b.backups[id]; found && ctx.Err() == nil {
			b.settleValidation(cur, res, prepare, wr)
		}
	})

	return doc, etag, wr, nil
}

func (bk *Backup) snapshotVersion() string {
	if bk.Snapshot != nil {
		return bk.Snapshot.Version
	}

	return defaultVersion
}

func (b *InMemoryBackend) settleValidation(bk *Backup, res BackupValidation, prepare bool, wr string) {
	now := b.nowUTC()
	res.TimeLast = &now

	if prepare && res.Status == validOK {
		res.Prepared, res.PreparedAt = true, &now
		bk.BackupType = backupFull

		logger.Load(b.runCtx).InfoContext(b.runCtx, "ocimysql: backup would be prepared for fast restore",
			"backup", bk.ID)
	}

	bk.Validation, bk.TimeUpdated, bk.Etag = &res, now, newEtag()
	b.finishWR(wr, "")
}

// restoreCheck loads dump into a new server and removes it again.
func restoreCheck(ctx context.Context, l Launcher, image string, dump []byte) BackupValidation {
	if len(dump) == 0 {
		return BackupValidation{Status: validOK, Duration: "PT0S"}
	}

	id := newOCID("mysqlvalidation")
	started := time.Now()

	inst, err := l.Launch(ctx, LaunchSpec{ID: id, Image: image, RootSecret: newSecret(), Fresh: true})
	if err != nil {
		return BackupValidation{Status: validRetry, Error: "the validation server did not start: " + err.Error()}
	}

	defer inst.Close(context.WithoutCancel(ctx), true)

	if err = inst.Restore(ctx, dump); err != nil {
		return BackupValidation{Status: validFailed, Error: "the backup cannot be restored: " + err.Error()}
	}

	return BackupValidation{
		Status: validOK, Duration: "PT" + strconv.FormatInt(int64(time.Since(started).Seconds()), 10) + "S",
	}
}

// CopyBackupIn is the CopyBackup body.
type CopyBackupIn struct {
	RetryToken     string `json:"-"`
	IfMatch        string `json:"-"`
	Retention      *int   `json:"backupCopyRetentionInDays"`
	CompartmentID  string `json:"compartmentId"`
	SourceBackupID string `json:"sourceBackupId"`
	SourceRegion   string `json:"sourceRegion"`
	DisplayName    string `json:"displayName"`
	Description    string `json:"description"`
}

// CopyBackup creates a copy of a backup held in another region.
func (b *InMemoryBackend) CopyBackup(in CopyBackupIn) (Doc, string, string, error) {
	b.mu.Lock("CopyBackup")
	defer b.mu.Unlock()

	id, rwr, hit, rerr := b.replayCreate("CopyBackup", in.RetryToken, func(id string) (string, bool) {
		bk, ok := b.backups[id]
		if !ok {
			return "", false
		}

		return bk.State, true
	})
	if hit {
		if rerr != nil {
			return nil, "", "", rerr
		}

		bk := b.backups[id]

		return b.backupDoc(bk, true), bk.Etag, rwr, nil
	}

	src, aerr := b.copySource(in)
	if aerr != nil {
		return nil, "", "", aerr
	}

	if aerr = b.checkCopyLimit(); aerr != nil {
		return nil, "", "", aerr
	}

	now := b.nowUTC()
	cp := *src
	cp.ID, cp.State, cp.Etag, cp.Seq = newOCID("mysqlbackup"), stateCreating, newEtag(), b.nextSeq()
	cp.TimeCreated, cp.TimeUpdated, cp.TimeCopied = now, now, &now
	cp.TransitionAt, cp.ScheduledAt, cp.RetainFrom, cp.Validation, cp.Details = nil, nil, nil, nil, ""
	cp.ImmediateSrc, cp.OriginalSrc = src.ID, orStr(src.OriginalSrc, src.ID)
	cp.Name = orStr(in.DisplayName, src.Name)
	cp.Description = orStr(in.Description, src.Description)
	cp.Tags = Tags{}

	if in.Retention != nil {
		cp.Retention = *in.Retention
	}

	if cp.Dump != nil {
		cp.State = stateActive
	} else {
		cp.TransitionAt = b.transitionAt()
	}

	b.backups[cp.ID] = &cp
	wr := b.newWR(wrCopyBackup, cp.CompartmentID, entityBackup, cp.ID, actionCreated, cp.TransitionAt)
	b.retry.Remember("CopyBackup", in.RetryToken, ociretry.Join(cp.ID, wr), now)

	return b.backupDoc(&cp, true), cp.Etag, wr, nil
}

func (b *InMemoryBackend) copySource(in CopyBackupIn) (*Backup, *APIError) {
	switch {
	case in.CompartmentID == "":
		return nil, errMissing("compartmentId is required")
	case in.SourceBackupID == "":
		return nil, errMissing("sourceBackupId is required")
	case in.SourceRegion == "":
		return nil, errMissing("sourceRegion is required")
	case in.SourceRegion == Region:
		return nil, errParam("sourceRegion must be a region other than " + Region + ", the region of this endpoint")
	}

	if aerr := firstErr(b.checkCompartment(in.CompartmentID), validRetention(in.Retention),
		validateOptName(in.DisplayName)); aerr != nil {
		return nil, aerr
	}

	b.settleID(in.SourceBackupID)

	src, ok := b.backups[in.SourceBackupID]
	if !ok || src.State == stateDeleted {
		return nil, errNotFound(in.SourceBackupID)
	}

	switch {
	case checkEtag(src.Etag, in.IfMatch) != nil:
		return nil, errEtag()
	case src.State != stateActive:
		return nil, errState("backup " + src.ID + " is in state " + src.State + "; a copy requires ACTIVE")
	case src.CompartmentID != in.CompartmentID:
		return nil, errParam("compartmentId must be the compartment of the source backup")
	case src.CreationType == creationAuto && in.Retention != nil && *in.Retention > maxAutoBackupDays:
		return nil, errParam(fmt.Sprintf("backupCopyRetentionInDays of an automatic backup is at most %d",
			maxAutoBackupDays))
	}

	return src, nil
}

func validateOptName(name string) *APIError {
	if name == "" {
		return nil
	}

	return validateName(name)
}

func (h *Handler) validateBackup(q *req) error {
	var in ValidateBackupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	d, etag, wr, err := h.Backend.ValidateBackup(q.param("backupId"), q.ifMatch(), q.r.Header.Get(headerRetry), in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}

func (h *Handler) copyBackup(q *req) error {
	var in CopyBackupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken, in.IfMatch = q.r.Header.Get(headerRetry), q.ifMatch()

	d, etag, wr, err := h.Backend.CopyBackup(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}
