package gcpsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	kindBackup          = "sql#backup"
	backupTypeFinal     = "FINAL"
	backupStateOK       = "SUCCESSFUL"
	backupStateRunning  = "RUNNING"
	backupStateFailed   = "FAILED"
	backupLocation      = "us"
	maxBackupTTLDays    = 365
	defaultFinalTTLDays = 30
	maxProjectBackups   = 200
	maxProjectBytes     = 512 << 20
	defaultBackupsPage  = 500
	maxBackupsPage      = 2000
	hoursPerDay         = 24
	backupNamePrefix    = "projects/"
	backupNameSegments  = 4
)

var (
	errBackupChecksum = errors.New("backup content does not match its recorded SHA-256")
	errBackupTooLarge = errors.New("backup would exceed the project's retained backup size limit")
)

type projBackup struct {
	Settings        *Instance       `json:"settings,omitempty"`
	Error           *OperationError `json:"error,omitempty"`
	Project         string          `json:"project"`
	ID              string          `json:"id"`
	Instance        string          `json:"instance"`
	Type            string          `json:"type"`
	State           string          `json:"state"`
	Description     string          `json:"description,omitempty"`
	DatabaseVersion string          `json:"databaseVersion"`
	Location        string          `json:"location"`
	StartTime       string          `json:"startTime"`
	EndTime         string          `json:"endTime,omitempty"`
	ExpiryTime      string          `json:"expiryTime,omitempty"`
	InstanceDeleted string          `json:"instanceDeletionTime,omitempty"`
	Sum             string          `json:"sha256,omitempty"`
	Dump            []byte          `json:"dump,omitempty"`
}

// Interval is the output-only time span of a backup.
type Interval struct {
	StartTime string `json:"startTime,omitempty"`
	EndTime   string `json:"endTime,omitempty"`
}

// Backup is the sql#backup resource.
type Backup struct {
	Error                *OperationError `json:"error,omitempty"`
	BackupInterval       *Interval       `json:"backupInterval,omitempty"`
	InstanceSettings     *Instance       `json:"instanceSettings,omitempty"`
	Kind                 string          `json:"kind"`
	Name                 string          `json:"name"`
	SelfLink             string          `json:"selfLink"`
	Type                 string          `json:"type"`
	State                string          `json:"state"`
	BackupKind           string          `json:"backupKind"`
	Instance             string          `json:"instance"`
	Location             string          `json:"location"`
	DatabaseVersion      string          `json:"databaseVersion"`
	TimeZone             string          `json:"timeZone"`
	Description          string          `json:"description,omitempty"`
	ExpiryTime           string          `json:"expiryTime,omitempty"`
	InstanceDeletionTime string          `json:"instanceDeletionTime,omitempty"`
	MaxChargeableBytes   string          `json:"maxChargeableBytes,omitempty"`
}

// BackupInput is the writable part of sql#backup.
type BackupInput struct {
	Instance    string `json:"instance"`
	Description string `json:"description"`
	Location    string `json:"location"`
	ExpiryTime  string `json:"expiryTime"`
	TTLDays     string `json:"ttlDays"`
}

// FinalBackupRequest asks instances.delete for a final backup.
type FinalBackupRequest struct {
	Description string
	ExpiryTime  string
	TTLDays     int64
	Enabled     bool
}

func backupName(project, id string) string { return backupNamePrefix + project + "/backups/" + id }

func backupLink(project, id string) string {
	return baseLink + "/" + backupName(project, id)
}

// ParseBackupName extracts the project and id of projects/{project}/backups/{id}.
func ParseBackupName(name string) (string, string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != backupNameSegments || parts[0] != "projects" || parts[2] != "backups" ||
		parts[1] == "" || parts[3] == "" {
		return "", "", false
	}

	return parts[1], parts[3], true
}

func (p *projBackup) view() *Backup {
	out := &Backup{
		Kind: kindBackup, Name: backupName(p.Project, p.ID), SelfLink: backupLink(p.Project, p.ID), Type: p.Type,
		State: p.State, BackupKind: "SNAPSHOT", Instance: p.Instance, Location: p.Location,
		DatabaseVersion: p.DatabaseVersion, TimeZone: "UTC", Description: p.Description, ExpiryTime: p.ExpiryTime,
		InstanceDeletionTime: p.InstanceDeleted, Error: p.Error,
		BackupInterval: &Interval{StartTime: p.StartTime, EndTime: p.EndTime},
	}

	if p.State == backupStateOK {
		out.MaxChargeableBytes = strconv.Itoa(len(p.Dump))
	}

	if p.Settings != nil {
		cp := *p.Settings
		cp.Settings = asMap(deepCopy(p.Settings.Settings))
		out.InstanceSettings = &cp
	}

	return out
}

func (p *projBackup) expired(now time.Time) bool {
	if p.ExpiryTime == "" {
		return false
	}

	at, err := time.Parse(timeLayout, p.ExpiryTime)

	return err == nil && !now.Before(at)
}

// resolveExpiry validates ttlDays or expiryTime (not both) against the 365 day ceiling counted from created.
func resolveExpiry(now, created time.Time, ttlDays int64, expiry string) (string, error) {
	if ttlDays != 0 && expiry != "" {
		return "", errInvalid("Specify either ttlDays or expiryTime, not both.")
	}

	ceiling := created.Add(maxBackupTTLDays * hoursPerDay * time.Hour)

	switch {
	case ttlDays < 0 || ttlDays > maxBackupTTLDays:
		return "", errInvalid("ttlDays must be between 1 and %d.", maxBackupTTLDays)
	case ttlDays > 0:
		return stamp(created.Add(time.Duration(ttlDays) * hoursPerDay * time.Hour)), nil
	case expiry == "":
		return "", nil
	}

	at, err := time.Parse(time.RFC3339, expiry)
	if err != nil {
		return "", errInvalid("Invalid expiryTime %q.", expiry)
	}

	if !at.After(now) || at.After(ceiling) {
		return "", errInvalid("expiryTime must be in the future and at most %d days after the backup is created.",
			maxBackupTTLDays)
	}

	return stamp(at), nil
}

func parseTTL(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}

	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, errInvalid("Invalid ttlDays %q.", v)
	}

	return n, nil
}

func (b *InMemoryBackend) projectBackupsLocked(project string) (int, int64) {
	var (
		count     int
		bytesUsed int64
	)

	for _, p := range b.projBackups {
		if p.Project == project {
			count++
			bytesUsed += int64(len(p.Dump))
		}
	}

	return count, bytesUsed
}

// liveBackupLocked finds an unexpired backup.
func (b *InMemoryBackend) liveBackupLocked(project, id string) (*projBackup, error) {
	p, ok := b.projBackups[instKey(project, id)]
	if !ok || p.expired(b.now()) {
		return nil, errNotFound("The backup does not exist.")
	}

	return p, nil
}

func (b *InMemoryBackend) newProjBackupLocked(rec *instRec, typ, desc, location, expiry string) *projBackup {
	now := stamp(b.now())
	view := b.viewLocked(rec)

	return &projBackup{
		Project: rec.Inst.Project, ID: b.nextBackupID(), Instance: rec.Inst.Name, Type: typ, State: backupStateRunning,
		Description: desc, DatabaseVersion: rec.Inst.DatabaseVersion, Location: orDefault(location, backupLocation),
		StartTime: now, ExpiryTime: expiry, Settings: view,
	}
}

// finishProjBackup stores dump on p under the project's size limit; the caller holds b.mu.
func (b *InMemoryBackend) finishProjBackupLocked(p *projBackup, dump []byte, derr error) error {
	p.EndTime = stamp(b.now())

	if derr == nil {
		_, used := b.projectBackupsLocked(p.Project)
		if len(dump) > maxDumpBytes || used+int64(len(dump)) > b.backupBytesLimitLocked() {
			derr = errBackupTooLarge
		}
	}

	if derr != nil {
		p.State, p.Dump, p.Sum = backupStateFailed, nil, ""
		p.Error = &OperationError{Kind: errKind, Code: opErrorDefault, Message: derr.Error()}

		return derr
	}

	sum := sha256.Sum256(dump)
	p.State, p.Dump, p.Sum = backupStateOK, dump, hex.EncodeToString(sum[:])

	return nil
}

func (b *InMemoryBackend) backupOpLocked(project, opType string, p *projBackup) *Operation {
	op := b.newOpLocked(project, p.Instance, opType, opMeta{})
	op.TargetID, op.TargetLink = p.ID, backupLink(project, p.ID)

	return op
}

func (b *InMemoryBackend) doneOpLocked(op *Operation) *Operation {
	now := stamp(b.now())
	op.Status, op.StartTime, op.EndTime = opDone, now, now
	cp := *op

	return &cp
}

// CreateBackup takes an on-demand backup of an instance that outlives the instance.
func (b *InMemoryBackend) CreateBackup(project string, in BackupInput) (*Operation, error) {
	ttl, err := parseTTL(in.TTLDays)
	if err != nil {
		return nil, err
	}

	if !instanceNameRe.MatchString(in.Instance) {
		return nil, errInvalid("Invalid instance name: %q.", in.Instance)
	}

	b.mu.Lock("CreateBackup")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, in.Instance)
	if err != nil {
		return nil, err
	}

	if rec.Inst.State == stateSuspended {
		return nil, errNotRunnable()
	}

	now := b.now()

	expiry, err := resolveExpiry(now, now, ttl, in.ExpiryTime)
	if err != nil {
		return nil, err
	}

	if n, _ := b.projectBackupsLocked(project); n >= b.backupLimitLocked() {
		return nil, newErr(http.StatusTooManyRequests, "quotaExceeded",
			"The project already holds %d backups; delete one first.", b.backupLimitLocked())
	}

	p := b.newProjBackupLocked(rec, "ON_DEMAND", in.Description, in.Location, expiry)
	b.projBackups[instKey(project, p.ID)] = p
	op := b.backupOpLocked(project, "BACKUP", p)
	op.BackupContext = map[string]any{"backupId": p.ID, keyKind: "sql#backupContext"}
	b.enqueueLocked(rec, task{op: op.Name, fn: func(ctx context.Context) error {
		dump, derr := b.dumpAll(ctx, rec)

		b.mu.Lock("CreateBackup.task")
		defer b.mu.Unlock()

		return b.finishProjBackupLocked(p, dump, derr)
	}})

	cp := *op

	return &cp, nil
}

// GetBackup returns a project backup.
func (b *InMemoryBackend) GetBackup(project, id string) (*Backup, error) {
	b.mu.RLock("GetBackup")
	defer b.mu.RUnlock()

	p, err := b.liveBackupLocked(project, id)
	if err != nil {
		return nil, err
	}

	return p.view(), nil
}

type backupFilter struct {
	since    time.Time
	instance string
	typ      string
	location string
}

func parseBackupFilter(expr string) (backupFilter, error) {
	var f backupFilter

	for tok := range strings.FieldsSeq(expr) {
		if tok == "AND" {
			continue
		}

		switch {
		case strings.HasPrefix(tok, "instance:"):
			f.instance = strings.TrimPrefix(tok, "instance:")
		case strings.HasPrefix(tok, "type:"):
			f.typ = strings.TrimPrefix(tok, "type:")
		case strings.HasPrefix(tok, "location:"):
			f.location = strings.TrimPrefix(tok, "location:")
		case strings.HasPrefix(tok, "backupInterval.startTime>="):
			at, err := time.Parse(time.RFC3339, strings.TrimPrefix(tok, "backupInterval.startTime>="))
			if err != nil {
				return f, errInvalid("Invalid filter %q.", tok)
			}

			f.since = at
		default:
			return f, errInvalid(
				"Unsupported filter %q; filter by type, instance, backupInterval.startTime or location.",
				tok,
			)
		}
	}

	return f, nil
}

func (f backupFilter) match(p *projBackup) bool {
	if f.instance != "" && p.Instance != f.instance || f.typ != "" && p.Type != f.typ ||
		f.location != "" && p.Location != f.location {
		return false
	}

	if f.since.IsZero() {
		return true
	}

	at, err := time.Parse(timeLayout, p.StartTime)

	return err == nil && !at.Before(f.since)
}

// ListBackups lists the unexpired backups of a project, newest first.
func (b *InMemoryBackend) ListBackups(project, filter, token string, size int) (page.Page[*Backup], error) {
	f, err := parseBackupFilter(filter)
	if err != nil {
		return page.Page[*Backup]{}, err
	}

	b.mu.RLock("ListBackups")

	now := b.now()
	all := make([]*projBackup, 0, len(b.projBackups))

	for _, p := range b.projBackups {
		if p.Project == project && !p.expired(now) && f.match(p) {
			all = append(all, p)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID > all[j].ID })

	views := make([]*Backup, len(all))
	for i, p := range all {
		views[i] = p.view()
	}
	b.mu.RUnlock()

	return page.New(views, token, min(size, maxBackupsPage), defaultBackupsPage), nil
}

// UpdateBackup changes the description or retention of a final backup.
func (b *InMemoryBackend) UpdateBackup(project, id string, in BackupInput, mask string) (*Operation, error) {
	fields, err := updateMaskFields(mask, in)
	if err != nil {
		return nil, err
	}

	ttl, err := parseTTL(in.TTLDays)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateBackup")
	defer b.mu.Unlock()

	p, err := b.liveBackupLocked(project, id)
	if err != nil {
		return nil, err
	}

	if p.Type != backupTypeFinal {
		return nil, errInvalid("Only final backups can be updated.")
	}

	if fields["description"] {
		p.Description = in.Description
	}

	if fields["expiryTime"] || fields["ttlDays"] {
		created, perr := time.Parse(timeLayout, p.StartTime)
		if perr != nil {
			created = b.now()
		}

		expiry, rerr := resolveExpiry(b.now(), created, ttl, in.ExpiryTime)
		if rerr != nil {
			return nil, rerr
		}

		p.ExpiryTime = expiry
	}

	return b.doneOpLocked(b.backupOpLocked(project, "UPDATE_BACKUP", p)), nil
}

// updateMaskFields lists the fields to change: the mask when given, else every field the body carries.
func updateMaskFields(mask string, in BackupInput) (map[string]bool, error) {
	out := map[string]bool{}

	if mask == "" {
		out["description"] = in.Description != ""
		out["expiryTime"] = in.ExpiryTime != ""
		out["ttlDays"] = in.TTLDays != ""

		return out, nil
	}

	for f := range strings.SplitSeq(mask, ",") {
		switch f = strings.TrimSpace(f); f {
		case "description", "expiryTime", "ttlDays":
			out[f] = true
		default:
			return nil, errInvalid("Field %q cannot be updated; only description and retention can.", f)
		}
	}

	return out, nil
}

// DeleteBackup removes a project backup.
func (b *InMemoryBackend) DeleteBackup(project, id string) (*Operation, error) {
	b.mu.Lock("DeleteBackup")
	defer b.mu.Unlock()

	p, err := b.liveBackupLocked(project, id)
	if err != nil {
		return nil, err
	}

	if p.State == backupStateRunning {
		return nil, errNotRunnable()
	}

	op := b.backupOpLocked(project, "DELETE_BACKUP", p)
	delete(b.projBackups, instKey(project, id))

	return b.doneOpLocked(op), nil
}

func (b *InMemoryBackend) sweepBackupsLocked(now time.Time) {
	for k, p := range b.projBackups {
		if p.State != backupStateRunning && p.expired(now) {
			delete(b.projBackups, k)
		}
	}
}

// RestoreProjectBackup restores the databases of instance from the project backup named by name.
func (b *InMemoryBackend) RestoreProjectBackup(project, instance, name string) (*Operation, error) {
	bp, id, ok := ParseBackupName(name)
	if !ok {
		return nil, errInvalid("Invalid backup name %q; want projects/{project}/backups/{id}.", name)
	}

	if bp != project {
		return nil, errInvalid("Restoring a backup from another project is not supported by the emulator.")
	}

	b.mu.Lock("RestoreProjectBackup")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, instance)
	if err != nil {
		return nil, err
	}

	p, err := b.liveBackupLocked(project, id)
	if err != nil {
		return nil, err
	}

	if p.State != backupStateOK {
		return nil, errInvalid("The backup is %s and cannot be restored.", p.State)
	}

	if p.DatabaseVersion != rec.Inst.DatabaseVersion {
		return nil, errInvalid("The backup was taken from %s and cannot be restored into %s.",
			p.DatabaseVersion, rec.Inst.DatabaseVersion)
	}

	dump, sum := p.Dump, p.Sum

	return b.submitLocked(rec, project, "RESTORE_VOLUME", opMeta{}, func(ctx context.Context) error {
		got := sha256.Sum256(dump)
		if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(got[:])), []byte(sum)) != 1 {
			return errBackupChecksum
		}

		return b.restoreDump(ctx, rec, dump)
	}), nil
}

// restoreDump loads dump into the engine of rec; a failed load is rolled back to the databases it found, and
// the instance is marked FAILED when even that fails.
func (b *InMemoryBackend) restoreDump(ctx context.Context, rec *instRec, dump []byte) error {
	eng := b.engineOf(rec)
	if eng == nil || len(dump) == 0 {
		return nil
	}

	before, err := eng.ListDatabases(ctx)
	if err != nil {
		return err
	}

	safety, err := b.dumpAll(ctx, rec)
	if err != nil {
		return err
	}

	if rerr := eng.Restore(ctx, "", dump); rerr != nil {
		if rbErr := b.rollbackRestore(ctx, rec, eng, safety, before); rbErr != nil {
			b.setState(rec, stateFailed)

			return errors.Join(rerr, rbErr)
		}

		return rerr
	}

	return b.reconcile(ctx, rec, eng)
}

func (b *InMemoryBackend) rollbackRestore(
	ctx context.Context, rec *instRec, eng Engine, safety []byte, before []string,
) error {
	after, err := eng.ListDatabases(ctx)
	if err != nil {
		return err
	}

	for _, name := range after {
		if !contains(before, name) && !isSystemDatabase(rec.Inst.DatabaseVersion, name) {
			if err = eng.DropDatabase(ctx, name); err != nil {
				return err
			}
		}
	}

	if len(bytes.TrimSpace(safety)) == 0 {
		return nil
	}

	return eng.Restore(ctx, "", safety)
}

// resolveFinalBackup merges the delete parameters with settings.finalBackupConfig.
func resolveFinalBackup(settings map[string]any, req FinalBackupRequest, now time.Time) (FinalBackupRequest, error) {
	cfg := asMap(settings["finalBackupConfig"])
	cfgOn, _ := cfg["enabled"].(bool)

	if !req.Enabled && !cfgOn {
		return FinalBackupRequest{}, nil
	}

	out := req
	out.Enabled = true

	if out.TTLDays == 0 && out.ExpiryTime == "" {
		out.TTLDays = policyInt(cfg["retentionDays"])
	}

	if out.TTLDays == 0 && out.ExpiryTime == "" {
		out.TTLDays = defaultFinalTTLDays
	}

	if _, err := resolveExpiry(now, now, out.TTLDays, out.ExpiryTime); err != nil {
		return FinalBackupRequest{}, err
	}

	return out, nil
}

// takeFinalBackup dumps rec into a FINAL project backup; the engine must still be running.
func (b *InMemoryBackend) takeFinalBackup(ctx context.Context, rec *instRec, req FinalBackupRequest) error {
	dump, derr := b.dumpAll(ctx, rec)

	b.mu.Lock("takeFinalBackup")
	defer b.mu.Unlock()

	now := b.now()
	expiry, _ := resolveExpiry(now, now, req.TTLDays, req.ExpiryTime)
	p := b.newProjBackupLocked(rec, backupTypeFinal, req.Description, "", expiry)
	p.InstanceDeleted = stamp(now)

	if err := b.finishProjBackupLocked(p, dump, derr); err != nil {
		return err
	}

	b.projBackups[instKey(p.Project, p.ID)] = p

	return nil
}

// SetBackupLimits bounds how many backups and how many bytes of backup content one project retains; zero keeps
// the default.
func (b *InMemoryBackend) SetBackupLimits(count int, bytesLimit int64) {
	b.mu.Lock("SetBackupLimits")
	defer b.mu.Unlock()

	b.maxBackups, b.maxBackupBytes = count, bytesLimit
}

func (b *InMemoryBackend) backupLimitLocked() int {
	if b.maxBackups > 0 {
		return b.maxBackups
	}

	return maxProjectBackups
}

func (b *InMemoryBackend) backupBytesLimitLocked() int64 {
	if b.maxBackupBytes > 0 {
		return b.maxBackupBytes
	}

	return maxProjectBytes
}

// BackupCount is the number of project backups held for project, expired ones included until swept.
func (b *InMemoryBackend) BackupCount(project string) int {
	b.mu.RLock("BackupCount")
	defer b.mu.RUnlock()

	n, _ := b.projectBackupsLocked(project)

	return n
}
