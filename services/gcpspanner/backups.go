package gcpspanner

import (
	"context"
	"encoding/json/v2"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxBackups     = 100
	maxBackupBytes = 256 << 20
	backupBudget   = 60 * time.Second
	minBackupLife  = 6 * time.Hour
	maxBackupLife  = 366 * 24 * time.Hour
	backupParts    = 6
)

var backupIDRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,58}[a-z0-9]$`)

// backupRec is a backup together with the schema and rows it captured.
type backupRec struct {
	pb   *databasepb.Backup
	enc  *databasepb.EncryptionConfig
	data dbDataSnap
	ddl  []string
}

func cloneBackup(pb *databasepb.Backup) *databasepb.Backup {
	out, _ := proto.Clone(pb).(*databasepb.Backup)

	return out
}

func cloneEncryption(pb *databasepb.EncryptionConfig) *databasepb.EncryptionConfig {
	out, _ := proto.Clone(pb).(*databasepb.EncryptionConfig)

	return out
}

func backupName(instance, id string) string { return instance + backupSegment + id }

// parseBackup parses projects/{p}/instances/{i}/backups/{b} into the instance name and the backup id.
func parseBackup(name string) (string, string, error) {
	p := strings.Split(name, "/")
	if len(p) != backupParts || p[4] != "backups" || p[5] == "" {
		return "", "", errInvalid("Invalid backup name %q.", name)
	}

	inst := strings.Join(p[:instanceParts], "/")
	if _, _, err := parseInstance(inst); err != nil {
		return "", "", err
	}

	return inst, p[5], nil
}

func errBackupNotFound(name string) error { return errNotFound("Backup not found: %s", name) }

func backupSize(ddl []string, data dbDataSnap) int64 {
	var n int64

	for _, s := range ddl {
		n += int64(len(s))
	}

	for _, t := range data.Tables {
		for _, r := range t.Rows {
			n += int64(len(r))
		}
	}

	return n
}

func (b *InMemoryBackend) backupLocked(name string) (*backupRec, error) {
	inst, id, err := parseBackup(name)
	if err != nil {
		return nil, err
	}

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	bk, ok := rec.backups[id]
	if !ok {
		return nil, errBackupNotFound(name)
	}

	return bk, nil
}

// backupView is a copy of the backup with its encryption information filled in.
func (b *InMemoryBackend) backupView(r *backupRec) *databasepb.Backup {
	out := cloneBackup(r.pb)
	if r.enc == nil {
		out.EncryptionInfo = &databasepb.EncryptionInfo{
			EncryptionType: databasepb.EncryptionInfo_GOOGLE_DEFAULT_ENCRYPTION,
		}
		out.EncryptionInformation = nil

		return out
	}

	infos := encryptionInfo(b.keyChecker(), r.enc)
	if len(infos) > 0 {
		out.EncryptionInfo = proto.Clone(infos[0]).(*databasepb.EncryptionInfo) //nolint:errcheck,forcetypeassert // own type
	}

	for _, i := range infos {
		i.EncryptionStatus = nil
	}

	out.EncryptionInformation = infos

	return out
}

func (b *InMemoryBackend) validateExpire(t *timestamppb.Timestamp, from time.Time, limit time.Time) error {
	if t == nil || !t.IsValid() {
		return errInvalid("expire_time is required.")
	}

	at := t.AsTime()

	if at.Before(b.now().Add(minBackupLife)) {
		return errInvalid("expire_time must be at least 6 hours from now.")
	}

	if at.After(limit) {
		return errInvalid("expire_time must be at most 366 days from %s.", from.UTC().Format(time.RFC3339))
	}

	return nil
}

// createEncryption maps the CreateBackup encryption choice to the key configuration of the backup.
func (b *InMemoryBackend) createEncryption(
	in *databasepb.CreateBackupEncryptionConfig, db *databasepb.Database, instanceConfig string,
) (*databasepb.EncryptionConfig, error) {
	cfg := &databasepb.EncryptionConfig{KmsKeyName: in.GetKmsKeyName(), KmsKeyNames: in.GetKmsKeyNames()}
	withKeys := in.GetKmsKeyName() != "" || len(in.GetKmsKeyNames()) > 0

	switch in.GetEncryptionType() {
	case databasepb.CreateBackupEncryptionConfig_ENCRYPTION_TYPE_UNSPECIFIED,
		databasepb.CreateBackupEncryptionConfig_USE_DATABASE_ENCRYPTION:
		if withKeys {
			return nil, errInvalid("kms_key_name and kms_key_names require CUSTOMER_MANAGED_ENCRYPTION.")
		}

		return cloneEncryption(db.GetEncryptionConfig()), nil
	case databasepb.CreateBackupEncryptionConfig_GOOGLE_DEFAULT_ENCRYPTION:
		if withKeys {
			return nil, errInvalid("kms_key_name and kms_key_names require CUSTOMER_MANAGED_ENCRYPTION.")
		}

		return nil, nil //nolint:nilnil // Google-managed keys
	case databasepb.CreateBackupEncryptionConfig_CUSTOMER_MANAGED_ENCRYPTION:
		return cfg, b.checkEncryption(cfg, instanceConfig)
	default:
		return nil, errInvalid("Unknown encryption_type %s.", in.GetEncryptionType())
	}
}

type backupSource struct {
	db       *databasepb.Database
	instance string
	config   string
	ddl      []string
}

func (b *InMemoryBackend) backupSourceOf(in *databasepb.CreateBackupRequest) (backupSource, error) {
	var src backupSource

	dbInst, dbID, err := parseDatabase(in.GetBackup().GetDatabase())
	if err != nil {
		return src, err
	}

	if dbInst != in.GetParent() {
		return src, errInvalid("The database must be in the same instance as the backup.")
	}

	b.mu.RLock("CreateBackup")
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		return src, err
	}

	if rec.pb.GetState() != instancepb.Instance_READY {
		return src, errPrecondition("Instance %s is not ready.", in.GetParent())
	}

	d, ok := rec.dbs[dbID]
	if !ok {
		return src, errDatabaseNotFound(in.GetBackup().GetDatabase())
	}

	return backupSource{
		db:       cloneDatabase(d.pb),
		ddl:      cloneDDL(d.ddl),
		instance: in.GetParent(),
		config:   rec.pb.GetConfig(),
	}, nil
}

func (b *InMemoryBackend) checkVersionTime(vt *timestamppb.Timestamp, db *databasepb.Database) error {
	if vt == nil {
		return nil
	}

	if !vt.IsValid() {
		return errInvalid("version_time is not a valid timestamp.")
	}

	at, now := vt.AsTime(), b.now()
	oldest := db.GetEarliestVersionTime().AsTime()

	if keep, ok := parseRetention(db.GetVersionRetentionPeriod()); ok && now.Add(-keep).After(oldest) {
		oldest = now.Add(-keep)
	}

	if at.After(now) || at.Before(oldest) {
		return errInvalid("version_time must be between %s and now.", oldest.UTC().Format(time.RFC3339))
	}

	return nil
}

// CreateBackup records a backup of a database and captures its schema and rows in the background.
func (b *InMemoryBackend) CreateBackup(
	_ context.Context, in *databasepb.CreateBackupRequest,
) (*longrunningpb.Operation, error) {
	if _, _, err := parseInstance(in.GetParent()); err != nil {
		return nil, err
	}

	id := in.GetBackupId()
	if !backupIDRe.MatchString(id) {
		return nil, errInvalid("backup_id %q must be 2-60 characters: lowercase letters, digits, _ or -, "+
			"starting with a letter and ending with a letter or digit.", id)
	}

	if in.GetBackup() == nil {
		return nil, errInvalid("backup is required.")
	}

	now := b.now()
	if err := b.validateExpire(in.GetBackup().GetExpireTime(), now, now.Add(maxBackupLife)); err != nil {
		return nil, err
	}

	src, err := b.backupSourceOf(in)
	if err != nil {
		return nil, err
	}

	if err = b.checkVersionTime(in.GetBackup().GetVersionTime(), src.db); err != nil {
		return nil, err
	}

	enc, err := b.createEncryption(in.GetEncryptionConfig(), src.db, src.config)
	if err != nil {
		return nil, err
	}

	created := b.ts()
	pb := &databasepb.Backup{
		Name: backupName(in.GetParent(), id), Database: in.GetBackup().GetDatabase(),
		ExpireTime: in.GetBackup().GetExpireTime(), CreateTime: created,
		VersionTime: firstTime(in.GetBackup().GetVersionTime(), created), State: databasepb.Backup_CREATING,
		DatabaseDialect: src.db.GetDatabaseDialect(),
		MaxExpireTime:   timestamppb.New(created.AsTime().Add(maxBackupLife)),
	}
	pb.OldestVersionTime = pb.GetVersionTime()

	if err = b.insertBackup(in.GetParent(), id, &backupRec{pb: pb, enc: enc, ddl: src.ddl}); err != nil {
		return nil, err
	}

	meta := func(end *timestamppb.Timestamp) proto.Message {
		return &databasepb.CreateBackupMetadata{
			Name: pb.GetName(), Database: pb.GetDatabase(), Progress: dbProgress(created, end),
		}
	}

	return b.begin(opName(pb.GetName(), ""), meta, func() (proto.Message, error) {
		return b.finishBackup(pb.GetName(), src.db, in.GetBackup().GetVersionTime())
	})
}

func dbProgress(start, end *timestamppb.Timestamp) *databasepb.OperationProgress {
	pct := int32(0)
	if end != nil {
		pct = percentDone
	}

	return &databasepb.OperationProgress{ProgressPercent: pct, StartTime: start, EndTime: end}
}

func firstTime(a, b *timestamppb.Timestamp) *timestamppb.Timestamp {
	if a != nil {
		return a
	}

	return b
}

func (b *InMemoryBackend) insertBackup(instance, id string, r *backupRec) error {
	b.mu.Lock("insertBackup")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(instance)
	if err != nil {
		return err
	}

	if _, dup := rec.backups[id]; dup {
		return errExists("Backup already exists: %s", backupName(instance, id))
	}

	if len(rec.backups) >= maxBackups {
		return errExhausted("At most %d backups can exist in an instance.", maxBackups)
	}

	rec.backups[id] = r

	return nil
}

// sweepExpiredBackups deletes READY backups past their expire_time that no restored database or copy refers to.
func (b *InMemoryBackend) sweepExpiredBackups() {
	b.mu.Lock("sweepExpiredBackups")
	defer b.mu.Unlock()

	now := b.now()

	for _, r := range b.instances {
		for id, bk := range r.backups {
			if bk.pb.GetState() == databasepb.Backup_READY && !bk.pb.GetExpireTime().AsTime().After(now) &&
				len(bk.pb.GetReferencingDatabases()) == 0 && len(bk.pb.GetReferencingBackups()) == 0 {
				delete(r.backups, id)
			}
		}
	}
}

func (b *InMemoryBackend) dropBackup(name string) {
	inst, id, err := parseBackup(name)
	if err != nil {
		return
	}

	b.mu.Lock("dropBackup")
	defer b.mu.Unlock()

	if rec, ok := b.instances[inst]; ok {
		delete(rec.backups, id)
	}
}

// captureData reads the rows of a database, as of at when given. Without a data plane the rows are the ones
// restored from a snapshot that no emulator has loaded yet.
func (b *InMemoryBackend) captureData(
	ctx context.Context, db *databasepb.Database, at *timestamppb.Timestamp, ddlBytes int64,
) (dbDataSnap, error) {
	room := b.backupRoom() - ddlBytes
	if room <= 0 {
		return dbDataSnap{}, errExhausted("Backups of the emulator hold at most %d bytes.", b.backupLimit())
	}

	ctx, cancel := context.WithTimeout(ctx, backupBudget)
	defer cancel()

	cc, err := b.emu.conn(ctx)

	switch {
	case err == nil:
		data, derr := dumpDatabaseAt(
			ctx, spannerpb.NewSpannerClient(cc), db.GetName(), databaseIsPostgres(db),
			&dumpBudgetState{maxBytes: int(min(room, maxBackupBytes))}, at,
		)
		if errors.Is(derr, errPersistLimit) {
			return dbDataSnap{}, errExhausted("The database is too large to back up in the emulator: %v", derr)
		}

		return data, derr
	case errors.Is(err, errNoDataPlane):
		b.mu.RLock("captureData")
		defer b.mu.RUnlock()

		return b.pendingData[db.GetName()], nil
	default:
		return dbDataSnap{}, err
	}
}

func (b *InMemoryBackend) finishBackup(
	name string, db *databasepb.Database, at *timestamppb.Timestamp,
) (proto.Message, error) {
	if hook := b.backupHookFn(); hook != nil {
		hook()
	}

	data, err := b.captureData(context.Background(), db, at, b.ddlBytes(name))
	if err != nil {
		b.dropBackup(name)

		return nil, err
	}

	b.mu.Lock("finishBackup")
	defer b.mu.Unlock()

	rec, err := b.backupLocked(name)
	if err != nil {
		return nil, errGone
	}

	size := backupSize(rec.ddl, data)

	if b.retainedBytesLocked()+size > b.backupLimitLocked() {
		b.dropBackupLocked(name)

		return nil, errExhausted("Backups of the emulator hold at most %d bytes.", b.backupLimitLocked())
	}

	rec.data = data
	rec.pb.SizeBytes, rec.pb.FreeableSizeBytes, rec.pb.ExclusiveSizeBytes = size, size, size
	rec.pb.State = databasepb.Backup_READY

	return b.backupView(rec), nil
}

func (b *InMemoryBackend) backupLimitLocked() int64 {
	if b.backupCap > 0 {
		return b.backupCap
	}

	return maxBackupBytes
}

func (b *InMemoryBackend) backupLimit() int64 {
	b.mu.RLock("backupLimit")
	defer b.mu.RUnlock()

	return b.backupLimitLocked()
}

// backupRoom is how many bytes of backup data the backend can still retain.
func (b *InMemoryBackend) backupRoom() int64 {
	b.mu.RLock("backupRoom")
	defer b.mu.RUnlock()

	return b.backupLimitLocked() - b.retainedBytesLocked()
}

func (b *InMemoryBackend) ddlBytes(name string) int64 {
	b.mu.RLock("ddlBytes")
	defer b.mu.RUnlock()

	rec, err := b.backupLocked(name)
	if err != nil {
		return 0
	}

	return backupSize(rec.ddl, dbDataSnap{})
}

func (b *InMemoryBackend) backupHookFn() func() {
	b.mu.RLock("backupHook")
	defer b.mu.RUnlock()

	return b.backupHook
}

func (b *InMemoryBackend) retainedBytesLocked() int64 {
	var n int64

	for _, r := range b.instances {
		for _, bk := range r.backups {
			if bk.pb.GetState() == databasepb.Backup_READY {
				n += bk.pb.GetSizeBytes()
			}
		}
	}

	return n
}

// CopyBackup copies a READY backup, possibly into another instance of the same project.
func (b *InMemoryBackend) CopyBackup(
	_ context.Context, in *databasepb.CopyBackupRequest,
) (*longrunningpb.Operation, error) {
	project, _, err := parseInstance(in.GetParent())
	if err != nil {
		return nil, err
	}

	id := in.GetBackupId()
	if !backupIDRe.MatchString(id) {
		return nil, errInvalid("backup_id %q must be 2-60 characters: lowercase letters, digits, _ or -, "+
			"starting with a letter and ending with a letter or digit.", id)
	}

	srcInst, _, err := parseBackup(in.GetSourceBackup())
	if err != nil {
		return nil, err
	}

	if srcProject, _, _ := parseInstance(srcInst); srcProject != project {
		return nil, errInvalid("The source backup must be in the same project as the new backup.")
	}

	dest, err := b.copyInto(in, id)
	if err != nil {
		return nil, err
	}

	created := dest.pb.GetCreateTime()

	meta := func(end *timestamppb.Timestamp) proto.Message {
		return &databasepb.CopyBackupMetadata{
			Name: dest.pb.GetName(), SourceBackup: in.GetSourceBackup(), Progress: dbProgress(created, end),
		}
	}

	return b.begin(opName(dest.pb.GetName(), ""), meta, func() (proto.Message, error) {
		return b.finishCopy(in.GetSourceBackup(), dest.pb.GetName())
	})
}

// copyEncryption maps the CopyBackup encryption choice to the key configuration of the copy.
func (b *InMemoryBackend) copyEncryption(
	in *databasepb.CopyBackupEncryptionConfig, source *backupRec, instanceConfig string,
) (*databasepb.EncryptionConfig, error) {
	cfg := &databasepb.EncryptionConfig{KmsKeyName: in.GetKmsKeyName(), KmsKeyNames: in.GetKmsKeyNames()}
	withKeys := in.GetKmsKeyName() != "" || len(in.GetKmsKeyNames()) > 0

	switch in.GetEncryptionType() {
	case databasepb.CopyBackupEncryptionConfig_ENCRYPTION_TYPE_UNSPECIFIED,
		databasepb.CopyBackupEncryptionConfig_USE_CONFIG_DEFAULT_OR_BACKUP_ENCRYPTION:
		if withKeys {
			return nil, errInvalid("kms_key_name and kms_key_names require CUSTOMER_MANAGED_ENCRYPTION.")
		}

		return cloneEncryption(source.enc), nil
	case databasepb.CopyBackupEncryptionConfig_GOOGLE_DEFAULT_ENCRYPTION:
		if withKeys {
			return nil, errInvalid("kms_key_name and kms_key_names require CUSTOMER_MANAGED_ENCRYPTION.")
		}

		return nil, nil //nolint:nilnil // Google-managed keys
	case databasepb.CopyBackupEncryptionConfig_CUSTOMER_MANAGED_ENCRYPTION:
		return cfg, b.checkEncryption(cfg, instanceConfig)
	default:
		return nil, errInvalid("Unknown encryption_type %s.", in.GetEncryptionType())
	}
}

func (b *InMemoryBackend) copyInto(in *databasepb.CopyBackupRequest, id string) (*backupRec, error) {
	b.sweepExpiredBackups()

	b.mu.RLock("CopyBackup")

	destInst, derr := b.instanceLocked(in.GetParent())

	var (
		source *backupRec
		serr   error
		cfg    string
	)

	if derr == nil {
		cfg = destInst.pb.GetConfig()
		source, serr = b.backupLocked(in.GetSourceBackup())
	}

	var srcPB *databasepb.Backup
	if source != nil {
		srcPB = cloneBackup(source.pb)
	}

	b.mu.RUnlock()

	switch {
	case derr != nil:
		return nil, derr
	case serr != nil:
		return nil, serr
	case srcPB.GetState() != databasepb.Backup_READY:
		return nil, errPrecondition("Source backup %s is not READY.", in.GetSourceBackup())
	case destInst.pb.GetState() != instancepb.Instance_READY:
		return nil, errPrecondition("Instance %s is not ready.", in.GetParent())
	}

	if err := b.validateExpire(in.GetExpireTime(), srcPB.GetCreateTime().AsTime(),
		minTime(srcPB.GetCreateTime().AsTime().Add(maxBackupLife), srcPB.GetMaxExpireTime().AsTime())); err != nil {
		return nil, err
	}

	enc, err := b.copyEncryption(in.GetEncryptionConfig(), source, cfg)
	if err != nil {
		return nil, err
	}

	created := b.ts()
	pb := &databasepb.Backup{
		Name: backupName(in.GetParent(), id), Database: srcPB.GetDatabase(), ExpireTime: in.GetExpireTime(),
		CreateTime: created, VersionTime: srcPB.GetVersionTime(), OldestVersionTime: srcPB.GetOldestVersionTime(),
		State: databasepb.Backup_CREATING, DatabaseDialect: srcPB.GetDatabaseDialect(),
		MaxExpireTime: srcPB.GetMaxExpireTime(),
	}
	dest := &backupRec{pb: pb, enc: enc}

	if err = b.insertBackup(in.GetParent(), id, dest); err != nil {
		return nil, err
	}

	if err = b.addBackupReference(in.GetSourceBackup(), pb.GetName()); err != nil {
		b.dropBackup(pb.GetName())

		return nil, err
	}

	return dest, nil
}

func minTime(a, c time.Time) time.Time {
	if c.IsZero() || a.Before(c) {
		return a
	}

	return c
}

func (b *InMemoryBackend) addBackupReference(source, dest string) error {
	b.mu.Lock("addBackupReference")
	defer b.mu.Unlock()

	rec, err := b.backupLocked(source)
	if err != nil {
		return err
	}

	rec.pb.ReferencingBackups = append(rec.pb.ReferencingBackups, dest)

	return nil
}

func (b *InMemoryBackend) finishCopy(source, dest string) (proto.Message, error) {
	b.mu.Lock("finishCopy")
	defer b.mu.Unlock()

	src, serr := b.backupLocked(source)
	rec, derr := b.backupLocked(dest)

	if src != nil {
		src.pb.ReferencingBackups = slices.DeleteFunc(
			src.pb.GetReferencingBackups(),
			func(n string) bool { return n == dest },
		)
	}

	switch {
	case derr != nil:
		return nil, errGone
	case serr != nil:
		b.dropBackupLocked(dest)

		return nil, errPrecondition("Source backup %s was deleted while it was copied.", source)
	}

	size := backupSize(src.ddl, src.data)
	if b.retainedBytesLocked()+size > b.backupLimitLocked() {
		b.dropBackupLocked(dest)

		return nil, errExhausted("Backups of the emulator hold at most %d bytes.", b.backupLimitLocked())
	}

	rec.ddl, rec.data = cloneDDL(src.ddl), src.data
	rec.pb.SizeBytes, rec.pb.FreeableSizeBytes, rec.pb.ExclusiveSizeBytes = size, size, size
	rec.pb.State = databasepb.Backup_READY

	return b.backupView(rec), nil
}

func (b *InMemoryBackend) dropBackupLocked(name string) {
	if inst, id, err := parseBackup(name); err == nil {
		if rec, ok := b.instances[inst]; ok {
			delete(rec.backups, id)
		}
	}
}

// GetBackup returns a backup.
func (b *InMemoryBackend) GetBackup(name string) (*databasepb.Backup, error) {
	b.sweepExpiredBackups()

	b.mu.RLock("GetBackup")
	defer b.mu.RUnlock()

	rec, err := b.backupLocked(name)
	if err != nil {
		return nil, err
	}

	return b.backupView(rec), nil
}

// UpdateBackup changes the expire_time of a READY backup.
func (b *InMemoryBackend) UpdateBackup(in *databasepb.UpdateBackupRequest) (*databasepb.Backup, error) {
	pb := in.GetBackup()
	if pb == nil {
		return nil, errInvalid("backup is required.")
	}

	if _, _, err := parseBackup(pb.GetName()); err != nil {
		return nil, err
	}

	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask is required.")
	}

	for _, p := range paths {
		if p != "expire_time" {
			return nil, errInvalid("Update mask path %q is not an updatable field.", p)
		}
	}

	b.sweepExpiredBackups()

	b.mu.Lock("UpdateBackup")
	defer b.mu.Unlock()

	rec, err := b.backupLocked(pb.GetName())
	if err != nil {
		return nil, err
	}

	if rec.pb.GetState() != databasepb.Backup_READY {
		return nil, errPrecondition("Backup %s is not READY.", pb.GetName())
	}

	limit := rec.pb.GetMaxExpireTime().AsTime()
	if err = b.validateExpire(pb.GetExpireTime(), rec.pb.GetCreateTime().AsTime(), limit); err != nil {
		return nil, err
	}

	rec.pb.ExpireTime = pb.GetExpireTime()

	return b.backupView(rec), nil
}

// DeleteBackup deletes a backup that no restored database or copy still refers to.
func (b *InMemoryBackend) DeleteBackup(name string) (*emptypb.Empty, error) {
	b.sweepExpiredBackups()

	b.mu.Lock("DeleteBackup")
	defer b.mu.Unlock()

	rec, err := b.backupLocked(name)
	if err != nil {
		return nil, err
	}

	switch {
	case rec.pb.GetState() == databasepb.Backup_CREATING:
		return nil, errPrecondition("Backup %s is still being created.", name)
	case len(rec.pb.GetReferencingDatabases()) > 0:
		return nil, errPrecondition("Backup %s is referenced by restored databases: %s.", name,
			strings.Join(rec.pb.GetReferencingDatabases(), ", "))
	case len(rec.pb.GetReferencingBackups()) > 0:
		return nil, errPrecondition("Backup %s is being copied.", name)
	}

	b.dropBackupLocked(name)

	return &emptypb.Empty{}, nil
}

func backupDoc(pb *databasepb.Backup) map[string]any {
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(pb)
	if err != nil {
		return nil
	}

	doc := map[string]any{}
	if err = json.Unmarshal(raw, &doc); err != nil {
		return nil
	}

	return doc
}

// ListBackups lists the backups of an instance in name order.
func (b *InMemoryBackend) ListBackups(in *databasepb.ListBackupsRequest) (*databasepb.ListBackupsResponse, error) {
	if _, _, err := parseInstance(in.GetParent()); err != nil {
		return nil, err
	}

	keep := opPred(func(map[string]any) bool { return true })

	if strings.TrimSpace(in.GetFilter()) != "" {
		var err error
		if keep, err = parseOpFilter(in.GetFilter()); err != nil {
			return nil, err
		}
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if err := page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, errInvalid("Invalid page_token.")
	}

	b.sweepExpiredBackups()

	b.mu.RLock("ListBackups")

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	ids := make([]string, 0, len(rec.backups))
	for id := range rec.backups {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	all := make([]*databasepb.Backup, 0, len(ids))

	for _, id := range ids {
		if v := b.backupView(rec.backups[id]); keep(backupDoc(v)) {
			all = append(all, v)
		}
	}
	b.mu.RUnlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return &databasepb.ListBackupsResponse{Backups: pg.Data, NextPageToken: pg.Next}, nil
}

// ListBackupOperations lists the CreateBackup and CopyBackup operations of an instance, newest first.
func (b *InMemoryBackend) ListBackupOperations(
	in *databasepb.ListBackupOperationsRequest,
) (*databasepb.ListBackupOperationsResponse, error) {
	if _, _, err := parseInstance(in.GetParent()); err != nil {
		return nil, err
	}

	if _, err := b.GetInstance(in.GetParent()); err != nil {
		return nil, err
	}

	ops, err := b.listOps(in.GetParent()+backupSegment, in.GetFilter(), in.GetPageSize(), in.GetPageToken())
	if err != nil {
		return nil, err
	}

	return &databasepb.ListBackupOperationsResponse{Operations: ops.Data, NextPageToken: ops.Next}, nil
}
