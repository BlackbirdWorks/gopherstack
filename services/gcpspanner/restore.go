package gcpspanner

import (
	"context"
	"errors"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const backupSegment = "/backups/"

// restoreEncryption maps the restore encryption choice to a database encryption_config; nil keeps the
// instance default.
func restoreEncryption(in *databasepb.RestoreDatabaseEncryptionConfig) (*databasepb.EncryptionConfig, error) {
	switch in.GetEncryptionType() {
	case databasepb.RestoreDatabaseEncryptionConfig_CUSTOMER_MANAGED_ENCRYPTION:
		return &databasepb.EncryptionConfig{KmsKeyName: in.GetKmsKeyName(), KmsKeyNames: in.GetKmsKeyNames()}, nil
	case databasepb.RestoreDatabaseEncryptionConfig_ENCRYPTION_TYPE_UNSPECIFIED,
		databasepb.RestoreDatabaseEncryptionConfig_USE_CONFIG_DEFAULT_OR_BACKUP_ENCRYPTION,
		databasepb.RestoreDatabaseEncryptionConfig_GOOGLE_DEFAULT_ENCRYPTION:
		if in.GetKmsKeyName() != "" || len(in.GetKmsKeyNames()) > 0 {
			return nil, errInvalid("kms_key_name and kms_key_names require CUSTOMER_MANAGED_ENCRYPTION.")
		}

		return nil, nil //nolint:nilnil // no customer-managed key
	default:
		return nil, errInvalid("Unknown encryption_type %s.", in.GetEncryptionType())
	}
}

func (b *InMemoryBackend) validateRestore(
	in *databasepb.RestoreDatabaseRequest,
) (string, *databasepb.EncryptionConfig, error) {
	if _, _, err := parseInstance(in.GetParent()); err != nil {
		return "", nil, err
	}

	id := in.GetDatabaseId()
	if !databaseIDRe.MatchString(id) {
		return "", nil, errInvalid("database_id %q must be 2-30 characters: lowercase letters, digits, _ or -, "+
			"starting with a letter and ending with a letter or digit.", id)
	}

	backup := in.GetBackup()
	if backup == "" {
		return "", nil, errInvalid("backup is required.")
	}

	if inst, _, ok := strings.Cut(backup, backupSegment); !ok || !strings.HasPrefix(inst, "projects/") {
		return "", nil, errInvalid("Invalid backup name %q.", backup)
	}

	enc, err := restoreEncryption(in.GetEncryptionConfig())
	if err != nil {
		return "", nil, err
	}

	b.mu.RLock("RestoreDatabase")
	rec, err := b.instanceLocked(in.GetParent())
	ready := err == nil && rec.pb.GetState() == instancepb.Instance_READY
	b.mu.RUnlock()

	switch {
	case err != nil:
		return "", nil, err
	case !ready:
		return "", nil, errPrecondition("Instance %s is not ready.", in.GetParent())
	}

	return id, enc, nil
}

// restoreSource returns a copy of the READY backup a restore reads.
func (b *InMemoryBackend) restoreSource(name string) (*backupRec, error) {
	b.mu.RLock("RestoreDatabase")
	defer b.mu.RUnlock()

	rec, err := b.backupLocked(name)
	if err != nil {
		return nil, err
	}

	if rec.pb.GetState() != databasepb.Backup_READY {
		return nil, errPrecondition("Backup %s is not READY.", name)
	}

	return &backupRec{
		pb: cloneBackup(rec.pb), enc: cloneEncryption(rec.enc), data: rec.data, ddl: cloneDDL(rec.ddl),
	}, nil
}

func sameProject(a, c string) bool {
	pa, _, errA := parseInstance(a)
	pc, _, errC := parseInstance(c)

	return errA == nil && errC == nil && pa == pc
}

// restoredEncryption is the key configuration of the restored database: the chosen one, or by default the
// backup's when restoring into its own instance.
func restoredEncryption(
	in *databasepb.RestoreDatabaseRequest, chosen *databasepb.EncryptionConfig, src *backupRec, sourceInstance string,
) *databasepb.EncryptionConfig {
	switch in.GetEncryptionConfig().GetEncryptionType() {
	case databasepb.RestoreDatabaseEncryptionConfig_ENCRYPTION_TYPE_UNSPECIFIED,
		databasepb.RestoreDatabaseEncryptionConfig_USE_CONFIG_DEFAULT_OR_BACKUP_ENCRYPTION:
		if sourceInstance == in.GetParent() {
			return cloneEncryption(src.enc)
		}

		return nil
	default:
		return chosen
	}
}

// RestoreDatabase creates a database from a backup: its schema and rows as captured, in the emulator when a
// data plane is available. The database stays READY_OPTIMIZING, and the backup referenced, until the
// optimize operation finishes.
func (b *InMemoryBackend) RestoreDatabase(
	ctx context.Context, in *databasepb.RestoreDatabaseRequest,
) (*longrunningpb.Operation, error) {
	id, chosen, err := b.validateRestore(in)
	if err != nil {
		return nil, err
	}

	srcInstance, _, err := parseBackup(in.GetBackup())
	if err != nil {
		return nil, err
	}

	if !sameProject(srcInstance, in.GetParent()) {
		return nil, errInvalid("The backup must be in the same project as the restored database.")
	}

	if err = b.checkDatabaseSlot(in.GetParent(), id); err != nil {
		return nil, err
	}

	src, err := b.restoreSource(in.GetBackup())
	if err != nil {
		return nil, err
	}

	var cfg string

	if inst, gerr := b.GetInstance(in.GetParent()); gerr == nil {
		cfg = inst.GetConfig()
	}

	enc := restoredEncryption(in, chosen, src, srcInstance)
	if chosen != nil {
		if err = b.checkEncryption(chosen, cfg); err != nil {
			return nil, err
		}
	}

	name := databaseName(in.GetParent(), id)
	ddl := cloneDDL(src.ddl)

	if ddl, err = b.materialize(ctx, in.GetParent(), id, src, ddl); err != nil {
		return nil, err
	}

	pb := b.newDatabase(name, src.pb.GetDatabaseDialect())
	pb.State = databasepb.Database_READY_OPTIMIZING
	pb.EncryptionConfig = enc
	info := &databasepb.BackupInfo{
		Backup: in.GetBackup(), VersionTime: src.pb.GetVersionTime(), CreateTime: src.pb.GetCreateTime(),
		SourceDatabase: src.pb.GetDatabase(),
	}
	pb.RestoreInfo = &databasepb.RestoreInfo{
		SourceType: databasepb.RestoreSourceType_BACKUP,
		SourceInfo: &databasepb.RestoreInfo_BackupInfo{BackupInfo: info},
	}
	applyDatabaseOptions(pb, ddl)

	if err = b.insertRestored(in.GetParent(), id, &dbRec{pb: pb, ddl: ddl}, in.GetBackup(), cfg); err != nil {
		b.discardMaterialized(ctx, name)

		return nil, err
	}

	start := b.ts()
	meta := &databasepb.RestoreDatabaseMetadata{
		Name: name, SourceType: databasepb.RestoreSourceType_BACKUP,
		SourceInfo: &databasepb.RestoreDatabaseMetadata_BackupInfo{BackupInfo: info},
		Progress:   dbProgress(start, b.ts()),
	}

	view, err := b.GetDatabase(name)
	if err != nil {
		return nil, err
	}

	op, err := b.finished(opName(name, ""), meta, view)
	if err != nil {
		return nil, err
	}

	b.startOptimize(name)

	return op, nil
}

// materialize creates the restored database in the emulator and loads its rows; without a data plane the rows
// wait with the other snapshot data until one starts.
func (b *InMemoryBackend) materialize(
	ctx context.Context, instance, id string, src *backupRec, ddl []string,
) ([]string, error) {
	name := databaseName(instance, id)
	cc, err := b.emu.conn(ctx)

	switch {
	case err == nil:
		d := replayDB{id: id, ddl: ddl, dialect: src.pb.GetDatabaseDialect()}
		if err = createDBOnEmulator(ctx, cc, instance, d); err != nil {
			return nil, err
		}

		if len(src.data.Tables) > 0 {
			if err = loadDatabase(ctx, spannerpb.NewSpannerClient(cc), name, src.data); err != nil {
				b.discardMaterialized(ctx, name)

				return nil, errPrecondition("The rows of backup %s cannot be restored: %v", src.pb.GetName(), err)
			}
		}

		return b.emulatorDDL(ctx, cc, name, ddl), nil
	case errors.Is(err, errNoDataPlane):
		if len(src.data.Tables) > 0 {
			b.mu.Lock("RestoreDatabase")

			if b.pendingData == nil {
				b.pendingData = map[string]dbDataSnap{}
			}

			b.pendingData[name] = src.data
			b.mu.Unlock()
		}

		return ddl, nil
	default:
		return nil, err
	}
}

// discardMaterialized undoes materialize after a later step failed.
func (b *InMemoryBackend) discardMaterialized(ctx context.Context, name string) {
	if cc := b.emu.current(); cc != nil {
		_, _ = databasepb.NewDatabaseAdminClient(cc).
			DropDatabase(context.WithoutCancel(ctx), &databasepb.DropDatabaseRequest{Database: name})
	}

	b.mu.Lock("RestoreDatabase")
	b.dropPendingData(name)
	b.mu.Unlock()
}

// insertRestored stores the restored database and makes the backup reference it.
func (b *InMemoryBackend) insertRestored(instance, id string, d *dbRec, backup, checkedConfig string) error {
	b.mu.Lock("insertRestored")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(instance)
	if err != nil {
		return err
	}

	if d.pb.GetEncryptionConfig() != nil && rec.pb.GetConfig() != checkedConfig {
		return status.Error(codes.Aborted, "The instance configuration changed while the encryption keys were checked.")
	}

	if _, dup := rec.dbs[id]; dup {
		return errExists("Database already exists: %s", databaseName(instance, id))
	}

	bk, err := b.backupLocked(backup)
	if err != nil {
		return err
	}

	rec.dbs[id] = d
	bk.pb.ReferencingDatabases = append(bk.pb.ReferencingDatabases, d.pb.GetName())

	return nil
}

// startOptimize runs the operation that finishes a restored database and drops its reference on the backup.
func (b *InMemoryBackend) startOptimize(name string) {
	start := b.ts()
	meta := func(end *timestamppb.Timestamp) proto.Message {
		return &databasepb.OptimizeRestoredDatabaseMetadata{Name: name, Progress: dbProgress(start, end)}
	}

	_, _ = b.begin(opName(name, ""), meta, func() (proto.Message, error) {
		if hook := b.optimizeHookFn(); hook != nil {
			hook()
		}

		return b.finishOptimize(name)
	})
}

func (b *InMemoryBackend) optimizeHookFn() func() {
	b.mu.RLock("optimizeHook")
	defer b.mu.RUnlock()

	return b.optimizeHook
}

func (b *InMemoryBackend) finishOptimize(name string) (proto.Message, error) {
	b.mu.Lock("finishOptimize")
	defer b.mu.Unlock()

	b.dropReferencesLocked(name)

	inst, id, err := parseDatabase(name)
	if err != nil {
		return nil, err
	}

	rec, ok := b.instances[inst]
	if !ok {
		return nil, errGone
	}

	d, ok := rec.dbs[id]
	if !ok {
		return nil, errGone
	}

	d.pb.State = databasepb.Database_READY

	return cloneDatabase(d.pb), nil
}

// dropReferencesLocked removes db from the referencing_databases of every backup.
func (b *InMemoryBackend) dropReferencesLocked(db string) {
	for _, r := range b.instances {
		for _, bk := range r.backups {
			bk.pb.ReferencingDatabases = slices.DeleteFunc(
				bk.pb.GetReferencingDatabases(),
				func(n string) bool { return n == db },
			)
		}
	}
}

// ListDatabaseOperations lists the operations of the databases of an instance.
func (b *InMemoryBackend) ListDatabaseOperations(
	in *databasepb.ListDatabaseOperationsRequest,
) (*databasepb.ListDatabaseOperationsResponse, error) {
	if _, _, err := parseInstance(in.GetParent()); err != nil {
		return nil, err
	}

	if _, err := b.GetInstance(in.GetParent()); err != nil {
		return nil, err
	}

	ops, err := b.listOps(in.GetParent()+"/databases/", in.GetFilter(), in.GetPageSize(), in.GetPageToken())
	if err != nil {
		return nil, err
	}

	return &databasepb.ListDatabaseOperationsResponse{Operations: ops.Data, NextPageToken: ops.Next}, nil
}
