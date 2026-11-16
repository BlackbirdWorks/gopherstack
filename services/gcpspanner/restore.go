package gcpspanner

import (
	"context"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
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

// RestoreDatabase restores a backup held by the data plane into a new database.
func (b *InMemoryBackend) RestoreDatabase(
	ctx context.Context, in *databasepb.RestoreDatabaseRequest,
) (*longrunningpb.Operation, error) {
	id, enc, err := b.validateRestore(in)
	if err != nil {
		return nil, err
	}

	var cfg string

	if enc != nil {
		if inst, gerr := b.GetInstance(in.GetParent()); gerr == nil {
			cfg = inst.GetConfig()
		}

		if err = b.checkEncryption(enc, cfg); err != nil {
			return nil, err
		}
	}

	if err = b.checkDatabaseSlot(in.GetParent(), id); err != nil {
		return nil, err
	}

	cc, err := b.emu.dataConn(ctx)
	if err != nil {
		return nil, err
	}

	client := databasepb.NewDatabaseAdminClient(cc)
	name := databaseName(in.GetParent(), id)

	op, err := client.RestoreDatabase(ctx, &databasepb.RestoreDatabaseRequest{
		Parent: in.GetParent(), DatabaseId: id,
		Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: in.GetBackup()},
	})
	if err != nil {
		return nil, explainUnimplemented("google.spanner.admin.database.v1.DatabaseAdmin/RestoreDatabase", err)
	}

	if err = waitOp(ctx, cc, op); err != nil {
		return nil, err
	}

	info := backupInfo(ctx, client, in.GetBackup())
	pb := b.newDatabase(name, databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED)
	pb.RestoreInfo = &databasepb.RestoreInfo{
		SourceType: databasepb.RestoreSourceType_BACKUP,
		SourceInfo: &databasepb.RestoreInfo_BackupInfo{BackupInfo: info},
	}
	pb.EncryptionConfig = enc

	if db, derr := client.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: name}); derr == nil {
		pb.DatabaseDialect = db.GetDatabaseDialect()
	}

	if err = b.insertDatabase(
		in.GetParent(),
		id,
		&dbRec{pb: pb, ddl: b.emulatorDDL(ctx, cc, name, nil)},
		cfg,
	); err != nil {
		return nil, err
	}

	start := b.ts()
	meta := &databasepb.RestoreDatabaseMetadata{
		Name: name, SourceType: databasepb.RestoreSourceType_BACKUP,
		SourceInfo: &databasepb.RestoreDatabaseMetadata_BackupInfo{BackupInfo: info},
		Progress:   &databasepb.OperationProgress{ProgressPercent: percentDone, StartTime: start, EndTime: b.ts()},
	}

	view, err := b.GetDatabase(name)
	if err != nil {
		return nil, err
	}

	return b.finished(opName(name, ""), meta, view)
}

// backupInfo describes the source backup; a backup the data plane can no longer read is reported by name only.
func backupInfo(ctx context.Context, c databasepb.DatabaseAdminClient, name string) *databasepb.BackupInfo {
	bk, err := c.GetBackup(ctx, &databasepb.GetBackupRequest{Name: name})
	if err != nil {
		return &databasepb.BackupInfo{Backup: name}
	}

	return &databasepb.BackupInfo{
		Backup:         name,
		VersionTime:    bk.GetVersionTime(),
		CreateTime:     bk.GetCreateTime(),
		SourceDatabase: bk.GetDatabase(),
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
