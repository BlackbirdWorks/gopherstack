package gcpbigtable

import (
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxCopyExpiry   = 30 * 24 * time.Hour
	maxDeletedTable = 100
	tableRetention  = 7 * 24 * time.Hour
	percentDone     = 100
)

func (b *InMemoryBackend) validateCopyExpiry(ts *timestamppb.Timestamp) error {
	if ts == nil {
		return errInvalid("expire_time is required.")
	}

	if d := ts.AsTime().Sub(b.now()); d < minBackupExpiry || d > maxCopyExpiry {
		return errInvalid("expire_time must be between 6 hours and 30 days from now.")
	}

	return nil
}

// CopyBackup copies a READY backup that is not itself a copy into a cluster, possibly of another instance.
func (b *InMemoryBackend) CopyBackup(in *adminpb.CopyBackupRequest) (*longrunningpb.Operation, error) {
	if !tableIDRe.MatchString(in.GetBackupId()) || len(in.GetBackupId()) > maxTableIDLen {
		return nil, errInvalid("Invalid backup id %q.", in.GetBackupId())
	}

	if err := b.validateCopyExpiry(in.GetExpireTime()); err != nil {
		return nil, err
	}

	dstInst, err := parseCluster(in.GetParent())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CopyBackup")
	defer b.mu.Unlock()

	_, src, err := b.backupLocked(in.GetSourceBackup())
	if err != nil {
		return nil, err
	}

	switch {
	case src.pb.GetState() != adminpb.Backup_READY:
		return nil, errPrecondition("Source backup %q is not READY.", in.GetSourceBackup())
	case src.pb.GetSourceBackup() != "":
		return nil, errInvalid("Source backup %q is itself a copy; copying a copied backup is not allowed.",
			in.GetSourceBackup())
	}

	dst, cid, err := b.clusterLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	name := backupName(dstInst, lastSegment(in.GetParent()), in.GetBackupId())
	if _, exists := dst.backups[name]; exists {
		return nil, errExists("Backup %q already exists.", name)
	}

	start := b.ts()
	snap := cloneTable(src.table)
	pb := &adminpb.Backup{
		Name:           name,
		SourceTable:    src.pb.GetSourceTable(),
		SourceBackup:   in.GetSourceBackup(),
		ExpireTime:     in.GetExpireTime(),
		StartTime:      start,
		EndTime:        b.ts(),
		SizeBytes:      src.pb.GetSizeBytes(),
		State:          adminpb.Backup_READY,
		BackupType:     adminpb.Backup_STANDARD,
		EncryptionInfo: encryptionInfo(dst.clusters[cid]),
	}

	dst.backups[name] = &backupRec{pb: pb, table: snap}

	progress := &adminpb.OperationProgress{ProgressPercent: percentDone, StartTime: start, EndTime: pb.GetEndTime()}
	info := &adminpb.BackupInfo{
		Backup: in.GetSourceBackup(), StartTime: src.pb.GetStartTime(), EndTime: src.pb.GetEndTime(),
		SourceTable: src.pb.GetSourceTable(),
	}

	return b.finish(
		dstInst,
		&adminpb.CopyBackupMetadata{Name: name, SourceBackupInfo: info, Progress: progress},
		clone(pb),
	)
}

type deletedTable struct {
	table *tableRec
	at    time.Time
	cmek  bool
}

// retainDeletedLocked remembers a deleted table for tableRetention, evicting the oldest beyond maxDeletedTable.
func (b *InMemoryBackend) retainDeletedLocked(rec *instRec, id string, t *tableRec) {
	now := b.now()

	for k, d := range rec.deleted {
		if now.Sub(d.at) > tableRetention {
			delete(rec.deleted, k)
		}
	}

	if _, replaced := rec.deleted[id]; !replaced && len(rec.deleted) >= maxDeletedTable {
		var oldest string

		for k, d := range rec.deleted {
			if oldest == "" || d.at.Before(rec.deleted[oldest].at) {
				oldest = k
			}
		}

		delete(rec.deleted, oldest)
	}

	cmek := false

	for _, c := range rec.clusters {
		cmek = cmek || c.GetEncryptionConfig().GetKmsKeyName() != ""
	}

	rec.deleted[id] = &deletedTable{table: t, at: now, cmek: cmek}
}

// UndeleteTable restores a table deleted within the last seven days. The table comes back with deletion
// protection enabled and without its IAM bindings, as the managing-tables page documents.
func (b *InMemoryBackend) UndeleteTable(in *adminpb.UndeleteTableRequest) (*longrunningpb.Operation, error) {
	instName, id, err := parseTable(in.GetName())
	if err != nil {
		return nil, err
	}

	start := b.ts()

	b.mu.Lock("UndeleteTable")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(instName)
	if err != nil {
		return nil, err
	}

	if _, exists := rec.tables[id]; exists {
		return nil, errExists("Table %q already exists.", in.GetName())
	}

	d, ok := rec.deleted[id]
	if !ok || b.now().Sub(d.at) > tableRetention {
		delete(rec.deleted, id)

		return nil, errNotFound("Table %q was not deleted in the last 7 days.", in.GetName())
	}

	if d.cmek {
		return nil, errPrecondition("Table %q had customer-managed encryption and cannot be undeleted.", in.GetName())
	}

	t := d.table
	t.policy = nil
	t.pb.DeletionProtection = true
	rec.tables[id] = t

	delete(rec.deleted, id)

	view := tableView(rec, t, adminpb.Table_SCHEMA_VIEW, adminpb.Table_SCHEMA_VIEW)

	return b.finish(
		instName,
		&adminpb.UndeleteTableMetadata{Name: in.GetName(), StartTime: start, EndTime: b.ts()},
		view,
	)
}
