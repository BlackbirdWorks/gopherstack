package gcpbigtable

import (
	"strings"
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	minBackupExpiry   = 6 * time.Hour
	maxBackupExpiry   = 90 * 24 * time.Hour
	defaultBackupPage = 100
)

func (b *InMemoryBackend) backupLocked(name string) (*instRec, *backupRec, error) {
	inst, err := parseBackup(name)
	if err != nil {
		return nil, nil, err
	}

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, nil, err
	}

	bk, ok := rec.backups[name]
	if !ok {
		return nil, nil, errBackupNotFound(name)
	}

	return rec, bk, nil
}

func (b *InMemoryBackend) validateExpiry(ts *timestamppb.Timestamp) error {
	if ts == nil {
		return errInvalid("backup.expire_time is required.")
	}

	d := ts.AsTime().Sub(b.now())
	if d < minBackupExpiry || d > maxBackupExpiry {
		return errInvalid("backup.expire_time must be between 6 hours and 90 days from now.")
	}

	return nil
}

// CreateBackup snapshots a table into a backup stored in a cluster.
func (b *InMemoryBackend) CreateBackup(in *adminpb.CreateBackupRequest) (*longrunningpb.Operation, error) {
	if !tableIDRe.MatchString(in.GetBackupId()) || len(in.GetBackupId()) > maxTableIDLen {
		return nil, errInvalid("Invalid backup id %q.", in.GetBackupId())
	}

	if err := b.validateExpiry(in.GetBackup().GetExpireTime()); err != nil {
		return nil, err
	}

	instName, err := parseCluster(in.GetParent())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateBackup")
	defer b.mu.Unlock()

	rec, cid, err := b.clusterLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	srcInst, srcID, err := parseTable(in.GetBackup().GetSourceTable())
	if err != nil {
		return nil, err
	}

	t, ok := rec.tables[srcID]
	if srcInst != instName || !ok {
		return nil, errTableNotFound(in.GetBackup().GetSourceTable())
	}

	name := backupName(instName, lastSegment(in.GetParent()), in.GetBackupId())
	if _, exists := rec.backups[name]; exists {
		return nil, errExists("Backup %q already exists.", name)
	}

	start := b.ts()
	snap := cloneTable(t)
	pb := &adminpb.Backup{
		Name: name, SourceTable: t.pb.GetName(), ExpireTime: in.GetBackup().GetExpireTime(),
		StartTime: start, EndTime: b.ts(), State: adminpb.Backup_READY, BackupType: adminpb.Backup_STANDARD,
		EncryptionInfo: encryptionInfo(rec.clusters[cid]),
	}

	for _, r := range snap.rows {
		pb.SizeBytes += r.size()
	}

	rec.backups[name] = &backupRec{pb: pb, table: snap}

	return b.finish(instName, &adminpb.CreateBackupMetadata{
		Name: name, SourceTable: t.pb.GetName(), StartTime: start, EndTime: pb.GetEndTime(),
	}, clone(pb))
}

// GetBackup returns a backup.
func (b *InMemoryBackend) GetBackup(name string) (*adminpb.Backup, error) {
	b.mu.RLock("GetBackup")
	defer b.mu.RUnlock()

	_, bk, err := b.backupLocked(name)
	if err != nil {
		return nil, err
	}

	return clone(bk.pb), nil
}

// UpdateBackup changes a backup's expiry.
func (b *InMemoryBackend) UpdateBackup(in *adminpb.UpdateBackupRequest) (*adminpb.Backup, error) {
	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask must be set.")
	}

	b.mu.Lock("UpdateBackup")
	defer b.mu.Unlock()

	_, bk, err := b.backupLocked(in.GetBackup().GetName())
	if err != nil {
		return nil, err
	}

	for _, p := range paths {
		if p != "expire_time" {
			return nil, errInvalid("Unsupported update_mask path %q for Backup.", p)
		}

		if err = b.validateExpiry(in.GetBackup().GetExpireTime()); err != nil {
			return nil, err
		}

		bk.pb.ExpireTime = in.GetBackup().GetExpireTime()
	}

	return clone(bk.pb), nil
}

// DeleteBackup deletes a backup.
func (b *InMemoryBackend) DeleteBackup(name string) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteBackup")
	defer b.mu.Unlock()

	rec, _, err := b.backupLocked(name)
	if err != nil {
		return nil, err
	}

	delete(rec.backups, name)

	return &emptypb.Empty{}, nil
}

// ListBackups lists the backups of a cluster, or of every cluster of an instance when the cluster is "-".
func (b *InMemoryBackend) ListBackups(in *adminpb.ListBackupsRequest) (*adminpb.ListBackupsResponse, error) {
	if in.GetFilter() != "" {
		return nil, errUnimplemented("ListBackups filter is not supported by this emulator.")
	}

	if in.GetOrderBy() != "" {
		return nil, errUnimplemented("ListBackups order_by is not supported by this emulator.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	ids, ok := parseRes(in.GetParent(), "projects", "instances", "clusters")
	if !ok {
		return nil, errInvalid("Invalid parent %q: want projects/{p}/instances/{i}/clusters/{c}.", in.GetParent())
	}

	b.mu.RLock("ListBackups")
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(instanceName(ids[0], ids[1]))
	if err != nil {
		return nil, err
	}

	if ids[2] != wildcard {
		if _, exists := rec.clusters[ids[2]]; !exists {
			return nil, errClusterNotFound(in.GetParent())
		}
	}

	all := []*adminpb.Backup{}

	for _, n := range collections.SortedKeys(rec.backups) {
		if ids[2] == wildcard || strings.HasPrefix(n, in.GetParent()+"/backups/") {
			all = append(all, clone(rec.backups[n].pb))
		}
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultBackupPage)

	return &adminpb.ListBackupsResponse{Backups: pg.Data, NextPageToken: pg.Next}, nil
}

// RestoreTable creates a new table from a backup.
func (b *InMemoryBackend) RestoreTable(in *adminpb.RestoreTableRequest) (*longrunningpb.Operation, error) {
	if !tableIDRe.MatchString(in.GetTableId()) || len(in.GetTableId()) > maxTableIDLen {
		return nil, errInvalid("Invalid table id %q.", in.GetTableId())
	}

	b.mu.Lock("RestoreTable")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetBackup() == "" {
		return nil, errInvalid("backup is required.")
	}

	_, bk, err := b.backupLocked(in.GetBackup())
	if err != nil {
		return nil, err
	}

	if _, exists := rec.tables[in.GetTableId()]; exists {
		return nil, errExists("Table %q already exists.", tableName(in.GetParent(), in.GetTableId()))
	}

	t := cloneTable(bk.table)
	t.pb.Name = tableName(in.GetParent(), in.GetTableId())
	t.pb.DeletionProtection = false
	t.policy = nil
	info := &adminpb.BackupInfo{
		Backup: bk.pb.GetName(), StartTime: bk.pb.GetStartTime(), EndTime: bk.pb.GetEndTime(),
		SourceTable: bk.pb.GetSourceTable(), SourceBackup: bk.pb.GetSourceBackup(),
	}
	t.pb.RestoreInfo = &adminpb.RestoreInfo{
		SourceType: adminpb.RestoreSourceType_BACKUP,
		SourceInfo: &adminpb.RestoreInfo_BackupInfo{BackupInfo: info},
	}
	rec.tables[in.GetTableId()] = t

	return b.finish(in.GetParent(), &adminpb.RestoreTableMetadata{
		Name: t.pb.GetName(), SourceType: adminpb.RestoreSourceType_BACKUP,
		SourceInfo: &adminpb.RestoreTableMetadata_BackupInfo{BackupInfo: info},
	}, tableView(rec, t, adminpb.Table_SCHEMA_VIEW, adminpb.Table_SCHEMA_VIEW))
}
