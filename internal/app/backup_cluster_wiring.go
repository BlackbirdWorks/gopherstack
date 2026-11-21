package app

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	fsxbackend "github.com/blackbirdworks/gopherstack/services/fsx"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
)

const (
	backupTypeAurora     = "Aurora"
	backupTypeDocumentDB = "DocumentDB"
	backupTypeNeptune    = "Neptune"
	backupTypeRedshift   = "Redshift"
)

// wireBackupClusters lets Backup snapshot and restore Aurora, DocumentDB, Neptune and Redshift clusters and FSx
// file systems through the owning services, and enables the Storage Gateway volume mock.
func wireBackupClusters(byName map[string]service.Registerable) {
	backupH, ok := byName["Backup"].(*backupbackend.Handler)
	if !ok {
		return
	}

	bk := backupH.Backend

	if h, hOk := byName["RDS"].(*rdsbackend.Handler); hOk {
		bk.SetClusterService(backupTypeAurora, rdsClusterAdapter{bk: h.Backend})
	}

	if h, hOk := byName["DocDB"].(*docdbbackend.Handler); hOk {
		bk.SetClusterService(backupTypeDocumentDB, docdbClusterAdapter{bk: h.Backend})
	}

	if h, hOk := byName["Neptune"].(*neptunebackend.Handler); hOk {
		if nb, nbOk := h.Backend.(*neptunebackend.InMemoryBackend); nbOk {
			bk.SetClusterService(backupTypeNeptune, neptuneClusterAdapter{bk: nb})
		}
	}

	if h, hOk := byName["Redshift"].(*redshiftbackend.Handler); hOk {
		if rb, rbOk := h.Backend.(*redshiftbackend.InMemoryBackend); rbOk {
			bk.SetClusterService(backupTypeRedshift, redshiftClusterAdapter{bk: rb})
		}
	}

	if h, hOk := byName["FSx"].(*fsxbackend.Handler); hOk {
		if fb, fbOk := h.Backend.(*fsxbackend.InMemoryBackend); fbOk {
			bk.SetFSxService(fsxAdapter{bk: fb})
		}
	}

	bk.SetStorageGatewayRegion(bk.Region(), bk.AccountID())
}

type rdsClusterAdapter struct{ bk *rdsbackend.InMemoryBackend }

func (a rdsClusterAdapter) HasCluster(_ context.Context, id string) bool {
	list, err := a.bk.DescribeDBClusters(id)

	return err == nil && len(list) > 0
}

func (a rdsClusterAdapter) CreateSnapshot(_ context.Context, snapshotID, clusterID string) error {
	_, err := a.bk.CreateDBClusterSnapshot(snapshotID, clusterID)

	return err //nolint:wrapcheck // surfaced as the backup job's StatusMessage
}

func (a rdsClusterAdapter) RestoreSnapshot(_ context.Context, snapshotID, clusterID string) (string, error) {
	c, err := a.bk.RestoreDBClusterFromSnapshot(clusterID, snapshotID, "", rdsbackend.DBClusterOptions{})
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return c.DBClusterArn, nil
}

func (a rdsClusterAdapter) DeleteSnapshot(_ context.Context, snapshotID string) error {
	_, err := a.bk.DeleteDBClusterSnapshot(snapshotID)

	return err //nolint:wrapcheck // logged by Backup
}

type docdbClusterAdapter struct{ bk *docdbbackend.InMemoryBackend }

func (a docdbClusterAdapter) HasCluster(ctx context.Context, id string) bool {
	list, err := a.bk.DescribeDBClusters(ctx, id)

	return err == nil && len(list) > 0
}

func (a docdbClusterAdapter) CreateSnapshot(ctx context.Context, snapshotID, clusterID string) error {
	_, err := a.bk.CreateDBClusterSnapshot(ctx, snapshotID, clusterID, nil)

	return err //nolint:wrapcheck // surfaced as the backup job's StatusMessage
}

func (a docdbClusterAdapter) RestoreSnapshot(ctx context.Context, snapshotID, clusterID string) (string, error) {
	c, err := a.bk.RestoreDBClusterFromSnapshot(ctx, snapshotID, clusterID, "", nil)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return c.DBClusterArn, nil
}

func (a docdbClusterAdapter) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	_, err := a.bk.DeleteDBClusterSnapshot(ctx, snapshotID)

	return err //nolint:wrapcheck // logged by Backup
}

type neptuneClusterAdapter struct {
	bk *neptunebackend.InMemoryBackend
}

func (a neptuneClusterAdapter) HasCluster(ctx context.Context, id string) bool {
	list, err := a.bk.DescribeDBClusters(ctx, id, neptunebackend.DBClusterFilters{})

	return err == nil && len(list) > 0
}

func (a neptuneClusterAdapter) CreateSnapshot(ctx context.Context, snapshotID, clusterID string) error {
	_, err := a.bk.CreateDBClusterSnapshot(ctx, snapshotID, clusterID)

	return err //nolint:wrapcheck // surfaced as the backup job's StatusMessage
}

func (a neptuneClusterAdapter) RestoreSnapshot(ctx context.Context, snapshotID, clusterID string) (string, error) {
	c, err := a.bk.RestoreDBClusterFromSnapshot(ctx, snapshotID, clusterID, neptunebackend.RestoreClusterOptions{})
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return c.DBClusterArn, nil
}

func (a neptuneClusterAdapter) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	_, err := a.bk.DeleteDBClusterSnapshot(ctx, snapshotID)

	return err //nolint:wrapcheck // logged by Backup
}

type redshiftClusterAdapter struct {
	bk *redshiftbackend.InMemoryBackend
}

func (a redshiftClusterAdapter) HasCluster(_ context.Context, id string) bool {
	list, _, err := a.bk.DescribeClusters(id, "", 0, nil, nil)

	return err == nil && len(list) > 0
}

func (a redshiftClusterAdapter) CreateSnapshot(_ context.Context, snapshotID, clusterID string) error {
	_, err := a.bk.CreateClusterSnapshot(snapshotID, clusterID)

	return err //nolint:wrapcheck // surfaced as the backup job's StatusMessage
}

func (a redshiftClusterAdapter) RestoreSnapshot(_ context.Context, snapshotID, clusterID string) (string, error) {
	c, err := a.bk.RestoreFromClusterSnapshot(
		clusterID,
		snapshotID,
		redshiftbackend.RestoreFromClusterSnapshotOptions{},
	)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return arn.Build("redshift", a.bk.Region(), a.bk.AccountID(), "cluster:"+c.ClusterIdentifier), nil
}

func (a redshiftClusterAdapter) DeleteSnapshot(_ context.Context, snapshotID string) error {
	_, err := a.bk.DeleteClusterSnapshot(snapshotID)

	return err //nolint:wrapcheck // logged by Backup
}

type fsxAdapter struct{ bk *fsxbackend.InMemoryBackend }

func (a fsxAdapter) HasFileSystem(id string) bool { return a.bk.FileSystemExists(id) }

func (a fsxAdapter) CreateBackup(fileSystemID string) (string, error) {
	b, err := a.bk.BackupFileSystem(fileSystemID)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the backup job's StatusMessage
	}

	return b.BackupID, nil
}

func (a fsxAdapter) RestoreBackup(backupID string, opts backupbackend.FileSystemRestore) (string, error) {
	fs, err := a.bk.RestoreFileSystemFromBackup(backupID, opts.KmsKeyID, opts.SubnetIDs, opts.SecurityGroupIDs)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return fs.ResourceARN, nil
}

func (a fsxAdapter) DeleteBackup(backupID string) error {
	return a.bk.DeleteBackup(backupID) //nolint:wrapcheck // logged by Backup
}
