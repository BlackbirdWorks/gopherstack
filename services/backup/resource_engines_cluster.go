package backup

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	resourceTypeAurora         = "Aurora"
	resourceTypeDocumentDB     = "DocumentDB"
	resourceTypeNeptune        = "Neptune"
	resourceTypeRedshift       = "Redshift"
	resourceTypeFSx            = "FSx"
	resourceTypeStorageGateway = "Storage Gateway"

	restoredSuffix    = "-restored"
	sgwVolumeIDDigits = 8
)

// clusterResourceTypes share the rds-namespace cluster ARN shape; the owning service is resolved at backup time.
func clusterResourceTypes() []string {
	return []string{resourceTypeAurora, resourceTypeDocumentDB, resourceTypeNeptune}
}

// ClusterSnapshotService is a cluster database service's snapshot surface; the snapshot is the recovery point.
type ClusterSnapshotService interface {
	HasCluster(ctx context.Context, clusterID string) bool
	CreateSnapshot(ctx context.Context, snapshotID, clusterID string) error
	// RestoreSnapshot returns the new cluster's ARN.
	RestoreSnapshot(ctx context.Context, snapshotID, clusterID string) (string, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
}

// FileSystemRestore holds the optional overrides for a file system restored from a backup.
type FileSystemRestore struct {
	KmsKeyID         string
	SubnetIDs        []string
	SecurityGroupIDs []string
}

// FSxService is the FSx surface used to back up and restore file systems.
type FSxService interface {
	HasFileSystem(id string) bool
	CreateBackup(fileSystemID string) (string, error)
	// RestoreBackup returns the new file system's ARN.
	RestoreBackup(backupID string, opts FileSystemRestore) (string, error)
	DeleteBackup(backupID string) error
}

// SetClusterService wires a cluster database service (Aurora, DocumentDB, Neptune or Redshift) so clusters are
// snapshotted at backup time and recreated from that snapshot on restore.
func (b *InMemoryBackend) SetClusterService(resourceType string, svc ClusterSnapshotService) {
	b.mu.Lock("SetClusterService")
	defer b.mu.Unlock()

	b.engines[resourceType] = &clusterEngine{svc: svc, resourceType: resourceType}
}

// SetFSxService wires FSx so file systems are backed up at backup time and recreated from that backup on restore.
func (b *InMemoryBackend) SetFSxService(svc FSxService) {
	b.mu.Lock("SetFSxService")
	defer b.mu.Unlock()

	b.engines[resourceTypeFSx] = &fsxEngine{svc: svc}
}

// SetStorageGatewayRegion enables Storage Gateway backups; no Storage Gateway emulator exists, so the volume
// snapshot and the volume restore are logged as "would" lines.
func (b *InMemoryBackend) SetStorageGatewayRegion(region, accountID string) {
	b.mu.Lock("SetStorageGatewayRegion")
	defer b.mu.Unlock()

	b.engines[resourceTypeStorageGateway] = &storageGatewayEngine{region: region, accountID: accountID}
}

// discarder is implemented by engines whose recovery point data points at a snapshot held by another service.
type discarder interface {
	discard(ctx context.Context, data jsontext.Value) error
}

type clusterSnapshot struct {
	SnapshotID string `json:"snapshotId"`
	ClusterID  string `json:"clusterId"`
}

type clusterEngine struct {
	svc          ClusterSnapshotService
	resourceType string
}

func (e *clusterEngine) capture(ctx context.Context, resourceArn string) (jsontext.Value, error) {
	id := resourceNameFromARN(resourceArn)
	if !e.svc.HasCluster(ctx, id) {
		return nil, nil
	}

	snap := clusterSnapshot{SnapshotID: awsid.New("awsbackup-"), ClusterID: id}
	if err := e.svc.CreateSnapshot(ctx, snap.SnapshotID, id); err != nil {
		return nil, fmt.Errorf("snapshot cluster %s: %w", id, err)
	}

	return json.Marshal(snap, jsonx.V1())
}

func (e *clusterEngine) restore(ctx context.Context, data jsontext.Value, md map[string]string) (string, error) {
	var snap clusterSnapshot
	if err := json.Unmarshal(data, &snap, jsonx.V1()); err != nil {
		return "", fmt.Errorf("decode %s snapshot: %w", e.resourceType, err)
	}

	target := snap.ClusterID + restoredSuffix
	if v := mdValue(md, "DBClusterIdentifier"); v != "" {
		target = v
	} else if v = mdValue(md, "ClusterIdentifier"); v != "" {
		target = v
	}

	clusterArn, err := e.svc.RestoreSnapshot(ctx, snap.SnapshotID, target)
	if err != nil {
		return "", fmt.Errorf("restore cluster %s: %w", target, err)
	}

	return clusterArn, nil
}

func (e *clusterEngine) discard(ctx context.Context, data jsontext.Value) error {
	var snap clusterSnapshot
	if err := json.Unmarshal(data, &snap, jsonx.V1()); err != nil {
		return fmt.Errorf("decode %s snapshot: %w", e.resourceType, err)
	}

	return e.svc.DeleteSnapshot(ctx, snap.SnapshotID)
}

type fsxSnapshot struct {
	BackupID     string `json:"backupId"`
	FileSystemID string `json:"fileSystemId"`
}

type fsxEngine struct{ svc FSxService }

func (e *fsxEngine) capture(_ context.Context, resourceArn string) (jsontext.Value, error) {
	id := resourceNameFromARN(resourceArn)
	if !e.svc.HasFileSystem(id) {
		return nil, nil
	}

	backupID, err := e.svc.CreateBackup(id)
	if err != nil {
		return nil, fmt.Errorf("back up file system %s: %w", id, err)
	}

	return json.Marshal(fsxSnapshot{BackupID: backupID, FileSystemID: id}, jsonx.V1())
}

func (e *fsxEngine) restore(_ context.Context, data jsontext.Value, md map[string]string) (string, error) {
	var snap fsxSnapshot
	if err := json.Unmarshal(data, &snap, jsonx.V1()); err != nil {
		return "", fmt.Errorf("decode FSx snapshot: %w", err)
	}

	opts := FileSystemRestore{
		KmsKeyID:         mdValue(md, "KmsKeyId"),
		SubnetIDs:        mdList(md, "SubnetIds"),
		SecurityGroupIDs: mdList(md, "SecurityGroupIds"),
	}

	fsArn, err := e.svc.RestoreBackup(snap.BackupID, opts)
	if err != nil {
		return "", fmt.Errorf("restore file system from backup %s: %w", snap.BackupID, err)
	}

	return fsArn, nil
}

func (e *fsxEngine) discard(_ context.Context, data jsontext.Value) error {
	var snap fsxSnapshot
	if err := json.Unmarshal(data, &snap, jsonx.V1()); err != nil {
		return fmt.Errorf("decode FSx snapshot: %w", err)
	}

	return e.svc.DeleteBackup(snap.BackupID)
}

// mdList reads a restore metadata list value, given either as a JSON array or comma separated.
func mdList(md map[string]string, key string) []string {
	v := strings.TrimSpace(mdValue(md, key))
	if v == "" {
		return nil
	}

	var list []string
	if json.Unmarshal([]byte(v), &list, jsonx.V1()) == nil {
		return list
	}

	var out []string
	for part := range strings.SplitSeq(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}

	return out
}

type storageGatewaySnapshot struct {
	VolumeArn string `json:"volumeArn"`
}

type storageGatewayEngine struct {
	region    string
	accountID string
}

func (e *storageGatewayEngine) capture(ctx context.Context, resourceArn string) (jsontext.Value, error) {
	if _, ok := storageGatewayVolumeFromARN(resourceArn); !ok {
		return nil, nil
	}

	logger.Load(ctx).InfoContext(ctx, "backup: would snapshot Storage Gateway volume", "volumeArn", resourceArn)

	return json.Marshal(storageGatewaySnapshot{VolumeArn: resourceArn}, jsonx.V1())
}

func (e *storageGatewayEngine) restore(ctx context.Context, data jsontext.Value, md map[string]string) (string, error) {
	var snap storageGatewaySnapshot
	if err := json.Unmarshal(data, &snap, jsonx.V1()); err != nil {
		return "", fmt.Errorf("decode Storage Gateway snapshot: %w", err)
	}

	gateway, _ := storageGatewayVolumeFromARN(snap.VolumeArn)
	if v := mdValue(md, "GatewayArn"); v != "" {
		if g, ok := storageGatewayFromARN(v); ok {
			gateway = g
		}
	}

	volume := awsid.NewN("vol-", sgwVolumeIDDigits)
	restored := arn.Build(storageGatewayService, e.region, e.accountID, "gateway/"+gateway+"/volume/"+volume)

	logger.Load(ctx).InfoContext(ctx, "backup: would create Storage Gateway volume from recovery point",
		"sourceVolumeArn", snap.VolumeArn, "gatewayId", gateway, "volumeArn", restored)

	return restored, nil
}

// storageGatewayVolumeFromARN splits arn:aws:storagegateway:r:a:gateway/{gw}/volume/{vol}.
func storageGatewayVolumeFromARN(resourceArn string) (string, bool) {
	parts := strings.SplitN(resourceArn, ":", arnResourceParts)
	if len(parts) < arnResourceParts || parts[2] != storageGatewayService {
		return "", false
	}

	seg := strings.Split(parts[5], "/")
	if len(seg) != storageGatewayVolumeSegments || seg[0] != "gateway" || seg[2] != "volume" {
		return "", false
	}

	return seg[1], true
}

func storageGatewayFromARN(gatewayArn string) (string, bool) {
	parts := strings.SplitN(gatewayArn, ":", arnResourceParts)
	if len(parts) < arnResourceParts || parts[2] != storageGatewayService {
		return "", false
	}

	id, ok := strings.CutPrefix(parts[5], "gateway/")

	return id, ok && id != ""
}

const (
	storageGatewayService        = "storagegateway"
	storageGatewayVolumeSegments = 4
)
