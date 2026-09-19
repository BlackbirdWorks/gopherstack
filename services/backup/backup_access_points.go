package backup

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateBackupAccessPoint creates a backup access point exposing an existing
// recovery point for on-demand, read-only access via an Amazon S3 access
// point (backup@v1.64.0 api_op_CreateBackupAccessPoint.go). Real AWS
// requires the recovery point to be an S3 recovery point in the
// AVAILABLE/STOPPED/COMPLETED state; this backend requires it to exist at
// all and stays permissive on ResourceType/status, the same
// unwired-hook-stays-permissive convention StartBackupJob's ResourceArn
// check documents (gopherstack-0o0q) -- rejecting on a distinction this
// backend doesn't track everywhere would be a fabricated rejection. Starts
// in CREATING; the Janitor (advanceBackupAccessPoints) advances it to
// AVAILABLE on the next sweep, mirroring advanceRestoreAccessVaults.
func (b *InMemoryBackend) CreateBackupAccessPoint(
	name, recoveryPointArn, accessPointPolicy string,
	metadata, kv map[string]string,
) (*AccessPoint, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if recoveryPointArn == "" {
		return nil, fmt.Errorf("%w: RecoveryPointArn is required", ErrValidation)
	}

	b.mu.Lock("CreateBackupAccessPoint")
	defer b.mu.Unlock()

	rp, ok := b.findRecoveryPointByArn(recoveryPointArn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: recovery point %s not found", ErrNotFound, recoveryPointArn,
		)
	}

	accessPointArn := arn.Build("backup", b.region, b.accountID, "backup-access-point:"+name)
	if b.backupAccessPoints.Has(accessPointArn) {
		return nil, fmt.Errorf("%w: backup access point %s already exists", ErrAlreadyExists, name)
	}

	md := make(map[string]string, len(metadata))
	maps.Copy(md, metadata)

	t := tags.New("backup.backup-access-point." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	bap := &AccessPoint{
		CreationTime:        time.Now().UTC(),
		Tags:                t,
		AccessPointMetadata: md,
		AccessPointArn:      accessPointArn,
		Name:                name,
		AccessPointPolicy:   accessPointPolicy,
		BackupVaultName:     rp.BackupVaultName,
		BackupVaultArn:      rp.BackupVaultArn,
		RecoveryPointArn:    recoveryPointArn,
		ResourceArn:         rp.ResourceArn,
		ResourceType:        rp.ResourceType,
		Status:              statusCreating,
	}
	b.backupAccessPoints.Put(bap)
	cp := *bap

	return &cp, nil
}

// DescribeBackupAccessPoint returns a backup access point by ARN.
func (b *InMemoryBackend) DescribeBackupAccessPoint(accessPointArn string) (*AccessPoint, error) {
	b.mu.RLock("DescribeBackupAccessPoint")
	defer b.mu.RUnlock()

	bap, ok := b.backupAccessPoints.Get(accessPointArn)
	if !ok {
		return nil, fmt.Errorf("%w: backup access point %s not found", ErrNotFound, accessPointArn)
	}
	cp := *bap

	return &cp, nil
}

// DeleteBackupAccessPoint deletes a backup access point by ARN.
func (b *InMemoryBackend) DeleteBackupAccessPoint(accessPointArn string) error {
	b.mu.Lock("DeleteBackupAccessPoint")
	defer b.mu.Unlock()

	bap, ok := b.backupAccessPoints.Get(accessPointArn)
	if !ok {
		return fmt.Errorf("%w: backup access point %s not found", ErrNotFound, accessPointArn)
	}

	if bap.Tags != nil {
		bap.Tags.Close()
	}

	b.backupAccessPoints.Delete(accessPointArn)

	return nil
}

func sortBackupAccessPoints(list []*AccessPoint) {
	slices.SortFunc(list, func(a, c *AccessPoint) int {
		return strings.Compare(a.AccessPointArn, c.AccessPointArn)
	})
}

func backupAccessPointArnKeyFn(v *AccessPoint) string { return v.AccessPointArn }

// ListBackupAccessPoints returns every backup access point in the account/region.
func (b *InMemoryBackend) ListBackupAccessPoints(
	maxResults int, nextToken string,
) ([]*AccessPoint, string) {
	b.mu.RLock("ListBackupAccessPoints")
	all := b.backupAccessPoints.All()
	list := make([]*AccessPoint, 0, len(all))

	for _, v := range all {
		cp := *v
		list = append(list, &cp)
	}
	b.mu.RUnlock()

	sortBackupAccessPoints(list)

	return paginateByID(list, backupAccessPointArnKeyFn, maxResults, nextToken)
}

// ListBackupAccessPointsByRecoveryPoint returns the backup access points
// created from the given recovery point ARN. Permissive on an unknown
// recovery point ARN (returns an empty page), matching this service's other
// List* filters over a store rather than validating the filter's target.
func (b *InMemoryBackend) ListBackupAccessPointsByRecoveryPoint(
	recoveryPointArn string, maxResults int, nextToken string,
) ([]*AccessPoint, string) {
	b.mu.RLock("ListBackupAccessPointsByRecoveryPoint")
	group := b.backupAccessPointsByRecovery.Get(recoveryPointArn)
	list := make([]*AccessPoint, 0, len(group))

	for _, v := range group {
		cp := *v
		list = append(list, &cp)
	}
	b.mu.RUnlock()

	sortBackupAccessPoints(list)

	return paginateByID(list, backupAccessPointArnKeyFn, maxResults, nextToken)
}

// ListBackupAccessPointsByResource returns the backup access points
// associated with the given backed-up resource ARN.
func (b *InMemoryBackend) ListBackupAccessPointsByResource(
	resourceArn string, maxResults int, nextToken string,
) ([]*AccessPoint, string) {
	b.mu.RLock("ListBackupAccessPointsByResource")
	group := b.backupAccessPointsByResource.Get(resourceArn)
	list := make([]*AccessPoint, 0, len(group))

	for _, v := range group {
		cp := *v
		list = append(list, &cp)
	}
	b.mu.RUnlock()

	sortBackupAccessPoints(list)

	return paginateByID(list, backupAccessPointArnKeyFn, maxResults, nextToken)
}

// s3AccessPointArnFor synthesizes the underlying S3 access point ARN a
// backup access point exposes once AVAILABLE. Real AWS documents that
// AccessPointMetadata gains S3AccessPointArn/S3AccessPointAlias at that
// point but does not publish their exact derivation; this emulator has no
// real S3-access-point subsystem to delegate to, so it synthesizes a
// plausible value from the real S3 access point ARN shape
// (arn:{partition}:s3:{region}:{account}:accesspoint/{name}), the same
// synthesize-a-plausible-value approach restore_jobs.go's CreatedResourceArn
// already documents for a comparable gap.
func s3AccessPointArnFor(region, accountID, name string) string {
	return arn.Build("s3", region, accountID, "accesspoint/"+name)
}

// s3AccessPointAliasFor synthesizes an S3 access point alias in the real
// "{name}-{accountId}.s3-accesspoint.amazonaws.com" hostname shape (see
// s3AccessPointArnFor's rationale -- same disclosed synthesis, not a real
// generated alias).
func s3AccessPointAliasFor(accountID, name string) string {
	return name + "-" + accountID + ".s3-accesspoint.amazonaws.com"
}
