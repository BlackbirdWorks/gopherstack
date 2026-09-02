package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const defaultAllocatedStorage int32 = 50

// mustDescribeReplicationInstances returns all replication instances without error (for internal use).
func (b *InMemoryBackend) mustDescribeReplicationInstances(ctx context.Context) []*ReplicationInstance {
	list, _ := b.DescribeReplicationInstances(ctx, "")

	return list
}

// ReplicationInstanceSettings carries the optional top-level
// ReplicationInstance settings CreateReplicationInstance/
// ModifyReplicationInstance accept beyond the original identifier/class/
// engineVersion/availabilityZone/allocatedStorage/multiAZ/... set -- see
// api_op_CreateReplicationInstance.go / api_op_ModifyReplicationInstance.go,
// databasemigrationservice@v1.66.4. KmsKeyID and ReplicationSubnetGroupID are
// create-only (neither is a ModifyReplicationInstanceInput member) and are
// ignored by ModifyReplicationInstance. VpcSecurityGroupIDs is accepted by
// both.
type ReplicationInstanceSettings struct {
	KmsKeyID                   string
	DNSNameServers             string
	NetworkType                string
	PreferredMaintenanceWindow string
	ReplicationSubnetGroupID   string
	VpcSecurityGroupIDs        []string
}

// CreateReplicationInstance creates a new DMS replication instance.
func (b *InMemoryBackend) CreateReplicationInstance(
	ctx context.Context,
	identifier, class, engineVersion, availabilityZone string,
	allocatedStorage int32,
	multiAZ, autoMinorVersionUpgrade, publiclyAccessible bool,
	kv map[string]string,
	settings ReplicationInstanceSettings,
) (*ReplicationInstance, error) {
	b.mu.Lock("CreateReplicationInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.replicationInstances.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf(
			"%w: replication instance %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	sgKey := regionKey(region, settings.ReplicationSubnetGroupID)
	if settings.ReplicationSubnetGroupID != "" && !b.replicationSubnetGroups.Has(sgKey) {
		return nil, fmt.Errorf(
			"%w: replication subnet group %s not found",
			ErrNotFound,
			settings.ReplicationSubnetGroupID,
		)
	}

	instanceARN := arn.Build("dms", region, b.accountID, "rep:"+identifier)
	t := tags.New("dms.replication-instance." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	if allocatedStorage == 0 {
		allocatedStorage = defaultAllocatedStorage
	}

	ri := &ReplicationInstance{
		ReplicationInstanceIdentifier: identifier,
		ReplicationInstanceArn:        instanceARN,
		ReplicationInstanceClass:      class,
		EngineVersion:                 engineVersion,
		AvailabilityZone:              availabilityZone,
		AllocatedStorage:              allocatedStorage,
		MultiAZ:                       multiAZ,
		AutoMinorVersionUpgrade:       autoMinorVersionUpgrade,
		PubliclyAccessible:            publiclyAccessible,
		ReplicationInstanceStatus:     statusAvailable,
		PrivateIPAddress:              "10.0.0.1",
		AccountID:                     b.accountID,
		Region:                        region,
		CreationTime:                  time.Now().UTC(),
		Tags:                          t,
		KmsKeyID:                      settings.KmsKeyID,
		DNSNameServers:                settings.DNSNameServers,
		NetworkType:                   settings.NetworkType,
		PreferredMaintenanceWindow:    settings.PreferredMaintenanceWindow,
		ReplicationSubnetGroupID:      settings.ReplicationSubnetGroupID,
		VpcSecurityGroupIDs:           settings.VpcSecurityGroupIDs,
	}
	b.replicationInstances.Put(ri)
	cp := *ri

	return &cp, nil
}

// DescribeReplicationInstances returns replication instances, optionally filtered by identifier or ARN.
func (b *InMemoryBackend) DescribeReplicationInstances(
	ctx context.Context,
	identifierOrArn string,
) ([]*ReplicationInstance, error) {
	b.mu.RLock("DescribeReplicationInstances")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describeByIdentifierOrARN(
		b.replicationInstances, b.replicationInstancesByARN, b.replicationInstancesByRegion, region, identifierOrArn,
	), nil
}

// DeleteReplicationInstance deletes a replication instance by ARN or identifier.
// AWS requires all replication tasks on the instance to be deleted first.
func (b *InMemoryBackend) DeleteReplicationInstance(ctx context.Context, arnOrID string) error {
	b.mu.Lock("DeleteReplicationInstance")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	deleteInstance := func(ri *ReplicationInstance, id string) error {
		// O(1) check via reverse index instead of scanning all tasks.
		if len(b.tasksByInstanceARN[ri.ReplicationInstanceArn]) > 0 {
			return fmt.Errorf(
				"%w: replication instance %s has tasks attached; delete all tasks first",
				ErrInvalidState,
				arnOrID,
			)
		}
		ri.Tags.Close()
		delete(b.tasksByInstanceARN, ri.ReplicationInstanceArn)
		b.replicationInstances.Delete(regionKey(region, id))

		return nil
	}

	// Try by identifier first, then by ARN index.
	if ri, ok := b.replicationInstances.Get(regionKey(region, arnOrID)); ok {
		return deleteInstance(ri, arnOrID)
	}
	if ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, arnOrID)); ok {
		return deleteInstance(ri, ri.ReplicationInstanceIdentifier)
	}

	return fmt.Errorf("%w: replication instance %s not found", ErrNotFound, arnOrID)
}

// AddReplicationInstanceInternal seeds a replication instance directly without HTTP.
func (b *InMemoryBackend) AddReplicationInstanceInternal(identifier, class string) {
	b.mu.Lock("AddReplicationInstanceInternal")
	defer b.mu.Unlock()
	instanceARN := arn.Build("dms", b.region, b.accountID, "rep:"+identifier)
	t := tags.New("dms.replication-instance." + identifier + ".tags")
	ri := &ReplicationInstance{
		ReplicationInstanceIdentifier: identifier,
		ReplicationInstanceArn:        instanceARN,
		ReplicationInstanceClass:      class,
		EngineVersion:                 defaultEngineVersion,
		ReplicationInstanceStatus:     statusAvailable,
		PrivateIPAddress:              "10.0.0.1",
		AllocatedStorage:              defaultAllocatedStorage,
		AccountID:                     b.accountID,
		Region:                        b.region,
		CreationTime:                  time.Now().UTC(),
		Tags:                          t,
	}
	b.replicationInstances.Put(ri)
}

// ModifyReplicationInstance updates a replication instance's class and engineVersion.
func (b *InMemoryBackend) ModifyReplicationInstance(
	ctx context.Context,
	arnOrID, class, engineVersion string,
	multiAZ, autoMinorVersionUpgrade *bool,
	allocatedStorage *int32,
	settings ReplicationInstanceSettings,
) (*ReplicationInstance, error) {
	b.mu.Lock("ModifyReplicationInstance")
	defer b.mu.Unlock()

	ri := b.findReplicationInstance(ctx, arnOrID)
	if ri == nil {
		return nil, fmt.Errorf("%w: replication instance %s not found", ErrNotFound, arnOrID)
	}

	if class != "" {
		ri.ReplicationInstanceClass = class
	}

	if engineVersion != "" {
		ri.EngineVersion = engineVersion
	}

	if multiAZ != nil {
		ri.MultiAZ = *multiAZ
	}

	if autoMinorVersionUpgrade != nil {
		ri.AutoMinorVersionUpgrade = *autoMinorVersionUpgrade
	}

	if allocatedStorage != nil {
		ri.AllocatedStorage = *allocatedStorage
	}

	if settings.NetworkType != "" {
		ri.NetworkType = settings.NetworkType
	}

	if settings.PreferredMaintenanceWindow != "" {
		ri.PreferredMaintenanceWindow = settings.PreferredMaintenanceWindow
	}

	if settings.VpcSecurityGroupIDs != nil {
		ri.VpcSecurityGroupIDs = settings.VpcSecurityGroupIDs
	}

	cp := *ri

	return &cp, nil
}

// findReplicationInstance locates a replication instance by identifier or ARN
// within the request region (must hold a lock).
func (b *InMemoryBackend) findReplicationInstance(ctx context.Context, arnOrID string) *ReplicationInstance {
	region := getRegion(ctx, b.region)
	if ri, ok := b.replicationInstances.Get(regionKey(region, arnOrID)); ok {
		return ri
	}

	if ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, arnOrID)); ok {
		return ri
	}

	return nil
}

// RebootReplicationInstance reboots a replication instance (no-op in memory).
func (b *InMemoryBackend) RebootReplicationInstance(ctx context.Context, arnOrID string) (*ReplicationInstance, error) {
	b.mu.RLock("RebootReplicationInstance")
	defer b.mu.RUnlock()

	ri := b.findReplicationInstance(ctx, arnOrID)
	if ri == nil {
		return nil, fmt.Errorf("%w: replication instance %s not found", ErrNotFound, arnOrID)
	}

	cp := *ri

	return &cp, nil
}
