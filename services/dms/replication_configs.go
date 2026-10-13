package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateReplicationConfig creates a replication config.
func (b *InMemoryBackend) CreateReplicationConfig(
	ctx context.Context,
	params CreateReplicationConfigParams,
	kv map[string]string,
) (*ReplicationConfig, error) {
	b.mu.Lock("CreateReplicationConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.replicationConfigs.Has(regionKey(region, params.Identifier)) {
		return nil, fmt.Errorf(
			"%w: replication config %s already exists",
			ErrAlreadyExists,
			params.Identifier,
		)
	}

	arnSuffix := params.ResourceIdentifier
	if arnSuffix == "" {
		arnSuffix = uuid.NewString()
	}

	configARN := arn.Build("dms", region, b.accountID, "replication-config:"+arnSuffix)
	t := tags.New("dms.replication-config." + params.Identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	rc := &ReplicationConfig{
		ReplicationConfigIdentifier: params.Identifier,
		ReplicationConfigArn:        configARN,
		ReplicationType:             params.ReplicationType,
		SourceEndpointArn:           params.SourceEndpointArn,
		TargetEndpointArn:           params.TargetEndpointArn,
		TableMappings:               params.TableMappings,
		ReplicationSettings:         params.ReplicationSettings,
		SupplementalSettings:        params.SupplementalSettings,
		ComputeConfig:               params.ComputeConfig,
		AccountID:                   b.accountID,
		Region:                      region,
		Status:                      statusCreated,
		Tags:                        t,
	}
	b.replicationConfigs.Put(rc)
	cp := *rc

	return &cp, nil
}

// DeleteReplicationConfig deletes a replication config by identifier or ARN.
// Real AWS: "You can't delete the configuration for an DMS Serverless
// replication that is ongoing. You can delete the configuration when the
// replication is in a non-RUNNING and non-STARTING state".
func (b *InMemoryBackend) DeleteReplicationConfig(ctx context.Context, identifierOrArn string) error {
	b.mu.Lock("DeleteReplicationConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if rc, ok := b.replicationConfigs.Get(regionKey(region, identifierOrArn)); ok {
		if rc.Status == statusRunning {
			return fmt.Errorf("%w: replication config %s is running", ErrInvalidState, identifierOrArn)
		}

		rc.Tags.Close()
		b.replicationConfigs.Delete(regionKey(region, identifierOrArn))

		return nil
	}

	if rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(region, identifierOrArn)); ok {
		if rc.Status == statusRunning {
			return fmt.Errorf("%w: replication config %s is running", ErrInvalidState, identifierOrArn)
		}

		rc.Tags.Close()
		b.replicationConfigs.Delete(regionKey(region, rc.ReplicationConfigIdentifier))

		return nil
	}

	return fmt.Errorf("%w: replication config %s not found", ErrNotFound, identifierOrArn)
}

// DescribeReplicationConfigs returns all replication configs.
func (b *InMemoryBackend) DescribeReplicationConfigs(ctx context.Context) ([]*ReplicationConfig, error) {
	b.mu.RLock("DescribeReplicationConfigs")
	defer b.mu.RUnlock()

	items := b.replicationConfigsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*ReplicationConfig, 0, len(items))
	for _, rc := range items {
		cp := *rc
		list = append(list, &cp)
	}

	return list, nil
}

func modifyReplicationConfigFields(rc *ReplicationConfig, p ModifyReplicationConfigParams) {
	if p.ReplicationType != "" {
		rc.ReplicationType = p.ReplicationType
	}

	if p.TableMappings != "" {
		rc.TableMappings = p.TableMappings
	}

	if p.SourceEndpointArn != "" {
		rc.SourceEndpointArn = p.SourceEndpointArn
	}

	if p.TargetEndpointArn != "" {
		rc.TargetEndpointArn = p.TargetEndpointArn
	}

	if p.ReplicationSettings != "" {
		rc.ReplicationSettings = p.ReplicationSettings
	}

	if p.SupplementalSettings != "" {
		rc.SupplementalSettings = p.SupplementalSettings
	}

	if p.ComputeConfig != nil {
		rc.ComputeConfig = p.ComputeConfig
	}
}

// ModifyReplicationConfig applies the supplied members to an existing
// replication config; omitted members keep their stored values.
func (b *InMemoryBackend) ModifyReplicationConfig(
	ctx context.Context,
	p ModifyReplicationConfigParams,
) (*ReplicationConfig, error) {
	b.mu.Lock("ModifyReplicationConfig")
	defer b.mu.Unlock()

	rc := b.findReplicationConfig(ctx, p.IdentifierOrArn)
	if rc == nil {
		return nil, fmt.Errorf("%w: replication config %s not found", ErrNotFound, p.IdentifierOrArn)
	}

	if err := rekey(b.replicationConfigs, getRegion(ctx, b.region), rc.ReplicationConfigIdentifier,
		p.NewIdentifier, "replication config", rc,
		func(n string) { rc.ReplicationConfigIdentifier = n }); err != nil {
		return nil, err
	}

	modifyReplicationConfigFields(rc, p)
	cp := *rc

	return &cp, nil
}

// findReplicationConfig locates a replication config by identifier or ARN
// within the request region (must hold a lock).
func (b *InMemoryBackend) findReplicationConfig(ctx context.Context, arnOrID string) *ReplicationConfig {
	region := getRegion(ctx, b.region)
	if rc, ok := b.replicationConfigs.Get(regionKey(region, arnOrID)); ok {
		return rc
	}

	if rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(region, arnOrID)); ok {
		return rc
	}

	return nil
}

// StartReplicationCDC carries StartReplicationInput's CDC window, echoed on the Replication.
type StartReplicationCDC struct {
	StartTime     *time.Time
	StartPosition string
	StopPosition  string
}

// StartReplication starts (or resumes) the DMS Serverless replication
// associated with a replication config. Real AWS rejects starting a
// replication that is already running.
func (b *InMemoryBackend) StartReplication(
	ctx context.Context,
	replicationConfigArn, startReplicationType string,
	cdc StartReplicationCDC,
) (*ReplicationConfig, error) {
	b.mu.Lock("StartReplication")
	defer b.mu.Unlock()

	rc := b.findReplicationConfig(ctx, replicationConfigArn)
	if rc == nil {
		return nil, fmt.Errorf("%w: replication config %s not found", ErrNotFound, replicationConfigArn)
	}

	if rc.Status == statusRunning {
		return nil, fmt.Errorf(
			"%w: replication for %s is already running",
			ErrInvalidState,
			replicationConfigArn,
		)
	}

	rc.Status = statusRunning
	rc.StartReplicationType = startReplicationType
	rc.CdcStartPosition = cdc.StartPosition
	rc.CdcStartTime = cdc.StartTime
	rc.CdcStopPosition = cdc.StopPosition
	cp := *rc

	return &cp, nil
}

// StopReplication stops the DMS Serverless replication associated with a
// replication config. Real AWS rejects stopping a replication that is not
// currently running.
func (b *InMemoryBackend) StopReplication(
	ctx context.Context,
	replicationConfigArn string,
) (*ReplicationConfig, error) {
	b.mu.Lock("StopReplication")
	defer b.mu.Unlock()

	rc := b.findReplicationConfig(ctx, replicationConfigArn)
	if rc == nil {
		return nil, fmt.Errorf("%w: replication config %s not found", ErrNotFound, replicationConfigArn)
	}

	if rc.Status != statusRunning {
		return nil, fmt.Errorf(
			"%w: replication for %s is not running",
			ErrInvalidState,
			replicationConfigArn,
		)
	}

	rc.Status = statusStopped
	cp := *rc

	return &cp, nil
}

// ReloadReplicationTables reloads the target tables of a running DMS
// Serverless replication with source data. Real AWS only permits this while
// the replication is RUNNING, otherwise it throws InvalidResourceStateFault.
func (b *InMemoryBackend) ReloadReplicationTables(ctx context.Context, arnOrID string) (*ReplicationConfig, error) {
	b.mu.Lock("ReloadReplicationTables")
	defer b.mu.Unlock()

	rc := b.findReplicationConfig(ctx, arnOrID)
	if rc == nil {
		return nil, fmt.Errorf("%w: replication config %s not found", ErrNotFound, arnOrID)
	}

	if rc.Status != statusRunning {
		return nil, fmt.Errorf(
			"%w: replication for %s must be running to reload tables; current status is %s",
			ErrInvalidState,
			arnOrID,
			rc.Status,
		)
	}

	cp := *rc

	return &cp, nil
}

// DescribeReplications returns DMS Serverless replication runtime state,
// backed by the replication configs that have had StartReplication called
// against them at least once. A config that has never been started still
// carries a valid Status ("created") from CreateReplicationConfig, matching
// AWS's behavior of DescribeReplications listing every config regardless of
// whether it has ever run. Matched against filters (valid filter names per
// api_op_DescribeReplications.go: replication-config-arn |
// replication-config-id).
func (b *InMemoryBackend) DescribeReplications(
	ctx context.Context,
	filters DescribeFilters,
) ([]*ReplicationConfig, error) {
	b.mu.RLock("DescribeReplications")
	defer b.mu.RUnlock()

	items := b.replicationConfigsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*ReplicationConfig, 0, len(items))

	for _, rc := range items {
		if !filters.Matches("replication-config-arn", rc.ReplicationConfigArn) {
			continue
		}

		if !filters.Matches("replication-config-id", rc.ReplicationConfigIdentifier) {
			continue
		}

		cp := *rc
		list = append(list, &cp)
	}

	return list, nil
}
