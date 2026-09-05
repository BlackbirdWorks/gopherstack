package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// mustDescribeReplicationTasks returns all replication tasks without error (for internal use).
func (b *InMemoryBackend) mustDescribeReplicationTasks(ctx context.Context) []*ReplicationTask {
	list, _ := b.DescribeReplicationTasks(ctx, "")

	return list
}

// ReplicationTaskCDCSettings carries the optional CDC/task-data members
// CreateReplicationTask/ModifyReplicationTask accept beyond the original
// identifier/endpoint/instance/migrationType/tableMappings/settings set --
// see api_op_CreateReplicationTask.go / api_op_ModifyReplicationTask.go,
// databasemigrationservice@v1.66.4. All three are also real top-level
// types.ReplicationTask response members (CdcStartTime is request-only,
// with no matching response field, and is not modeled here).
type ReplicationTaskCDCSettings struct {
	CdcStartPosition string
	CdcStopPosition  string
	TaskData         string
}

// CreateReplicationTask creates a new DMS replication task.
func (b *InMemoryBackend) CreateReplicationTask(
	ctx context.Context,
	identifier, sourceEndpointArn, targetEndpointArn, replicationInstanceArn,
	migrationType, tableMappings, settings string,
	kv map[string]string,
	cdcSettings ReplicationTaskCDCSettings,
) (*ReplicationTask, error) {
	b.mu.Lock("CreateReplicationTask")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.replicationTasks.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf(
			"%w: replication task %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	// Validate referenced resources exist (real AWS returns ResourceNotFoundFault).
	if _, ok := lookupUnique(b.endpointsByARN, regionKey(region, sourceEndpointArn)); !ok {
		return nil, fmt.Errorf("%w: source endpoint %s not found", ErrNotFound, sourceEndpointArn)
	}

	if _, ok := lookupUnique(b.endpointsByARN, regionKey(region, targetEndpointArn)); !ok {
		return nil, fmt.Errorf("%w: target endpoint %s not found", ErrNotFound, targetEndpointArn)
	}

	if _, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, replicationInstanceArn)); !ok {
		return nil, fmt.Errorf(
			"%w: replication instance %s not found",
			ErrNotFound,
			replicationInstanceArn,
		)
	}

	taskARN := arn.Build("dms", region, b.accountID, "task:"+uuid.NewString())
	t := tags.New("dms.task." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	rt := &ReplicationTask{
		ReplicationTaskIdentifier: identifier,
		ReplicationTaskArn:        taskARN,
		SourceEndpointArn:         sourceEndpointArn,
		TargetEndpointArn:         targetEndpointArn,
		ReplicationInstanceArn:    replicationInstanceArn,
		MigrationType:             migrationType,
		TableMappings:             tableMappings,
		ReplicationTaskSettings:   settings,
		Status:                    statusReady,
		AccountID:                 b.accountID,
		Region:                    region,
		CreationTime:              time.Now().UTC(),
		Tags:                      t,
		CdcStartPosition:          cdcSettings.CdcStartPosition,
		CdcStopPosition:           cdcSettings.CdcStopPosition,
		TaskData:                  cdcSettings.TaskData,
	}
	b.replicationTasks.Put(rt)
	if b.tasksByInstanceARN[replicationInstanceArn] == nil {
		b.tasksByInstanceARN[replicationInstanceArn] = make(map[string]struct{})
	}
	b.tasksByInstanceARN[replicationInstanceArn][taskARN] = struct{}{}
	if b.tasksByEndpointARN[sourceEndpointArn] == nil {
		b.tasksByEndpointARN[sourceEndpointArn] = make(map[string]struct{})
	}
	b.tasksByEndpointARN[sourceEndpointArn][taskARN] = struct{}{}
	if b.tasksByEndpointARN[targetEndpointArn] == nil {
		b.tasksByEndpointARN[targetEndpointArn] = make(map[string]struct{})
	}
	b.tasksByEndpointARN[targetEndpointArn][taskARN] = struct{}{}
	cp := *rt

	return &cp, nil
}

// DescribeReplicationTasks returns replication tasks, optionally filtered by ARN or identifier.
func (b *InMemoryBackend) DescribeReplicationTasks(ctx context.Context, arnOrID string) ([]*ReplicationTask, error) {
	b.mu.RLock("DescribeReplicationTasks")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describeByIdentifierOrARN(
		b.replicationTasks, b.replicationTasksByARN, b.replicationTasksByRegion, region, arnOrID,
	), nil
}

// StartReplicationTask transitions a replication task to running status.
func (b *InMemoryBackend) StartReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("StartReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if rt.Status == statusRunning {
		return nil, fmt.Errorf(
			"%w: replication task %s is already running",
			ErrInvalidState,
			arnOrID,
		)
	}

	rt.Status = statusRunning
	b.appendEvent(
		getRegion(ctx, b.region), rt.ReplicationTaskArn, "replication-task",
		"Replication task "+rt.ReplicationTaskIdentifier+" started", []string{eventCategoryStateChange},
	)
	cp := *rt

	return &cp, nil
}

// StopReplicationTask transitions a replication task to stopped status.
// Real AWS rejects stopping a task that is not currently running.
func (b *InMemoryBackend) StopReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("StopReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if rt.Status != statusRunning {
		return nil, fmt.Errorf(
			"%w: replication task %s cannot be stopped; current status is %s",
			ErrInvalidState,
			arnOrID,
			rt.Status,
		)
	}

	rt.Status = statusStopped
	b.appendEvent(
		getRegion(ctx, b.region), rt.ReplicationTaskArn, "replication-task",
		"Replication task "+rt.ReplicationTaskIdentifier+" stopped", []string{eventCategoryStateChange},
	)
	cp := *rt

	return &cp, nil
}

// DeleteReplicationTask deletes a replication task by ARN or identifier.
// AWS does not allow deleting a task while it is running.
func (b *InMemoryBackend) DeleteReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("DeleteReplicationTask")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	deleteTask := func(rt *ReplicationTask, id string) (*ReplicationTask, error) {
		if rt.Status == statusRunning {
			return nil, fmt.Errorf(
				"%w: replication task %s cannot be deleted while running; stop it first",
				ErrInvalidState,
				arnOrID,
			)
		}
		cp := *rt
		rt.Tags.Close()
		b.replicationTasks.Delete(regionKey(region, id))
		// Remove from reverse instance→tasks index.
		if instTasks := b.tasksByInstanceARN[rt.ReplicationInstanceArn]; instTasks != nil {
			delete(instTasks, rt.ReplicationTaskArn)
		}
		// Remove from reverse endpoint→tasks index.
		if epTasks := b.tasksByEndpointARN[rt.SourceEndpointArn]; epTasks != nil {
			delete(epTasks, rt.ReplicationTaskArn)
		}
		if epTasks := b.tasksByEndpointARN[rt.TargetEndpointArn]; epTasks != nil {
			delete(epTasks, rt.ReplicationTaskArn)
		}

		return &cp, nil
	}

	// Try by identifier first, then by ARN index.
	if rt, ok := b.replicationTasks.Get(regionKey(region, arnOrID)); ok {
		return deleteTask(rt, arnOrID)
	}
	if rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, arnOrID)); ok {
		return deleteTask(rt, rt.ReplicationTaskIdentifier)
	}

	return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
}

// findTask locates a replication task by identifier or ARN within the request
// region (must hold a lock).
func (b *InMemoryBackend) findTask(ctx context.Context, arnOrID string) *ReplicationTask {
	region := getRegion(ctx, b.region)
	if rt, ok := b.replicationTasks.Get(regionKey(region, arnOrID)); ok {
		return rt
	}

	if rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, arnOrID)); ok {
		return rt
	}

	return nil
}

// AddReplicationTaskInternal seeds a replication task directly without HTTP.
func (b *InMemoryBackend) AddReplicationTaskInternal(
	identifier, srcARN, tgtARN, instARN, migrationType string,
) {
	b.mu.Lock("AddReplicationTaskInternal")
	defer b.mu.Unlock()
	taskARN := arn.Build("dms", b.region, b.accountID, "task:"+uuid.NewString())
	t := tags.New("dms.task." + identifier + ".tags")
	rt := &ReplicationTask{
		ReplicationTaskIdentifier: identifier,
		ReplicationTaskArn:        taskARN,
		SourceEndpointArn:         srcARN,
		TargetEndpointArn:         tgtARN,
		ReplicationInstanceArn:    instARN,
		MigrationType:             migrationType,
		Status:                    statusReady,
		AccountID:                 b.accountID,
		Region:                    b.region,
		CreationTime:              time.Now().UTC(),
		Tags:                      t,
	}
	b.replicationTasks.Put(rt)
	if b.tasksByInstanceARN[instARN] == nil {
		b.tasksByInstanceARN[instARN] = make(map[string]struct{})
	}
	b.tasksByInstanceARN[instARN][taskARN] = struct{}{}
	if b.tasksByEndpointARN[srcARN] == nil {
		b.tasksByEndpointARN[srcARN] = make(map[string]struct{})
	}
	b.tasksByEndpointARN[srcARN][taskARN] = struct{}{}
	if b.tasksByEndpointARN[tgtARN] == nil {
		b.tasksByEndpointARN[tgtARN] = make(map[string]struct{})
	}
	b.tasksByEndpointARN[tgtARN][taskARN] = struct{}{}
}

// ModifyReplicationTask updates task settings.
// AWS does not allow modifying a running task.
func (b *InMemoryBackend) ModifyReplicationTask(
	ctx context.Context,
	arnOrID, migrationType, tableMappings, replicationTaskSettings string,
	cdcSettings ReplicationTaskCDCSettings,
) (*ReplicationTask, error) {
	b.mu.Lock("ModifyReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if rt.Status == statusRunning {
		return nil, fmt.Errorf(
			"%w: replication task %s cannot be modified while running; stop it first",
			ErrInvalidState,
			arnOrID,
		)
	}

	if migrationType != "" {
		rt.MigrationType = migrationType
	}

	if tableMappings != "" {
		rt.TableMappings = tableMappings
	}

	if replicationTaskSettings != "" {
		rt.ReplicationTaskSettings = replicationTaskSettings
	}

	if cdcSettings.CdcStartPosition != "" {
		rt.CdcStartPosition = cdcSettings.CdcStartPosition
	}

	if cdcSettings.CdcStopPosition != "" {
		rt.CdcStopPosition = cdcSettings.CdcStopPosition
	}

	if cdcSettings.TaskData != "" {
		rt.TaskData = cdcSettings.TaskData
	}

	cp := *rt

	return &cp, nil
}

// ReloadTables reloads the target tables of a running replication task with
// source data. Real AWS only permits this while the task is RUNNING,
// otherwise it throws InvalidResourceStateFault.
func (b *InMemoryBackend) ReloadTables(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("ReloadTables")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if rt.Status != statusRunning {
		return nil, fmt.Errorf(
			"%w: replication task %s must be running to reload tables; current status is %s",
			ErrInvalidState,
			arnOrID,
			rt.Status,
		)
	}

	cp := *rt

	return &cp, nil
}

// MoveReplicationTask moves a replication task to a different instance.
func (b *InMemoryBackend) MoveReplicationTask(
	ctx context.Context,
	taskArnOrID, targetInstanceArn string,
) (*ReplicationTask, error) {
	b.mu.Lock("MoveReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, taskArnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, taskArnOrID)
	}

	rt.ReplicationInstanceArn = targetInstanceArn
	cp := *rt

	return &cp, nil
}
