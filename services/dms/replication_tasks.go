package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// mustDescribeReplicationTasks returns all replication tasks without error (for internal use).
func (b *InMemoryBackend) mustDescribeReplicationTasks(ctx context.Context) []*ReplicationTask {
	list, _ := b.DescribeReplicationTasks(ctx, DescribeFilters{})

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
	CdcStartPosition   string
	CdcStopPosition    string
	StartType          string
	TaskData           string
	ResourceIdentifier string
	NewIdentifier      string
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

	taskID := cdcSettings.ResourceIdentifier
	if taskID == "" {
		taskID = uuid.NewString()
	}

	taskARN := arn.Build("dms", region, b.accountID, "task:"+taskID)
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

	return b.viewTask(rt), nil
}

func taskActive(rt *ReplicationTask) bool {
	return rt.Status == statusRunning || rt.Status == statusStarting || rt.Status == statusStopping
}

// DescribeReplicationTasks returns replication tasks matching filters (valid
// filter names per api_op_DescribeReplicationTasks.go: replication-task-arn |
// replication-task-id | migration-type | endpoint-arn |
// replication-instance-arn; endpoint-arn matches either the task's source or
// target endpoint).
func (b *InMemoryBackend) DescribeReplicationTasks(
	ctx context.Context,
	filters DescribeFilters,
) ([]*ReplicationTask, error) {
	b.mu.Lock("DescribeReplicationTasks")
	defer b.mu.Unlock()

	items := b.replicationTasksByRegion.Get(getRegion(ctx, b.region))
	result := make([]*ReplicationTask, 0, len(items))

	for _, rt := range items {
		if !replicationTaskMatchesFilters(rt, filters) {
			continue
		}

		result = append(result, b.viewTask(rt))

		if rt.Status == statusStarting && !b.hasRunLocked(rt.ReplicationTaskArn) {
			rt.Status = statusRunning
		}
	}

	return result, nil
}

// replicationTaskMatchesFilters applies every documented
// DescribeReplicationTasks filter name to a single task.
func replicationTaskMatchesFilters(rt *ReplicationTask, filters DescribeFilters) bool {
	if !filters.Matches("replication-task-id", rt.ReplicationTaskIdentifier) {
		return false
	}

	if !filters.Matches("replication-task-arn", rt.ReplicationTaskArn) {
		return false
	}

	if !filters.Matches("migration-type", rt.MigrationType) {
		return false
	}

	if !filters.MatchesAny("endpoint-arn", rt.SourceEndpointArn, rt.TargetEndpointArn) {
		return false
	}

	return filters.Matches("replication-instance-arn", rt.ReplicationInstanceArn)
}

// StartReplicationTask starts a replication task. With the engine enabled and both endpoints supported the task
// moves data; otherwise it only changes state.
func (b *InMemoryBackend) StartReplicationTask(
	ctx context.Context, arnOrID string, cdc ReplicationTaskCDCSettings,
) (*ReplicationTask, error) {
	b.mu.Lock("StartReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if taskActive(rt) {
		return nil, fmt.Errorf(
			"%w: replication task %s is already running",
			ErrInvalidState,
			arnOrID,
		)
	}

	region := getRegion(ctx, b.region)

	ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, rt.ReplicationInstanceArn))
	if ok && ri.ReplicationInstanceStatus != statusAvailable {
		return nil, fmt.Errorf(
			"%w: replication instance %s is %s; it must be available to start a task",
			ErrInvalidState, ri.ReplicationInstanceIdentifier, ri.ReplicationInstanceStatus,
		)
	}

	if cdc.CdcStartPosition != "" {
		rt.CdcStartPosition = cdc.CdcStartPosition
	}

	if cdc.CdcStopPosition != "" {
		rt.CdcStopPosition = cdc.CdcStopPosition
	}

	b.beginStartLocked(rt, cdc.StartType)

	if src, tgt, found := b.taskEndpoints(region, rt); found && b.engineEligible(src, tgt) {
		b.launchLocked(region, rt, src, tgt, cdc.StartType)
	}

	b.appendEvent(
		region, rt.ReplicationTaskArn, "replication-task",
		"Replication task "+rt.ReplicationTaskIdentifier+" started", []string{eventCategoryStateChange},
	)

	return b.viewTask(rt), nil
}

// StopReplicationTask stops a replication task. Real AWS rejects stopping a task that is not running; an engine
// run reports "stopping" until its goroutine has wound down.
func (b *InMemoryBackend) StopReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("StopReplicationTask")
	defer b.mu.Unlock()

	rt := b.findTask(ctx, arnOrID)
	if rt == nil {
		return nil, fmt.Errorf("%w: replication task %s not found", ErrNotFound, arnOrID)
	}

	if !taskActive(rt) {
		return nil, fmt.Errorf(
			"%w: replication task %s cannot be stopped; current status is %s",
			ErrInvalidState,
			arnOrID,
			rt.Status,
		)
	}

	if b.stopRunLocked(rt) {
		rt.Status = statusStopping

		return b.viewTask(rt), nil
	}

	rt.Status, rt.StopDate, rt.StopReason = statusStopped, b.clock(), "Stop Reason NORMAL"
	b.appendEvent(
		getRegion(ctx, b.region), rt.ReplicationTaskArn, "replication-task",
		"Replication task "+rt.ReplicationTaskIdentifier+" stopped", []string{eventCategoryStateChange},
	)

	return b.viewTask(rt), nil
}

// DeleteReplicationTask deletes a replication task by ARN or identifier.
// AWS does not allow deleting a task while it is running.
func (b *InMemoryBackend) DeleteReplicationTask(ctx context.Context, arnOrID string) (*ReplicationTask, error) {
	b.mu.Lock("DeleteReplicationTask")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	deleteTask := func(rt *ReplicationTask, id string) (*ReplicationTask, error) {
		if taskActive(rt) {
			return nil, fmt.Errorf(
				"%w: replication task %s cannot be deleted while running; stop it first",
				ErrInvalidState,
				arnOrID,
			)
		}
		cp := *rt
		if src, _, found := b.taskEndpoints(region, rt); found {
			b.releaseSlotAsync(src, rt.ReplicationTaskArn)
		}
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

	if taskActive(rt) {
		return nil, fmt.Errorf(
			"%w: replication task %s cannot be modified while running; stop it first",
			ErrInvalidState,
			arnOrID,
		)
	}

	if err := rekey(b.replicationTasks, getRegion(ctx, b.region), rt.ReplicationTaskIdentifier,
		cdcSettings.NewIdentifier, "replication task", rt,
		func(n string) { rt.ReplicationTaskIdentifier = n }); err != nil {
		return nil, err
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

	return b.viewTask(rt), nil
}

// ReloadTables reloads tables of a running replication task from the source. Real AWS only permits this while
// the task is RUNNING, otherwise it throws InvalidResourceStateFault. Engine-backed tasks re-copy the named
// tables asynchronously; an empty refs list reloads nothing.
func (b *InMemoryBackend) ReloadTables(
	ctx context.Context, arnOrID string, refs []dmsengine.TableRef,
) (*ReplicationTask, error) {
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

	if run, ok := b.eng.runsFor(rt.ReplicationTaskArn); ok && run.task != nil {
		for _, ref := range refs {
			if !run.task.Has(ref) {
				return nil, fmt.Errorf(
					"%w: table %s.%s is not part of task %s",
					ErrNotFound,
					ref.Schema,
					ref.Name,
					arnOrID,
				)
			}
		}

		b.reloadRun(run, run.task, refs)
	}

	return b.viewTask(rt), nil
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
