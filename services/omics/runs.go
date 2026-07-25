package omics

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ────────────────────────────────────────────────────────────────────────────
// RunGroup
// ────────────────────────────────────────────────────────────────────────────

// CreateRunGroup creates a new run group.
func (b *InMemoryBackend) CreateRunGroup(
	name string,
	maxCPUs, maxRuns, maxDuration int,
	maxGPUs int,
	tags map[string]string,
) (*RunGroup, error) {
	b.mu.Lock("CreateRunGroup")
	defer b.mu.Unlock()

	id := newID()
	rg := &RunGroup{
		ID:           id,
		Name:         name,
		MaxCPUs:      maxCPUs,
		MaxRuns:      maxRuns,
		MaxDuration:  maxDuration,
		MaxGPUs:      maxGPUs,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	rg.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "runGroup/"+id)

	b.runGroups.Put(rg)

	if tags != nil {
		b.tags[rg.Arn] = copyTags(tags)
	}

	result := *rg

	return &result, nil
}

// DeleteRunGroup deletes a run group.
func (b *InMemoryBackend) DeleteRunGroup(id string) error {
	b.mu.Lock("DeleteRunGroup")
	defer b.mu.Unlock()

	rg, ok := b.runGroups.Get(id)
	if !ok {
		return fmt.Errorf("%w: run group %s not found", ErrNotFound, id)
	}

	delete(b.tags, rg.Arn)
	b.runGroups.Delete(id)

	return nil
}

// GetRunGroup retrieves a run group.
func (b *InMemoryBackend) GetRunGroup(id string) (*RunGroup, error) {
	b.mu.RLock("GetRunGroup")
	defer b.mu.RUnlock()

	rg, ok := b.runGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run group %s not found", ErrNotFound, id)
	}

	result := *rg

	return &result, nil
}

// ListRunGroups lists run groups, optionally filtered by name (real AWS
// ListRunGroupsInput takes "name" as a query parameter).
func (b *InMemoryBackend) ListRunGroups(
	filter *RunGroupFilter,
	maxResults int,
	nextToken string,
) ([]*RunGroup, string, error) {
	b.mu.RLock("ListRunGroups")
	defer b.mu.RUnlock()

	all := b.runGroups.All()
	ids := make([]string, 0, len(all))

	for _, rg := range all {
		if filter != nil && filter.Name != "" && rg.Name != filter.Name {
			continue
		}

		ids = append(ids, rg.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runGroups.Get)

	return result, outToken, nil
}

// UpdateRunGroup updates a run group.
func (b *InMemoryBackend) UpdateRunGroup(
	id, name string,
	maxCPUs, maxRuns, maxDuration int,
	maxGPUs int,
) (*RunGroup, error) {
	b.mu.Lock("UpdateRunGroup")
	defer b.mu.Unlock()

	rg, ok := b.runGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run group %s not found", ErrNotFound, id)
	}

	if name != "" {
		rg.Name = name
	}

	if maxCPUs > 0 {
		rg.MaxCPUs = maxCPUs
	}

	if maxRuns > 0 {
		rg.MaxRuns = maxRuns
	}

	if maxDuration > 0 {
		rg.MaxDuration = maxDuration
	}

	if maxGPUs > 0 {
		rg.MaxGPUs = maxGPUs
	}

	result := *rg

	return &result, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Run
// ────────────────────────────────────────────────────────────────────────────

// StartRun starts a new workflow run. networkingMode and runOutputURI are
// optional (real StartRunInput fields); runGroupID/runBatchID associate the
// run with a RunGroup/RunBatch for filtering via ListRuns.
func (b *InMemoryBackend) StartRun(
	workflowID, roleARN, name, runGroupID, runBatchID, networkingMode, runOutputURI string,
	params map[string]any,
	tags map[string]string,
) (*Run, error) {
	b.mu.Lock("StartRun")
	defer b.mu.Unlock()

	id := newID()
	now := time.Now().UTC()
	run := &Run{
		ID:             id,
		Name:           name,
		WorkflowID:     workflowID,
		RoleARN:        roleARN,
		RunGroupID:     runGroupID,
		RunBatchID:     runBatchID,
		NetworkingMode: networkingMode,
		RunOutputURI:   runOutputURI,
		UUID:           newID(),
		Params:         params,
		Tags:           copyTags(tags),
		Status:         statusPending,
		CreationTime:   now,
	}
	run.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "run/"+id)

	b.runs.Put(run)

	taskID := newID()
	b.runTasks.Put(&RunTask{
		TaskID:       taskID,
		RunID:        id,
		Name:         "task-1",
		Status:       statusPending,
		CPUs:         stubTaskCPUs,
		Memory:       stubTaskMemory,
		CreationTime: now,
	})

	if tags != nil {
		b.tags[run.Arn] = copyTags(tags)
	}

	result := *run

	return &result, nil
}

// CancelRun cancels a run.
func (b *InMemoryBackend) CancelRun(id string) error {
	b.mu.Lock("CancelRun")
	defer b.mu.Unlock()

	run, ok := b.runs.Get(id)
	if !ok {
		return fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	if run.Status == statusCompleted || run.Status == statusCancelled || run.Status == statusFailed {
		return fmt.Errorf("%w: run %s is already in terminal state %s", ErrValidation, id, run.Status)
	}

	run.Status = statusCancelled

	return nil
}

// DeleteRun deletes a run.
func (b *InMemoryBackend) DeleteRun(id string) error {
	b.mu.Lock("DeleteRun")
	defer b.mu.Unlock()

	run, ok := b.runs.Get(id)
	if !ok {
		return fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	delete(b.tags, run.Arn)
	b.runs.Delete(id)

	for _, t := range slices.Clone(b.runTasksByRun.Get(id)) {
		b.runTasks.Delete(parentKey(id, t.TaskID))
	}

	return nil
}

// GetRun retrieves a run, advancing PENDING→RUNNING→COMPLETED across polls
// (real RunRunningWaiter/RunCompletedWaiter clients poll GetRun until Status
// reaches RUNNING / COMPLETED respectively).
func (b *InMemoryBackend) GetRun(id string) (*Run, error) {
	b.mu.Lock("GetRun")
	defer b.mu.Unlock()

	run, ok := b.runs.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run %s not found", ErrNotFound, id)
	}

	advanceRunStatus(run)

	result := *run

	return &result, nil
}

// advanceRunStatus advances a run's status by one step per poll:
// PENDING → RUNNING on the first poll, RUNNING → COMPLETED on the next.
// Terminal states (COMPLETED/FAILED/CANCELLED) are left untouched.
func advanceRunStatus(run *Run) {
	switch run.Status {
	case statusPending:
		run.pollCount++
		if run.pollCount >= 1 {
			run.Status = statusRunning
			now := time.Now().UTC()
			run.StartTime = &now
		}
	case statusRunning:
		run.pollCount++
		if run.pollCount >= pollCountRunningToCompleted {
			run.Status = statusCompleted
			now := time.Now().UTC()
			run.StopTime = &now
		}
	}
}

// ListRuns lists runs, optionally filtered by name/runGroupId/batchId/status
// (real AWS ListRunsInput query parameters).
func (b *InMemoryBackend) ListRuns(filter *RunFilter, maxResults int, nextToken string) ([]*Run, string, error) {
	b.mu.RLock("ListRuns")
	defer b.mu.RUnlock()

	all := b.runs.All()
	ids := make([]string, 0, len(all))

	for _, r := range all {
		if runMatchesFilter(r, filter) {
			ids = append(ids, r.ID)
		}
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runs.Get)

	return result, outToken, nil
}

// runMatchesFilter reports whether r satisfies every non-empty field of filter.
func runMatchesFilter(r *Run, filter *RunFilter) bool {
	if filter == nil {
		return true
	}

	return (filter.Name == "" || r.Name == filter.Name) &&
		(filter.RunGroupID == "" || r.RunGroupID == filter.RunGroupID) &&
		(filter.BatchID == "" || r.RunBatchID == filter.BatchID) &&
		(filter.Status == "" || r.Status == filter.Status)
}

// GetRunTask retrieves a task within a run, advancing PENDING→RUNNING→
// COMPLETED across polls (real TaskRunningWaiter/TaskCompletedWaiter clients
// poll GetRunTask until Status reaches RUNNING / COMPLETED respectively).
func (b *InMemoryBackend) GetRunTask(runID, taskID string) (*RunTask, error) {
	b.mu.Lock("GetRunTask")
	defer b.mu.Unlock()

	if !b.runs.Has(runID) {
		return nil, fmt.Errorf("%w: run %s not found", ErrNotFound, runID)
	}

	task, ok := b.runTasks.Get(parentKey(runID, taskID))
	if !ok {
		return nil, fmt.Errorf("%w: task %s not found", ErrNotFound, taskID)
	}

	switch task.Status {
	case statusPending:
		task.pollCount++
		if task.pollCount >= 1 {
			task.Status = statusRunning
			now := time.Now().UTC()
			task.StartTime = &now
		}
	case statusRunning:
		task.pollCount++
		if task.pollCount >= pollCountRunningToCompleted {
			task.Status = statusCompleted
			now := time.Now().UTC()
			task.StopTime = &now
		}
	}

	result := *task

	return &result, nil
}

// ListRunTasks lists tasks within a run, optionally filtered by status (real
// AWS ListRunTasksInput "status" query parameter).
//
//nolint:dupl // structurally-identical parent-scoped List op (already deduped via listChildFiltered)
func (b *InMemoryBackend) ListRunTasks(
	runID string,
	filter *RunTaskFilter,
	maxResults int,
	nextToken string,
) ([]*RunTask, string, error) {
	b.mu.RLock("ListRunTasks")
	defer b.mu.RUnlock()

	if !b.runs.Has(runID) {
		return nil, "", fmt.Errorf("%w: run %s not found", ErrNotFound, runID)
	}

	group := b.runTasksByRun.Get(runID)
	result, outToken := listChildFiltered(
		group,
		func(t *RunTask) string { return t.TaskID },
		func(t *RunTask) bool { return filter == nil || filter.Status == "" || t.Status == filter.Status },
		nextToken, maxResults,
		func(id string) (*RunTask, bool) { return b.runTasks.Get(parentKey(runID, id)) },
	)

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// RunCache
// ────────────────────────────────────────────────────────────────────────────

// CreateRunCache creates a new run cache.
func (b *InMemoryBackend) CreateRunCache(
	name, cacheS3Location string,
	tags map[string]string,
) (*RunCache, error) {
	b.mu.Lock("CreateRunCache")
	defer b.mu.Unlock()

	id := newID()
	rc := &RunCache{
		ID:              id,
		Name:            name,
		CacheS3Location: cacheS3Location,
		Status:          statusActive,
		Tags:            copyTags(tags),
		CreationTime:    time.Now().UTC(),
	}
	rc.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "runCache/"+id)

	b.runCaches.Put(rc)

	if tags != nil {
		b.tags[rc.Arn] = copyTags(tags)
	}

	result := *rc

	return &result, nil
}

// DeleteRunCache deletes a run cache.
func (b *InMemoryBackend) DeleteRunCache(id string) error {
	b.mu.Lock("DeleteRunCache")
	defer b.mu.Unlock()

	rc, ok := b.runCaches.Get(id)
	if !ok {
		return fmt.Errorf("%w: run cache %s not found", ErrNotFound, id)
	}

	delete(b.tags, rc.Arn)
	b.runCaches.Delete(id)

	return nil
}

// GetRunCache retrieves a run cache.
func (b *InMemoryBackend) GetRunCache(id string) (*RunCache, error) {
	b.mu.RLock("GetRunCache")
	defer b.mu.RUnlock()

	rc, ok := b.runCaches.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run cache %s not found", ErrNotFound, id)
	}

	result := *rc

	return &result, nil
}

// ListRunCaches lists run caches.
func (b *InMemoryBackend) ListRunCaches(
	maxResults int,
	nextToken string,
) ([]*RunCache, string, error) {
	b.mu.RLock("ListRunCaches")
	defer b.mu.RUnlock()

	all := b.runCaches.All()
	ids := make([]string, 0, len(all))

	for _, rc := range all {
		ids = append(ids, rc.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runCaches.Get)

	return result, outToken, nil
}

// UpdateRunCache updates a run cache.
func (b *InMemoryBackend) UpdateRunCache(id, name, description string) error {
	b.mu.Lock("UpdateRunCache")
	defer b.mu.Unlock()

	rc, ok := b.runCaches.Get(id)
	if !ok {
		return fmt.Errorf("%w: run cache %s not found", ErrNotFound, id)
	}

	if name != "" {
		rc.Name = name
	}

	if description != "" {
		rc.Description = description
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// RunBatch
// ────────────────────────────────────────────────────────────────────────────

// StartRunBatch starts a new run batch.
func (b *InMemoryBackend) StartRunBatch(workflowID, roleARN, name string) (*RunBatch, error) {
	b.mu.Lock("StartRunBatch")
	defer b.mu.Unlock()

	id := newID()
	rb := &RunBatch{
		ID:           id,
		Name:         name,
		WorkflowID:   workflowID,
		RoleARN:      roleARN,
		Status:       statusProcessed,
		CreationTime: time.Now().UTC(),
	}
	rb.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "runBatch/"+id)

	b.runBatches.Put(rb)

	result := *rb

	return &result, nil
}

// CancelRunBatch cancels a run batch.
func (b *InMemoryBackend) CancelRunBatch(id string) error {
	b.mu.Lock("CancelRunBatch")
	defer b.mu.Unlock()

	rb, ok := b.runBatches.Get(id)
	if !ok {
		return fmt.Errorf("%w: run batch %s not found", ErrNotFound, id)
	}

	if isRunBatchTerminal(rb.Status) {
		return fmt.Errorf("%w: run batch %s is already in terminal state %s", ErrValidation, id, rb.Status)
	}

	rb.Status = statusCancelled

	return nil
}

// runBatchDeletableStatuses are the real AWS BatchStatus values DeleteBatch
// requires before it will delete a run batch resource: PROCESSED, FAILED,
// CANCELLED, or RUNS_DELETED.
var runBatchDeletableStatuses = map[string]bool{ //nolint:gochecknoglobals // read-only status set
	statusProcessed:   true,
	statusFailed:      true,
	statusCancelled:   true,
	statusRunsDeleted: true,
}

// isRunBatchTerminal reports whether status is one of RunBatch's terminal
// BatchStatus values (PROCESSED/FAILED/CANCELLED/RUNS_DELETED).
func isRunBatchTerminal(status string) bool {
	return runBatchDeletableStatuses[status]
}

// DeleteRunBatch deletes a single run batch resource (real AWS DeleteBatch
// semantics). Real AWS requires the batch to already be in a terminal state
// (PROCESSED/FAILED/CANCELLED/RUNS_DELETED) before it will delete the batch
// metadata; attempting to delete a batch still in progress returns a
// ConflictException.
func (b *InMemoryBackend) DeleteRunBatch(id string) error {
	b.mu.Lock("DeleteRunBatch")
	defer b.mu.Unlock()

	rb, ok := b.runBatches.Get(id)
	if !ok {
		return fmt.Errorf("%w: run batch %s not found", ErrNotFound, id)
	}

	if !isRunBatchTerminal(rb.Status) {
		return fmt.Errorf(
			"%w: run batch %s must be in a terminal state (PROCESSED, FAILED, CANCELLED, or RUNS_DELETED) "+
				"to be deleted, current state is %s",
			ErrInvalidState, id, rb.Status,
		)
	}

	delete(b.tags, rb.Arn)
	b.runBatches.Delete(id)

	return nil
}

// GetRunBatch retrieves a run batch.
func (b *InMemoryBackend) GetRunBatch(id string) (*RunBatch, error) {
	b.mu.RLock("GetRunBatch")
	defer b.mu.RUnlock()

	rb, ok := b.runBatches.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: run batch %s not found", ErrNotFound, id)
	}

	result := *rb

	return &result, nil
}

// ListRunBatches lists run batches, optionally filtered by name/status (real
// AWS ListBatchInput query parameters). filter.RunGroupID is accepted for
// wire compatibility but not applied: real ListBatch filters by the run
// group of the batch's *contained runs*, and this simplified RunBatch model
// doesn't track a run-group association on the batch resource itself (see
// the PARITY.md RunBatch note on the broader BatchRunSettings/RunSummary gap).
func (b *InMemoryBackend) ListRunBatches(
	filter *RunBatchFilter,
	maxResults int,
	nextToken string,
) ([]*RunBatch, string, error) {
	b.mu.RLock("ListRunBatches")
	defer b.mu.RUnlock()

	all := b.runBatches.All()
	ids := make([]string, 0, len(all))

	for _, rb := range all {
		if filter != nil {
			if filter.Name != "" && rb.Name != filter.Name {
				continue
			}

			if filter.Status != "" && rb.Status != filter.Status {
				continue
			}
		}

		ids = append(ids, rb.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runBatches.Get)

	return result, outToken, nil
}

// DeleteRunsInBatch deletes the individual workflow runs that belong to a run
// batch (real DeleteRunBatch semantics: POST /runBatch/delete with a single
// batchId in the body). The run batch resource itself is left intact; use
// DeleteRunBatch (DELETE /runBatch/{batchId}, real DeleteBatch semantics) to
// remove the batch metadata afterward.
func (b *InMemoryBackend) DeleteRunsInBatch(batchID string) error {
	b.mu.Lock("DeleteRunsInBatch")
	defer b.mu.Unlock()

	rb, ok := b.runBatches.Get(batchID)
	if !ok {
		return fmt.Errorf("%w: run batch %s not found", ErrNotFound, batchID)
	}

	for _, r := range b.runs.All() {
		if r.RunBatchID != batchID {
			continue
		}

		delete(b.tags, r.Arn)
		b.runs.Delete(r.ID)

		for _, t := range slices.Clone(b.runTasksByRun.Get(r.ID)) {
			b.runTasks.Delete(parentKey(r.ID, t.TaskID))
		}
	}

	// Real AWS transitions the batch through RUNS_DELETING to RUNS_DELETED;
	// this backend completes synchronously so it goes straight to the
	// terminal RUNS_DELETED state (same synchronous-completion precedent as
	// the other job families -- see the PARITY.md note on RunBatch).
	rb.Status = statusRunsDeleted

	return nil
}

// ListRunsInBatch lists runs that belong to a run batch, optionally filtered
// by runId (real AWS ListRunsInBatchInput "runId" query parameter).
// filter.RunSettingID/SubmissionStatus are accepted for wire compatibility
// but not applied: this backend has no concept of a per-run "run setting ID"
// or async submission-status, since batches complete synchronously (see the
// PARITY.md RunBatch note).
func (b *InMemoryBackend) ListRunsInBatch(
	batchID string,
	filter *RunsInBatchFilter,
	maxResults int,
	nextToken string,
) ([]*Run, string, error) {
	b.mu.RLock("ListRunsInBatch")
	defer b.mu.RUnlock()

	if !b.runBatches.Has(batchID) {
		return nil, "", fmt.Errorf("%w: run batch %s not found", ErrNotFound, batchID)
	}

	var ids []string

	for _, r := range b.runs.All() {
		if r.RunBatchID != batchID {
			continue
		}

		if filter != nil && filter.RunID != "" && r.ID != filter.RunID {
			continue
		}

		ids = append(ids, r.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.runs.Get)

	return result, outToken, nil
}
