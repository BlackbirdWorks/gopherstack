package glue

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var ErrMaterializedViewRunNotFound = fmt.Errorf("materialized view refresh run not found: %w", ErrNotFound)

// ErrMaterializedViewRefreshTaskNotRunning is returned by
// StopMaterializedViewRefreshTaskRun when no refresh run is in progress for
// the given table. StopMaterializedViewRefreshTaskRun's error switch
// (glue@v1.152.0 deserializers.go) has no EntityNotFoundException case;
// MaterializedViewRefreshTaskNotRunningException is the code it models for
// this condition.
var ErrMaterializedViewRefreshTaskNotRunning = awserr.New(
	"MaterializedViewRefreshTaskNotRunningException", awserr.ErrInvalidParameter,
)

// StartMaterializedViewRefreshTaskRun starts a refresh run.
func (b *InMemoryBackend) StartMaterializedViewRefreshTaskRun(
	dbName, tableName string,
) (*MaterializedViewRefreshRun, error) {
	b.mu.Lock("StartMaterializedViewRefreshTaskRun")
	defer b.mu.Unlock()

	taskID := "mvr-" + uuid.NewString()[:8]
	run := &MaterializedViewRefreshRun{
		DatabaseName: dbName,
		TableName:    tableName,
		TaskRunID:    taskID,
		Status:       stateRunning,
		StartedOn:    float64(time.Now().Unix()),
	}
	b.materializedViewRuns.Put(run)
	cp := *run

	return &cp, nil
}

// StopMaterializedViewRefreshTaskRun stops the most recently started refresh
// run for a table. The real StopMaterializedViewRefreshTaskRunInput
// identifies the run by DatabaseName+TableName, not a run ID (glue@v1.152.0
// api_op_StopMaterializedViewRefreshTaskRun.go has no run-ID member at all).
func (b *InMemoryBackend) StopMaterializedViewRefreshTaskRun(dbName, tableName string) error {
	b.mu.Lock("StopMaterializedViewRefreshTaskRun")
	defer b.mu.Unlock()

	var latest *MaterializedViewRefreshRun

	for _, r := range b.materializedViewRuns.All() {
		if r.DatabaseName != dbName || r.TableName != tableName {
			continue
		}

		if latest == nil || r.StartedOn > latest.StartedOn {
			latest = r
		}
	}

	if latest == nil {
		return ErrMaterializedViewRefreshTaskNotRunning
	}

	latest.Status = stateStopped

	return nil
}

// GetMaterializedViewRefreshTaskRun returns a refresh run.
func (b *InMemoryBackend) GetMaterializedViewRefreshTaskRun(taskRunID string) (*MaterializedViewRefreshRun, error) {
	b.mu.RLock("GetMaterializedViewRefreshTaskRun")
	defer b.mu.RUnlock()

	r, ok := b.materializedViewRuns.Get(taskRunID)
	if !ok {
		return nil, ErrMaterializedViewRunNotFound
	}

	cp := *r

	return &cp, nil
}

// ListMaterializedViewRefreshTaskRuns returns all refresh runs.
func (b *InMemoryBackend) ListMaterializedViewRefreshTaskRuns() []*MaterializedViewRefreshRun {
	b.mu.RLock("ListMaterializedViewRefreshTaskRuns")
	defer b.mu.RUnlock()

	src := b.materializedViewRuns.All()
	runs := make([]*MaterializedViewRefreshRun, 0, len(src))
	for _, r := range src {
		cp := *r
		runs = append(runs, &cp)
	}

	sort.Slice(runs, func(i, k int) bool {
		return runs[i].StartedOn < runs[k].StartedOn
	})

	return runs
}
