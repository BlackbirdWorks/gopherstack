package glue

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

var ErrMaterializedViewRunNotFound = fmt.Errorf("materialized view refresh run not found: %w", ErrNotFound)

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

// StopMaterializedViewRefreshTaskRun stops a refresh run.
func (b *InMemoryBackend) StopMaterializedViewRefreshTaskRun(taskRunID string) error {
	b.mu.Lock("StopMaterializedViewRefreshTaskRun")
	defer b.mu.Unlock()

	r, ok := b.materializedViewRuns.Get(taskRunID)
	if !ok {
		return ErrMaterializedViewRunNotFound
	}

	r.Status = stateStopped

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
