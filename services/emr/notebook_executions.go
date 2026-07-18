package emr

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) notebookExecutionGet(region, id string) (*NotebookExecution, bool) {
	return b.notebookExecutions.Get(regionKey(region, id))
}

func (b *InMemoryBackend) notebookExecutionPut(v *NotebookExecution) { b.notebookExecutions.Put(v) }

func (b *InMemoryBackend) notebookExecutionsInRegion(region string) []*NotebookExecution {
	return b.notebookExecutionsByRegion.Get(region)
}

// StartNotebookExecution creates a new notebook execution in RUNNING state.
func (b *InMemoryBackend) StartNotebookExecution(
	ctx context.Context,
	editorID, name, params, engineID string,
	tags []Tag,
) (*NotebookExecution, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartNotebookExecution")
	defer b.mu.Unlock()

	id := b.nextNotebookExecID()

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	ne := &NotebookExecution{
		NotebookExecutionID:   id,
		EditorID:              editorID,
		NotebookExecutionName: name,
		NotebookParams:        params,
		ExecutionEngineID:     engineID,
		Status:                NotebookStatusRunning,
		StartTime:             awstime.Epoch(time.Now()),
		Tags:                  tagsCopy,
		region:                region,
	}

	b.notebookExecutionPut(ne)

	cp := *ne

	return &cp, nil
}

// StopNotebookExecution transitions a RUNNING execution to STOPPED.
func (b *InMemoryBackend) StopNotebookExecution(ctx context.Context, id string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopNotebookExecution")
	defer b.mu.Unlock()

	ne, ok := b.notebookExecutionGet(region, id)
	if !ok {
		return fmt.Errorf("%w: notebook execution %s not found", ErrNotFound, id)
	}

	if ne.Status == NotebookStatusRunning || ne.Status == NotebookStatusStopping {
		ne.Status = NotebookStatusStopped
		ne.EndTime = awstime.Epoch(time.Now())
	}

	return nil
}

// DescribeNotebookExecution returns a notebook execution by ID.
func (b *InMemoryBackend) DescribeNotebookExecution(ctx context.Context, id string) (*NotebookExecution, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeNotebookExecution")
	defer b.mu.RUnlock()

	ne, ok := b.notebookExecutionGet(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: notebook execution %s not found", ErrNotFound, id)
	}

	cp := *ne

	return &cp, nil
}

// ListNotebookExecutions returns paginated notebook executions matching the filter.
func (b *InMemoryBackend) ListNotebookExecutions(
	ctx context.Context, params ListNotebookExecutionsParams,
) ([]NotebookExecution, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListNotebookExecutions")
	defer b.mu.RUnlock()

	executions := b.notebookExecutionsInRegion(region)
	list := make([]NotebookExecution, 0, len(executions))

	for _, ne := range executions {
		if params.EditorID != "" && ne.EditorID != params.EditorID {
			continue
		}

		if params.Status != "" && ne.Status != params.Status {
			continue
		}

		list = append(list, *ne)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].NotebookExecutionID < list[j].NotebookExecutionID
	})

	p := page.New(list, params.Marker, listNotebookExecPageSize, listNotebookExecPageSize)

	return p.Data, p.Next
}
