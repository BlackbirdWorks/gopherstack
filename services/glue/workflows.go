package glue

import (
	"fmt"
	"maps"
	mrand "math/rand/v2"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) StopWorkflowRun(workflowName, runID string) error {
	b.mu.Lock("StopWorkflowRun")
	defer b.mu.Unlock()

	runs, ok := b.workflowRuns[workflowName]
	if !ok {
		return fmt.Errorf("workflow %q not found: %w", workflowName, ErrNotFound)
	}
	for _, r := range runs {
		if r.RunID == runID {
			r.Status = stateStopping

			return nil
		}
	}

	return fmt.Errorf(
		"workflow run %q not found for workflow %q: %w",
		runID,
		workflowName,
		ErrNotFound,
	)
}

func (b *InMemoryBackend) PutWorkflowRunProperties(
	workflowName, runID string,
	props map[string]string,
) error {
	b.mu.Lock("PutWorkflowRunProperties")
	defer b.mu.Unlock()

	runs, ok := b.workflowRuns[workflowName]
	if !ok {
		return fmt.Errorf("workflow %q not found: %w", workflowName, ErrNotFound)
	}
	for _, r := range runs {
		if r.RunID == runID {
			if r.Properties == nil {
				r.Properties = make(map[string]string)
			}
			maps.Copy(r.Properties, props)

			return nil
		}
	}

	return fmt.Errorf(
		"workflow run %q not found for workflow %q: %w",
		runID,
		workflowName,
		ErrNotFound,
	)
}

// ResumeWorkflowRun looks up the workflow run and returns its ID along with
// an empty node-ID list (AWS returns node IDs that were actually resumed).
func (b *InMemoryBackend) ResumeWorkflowRun(workflowName, runID string) (string, []string, error) {
	b.mu.Lock("ResumeWorkflowRun")
	defer b.mu.Unlock()

	runs, ok := b.workflowRuns[workflowName]
	if !ok {
		return "", nil, fmt.Errorf("workflow %q not found: %w", workflowName, ErrNotFound)
	}

	for _, run := range runs {
		if run.RunID == runID {
			run.Status = stateRunning

			return runID, []string{}, nil
		}
	}

	return "", nil, fmt.Errorf("workflow run %q not found: %w", runID, ErrNotFound)
}

// cloneWorkflow returns a shallow copy of a Workflow with cloned maps.
func cloneWorkflow(w *Workflow) *Workflow {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)
	cp.DefaultRunProperties = maps.Clone(w.DefaultRunProperties)

	return &cp
}

// workflowARN returns the ARN for a Glue workflow.
func (b *InMemoryBackend) workflowARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "workflow/"+name)
}

// CreateWorkflow creates a new Glue workflow.
func (b *InMemoryBackend) CreateWorkflow(w Workflow, tags map[string]string) (*Workflow, error) {
	b.mu.Lock("CreateWorkflow")
	defer b.mu.Unlock()

	if w.Name == "" {
		return nil, ErrValidation
	}

	if b.workflows.Has(w.Name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	stored := cloneWorkflow(&w)
	stored.ARN = b.workflowARN(w.Name)
	stored.Tags = maps.Clone(tags)
	stored.CreatedOn = now
	stored.LastModifiedOn = now

	b.workflows.Put(stored)

	return cloneWorkflow(stored), nil
}

// GetWorkflow retrieves a Glue workflow by name.
func (b *InMemoryBackend) GetWorkflow(name string) (*Workflow, error) {
	b.mu.RLock("GetWorkflow")
	defer b.mu.RUnlock()

	w, ok := b.workflows.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneWorkflow(w), nil
}

// GetWorkflows returns all Glue workflows sorted by name.
func (b *InMemoryBackend) GetWorkflows() []string {
	b.mu.RLock("GetWorkflows")
	defer b.mu.RUnlock()

	src := b.workflows.Snapshot()
	out := make([]string, len(src))
	for i, w := range src {
		out[i] = w.Name
	}

	return out
}

// BatchGetWorkflows retrieves multiple workflows by name.
func (b *InMemoryBackend) BatchGetWorkflows(names []string) ([]*Workflow, []string) {
	b.mu.RLock("BatchGetWorkflows")
	defer b.mu.RUnlock()

	found := make([]*Workflow, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		w, ok := b.workflows.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		found = append(found, cloneWorkflow(w))
	}

	return found, missing
}

// UpdateWorkflow updates an existing Glue workflow.
func (b *InMemoryBackend) UpdateWorkflow(name string, update Workflow) error {
	b.mu.Lock("UpdateWorkflow")
	defer b.mu.Unlock()

	w, ok := b.workflows.Get(name)
	if !ok {
		return ErrNotFound
	}

	w.Description = update.Description
	w.DefaultRunProperties = maps.Clone(update.DefaultRunProperties)
	w.MaxConcurrentRuns = update.MaxConcurrentRuns
	w.LastModifiedOn = float64(time.Now().Unix())

	return nil
}

// DeleteWorkflow deletes a Glue workflow and all its runs by name.
func (b *InMemoryBackend) DeleteWorkflow(name string) error {
	b.mu.Lock("DeleteWorkflow")
	defer b.mu.Unlock()

	if !b.workflows.Has(name) {
		return ErrNotFound
	}

	b.workflows.Delete(name)
	delete(b.workflowRuns, name)

	return nil
}

// StartWorkflowRun creates a new workflow run record.
func (b *InMemoryBackend) StartWorkflowRun(name string) (*WorkflowRun, error) {
	b.mu.Lock("StartWorkflowRun")
	defer b.mu.Unlock()

	w, ok := b.workflows.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	if w.MaxConcurrentRuns > 0 {
		active := 0
		for _, r := range b.workflowRuns[name] {
			if r.Status == stateRunning || r.Status == stateStopping {
				active++
			}
		}
		if active >= w.MaxConcurrentRuns {
			return nil, ErrConcurrentRunsExceeded
		}
	}

	runID := fmt.Sprintf(
		"wr_%d_%04d",
		time.Now().UnixNano(),
		mrand.IntN(10000), //nolint:gosec,mnd // non-security mock run ID
	)
	run := &WorkflowRun{
		WorkflowName: name,
		RunID:        runID,
		Status:       stateRunning,
		StartedOn:    float64(time.Now().Unix()),
	}
	b.workflowRuns[name] = append(b.workflowRuns[name], run)

	return run, nil
}

// GetWorkflowRun retrieves a specific workflow run by workflow name and run ID.
func (b *InMemoryBackend) GetWorkflowRun(workflowName, runID string) (*WorkflowRun, error) {
	b.mu.RLock("GetWorkflowRun")
	defer b.mu.RUnlock()

	for _, run := range b.workflowRuns[workflowName] {
		if run.RunID == runID {
			cp := *run

			return &cp, nil
		}
	}

	return nil, ErrNotFound
}

// GetWorkflowRuns returns all runs for a workflow.
func (b *InMemoryBackend) GetWorkflowRuns(workflowName string) ([]*WorkflowRun, error) {
	b.mu.RLock("GetWorkflowRuns")
	defer b.mu.RUnlock()

	if !b.workflows.Has(workflowName) {
		return nil, ErrNotFound
	}

	src := b.workflowRuns[workflowName]
	out := make([]*WorkflowRun, 0, len(src))
	for _, run := range src {
		cp := *run
		out = append(out, &cp)
	}

	return out, nil
}
