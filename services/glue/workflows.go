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

// ResumeWorkflowRun echoes nodeIDs back as "the new nodes that were actually
// restarted" (ResumeWorkflowRunOutput.NodeIds) -- this backend has no
// per-node run-attempt state (WorkflowRun.Graph is a disclosed gap, see
// PARITY.md), so every requested node is honestly reported as restarted
// rather than silently dropped from the response.
func (b *InMemoryBackend) ResumeWorkflowRun(workflowName, runID string, nodeIDs []string) (string, []string, error) {
	b.mu.Lock("ResumeWorkflowRun")
	defer b.mu.Unlock()

	runs, ok := b.workflowRuns[workflowName]
	if !ok {
		return "", nil, fmt.Errorf("workflow %q not found: %w", workflowName, ErrNotFound)
	}

	for _, run := range runs {
		if run.RunID == runID {
			run.Status = stateRunning

			return runID, nodeIDs, nil
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

// GetWorkflow retrieves a Glue workflow by name. LastRun is always populated
// from real run history; Graph is populated only when includeGraph is set,
// matching GetWorkflowInput.IncludeGraph (api_op_GetWorkflow.go). Advances due
// lifecycle transitions first -- see GetWorkflowRun -- since LastRun.Statistics
// depends on job/crawler run state.
func (b *InMemoryBackend) GetWorkflow(name string, includeGraph bool) (*Workflow, error) {
	b.advanceStates(time.Now())

	b.mu.RLock("GetWorkflow")
	defer b.mu.RUnlock()

	w, ok := b.workflows.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return b.decorateWorkflowLocked(w, includeGraph), nil
}

// decorateWorkflowLocked clones w and attaches LastRun (always) and Graph
// (when includeGraph is set). Caller must hold b.mu.
func (b *InMemoryBackend) decorateWorkflowLocked(w *Workflow, includeGraph bool) *Workflow {
	cp := cloneWorkflow(w)

	if runs := b.workflowRuns[w.Name]; len(runs) > 0 {
		last := *runs[len(runs)-1]
		last.Statistics = b.computeWorkflowRunStatisticsLocked(last.RunID)
		cp.LastRun = &last
	}

	if includeGraph {
		cp.Graph = b.workflowGraphLocked(w.Name)
	}

	return cp
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

// BatchGetWorkflows retrieves multiple workflows by name. See GetWorkflow for
// the LastRun/Graph population rules.
func (b *InMemoryBackend) BatchGetWorkflows(names []string, includeGraph bool) ([]*Workflow, []string) {
	b.advanceStates(time.Now())

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

		found = append(found, b.decorateWorkflowLocked(w, includeGraph))
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

// DeleteWorkflow deletes a Glue workflow and all its runs by name. Its error
// switch has no EntityNotFoundException case, unlike GetWorkflow's, so an
// unknown Name surfaces as InvalidInputException.
func (b *InMemoryBackend) DeleteWorkflow(name string) error {
	b.mu.Lock("DeleteWorkflow")
	defer b.mu.Unlock()

	if !b.workflows.Has(name) {
		return fmt.Errorf("workflow %q not found: %w", name, ErrValidation)
	}

	b.workflows.Delete(name)
	delete(b.workflowRuns, name)

	return nil
}

// entryTriggerFire is a workflow entry-point trigger's name and a clone of its
// actions, captured under b.mu so it can be fired after the lock is released.
type entryTriggerFire struct {
	triggerName string
	actions     []TriggerAction
}

// entryTriggersLocked returns the workflow's entry-point triggers: those with
// WorkflowName == name and no Predicate. AWS docs call this a workflow's "start
// trigger" (workflows_overview.html: "each workflow has a start trigger"),
// fired when StartWorkflowRun is called; predicate-gated (conditional)
// triggers fire later, from other actions completing -- a chain this backend
// does not evaluate (see StartWorkflowRun). Caller must hold b.mu.
func (b *InMemoryBackend) entryTriggersLocked(name string) []entryTriggerFire {
	var fires []entryTriggerFire

	for _, t := range b.triggers.All() {
		if t.WorkflowName == name && t.Predicate == nil {
			fires = append(fires, entryTriggerFire{
				triggerName: t.Name,
				actions:     append([]TriggerAction(nil), t.Actions...),
			})
		}
	}

	return fires
}

// StartWorkflowRun creates a new workflow run record and fires the workflow's
// entry-point trigger(s), stamping the new run's ID onto the job runs/crawls
// they start (see fireTriggerActions). Downstream conditional triggers within
// the workflow are never evaluated by this backend (no predicate-evaluation
// engine exists), so only the entry trigger's own actions are ever linked to
// a workflow run -- WorkflowRunStatistics reflects exactly that real subset,
// not the full DAG.
func (b *InMemoryBackend) StartWorkflowRun(name string) (*WorkflowRun, error) {
	var fires []entryTriggerFire

	run, err := func() (*WorkflowRun, error) {
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

		fires = b.entryTriggersLocked(name)

		return run, nil
	}()
	if err != nil {
		return nil, err
	}

	// Fire outside the lock: StartJobRun/StartCrawler take the same coarse
	// backend lock, and it is not reentrant.
	for _, f := range fires {
		b.fireTriggerActions(f.actions, f.triggerName, run.RunID)
	}

	return run, nil
}

// GetWorkflowRun retrieves a specific workflow run by workflow name and run ID.
// Statistics depends on job/crawler run state, so this advances due lifecycle
// transitions first -- same as GetJobRun/GetCrawler -- rather than reporting
// stale STARTING/RUNNING states that would already read differently elsewhere.
func (b *InMemoryBackend) GetWorkflowRun(workflowName, runID string) (*WorkflowRun, error) {
	b.advanceStates(time.Now())

	b.mu.RLock("GetWorkflowRun")
	defer b.mu.RUnlock()

	for _, run := range b.workflowRuns[workflowName] {
		if run.RunID == runID {
			cp := *run
			cp.Statistics = b.computeWorkflowRunStatisticsLocked(runID)

			return &cp, nil
		}
	}

	return nil, ErrNotFound
}

// GetWorkflowRuns returns all runs for a workflow. See GetWorkflowRun for why
// it advances lifecycle transitions before computing Statistics.
func (b *InMemoryBackend) GetWorkflowRuns(workflowName string) ([]*WorkflowRun, error) {
	b.advanceStates(time.Now())

	b.mu.RLock("GetWorkflowRuns")
	defer b.mu.RUnlock()

	if !b.workflows.Has(workflowName) {
		return nil, ErrNotFound
	}

	src := b.workflowRuns[workflowName]
	out := make([]*WorkflowRun, 0, len(src))
	for _, run := range src {
		cp := *run
		cp.Statistics = b.computeWorkflowRunStatisticsLocked(run.RunID)
		out = append(out, &cp)
	}

	return out, nil
}

// computeWorkflowRunStatisticsLocked tallies the job runs and crawls stamped
// with this workflow run's ID into WorkflowRunStatistics's outcome buckets.
// ErroredActions and WaitingActions count job runs only -- the SDK's own doc
// comments for those two fields say "count of job runs in the ERROR/WAITING
// state" (aws-sdk-go-v2/service/glue@v1.152.0 types.go:13224-13225), unlike
// the other fields' generic "Actions" wording -- so crawls never contribute to
// them. Caller must hold b.mu.
func (b *InMemoryBackend) computeWorkflowRunStatisticsLocked(runID string) *WorkflowRunStatistics {
	stats := &WorkflowRunStatistics{}

	for _, runs := range b.jobRuns {
		for _, r := range runs {
			if r.WorkflowRunID != runID {
				continue
			}

			stats.TotalActions++
			tallyJobRunAction(stats, r.JobRunState)
		}
	}

	for _, hist := range b.crawlHistory {
		for _, e := range hist {
			if e.WorkflowRunID != runID {
				continue
			}

			stats.TotalActions++
			tallyCrawlAction(stats, e.State)
		}
	}

	return stats
}

// tallyJobRunAction buckets a single job run's state into stats. States this
// backend never produces (see reconciler.go) simply never increment a bucket.
func tallyJobRunAction(stats *WorkflowRunStatistics, state string) {
	switch state {
	case stateSucceeded:
		stats.SucceededActions++
	case stateFailed:
		stats.FailedActions++
	case stateStopped:
		stats.StoppedActions++
	case stateRunning:
		stats.RunningActions++
	case stateTimeout:
		stats.TimeoutActions++
	case stateError:
		stats.ErroredActions++
	case stateWaiting:
		stats.WaitingActions++
	}
}

// tallyCrawlAction buckets a single crawl's state into stats, using the exact
// three values finishCrawlHistoryLocked/StartCrawler ever set on
// CrawlHistoryEntry.State (RUNNING/COMPLETED/STOPPED -- see crawlers.go).
func tallyCrawlAction(stats *WorkflowRunStatistics, state string) {
	switch state {
	case stateCompleted:
		stats.SucceededActions++
	case stateStopped:
		stats.StoppedActions++
	case stateRunning:
		stats.RunningActions++
	}
}
