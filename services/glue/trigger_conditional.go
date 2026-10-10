package glue

import "slices"

const (
	predicateLogicalAnd    = "AND"
	triggerTypeConditional = "CONDITIONAL"
	triggerStateActivated  = "ACTIVATED"
)

// runCompletion is a job run or crawl reaching a terminal state; exactly one of
// job/crawler is set.
type runCompletion struct {
	job           string
	crawler       string
	runID         string
	state         string
	triggerName   string
	workflowRunID string
}

type conditionalFire struct {
	triggerName   string
	workflowRunID string
	actions       []TriggerAction
	preds         []Predecessor
}

func (b *InMemoryBackend) recordJobCompletionLocked(run *JobRun, prevState string) {
	if run.JobRunState == prevState || !isTerminalJobRunState(run.JobRunState) {
		return
	}

	b.triggerEvents = append(b.triggerEvents, runCompletion{
		job: run.JobName, runID: run.ID, state: run.JobRunState,
		triggerName: run.TriggerName, workflowRunID: run.WorkflowRunID,
	})
}

func (b *InMemoryBackend) recordCrawlCompletionLocked(crawler, state string) {
	var triggerName, workflowRunID string

	if hist := b.crawlHistory[crawler]; len(hist) > 0 {
		triggerName = hist[len(hist)-1].TriggerName
		workflowRunID = hist[len(hist)-1].WorkflowRunID
	}

	b.triggerEvents = append(b.triggerEvents, runCompletion{
		crawler: crawler, state: state, triggerName: triggerName, workflowRunID: workflowRunID,
	})
}

func (ev runCompletion) matches(c TriggerCondition) bool {
	if c.LogicalOperator != "" && c.LogicalOperator != "EQUALS" {
		return false
	}

	if ev.job != "" {
		return c.JobName == ev.job && c.State == ev.state
	}

	return c.CrawlerName == ev.crawler && c.CrawlState == ev.state
}

// dispatchTriggerEvents fires every ACTIVATED CONDITIONAL trigger whose predicate
// is satisfied by a completed run. Per the Glue developer guide, dependents only
// start when the completing job or crawler was itself started by a trigger. An AND
// trigger fires at most once per batch; each later completion that finds the
// predicate satisfied fires it again.
func (b *InMemoryBackend) dispatchTriggerEvents(events []runCompletion) {
	if len(events) == 0 {
		return
	}

	for _, f := range b.collectConditionalFires(events) {
		b.fireTriggerActions(f.actions, f.triggerName, f.workflowRunID, f.preds)
	}
}

func (b *InMemoryBackend) collectConditionalFires(events []runCompletion) []conditionalFire {
	b.mu.RLock("dispatchTriggerEvents")
	defer b.mu.RUnlock()

	var fires []conditionalFire

	firedAnd := map[string]bool{}

	for _, ev := range events {
		if ev.triggerName == "" {
			continue
		}

		for _, t := range b.triggers.All() {
			preds, ok := b.triggerFiresLocked(t, ev, firedAnd)
			if !ok {
				continue
			}

			fires = append(fires, conditionalFire{
				triggerName:   t.Name,
				workflowRunID: ev.workflowRunID,
				actions:       slices.Clone(t.Actions),
				preds:         preds,
			})
		}
	}

	return fires
}

func (b *InMemoryBackend) triggerFiresLocked(
	t *Trigger,
	ev runCompletion,
	firedAnd map[string]bool,
) ([]Predecessor, bool) {
	if t.Type != triggerTypeConditional || t.State != triggerStateActivated || t.Predicate == nil {
		return nil, false
	}

	if !slices.ContainsFunc(t.Predicate.Conditions, ev.matches) ||
		b.workflowNameOfRunLocked(ev.workflowRunID) != t.WorkflowName {
		return nil, false
	}

	and := t.Predicate.Logical == predicateLogicalAnd
	if and && firedAnd[t.Name] {
		return nil, false
	}

	preds, ok := b.predicateSatisfiedLocked(t.Predicate, ev)
	if ok && and {
		firedAnd[t.Name] = true
	}

	return preds, ok
}

func (b *InMemoryBackend) workflowNameOfRunLocked(workflowRunID string) string {
	if workflowRunID == "" {
		return ""
	}

	for name, runs := range b.workflowRuns {
		if slices.ContainsFunc(runs, func(r *WorkflowRun) bool { return r.RunID == workflowRunID }) {
			return name
		}
	}

	return ""
}

// predicateSatisfiedLocked reports whether p holds given ev, returning the job runs
// that satisfied it. ANY (and an unset Logical) is satisfied by ev alone; AND needs
// every condition's watched job or crawler to have its most recent finished run in
// the expected state.
func (b *InMemoryBackend) predicateSatisfiedLocked(p *TriggerPredicate, ev runCompletion) ([]Predecessor, bool) {
	if p.Logical != predicateLogicalAnd {
		if ev.job == "" {
			return nil, true
		}

		return []Predecessor{{JobName: ev.job, RunID: ev.runID}}, true
	}

	var preds []Predecessor

	for _, c := range p.Conditions {
		if c.LogicalOperator != "" && c.LogicalOperator != "EQUALS" {
			return nil, false
		}

		if c.JobName != "" {
			run := b.latestFinishedJobRunLocked(c.JobName, ev.workflowRunID)
			if run == nil || run.JobRunState != c.State {
				return nil, false
			}

			preds = append(preds, Predecessor{JobName: run.JobName, RunID: run.ID})

			continue
		}

		if b.latestFinishedCrawlStateLocked(c.CrawlerName, ev.workflowRunID) != c.CrawlState {
			return nil, false
		}
	}

	return preds, true
}

func (b *InMemoryBackend) latestFinishedJobRunLocked(job, workflowRunID string) *JobRun {
	runs := b.jobRuns[job]
	for _, r := range slices.Backward(runs) {
		if isTerminalJobRunState(r.JobRunState) && r.WorkflowRunID == workflowRunID {
			return r
		}
	}

	return nil
}

func (b *InMemoryBackend) latestFinishedCrawlStateLocked(crawler, workflowRunID string) string {
	hist := b.crawlHistory[crawler]
	for _, e := range slices.Backward(hist) {
		if e.EndTime == 0 || e.WorkflowRunID != workflowRunID {
			continue
		}

		if e.State == "COMPLETED" {
			return "SUCCEEDED"
		}

		return "CANCELLED"
	}

	return ""
}
