package glue

import (
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// triggerTypeOnDemand is the TriggerType wire value for on-demand triggers. Per AWS
// docs (about-triggers.html): "On-demand triggers never enter the ACTIVATED or
// DEACTIVATED state. They always remain in the CREATED state," and firing one
// (StartTrigger) immediately runs its actions rather than switching to a
// long-lived "active" monitoring state like SCHEDULED/CONDITIONAL/EVENT triggers do.
const triggerTypeOnDemand = "ON_DEMAND"

// maxCrawlerActionsPerTrigger is AWS's documented soft limit (about-triggers.html):
// you can create up to 2 crawler actions per trigger, but you can create any
// number of job actions per trigger.
const maxCrawlerActionsPerTrigger = 2

// validateTriggerActions enforces the max-2-crawler-actions-per-trigger limit.
func validateTriggerActions(actions []TriggerAction) error {
	crawlerActions := 0
	for _, a := range actions {
		if a.CrawlerName != "" {
			crawlerActions++
		}
	}

	if crawlerActions > maxCrawlerActionsPerTrigger {
		return fmt.Errorf(
			"%w: a trigger may specify at most %d crawler actions",
			ErrValidation, maxCrawlerActionsPerTrigger,
		)
	}

	return nil
}

// cloneTrigger returns a shallow copy of a Trigger with cloned maps/slices.
func cloneTrigger(t *Trigger) *Trigger {
	cp := *t
	cp.Tags = maps.Clone(t.Tags)
	if len(t.Actions) > 0 {
		cp.Actions = make([]TriggerAction, len(t.Actions))
		for i, a := range t.Actions {
			ac := a
			ac.Arguments = maps.Clone(a.Arguments)
			cp.Actions[i] = ac
		}
	}

	if t.Predicate != nil {
		pred := *t.Predicate
		if len(t.Predicate.Conditions) > 0 {
			pred.Conditions = make([]TriggerCondition, len(t.Predicate.Conditions))
			copy(pred.Conditions, t.Predicate.Conditions)
		}
		cp.Predicate = &pred
	}

	if t.EventBatchingCondition != nil {
		ebc := *t.EventBatchingCondition
		cp.EventBatchingCondition = &ebc
	}

	return &cp
}

// triggerARN returns the ARN for a Glue trigger.
func (b *InMemoryBackend) triggerARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "trigger/"+name)
}

// CreateTrigger creates a new Glue trigger.
func (b *InMemoryBackend) CreateTrigger(t Trigger, tags map[string]string) (*Trigger, error) {
	b.mu.Lock("CreateTrigger")
	defer b.mu.Unlock()

	if t.Name == "" {
		return nil, ErrValidation
	}

	if err := validateTriggerActions(t.Actions); err != nil {
		return nil, err
	}

	if b.triggers.Has(t.Name) {
		return nil, ErrAlreadyExists
	}

	stored := cloneTrigger(&t)
	stored.ARN = b.triggerARN(t.Name)
	stored.Tags = maps.Clone(tags)
	if stored.State == "" {
		stored.State = "CREATED"
	}

	// StartOnCreation only applies to SCHEDULED/CONDITIONAL/EVENT triggers; AWS
	// docs state it is "not supported for ON_DEMAND triggers" (which never leave
	// CREATED anyway).
	if stored.StartOnCreation && stored.Type != triggerTypeOnDemand {
		stored.State = "ACTIVATED"
	}

	stored.StartOnCreation = false

	b.triggers.Put(stored)

	return cloneTrigger(stored), nil
}

// GetTrigger retrieves a Glue trigger by name.
func (b *InMemoryBackend) GetTrigger(name string) (*Trigger, error) {
	b.mu.RLock("GetTrigger")
	defer b.mu.RUnlock()

	t, ok := b.triggers.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneTrigger(t), nil
}

// GetTriggers returns all Glue triggers sorted by name.
func (b *InMemoryBackend) GetTriggers() []*Trigger {
	b.mu.RLock("GetTriggers")
	defer b.mu.RUnlock()

	src := b.triggers.Snapshot()
	out := make([]*Trigger, 0, len(src))
	for _, t := range src {
		out = append(out, cloneTrigger(t))
	}

	return out
}

// BatchGetTriggers retrieves multiple triggers by name.
func (b *InMemoryBackend) BatchGetTriggers(names []string) ([]*Trigger, []string) {
	b.mu.RLock("BatchGetTriggers")
	defer b.mu.RUnlock()

	found := make([]*Trigger, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		t, ok := b.triggers.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		found = append(found, cloneTrigger(t))
	}

	return found, missing
}

// UpdateTrigger updates an existing Glue trigger.
func (b *InMemoryBackend) UpdateTrigger(name string, update Trigger) error {
	b.mu.Lock("UpdateTrigger")
	defer b.mu.Unlock()

	if err := validateTriggerActions(update.Actions); err != nil {
		return err
	}

	t, ok := b.triggers.Get(name)
	if !ok {
		return ErrNotFound
	}

	t.Schedule = update.Schedule
	t.Actions = update.Actions
	t.Predicate = update.Predicate
	t.Description = update.Description

	if update.EventBatchingCondition != nil {
		ebc := *update.EventBatchingCondition
		t.EventBatchingCondition = &ebc
	}

	return nil
}

// DeleteTrigger deletes a Glue trigger by name.
func (b *InMemoryBackend) DeleteTrigger(name string) error {
	b.mu.Lock("DeleteTrigger")
	defer b.mu.Unlock()

	if !b.triggers.Has(name) {
		return ErrNotFound
	}

	b.triggers.Delete(name)

	return nil
}

// StartTrigger activates a Glue trigger. Per AWS docs (about-triggers.html),
// on-demand triggers never enter the ACTIVATED state — they always remain CREATED —
// and firing one immediately runs its actions (job runs / crawler runs) rather than
// switching to a long-lived "active" monitoring state the way SCHEDULED/CONDITIONAL/
// EVENT triggers do.
func (b *InMemoryBackend) StartTrigger(name string) error {
	var (
		found    bool
		onDemand bool
		actions  []TriggerAction
	)

	func() {
		b.mu.Lock("StartTrigger")
		defer b.mu.Unlock()

		t, ok := b.triggers.Get(name)
		if !ok {
			return
		}

		found = true
		onDemand = t.Type == triggerTypeOnDemand
		if !onDemand {
			t.State = "ACTIVATED"
		}

		actions = append([]TriggerAction(nil), t.Actions...)
	}()

	if !found {
		return ErrNotFound
	}

	if onDemand {
		// Fire outside the trigger lock: StartJobRun/StartCrawler take the same
		// coarse backend lock, and it is not reentrant. workflowRunID is empty
		// here: firing a trigger directly (rather than via StartWorkflowRun)
		// creates no WorkflowRun to link the resulting runs to.
		b.fireTriggerActions(actions, name, "")
	}

	return nil
}

// fireTriggerActions starts the job run or crawler run for each action of a
// fired trigger, stamping TriggerName (JobRun's real wire field, aws-sdk-go-v2/
// service/glue@v1.152.0 types.go:7350-7351) and workflowRunID (this backend's
// internal correlation used only for WorkflowRunStatistics -- Crawl/
// CrawlerHistory and JobRun have no run-scoped link on the wire; see
// types.go:2815-2836,7134-7352). Per-action errors are not propagated: AWS's
// StartTrigger/StartWorkflowRun return as soon as the trigger fires, before the
// resulting job runs/crawls complete or are even guaranteed to start successfully.
func (b *InMemoryBackend) fireTriggerActions(actions []TriggerAction, triggerName, workflowRunID string) {
	for _, a := range actions {
		switch {
		case a.JobName != "":
			if run, err := b.StartJobRun(a.JobName, a.Arguments); err == nil {
				b.stampTriggeredJobRun(run, triggerName, workflowRunID)
			}
		case a.CrawlerName != "":
			if err := b.StartCrawler(a.CrawlerName); err == nil {
				b.stampTriggeredCrawl(a.CrawlerName, workflowRunID)
			}
		}
	}
}

// stampTriggeredJobRun sets the trigger/workflow-run link on a just-started job
// run. run is the live pointer StartJobRun stored in b.jobRuns, so this mutates
// the real record. Reacquires b.mu itself since fireTriggerActions runs after
// StartJobRun has already released it.
func (b *InMemoryBackend) stampTriggeredJobRun(run *JobRun, triggerName, workflowRunID string) {
	b.mu.Lock("stampTriggeredJobRun")
	defer b.mu.Unlock()

	run.TriggerName = triggerName
	run.WorkflowRunID = workflowRunID
}

// stampTriggeredCrawl sets workflowRunID on the crawl StartCrawler just
// appended for crawlerName. StartCrawler having just succeeded guarantees that
// entry is the crawler's sole in-flight crawl (StartCrawler rejects a second
// concurrent run), so grabbing the last history entry is unambiguous.
func (b *InMemoryBackend) stampTriggeredCrawl(crawlerName, workflowRunID string) {
	b.mu.Lock("stampTriggeredCrawl")
	defer b.mu.Unlock()

	hist := b.crawlHistory[crawlerName]
	if len(hist) == 0 {
		return
	}

	hist[len(hist)-1].WorkflowRunID = workflowRunID
}

// StopTrigger deactivates a Glue trigger. On-demand triggers never enter the
// DEACTIVATED state (AWS: they always remain CREATED), so StopTrigger is a no-op
// for them beyond existence-checking.
func (b *InMemoryBackend) StopTrigger(name string) error {
	b.mu.Lock("StopTrigger")
	defer b.mu.Unlock()

	t, ok := b.triggers.Get(name)
	if !ok {
		return ErrNotFound
	}

	if t.Type != triggerTypeOnDemand {
		t.State = "DEACTIVATED"
	}

	return nil
}
