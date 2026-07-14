package glue

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// triggerTypeOnDemand is the TriggerType wire value for on-demand triggers. Per AWS
// docs (about-triggers.html): "On-demand triggers never enter the ACTIVATED or
// DEACTIVATED state. They always remain in the CREATED state," and firing one
// (StartTrigger) immediately runs its actions rather than switching to a
// long-lived "active" monitoring state like SCHEDULED/CONDITIONAL/EVENT triggers do.
const triggerTypeOnDemand = "ON_DEMAND"

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

	t, ok := b.triggers.Get(name)
	if !ok {
		return ErrNotFound
	}

	t.Schedule = update.Schedule
	t.Actions = update.Actions
	t.Predicate = update.Predicate

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
	b.mu.Lock("StartTrigger")

	t, ok := b.triggers.Get(name)
	if !ok {
		b.mu.Unlock()

		return ErrNotFound
	}

	onDemand := t.Type == triggerTypeOnDemand
	if !onDemand {
		t.State = "ACTIVATED"
	}

	actions := append([]TriggerAction(nil), t.Actions...)
	b.mu.Unlock()

	if onDemand {
		// Fire outside the trigger lock: StartJobRun/StartCrawler take the same
		// coarse backend lock, and it is not reentrant.
		b.fireTriggerActions(actions)
	}

	return nil
}

// fireTriggerActions starts the job run or crawler run for each action of a fired
// on-demand trigger. Per-action errors are not propagated: AWS's StartTrigger
// returns as soon as the trigger fires, before the resulting job runs/crawls
// complete or are even guaranteed to start successfully.
func (b *InMemoryBackend) fireTriggerActions(actions []TriggerAction) {
	for _, a := range actions {
		switch {
		case a.JobName != "":
			_, _ = b.StartJobRun(a.JobName, a.Arguments)
		case a.CrawlerName != "":
			_ = b.StartCrawler(a.CrawlerName)
		}
	}
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
