package glue

import (
	"fmt"
	"maps"
	mrand "math/rand/v2"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// TriggerAction represents an action for a Glue trigger.
type TriggerAction struct {
	Arguments map[string]string `json:"Arguments,omitempty"`
	JobName   string            `json:"JobName,omitempty"`
}

// TriggerPredicate represents a predicate for a conditional trigger.
type TriggerPredicate struct {
	Logical    string             `json:"Logical,omitempty"`
	Conditions []TriggerCondition `json:"Conditions,omitempty"`
}

// TriggerCondition represents a condition within a trigger predicate.
type TriggerCondition struct {
	JobName         string `json:"JobName,omitempty"`
	LogicalOperator string `json:"LogicalOperator,omitempty"`
	State           string `json:"State,omitempty"`
}

// Trigger represents a Glue trigger.
type Trigger struct {
	Tags      map[string]string `json:"-"`
	Predicate *TriggerPredicate `json:"Predicate,omitempty"`
	ARN       string            `json:"Arn,omitempty"`
	Name      string            `json:"Name"`
	Type      string            `json:"Type,omitempty"`
	State     string            `json:"State,omitempty"`
	Schedule  string            `json:"Schedule,omitempty"`
	Actions   []TriggerAction   `json:"Actions,omitempty"`
}

// Workflow represents a Glue workflow.
type Workflow struct {
	Tags                 map[string]string `json:"-"`
	DefaultRunProperties map[string]string `json:"DefaultRunProperties,omitempty"`
	Name                 string            `json:"Name"`
	Description          string            `json:"Description,omitempty"`
	ARN                  string            `json:"Arn,omitempty"`
	CreatedOn            float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn       float64           `json:"LastModifiedOn,omitempty"`
}

// WorkflowRun represents a single run of a Glue workflow.
type WorkflowRun struct {
	Properties   map[string]string `json:"WorkflowRunProperties,omitempty"`
	WorkflowName string            `json:"WorkflowName"`
	RunID        string            `json:"WorkflowRunId"`
	Status       string            `json:"Status"`
	StartedOn    float64           `json:"StartedOn,omitempty"`
	CompletedOn  float64           `json:"CompletedOn,omitempty"`
}

// GrokClassifier is a Grok-based classifier.
type GrokClassifier struct {
	Name           string `json:"Name"`
	Classification string `json:"Classification,omitempty"`
	GrokPattern    string `json:"GrokPattern,omitempty"`
	CustomPatterns string `json:"CustomPatterns,omitempty"`
}

// XMLClassifier is an XML-based classifier.
type XMLClassifier struct {
	Name           string `json:"Name"`
	Classification string `json:"Classification,omitempty"`
	RowTag         string `json:"RowTag,omitempty"`
}

// JSONClassifier is a JSON-based classifier.
type JSONClassifier struct {
	Name     string `json:"Name"`
	JSONPath string `json:"JsonPath,omitempty"`
}

// CsvClassifier is a CSV-based classifier.
type CsvClassifier struct {
	Name        string   `json:"Name"`
	Delimiter   string   `json:"Delimiter,omitempty"`
	QuoteSymbol string   `json:"QuoteSymbol,omitempty"`
	Header      []string `json:"Header,omitempty"`
}

// Classifier wraps the four classifier types.
type Classifier struct {
	GrokClassifier *GrokClassifier `json:"GrokClassifier,omitempty"`
	XMLClassifier  *XMLClassifier  `json:"XMLClassifier,omitempty"`
	JSONClassifier *JSONClassifier `json:"JsonClassifier,omitempty"`
	CsvClassifier  *CsvClassifier  `json:"CsvClassifier,omitempty"`
}

// classifierName returns the primary name for a classifier.
func classifierName(c *Classifier) string {
	switch {
	case c.GrokClassifier != nil:
		return c.GrokClassifier.Name
	case c.XMLClassifier != nil:
		return c.XMLClassifier.Name
	case c.JSONClassifier != nil:
		return c.JSONClassifier.Name
	case c.CsvClassifier != nil:
		return c.CsvClassifier.Name
	}

	return ""
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

	return &cp
}

// cloneWorkflow returns a shallow copy of a Workflow with cloned maps.
func cloneWorkflow(w *Workflow) *Workflow {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)
	cp.DefaultRunProperties = maps.Clone(w.DefaultRunProperties)

	return &cp
}

// triggerARN returns the ARN for a Glue trigger.
func (b *InMemoryBackend) triggerARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "trigger/"+name)
}

// workflowARN returns the ARN for a Glue workflow.
func (b *InMemoryBackend) workflowARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "workflow/"+name)
}

// --- Trigger operations ---

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

// StartTrigger activates a Glue trigger.
func (b *InMemoryBackend) StartTrigger(name string) error {
	b.mu.Lock("StartTrigger")
	defer b.mu.Unlock()

	t, ok := b.triggers.Get(name)
	if !ok {
		return ErrNotFound
	}

	t.State = "ACTIVATED"

	return nil
}

// StopTrigger deactivates a Glue trigger.
func (b *InMemoryBackend) StopTrigger(name string) error {
	b.mu.Lock("StopTrigger")
	defer b.mu.Unlock()

	t, ok := b.triggers.Get(name)
	if !ok {
		return ErrNotFound
	}

	t.State = "DEACTIVATED"

	return nil
}

// --- Workflow operations ---

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

	if !b.workflows.Has(name) {
		return nil, ErrNotFound
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

// --- Classifier operations ---

// CreateClassifier creates a new Glue classifier.
func (b *InMemoryBackend) CreateClassifier(c Classifier) error {
	b.mu.Lock("CreateClassifier")
	defer b.mu.Unlock()

	name := classifierName(&c)
	if name == "" {
		return ErrValidation
	}

	if b.classifiers.Has(name) {
		return ErrAlreadyExists
	}

	cp := c
	b.classifiers.Put(&cp)

	return nil
}

// GetClassifier retrieves a Glue classifier by name.
func (b *InMemoryBackend) GetClassifier(name string) (*Classifier, error) {
	b.mu.RLock("GetClassifier")
	defer b.mu.RUnlock()

	c, ok := b.classifiers.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *c

	return &cp, nil
}

// GetClassifiers returns all Glue classifiers sorted by name.
func (b *InMemoryBackend) GetClassifiers() []*Classifier {
	b.mu.RLock("GetClassifiers")
	defer b.mu.RUnlock()

	src := b.classifiers.Snapshot()
	out := make([]*Classifier, 0, len(src))
	for _, c := range src {
		cp := *c
		out = append(out, &cp)
	}

	return out
}

// UpdateClassifier updates an existing Glue classifier.
func (b *InMemoryBackend) UpdateClassifier(c Classifier) error {
	b.mu.Lock("UpdateClassifier")
	defer b.mu.Unlock()

	name := classifierName(&c)
	if name == "" {
		return ErrValidation
	}

	if !b.classifiers.Has(name) {
		return ErrNotFound
	}

	cp := c
	b.classifiers.Put(&cp)

	return nil
}

// DeleteClassifier deletes a Glue classifier by name.
func (b *InMemoryBackend) DeleteClassifier(name string) error {
	b.mu.Lock("DeleteClassifier")
	defer b.mu.Unlock()

	if !b.classifiers.Has(name) {
		return ErrNotFound
	}

	b.classifiers.Delete(name)

	return nil
}

// --- DevEndpoint full CRUD ---

// CreateDevEndpoint creates a new Glue dev endpoint.
func (b *InMemoryBackend) CreateDevEndpoint(name string) (*DevEndpoint, error) {
	b.mu.Lock("CreateDevEndpoint")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrValidation
	}

	if b.devEndpoints.Has(name) {
		return nil, ErrAlreadyExists
	}

	dep := &DevEndpoint{
		EndpointName: name,
		Status:       stateReady,
	}
	b.devEndpoints.Put(dep)

	cp := *dep

	return &cp, nil
}

// GetDevEndpoint retrieves a Glue dev endpoint by name.
func (b *InMemoryBackend) GetDevEndpoint(name string) (*DevEndpoint, error) {
	b.mu.RLock("GetDevEndpoint")
	defer b.mu.RUnlock()

	dep, ok := b.devEndpoints.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *dep

	return &cp, nil
}

// GetAllDevEndpoints returns all dev endpoints sorted by name.
func (b *InMemoryBackend) GetAllDevEndpoints() []*DevEndpoint {
	b.mu.RLock("GetAllDevEndpoints")
	defer b.mu.RUnlock()

	src := b.devEndpoints.Snapshot()
	out := make([]*DevEndpoint, 0, len(src))
	for _, dep := range src {
		cp := *dep
		out = append(out, &cp)
	}

	return out
}

// DeleteDevEndpoint deletes a Glue dev endpoint by name.
func (b *InMemoryBackend) DeleteDevEndpoint(name string) error {
	b.mu.Lock("DeleteDevEndpoint")
	defer b.mu.Unlock()

	if !b.devEndpoints.Has(name) {
		return ErrNotFound
	}

	b.devEndpoints.Delete(name)

	return nil
}
