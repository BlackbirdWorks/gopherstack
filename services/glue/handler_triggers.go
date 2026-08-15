package glue

import (
	"context"
)

// batchGetTriggersInput holds input for BatchGetTriggers.
type batchGetTriggersInput struct {
	TriggerNames []string `json:"TriggerNames"`
}

// batchGetTriggersOutput holds the result for BatchGetTriggers.
type batchGetTriggersOutput struct {
	Triggers         []*Trigger `json:"Triggers"`
	TriggersNotFound []string   `json:"TriggersNotFound"`
}

func (h *Handler) handleBatchGetTriggers(
	_ context.Context,
	in *batchGetTriggersInput,
) (*batchGetTriggersOutput, error) {
	found, missing := h.Backend.BatchGetTriggers(in.TriggerNames)

	return &batchGetTriggersOutput{Triggers: found, TriggersNotFound: missing}, nil
}

// createTriggerInput holds input for CreateTrigger.
type createTriggerInput struct {
	Tags                   map[string]string              `json:"Tags,omitempty"`
	Predicate              *TriggerPredicate              `json:"Predicate,omitempty"`
	EventBatchingCondition *TriggerEventBatchingCondition `json:"EventBatchingCondition,omitempty"`
	Schedule               string                         `json:"Schedule,omitempty"`
	Name                   string                         `json:"Name"`
	Type                   string                         `json:"Type,omitempty"`
	Description            string                         `json:"Description,omitempty"`
	WorkflowName           string                         `json:"WorkflowName,omitempty"`
	Actions                []TriggerAction                `json:"Actions,omitempty"`
	StartOnCreation        bool                           `json:"StartOnCreation,omitempty"`
}

// createTriggerOutput holds the result for CreateTrigger.
type createTriggerOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateTrigger(
	_ context.Context,
	in *createTriggerInput,
) (*createTriggerOutput, error) {
	t := Trigger{
		Name:                   in.Name,
		Type:                   in.Type,
		Schedule:               in.Schedule,
		Actions:                in.Actions,
		Predicate:              in.Predicate,
		StartOnCreation:        in.StartOnCreation,
		Description:            in.Description,
		WorkflowName:           in.WorkflowName,
		EventBatchingCondition: in.EventBatchingCondition,
	}

	created, err := h.Backend.CreateTrigger(t, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createTriggerOutput{Name: created.Name}, nil
}

// deleteTriggerInput holds input for DeleteTrigger.
type deleteTriggerInput struct {
	Name string `json:"Name"`
}

// deleteTriggerOutput holds the result for DeleteTrigger.
type deleteTriggerOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteTrigger(
	_ context.Context,
	in *deleteTriggerInput,
) (*deleteTriggerOutput, error) {
	if err := h.Backend.DeleteTrigger(in.Name); err != nil {
		return nil, err
	}

	return &deleteTriggerOutput{Name: in.Name}, nil
}

// getTriggerInput holds input for GetTrigger.
type getTriggerInput struct {
	Name string `json:"Name"`
}

// getTriggerOutput holds the result for GetTrigger.
type getTriggerOutput struct {
	Trigger *Trigger `json:"Trigger"`
}

func (h *Handler) handleGetTrigger(
	_ context.Context,
	in *getTriggerInput,
) (*getTriggerOutput, error) {
	t, err := h.Backend.GetTrigger(in.Name)
	if err != nil {
		return nil, err
	}

	return &getTriggerOutput{Trigger: t}, nil
}

// defaultGetTriggersLimit is used when MaxResults is unset on
// GetTriggersInput/ListTriggersInput.
const defaultGetTriggersLimit = 100

// triggerHasDependentJob reports whether t has an action that starts job.
func triggerHasDependentJob(t *Trigger, job string) bool {
	for _, a := range t.Actions {
		if a.JobName == job {
			return true
		}
	}

	return false
}

// filterByDependentJobName mirrors GetTriggers/ListTriggers'
// DependentJobName semantics (api_op_GetTriggers.go: "The trigger that can
// start this job is returned, and if there is no such trigger, all triggers
// are returned"): filter to triggers with a matching action, but fall back
// to the full list when nothing matches rather than returning empty.
func filterByDependentJobName(triggers []*Trigger, job string) []*Trigger {
	if job == "" {
		return triggers
	}

	matching := make([]*Trigger, 0, len(triggers))

	for _, t := range triggers {
		if triggerHasDependentJob(t, job) {
			matching = append(matching, t)
		}
	}

	if len(matching) == 0 {
		return triggers
	}

	return matching
}

// getTriggersInput holds input for GetTriggers.
type getTriggersInput struct {
	DependentJobName string `json:"DependentJobName,omitempty"`
	NextToken        string `json:"NextToken,omitempty"`
	MaxResults       int32  `json:"MaxResults,omitempty"`
}

// getTriggersOutput holds the result for GetTriggers.
type getTriggersOutput struct {
	NextToken string     `json:"NextToken,omitempty"`
	Triggers  []*Trigger `json:"Triggers"`
}

func (h *Handler) handleGetTriggers(
	_ context.Context,
	in *getTriggersInput,
) (*getTriggersOutput, error) {
	triggers := filterByDependentJobName(h.Backend.GetTriggers(), in.DependentJobName)

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetTriggersLimit
	}

	page, next := paginateSlice(triggers, in.NextToken, limit)

	return &getTriggersOutput{Triggers: page, NextToken: next}, nil
}

// listTriggersInput holds input for ListTriggers.
type listTriggersInput struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	DependentJobName string            `json:"DependentJobName,omitempty"`
	NextToken        string            `json:"NextToken,omitempty"`
	MaxResults       int32             `json:"MaxResults,omitempty"`
}

// listTriggersOutput holds the result for ListTriggers.
type listTriggersOutput struct {
	NextToken    string   `json:"NextToken,omitempty"`
	TriggerNames []string `json:"TriggerNames"`
}

func (h *Handler) handleListTriggers(
	_ context.Context,
	in *listTriggersInput,
) (*listTriggersOutput, error) {
	triggers := filterByDependentJobName(h.Backend.GetTriggers(), in.DependentJobName)

	if len(in.Tags) > 0 {
		filtered := make([]*Trigger, 0, len(triggers))

		for _, t := range triggers {
			if matchesTagFilter(t.Tags, in.Tags) {
				filtered = append(filtered, t)
			}
		}

		triggers = filtered
	}

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetTriggersLimit
	}

	page, next := paginateSlice(triggers, in.NextToken, limit)

	names := make([]string, 0, len(page))
	for _, t := range page {
		names = append(names, t.Name)
	}

	return &listTriggersOutput{TriggerNames: names, NextToken: next}, nil
}

// startTriggerInput holds input for StartTrigger.
type startTriggerInput struct {
	Name string `json:"Name"`
}

// startTriggerOutput holds the result for StartTrigger.
type startTriggerOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartTrigger(
	_ context.Context,
	in *startTriggerInput,
) (*startTriggerOutput, error) {
	if err := h.Backend.StartTrigger(in.Name); err != nil {
		return nil, err
	}

	return &startTriggerOutput{Name: in.Name}, nil
}

// stopTriggerInput holds input for StopTrigger.
type stopTriggerInput struct {
	Name string `json:"Name"`
}

// stopTriggerOutput holds the result for StopTrigger.
type stopTriggerOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStopTrigger(
	_ context.Context,
	in *stopTriggerInput,
) (*stopTriggerOutput, error) {
	if err := h.Backend.StopTrigger(in.Name); err != nil {
		return nil, err
	}

	return &stopTriggerOutput{Name: in.Name}, nil
}

// updateTriggerInput holds input for UpdateTrigger.
type updateTriggerInput struct {
	Name          string        `json:"Name"`
	TriggerUpdate triggerUpdate `json:"TriggerUpdate"`
}

// triggerUpdate holds the mutable fields for UpdateTrigger.
type triggerUpdate struct {
	Predicate              *TriggerPredicate              `json:"Predicate,omitempty"`
	EventBatchingCondition *TriggerEventBatchingCondition `json:"EventBatchingCondition,omitempty"`
	Schedule               string                         `json:"Schedule,omitempty"`
	Description            string                         `json:"Description,omitempty"`
	Actions                []TriggerAction                `json:"Actions,omitempty"`
}

// updateTriggerOutput holds the result for UpdateTrigger.
type updateTriggerOutput struct {
	Trigger *Trigger `json:"Trigger"`
}

func (h *Handler) handleUpdateTrigger(
	_ context.Context,
	in *updateTriggerInput,
) (*updateTriggerOutput, error) {
	update := Trigger{
		Schedule:               in.TriggerUpdate.Schedule,
		Actions:                in.TriggerUpdate.Actions,
		Predicate:              in.TriggerUpdate.Predicate,
		Description:            in.TriggerUpdate.Description,
		EventBatchingCondition: in.TriggerUpdate.EventBatchingCondition,
	}
	if err := h.Backend.UpdateTrigger(in.Name, update); err != nil {
		return nil, err
	}

	t, err := h.Backend.GetTrigger(in.Name)
	if err != nil {
		return nil, err
	}

	return &updateTriggerOutput{Trigger: t}, nil
}
