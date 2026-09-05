package glue

import (
	"context"
)

// batchGetWorkflowsInput holds input for BatchGetWorkflows.
type batchGetWorkflowsInput struct {
	Names        []string `json:"Names"`
	IncludeGraph bool     `json:"IncludeGraph,omitempty"`
}

// batchGetWorkflowsOutput holds the result for BatchGetWorkflows.
type batchGetWorkflowsOutput struct {
	Workflows        []*Workflow `json:"Workflows"`
	MissingWorkflows []string    `json:"MissingWorkflows"`
}

func (h *Handler) handleBatchGetWorkflows(
	_ context.Context,
	in *batchGetWorkflowsInput,
) (*batchGetWorkflowsOutput, error) {
	found, missing := h.Backend.BatchGetWorkflows(in.Names, in.IncludeGraph)

	return &batchGetWorkflowsOutput{Workflows: found, MissingWorkflows: missing}, nil
}

// createWorkflowInput holds input for CreateWorkflow.
type createWorkflowInput struct {
	Tags                 map[string]string `json:"Tags,omitempty"`
	DefaultRunProperties map[string]string `json:"DefaultRunProperties,omitempty"`
	Name                 string            `json:"Name"`
	Description          string            `json:"Description,omitempty"`
	MaxConcurrentRuns    int               `json:"MaxConcurrentRuns,omitempty"`
}

// createWorkflowOutput holds the result for CreateWorkflow.
type createWorkflowOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateWorkflow(
	_ context.Context,
	in *createWorkflowInput,
) (*createWorkflowOutput, error) {
	w := Workflow{
		Name:                 in.Name,
		Description:          in.Description,
		DefaultRunProperties: in.DefaultRunProperties,
		MaxConcurrentRuns:    in.MaxConcurrentRuns,
	}

	created, err := h.Backend.CreateWorkflow(w, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createWorkflowOutput{Name: created.Name}, nil
}

// deleteWorkflowInput holds input for DeleteWorkflow.
type deleteWorkflowInput struct {
	Name string `json:"Name"`
}

// deleteWorkflowOutput holds the result for DeleteWorkflow.
type deleteWorkflowOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteWorkflow(
	_ context.Context,
	in *deleteWorkflowInput,
) (*deleteWorkflowOutput, error) {
	if err := h.Backend.DeleteWorkflow(in.Name); err != nil {
		return nil, err
	}

	return &deleteWorkflowOutput{Name: in.Name}, nil
}

// getWorkflowInput holds input for GetWorkflow.
type getWorkflowInput struct {
	Name         string `json:"Name"`
	IncludeGraph bool   `json:"IncludeGraph,omitempty"`
}

// getWorkflowOutput holds the result for GetWorkflow.
type getWorkflowOutput struct {
	Workflow *Workflow `json:"Workflow"`
}

func (h *Handler) handleGetWorkflow(
	_ context.Context,
	in *getWorkflowInput,
) (*getWorkflowOutput, error) {
	w, err := h.Backend.GetWorkflow(in.Name, in.IncludeGraph)
	if err != nil {
		return nil, err
	}

	return &getWorkflowOutput{Workflow: w}, nil
}

// getWorkflowRunInput holds input for GetWorkflowRun.
type getWorkflowRunInput struct {
	Name  string `json:"Name"`
	RunID string `json:"RunId"`
}

// getWorkflowRunOutput holds the result for GetWorkflowRun.
type getWorkflowRunOutput struct {
	Run *WorkflowRun `json:"Run"`
}

func (h *Handler) handleGetWorkflowRun(
	_ context.Context,
	in *getWorkflowRunInput,
) (*getWorkflowRunOutput, error) {
	run, err := h.Backend.GetWorkflowRun(in.Name, in.RunID)
	if err != nil {
		return nil, err
	}

	return &getWorkflowRunOutput{Run: run}, nil
}

// getWorkflowRunPropertiesInput holds input for GetWorkflowRunProperties.
type getWorkflowRunPropertiesInput struct {
	Name  string `json:"Name"`
	RunID string `json:"RunId"`
}

// getWorkflowRunPropertiesOutput holds the result for GetWorkflowRunProperties.
type getWorkflowRunPropertiesOutput struct {
	RunProperties map[string]string `json:"RunProperties"`
}

func (h *Handler) handleGetWorkflowRunProperties(
	_ context.Context,
	in *getWorkflowRunPropertiesInput,
) (*getWorkflowRunPropertiesOutput, error) {
	if in.Name != "" && in.RunID != "" {
		run, err := h.Backend.GetWorkflowRun(in.Name, in.RunID)
		if err == nil && run.Properties != nil {
			return &getWorkflowRunPropertiesOutput{RunProperties: run.Properties}, nil
		}
	}

	return &getWorkflowRunPropertiesOutput{RunProperties: map[string]string{}}, nil
}

// getWorkflowRunsInput holds input for GetWorkflowRuns.
type getWorkflowRunsInput struct {
	Name string `json:"Name"`
}

// getWorkflowRunsOutput holds the result for GetWorkflowRuns.
type getWorkflowRunsOutput struct {
	Runs []*WorkflowRun `json:"Runs"`
}

func (h *Handler) handleGetWorkflowRuns(
	_ context.Context,
	in *getWorkflowRunsInput,
) (*getWorkflowRunsOutput, error) {
	runs, err := h.Backend.GetWorkflowRuns(in.Name)
	if err != nil {
		return nil, err
	}

	return &getWorkflowRunsOutput{Runs: runs}, nil
}

// defaultListWorkflowsLimit is used when ListWorkflowsInput.MaxResults is unset.
const defaultListWorkflowsLimit = 100

// listWorkflowsInput holds input for ListWorkflows.
type listWorkflowsInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// listWorkflowsOutput holds the result for ListWorkflows.
type listWorkflowsOutput struct {
	NextToken string   `json:"NextToken,omitempty"`
	Workflows []string `json:"Workflows"`
}

func (h *Handler) handleListWorkflows(
	_ context.Context,
	in *listWorkflowsInput,
) (*listWorkflowsOutput, error) {
	all := h.Backend.GetWorkflows()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultListWorkflowsLimit
	}

	page, next := paginateSlice(all, in.NextToken, limit)

	return &listWorkflowsOutput{Workflows: page, NextToken: next}, nil
}

// putWorkflowRunPropertiesInput holds input for PutWorkflowRunProperties.
type putWorkflowRunPropertiesInput struct {
	RunProperties map[string]string `json:"RunProperties"`
	Name          string            `json:"Name"`
	RunID         string            `json:"RunId"`
}

func (h *Handler) handlePutWorkflowRunProperties(
	_ context.Context,
	in *putWorkflowRunPropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutWorkflowRunProperties(in.Name, in.RunID, in.RunProperties)
}

// resumeWorkflowRunInput holds input for ResumeWorkflowRun.
type resumeWorkflowRunInput struct {
	Name    string   `json:"Name"`
	RunID   string   `json:"RunId"`
	NodeIDs []string `json:"NodeIds"`
}

// resumeWorkflowRunOutput holds the result for ResumeWorkflowRun.
type resumeWorkflowRunOutput struct {
	RunID   string   `json:"RunId"`
	NodeIDs []string `json:"NodeIds"`
}

func (h *Handler) handleResumeWorkflowRun(
	_ context.Context,
	in *resumeWorkflowRunInput,
) (*resumeWorkflowRunOutput, error) {
	if in.Name == "" || in.RunID == "" {
		return &resumeWorkflowRunOutput{NodeIDs: []string{}}, nil
	}

	runID, nodeIDs, err := h.Backend.ResumeWorkflowRun(in.Name, in.RunID, in.NodeIDs)
	if err != nil {
		return nil, err
	}

	return &resumeWorkflowRunOutput{RunID: runID, NodeIDs: nodeIDs}, nil
}

// startWorkflowRunInput holds input for StartWorkflowRun.
type startWorkflowRunInput struct {
	Name string `json:"Name"`
}

// startWorkflowRunOutput holds the result for StartWorkflowRun.
type startWorkflowRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartWorkflowRun(
	_ context.Context,
	in *startWorkflowRunInput,
) (*startWorkflowRunOutput, error) {
	run, err := h.Backend.StartWorkflowRun(in.Name)
	if err != nil {
		return nil, err
	}

	return &startWorkflowRunOutput{RunID: run.RunID}, nil
}

// stopWorkflowRunInput holds input for StopWorkflowRun.
type stopWorkflowRunInput struct {
	Name  string `json:"Name"`
	RunID string `json:"RunId"`
}

func (h *Handler) handleStopWorkflowRun(
	_ context.Context,
	in *stopWorkflowRunInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StopWorkflowRun(in.Name, in.RunID)
}

// updateWorkflowInput holds input for UpdateWorkflow.
type updateWorkflowInput struct {
	DefaultRunProperties map[string]string `json:"DefaultRunProperties,omitempty"`
	Name                 string            `json:"Name"`
	Description          string            `json:"Description,omitempty"`
	MaxConcurrentRuns    int               `json:"MaxConcurrentRuns,omitempty"`
}

// updateWorkflowOutput holds the result for UpdateWorkflow.
type updateWorkflowOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateWorkflow(
	_ context.Context,
	in *updateWorkflowInput,
) (*updateWorkflowOutput, error) {
	update := Workflow{
		Description:          in.Description,
		DefaultRunProperties: in.DefaultRunProperties,
		MaxConcurrentRuns:    in.MaxConcurrentRuns,
	}
	if err := h.Backend.UpdateWorkflow(in.Name, update); err != nil {
		return nil, err
	}

	return &updateWorkflowOutput{Name: in.Name}, nil
}
