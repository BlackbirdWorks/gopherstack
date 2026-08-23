package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type copyStepDetailsInput struct {
	DestinationFileLocation map[string]any `json:"DestinationFileLocation,omitempty"`
	Name                    string         `json:"Name,omitempty"`
	SourceFileLocation      string         `json:"SourceFileLocation,omitempty"`
	OverwriteExisting       string         `json:"OverwriteExisting,omitempty"`
}

type customStepDetailsInput struct {
	Name               string `json:"Name,omitempty"`
	Target             string `json:"Target,omitempty"`
	SourceFileLocation string `json:"SourceFileLocation,omitempty"`
	Timeout            int32  `json:"Timeout,omitempty"`
}

type deleteStepDetailsInput struct {
	Name               string `json:"Name,omitempty"`
	SourceFileLocation string `json:"SourceFileLocation,omitempty"`
}

type tagStepDetailsInput struct {
	Name               string           `json:"Name,omitempty"`
	SourceFileLocation string           `json:"SourceFileLocation,omitempty"`
	Tags               []map[string]any `json:"Tags,omitempty"`
}

type decryptStepDetailsInput struct {
	DestinationFileLocation map[string]any `json:"DestinationFileLocation,omitempty"`
	Name                    string         `json:"Name,omitempty"`
	Type                    string         `json:"Type,omitempty"`
	SourceFileLocation      string         `json:"SourceFileLocation,omitempty"`
	OverwriteExisting       string         `json:"OverwriteExisting,omitempty"`
}

type workflowStepInput struct {
	CopyStepDetails    *copyStepDetailsInput    `json:"CopyStepDetails,omitempty"`
	CustomStepDetails  *customStepDetailsInput  `json:"CustomStepDetails,omitempty"`
	DeleteStepDetails  *deleteStepDetailsInput  `json:"DeleteStepDetails,omitempty"`
	TagStepDetails     *tagStepDetailsInput     `json:"TagStepDetails,omitempty"`
	DecryptStepDetails *decryptStepDetailsInput `json:"DecryptStepDetails,omitempty"`
	Type               string                   `json:"Type"`
}

type createWorkflowInput struct {
	Description      string              `json:"Description"`
	Steps            []workflowStepInput `json:"Steps,omitempty"`
	OnExceptionSteps []workflowStepInput `json:"OnExceptionSteps,omitempty"`
	Tags             []map[string]string `json:"Tags"`
}

type createWorkflowOutput struct {
	WorkflowID string `json:"WorkflowId"`
}

func toWorkflowStep(s workflowStepInput) WorkflowStep {
	ws := WorkflowStep{Type: s.Type}

	if s.CopyStepDetails != nil {
		ws.CopyStepDetails = &CopyStepDetails{
			Name:                    s.CopyStepDetails.Name,
			SourceFileLocation:      s.CopyStepDetails.SourceFileLocation,
			OverwriteExisting:       s.CopyStepDetails.OverwriteExisting,
			DestinationFileLocation: s.CopyStepDetails.DestinationFileLocation,
		}
	}

	if s.CustomStepDetails != nil {
		ws.CustomStepDetails = &CustomStepDetails{
			Name:               s.CustomStepDetails.Name,
			Target:             s.CustomStepDetails.Target,
			SourceFileLocation: s.CustomStepDetails.SourceFileLocation,
			Timeout:            s.CustomStepDetails.Timeout,
		}
	}

	if s.DeleteStepDetails != nil {
		ws.DeleteStepDetails = &DeleteStepDetails{
			Name:               s.DeleteStepDetails.Name,
			SourceFileLocation: s.DeleteStepDetails.SourceFileLocation,
		}
	}

	if s.TagStepDetails != nil {
		ws.TagStepDetails = &TagStepDetails{
			Name:               s.TagStepDetails.Name,
			SourceFileLocation: s.TagStepDetails.SourceFileLocation,
			Tags:               s.TagStepDetails.Tags,
		}
	}

	if s.DecryptStepDetails != nil {
		ws.DecryptStepDetails = &DecryptStepDetails{
			Name:                    s.DecryptStepDetails.Name,
			Type:                    s.DecryptStepDetails.Type,
			SourceFileLocation:      s.DecryptStepDetails.SourceFileLocation,
			OverwriteExisting:       s.DecryptStepDetails.OverwriteExisting,
			DestinationFileLocation: s.DecryptStepDetails.DestinationFileLocation,
		}
	}

	return ws
}

func toWorkflowSteps(inputs []workflowStepInput) []WorkflowStep {
	if inputs == nil {
		return nil
	}

	out := make([]WorkflowStep, len(inputs))
	for i, s := range inputs {
		out[i] = toWorkflowStep(s)
	}

	return out
}

func workflowStepToMap(s WorkflowStep) map[string]any {
	m := map[string]any{keyStepType: s.Type}

	if s.CopyStepDetails != nil {
		m["CopyStepDetails"] = map[string]any{
			keyStepName:               s.CopyStepDetails.Name,
			keySourceFileLoc:          s.CopyStepDetails.SourceFileLocation,
			"OverwriteExisting":       s.CopyStepDetails.OverwriteExisting,
			"DestinationFileLocation": s.CopyStepDetails.DestinationFileLocation,
		}
	}

	if s.CustomStepDetails != nil {
		m["CustomStepDetails"] = map[string]any{
			keyStepName:      s.CustomStepDetails.Name,
			"Target":         s.CustomStepDetails.Target,
			keySourceFileLoc: s.CustomStepDetails.SourceFileLocation,
			// transfer@v1.75.4 deserializers.go's real member is
			// TimeoutSeconds, not Timeout.
			"TimeoutSeconds": s.CustomStepDetails.Timeout,
		}
	}

	if s.DeleteStepDetails != nil {
		m["DeleteStepDetails"] = map[string]any{
			keyStepName:      s.DeleteStepDetails.Name,
			keySourceFileLoc: s.DeleteStepDetails.SourceFileLocation,
		}
	}

	if s.TagStepDetails != nil {
		m["TagStepDetails"] = map[string]any{
			keyStepName:      s.TagStepDetails.Name,
			keySourceFileLoc: s.TagStepDetails.SourceFileLocation,
			keyTags:          s.TagStepDetails.Tags,
		}
	}

	if s.DecryptStepDetails != nil {
		m["DecryptStepDetails"] = map[string]any{
			keyStepName:               s.DecryptStepDetails.Name,
			keyStepType:               s.DecryptStepDetails.Type,
			keySourceFileLoc:          s.DecryptStepDetails.SourceFileLocation,
			"OverwriteExisting":       s.DecryptStepDetails.OverwriteExisting,
			"DestinationFileLocation": s.DecryptStepDetails.DestinationFileLocation,
		}
	}

	return m
}

func (h *Handler) handleCreateWorkflow(
	_ context.Context,
	in *createWorkflowInput,
) (*createWorkflowOutput, error) {
	tags := tagsFromList(in.Tags)

	wf, err := h.Backend.CreateWorkflow(
		in.Description,
		toWorkflowSteps(in.Steps),
		toWorkflowSteps(in.OnExceptionSteps),
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createWorkflowOutput{WorkflowID: wf.WorkflowID}, nil
}

// workflowARN builds the ARN for a Transfer workflow.
func workflowARN(accountID, region, workflowID string) string {
	return arn.Build("transfer", region, accountID, "workflow/"+workflowID)
}

type deleteWorkflowInput struct {
	WorkflowID string `json:"WorkflowId"`
}

func (h *Handler) handleDeleteWorkflow(
	_ context.Context,
	in *deleteWorkflowInput,
) (*struct{}, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWorkflow(in.WorkflowID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeWorkflowInput struct {
	WorkflowID string `json:"WorkflowId"`
}

type describeWorkflowOutput struct {
	Workflow map[string]any `json:"Workflow"`
}

func (h *Handler) handleDescribeWorkflow(
	_ context.Context,
	in *describeWorkflowInput,
) (*describeWorkflowOutput, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	wf, err := h.Backend.DescribeWorkflow(in.WorkflowID)
	if err != nil {
		return nil, err
	}

	stepsToList := func(steps []WorkflowStep) []any {
		out := make([]any, len(steps))
		for i, s := range steps {
			out[i] = workflowStepToMap(s)
		}

		return out
	}

	return &describeWorkflowOutput{
		Workflow: map[string]any{
			keyWorkflowID:      wf.WorkflowID,
			keyDescription:     wf.Description,
			"Steps":            stepsToList(wf.Steps),
			"OnExceptionSteps": stepsToList(wf.OnExceptionSteps),
			keyArn:             workflowARN(wf.AccountID, wf.Region, wf.WorkflowID),
			keyTags:            tagsToList(wf.Tags),
		},
	}, nil
}

type listWorkflowsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listWorkflowsOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	Workflows []map[string]any `json:"Workflows"`
}

func (h *Handler) handleListWorkflows(
	_ context.Context,
	in *listWorkflowsInput,
) (*listWorkflowsOutput, error) {
	items := h.Backend.ListWorkflows()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, wf := range page {
		out[i] = map[string]any{
			keyWorkflowID:  wf.WorkflowID,
			keyDescription: wf.Description,
			keyArn:         workflowARN(wf.AccountID, wf.Region, wf.WorkflowID),
		}
	}

	return &listWorkflowsOutput{Workflows: out, NextToken: next}, nil
}

type listExecutionsInput struct {
	WorkflowID string `json:"WorkflowId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

func (h *Handler) handleListExecutions(
	_ context.Context,
	in *listExecutionsInput,
) (*map[string]any, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListExecutions(in.WorkflowID)
	if err != nil {
		return nil, err
	}

	pageItems, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]any, len(pageItems))

	for i, e := range pageItems {
		out[i] = map[string]any{
			"ExecutionId": e.ExecutionID,
			keyWorkflowID: e.WorkflowID,
			keyStatus:     e.Status,
		}
	}

	return &map[string]any{"Executions": out, keyWorkflowID: in.WorkflowID, "NextToken": next}, nil
}

type describeExecutionInput struct {
	WorkflowID  string `json:"WorkflowId"`
	ExecutionID string `json:"ExecutionId"`
}

func (h *Handler) handleDescribeExecution(
	_ context.Context,
	in *describeExecutionInput,
) (*map[string]any, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	if in.ExecutionID == "" {
		return nil, fmt.Errorf("%w: ExecutionId is required", errInvalidRequest)
	}

	e, err := h.Backend.DescribeExecution(in.WorkflowID, in.ExecutionID)
	if err != nil {
		return nil, err
	}

	return &map[string]any{
		"Execution": map[string]any{
			"ExecutionId": e.ExecutionID,
			keyWorkflowID: e.WorkflowID,
			keyStatus:     e.Status,
		},
		keyWorkflowID: in.WorkflowID,
	}, nil
}

type sendWorkflowStepStateInput struct {
	WorkflowID  string `json:"WorkflowId"`
	ExecutionID string `json:"ExecutionId"`
	Token       string `json:"Token"`
	Status      string `json:"Status"`
}

func (h *Handler) handleSendWorkflowStepState(
	_ context.Context,
	in *sendWorkflowStepStateInput,
) (*struct{}, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	if in.ExecutionID == "" {
		return nil, fmt.Errorf("%w: ExecutionId is required", errInvalidRequest)
	}

	if in.Token == "" {
		return nil, fmt.Errorf("%w: Token is required", errInvalidRequest)
	}

	if err := h.Backend.SendWorkflowStepStateRecord(in.WorkflowID, in.ExecutionID, in.Token, in.Status); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
