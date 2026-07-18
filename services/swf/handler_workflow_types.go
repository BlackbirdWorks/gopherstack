package swf

import (
	"context"
	"sort"
)

// --- RegisterWorkflowType ---

type registerWorkflowTypeOutput struct{}

type taskListInput struct {
	Name string `json:"name"`
}

type handleRegisterWorkflowTypeInput struct {
	Domain                              string         `json:"domain"`
	Name                                string         `json:"name"`
	Version                             string         `json:"version"`
	Description                         string         `json:"description,omitempty"`
	DefaultTaskList                     *taskListInput `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority                 string         `json:"defaultTaskPriority,omitempty"`
	DefaultTaskStartToCloseTimeout      string         `json:"defaultTaskStartToCloseTimeout,omitempty"`
	DefaultExecutionStartToCloseTimeout string         `json:"defaultExecutionStartToCloseTimeout,omitempty"`
	DefaultChildPolicy                  string         `json:"defaultChildPolicy,omitempty"`
	DefaultLambdaRole                   string         `json:"defaultLambdaRole,omitempty"`
}

func (h *Handler) handleRegisterWorkflowType(
	_ context.Context,
	in *handleRegisterWorkflowTypeInput,
) (*registerWorkflowTypeOutput, error) {
	defaults := WorkflowTypeDefaults{
		DefaultTaskPriority:                 in.DefaultTaskPriority,
		DefaultTaskStartToCloseTimeout:      in.DefaultTaskStartToCloseTimeout,
		DefaultExecutionStartToCloseTimeout: in.DefaultExecutionStartToCloseTimeout,
		DefaultChildPolicy:                  in.DefaultChildPolicy,
		DefaultLambdaRole:                   in.DefaultLambdaRole,
	}
	if in.DefaultTaskList != nil {
		defaults.DefaultTaskList = in.DefaultTaskList.Name
	}
	if err := h.Backend.RegisterWorkflowType(in.Domain, in.Name, in.Version, in.Description, defaults); err != nil {
		return nil, err
	}

	return &registerWorkflowTypeOutput{}, nil
}

// --- ListWorkflowTypes ---

type workflowTypeRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type workflowTypeInfoOutput struct {
	WorkflowType *workflowTypeRef `json:"workflowType"`
	Status       string           `json:"status"`
	Description  string           `json:"description,omitempty"`
	CreationDate float64          `json:"creationDate,omitempty"`
}

type listWorkflowTypesOutput struct {
	NextPageToken string                   `json:"nextPageToken,omitempty"`
	TypeInfos     []workflowTypeInfoOutput `json:"typeInfos"`
}

type handleListWorkflowTypesInput struct {
	Domain             string `json:"domain"`
	RegistrationStatus string `json:"registrationStatus,omitempty"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

//nolint:dupl // WorkflowType and ActivityType have parallel list structure
func (h *Handler) handleListWorkflowTypes(
	_ context.Context,
	in *handleListWorkflowTypesInput,
) (*listWorkflowTypesOutput, error) {
	wts, err := h.Backend.ListWorkflowTypes(in.Domain, in.RegistrationStatus)
	if err != nil {
		return nil, err
	}
	infos := make([]workflowTypeInfoOutput, len(wts))
	for i, wt := range wts {
		infos[i] = workflowTypeInfoOutput{
			WorkflowType: &workflowTypeRef{Name: wt.Name, Version: wt.Version},
			Status:       wt.Status,
			Description:  wt.Description,
			CreationDate: wt.CreationDate,
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].WorkflowType.Name < infos[j].WorkflowType.Name
	})
	infos, nextPageToken := applyPageTokenSlice(infos, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowTypesOutput{TypeInfos: infos, NextPageToken: nextPageToken}, nil
}

// --- DescribeWorkflowType ---

type workflowTypeConfigOutput struct {
	DefaultTaskList                     *taskListRef `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority                 string       `json:"defaultTaskPriority,omitempty"`
	DefaultTaskStartToCloseTimeout      string       `json:"defaultTaskStartToCloseTimeout,omitempty"`
	DefaultExecutionStartToCloseTimeout string       `json:"defaultExecutionStartToCloseTimeout,omitempty"`
	DefaultChildPolicy                  string       `json:"defaultChildPolicy,omitempty"`
	DefaultLambdaRole                   string       `json:"defaultLambdaRole,omitempty"`
}

type taskListRef struct {
	Name string `json:"name"`
}

type describeWorkflowTypeOutput struct {
	Configuration workflowTypeConfigOutput `json:"configuration"`
	TypeInfo      workflowTypeInfoOutput   `json:"typeInfo"`
}

type handleDescribeWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

//nolint:dupl // mirrors ActivityType describe structure
func (h *Handler) handleDescribeWorkflowType(
	_ context.Context,
	in *handleDescribeWorkflowTypeInput,
) (*describeWorkflowTypeOutput, error) {
	wt, err := h.Backend.DescribeWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version)
	if err != nil {
		return nil, err
	}
	cfg := workflowTypeConfigOutput{
		DefaultTaskPriority:                 wt.Defaults.DefaultTaskPriority,
		DefaultTaskStartToCloseTimeout:      wt.Defaults.DefaultTaskStartToCloseTimeout,
		DefaultExecutionStartToCloseTimeout: wt.Defaults.DefaultExecutionStartToCloseTimeout,
		DefaultChildPolicy:                  wt.Defaults.DefaultChildPolicy,
		DefaultLambdaRole:                   wt.Defaults.DefaultLambdaRole,
	}
	if wt.Defaults.DefaultTaskList != "" {
		cfg.DefaultTaskList = &taskListRef{Name: wt.Defaults.DefaultTaskList}
	}

	return &describeWorkflowTypeOutput{
		TypeInfo: workflowTypeInfoOutput{
			WorkflowType: &workflowTypeRef{Name: wt.Name, Version: wt.Version},
			Status:       wt.Status,
			Description:  wt.Description,
			CreationDate: wt.CreationDate,
		},
		Configuration: cfg,
	}, nil
}

// --- DeprecateWorkflowType ---

type deprecateWorkflowTypeOutput struct{}

type handleDeprecateWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

func (h *Handler) handleDeprecateWorkflowType(
	_ context.Context,
	in *handleDeprecateWorkflowTypeInput,
) (*deprecateWorkflowTypeOutput, error) {
	if err := h.Backend.DeprecateWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &deprecateWorkflowTypeOutput{}, nil
}

// --- UndeprecateWorkflowType ---

type undeprecateWorkflowTypeOutput struct{}

type handleUndeprecateWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

func (h *Handler) handleUndeprecateWorkflowType(
	_ context.Context,
	in *handleUndeprecateWorkflowTypeInput,
) (*undeprecateWorkflowTypeOutput, error) {
	if err := h.Backend.UndeprecateWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &undeprecateWorkflowTypeOutput{}, nil
}

// --- DeleteWorkflowType ---

type deleteWorkflowTypeOutput struct{}

type handleDeleteWorkflowTypeInput struct {
	Domain       string          `json:"domain"`
	WorkflowType workflowTypeRef `json:"workflowType"`
}

func (h *Handler) handleDeleteWorkflowType(
	_ context.Context,
	in *handleDeleteWorkflowTypeInput,
) (*deleteWorkflowTypeOutput, error) {
	if err := h.Backend.DeleteWorkflowType(in.Domain, in.WorkflowType.Name, in.WorkflowType.Version); err != nil {
		return nil, err
	}

	return &deleteWorkflowTypeOutput{}, nil
}
