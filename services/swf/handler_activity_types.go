package swf

import (
	"context"
	"sort"
)

// --- RegisterActivityType ---

type registerActivityTypeOutput struct{}

type handleRegisterActivityTypeInput struct {
	Domain                            string         `json:"domain"`
	Name                              string         `json:"name"`
	Version                           string         `json:"version"`
	Description                       string         `json:"description,omitempty"`
	DefaultTaskList                   *taskListInput `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority               string         `json:"defaultTaskPriority,omitempty"`
	DefaultTaskHeartbeatTimeout       string         `json:"defaultTaskHeartbeatTimeout,omitempty"`
	DefaultTaskScheduleToCloseTimeout string         `json:"defaultTaskScheduleToCloseTimeout,omitempty"`
	DefaultTaskScheduleToStartTimeout string         `json:"defaultTaskScheduleToStartTimeout,omitempty"`
	DefaultTaskStartToCloseTimeout    string         `json:"defaultTaskStartToCloseTimeout,omitempty"`
}

func (h *Handler) handleRegisterActivityType(
	_ context.Context,
	in *handleRegisterActivityTypeInput,
) (*registerActivityTypeOutput, error) {
	defaults := ActivityTypeDefaults{
		DefaultTaskPriority:               in.DefaultTaskPriority,
		DefaultTaskHeartbeatTimeout:       in.DefaultTaskHeartbeatTimeout,
		DefaultTaskScheduleToCloseTimeout: in.DefaultTaskScheduleToCloseTimeout,
		DefaultTaskScheduleToStartTimeout: in.DefaultTaskScheduleToStartTimeout,
		DefaultTaskStartToCloseTimeout:    in.DefaultTaskStartToCloseTimeout,
	}
	if in.DefaultTaskList != nil {
		defaults.DefaultTaskList = in.DefaultTaskList.Name
	}
	if err := h.Backend.RegisterActivityType(in.Domain, in.Name, in.Version, in.Description, defaults); err != nil {
		return nil, err
	}

	return &registerActivityTypeOutput{}, nil
}

// --- ListActivityTypes ---

type activityTypeRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type activityTypeInfoOutput struct {
	ActivityType *activityTypeRef `json:"activityType"`
	Status       string           `json:"status"`
	Description  string           `json:"description,omitempty"`
	CreationDate float64          `json:"creationDate,omitempty"`
}

type listActivityTypesOutput struct {
	NextPageToken string                   `json:"nextPageToken,omitempty"`
	TypeInfos     []activityTypeInfoOutput `json:"typeInfos"`
}

type handleListActivityTypesInput struct {
	Domain             string `json:"domain"`
	RegistrationStatus string `json:"registrationStatus,omitempty"`
	NextPageToken      string `json:"nextPageToken,omitempty"`
	MaximumPageSize    int    `json:"maximumPageSize,omitempty"`
}

//nolint:dupl // ActivityType list mirrors WorkflowType list structure
func (h *Handler) handleListActivityTypes(
	_ context.Context,
	in *handleListActivityTypesInput,
) (*listActivityTypesOutput, error) {
	ats, err := h.Backend.ListActivityTypes(in.Domain, in.RegistrationStatus)
	if err != nil {
		return nil, err
	}
	infos := make([]activityTypeInfoOutput, len(ats))
	for i, at := range ats {
		infos[i] = activityTypeInfoOutput{
			ActivityType: &activityTypeRef{Name: at.Name, Version: at.Version},
			Status:       at.Status,
			Description:  at.Description,
			CreationDate: at.CreationDate,
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].ActivityType.Name < infos[j].ActivityType.Name
	})
	infos, nextPageToken := applyPageTokenSlice(infos, in.NextPageToken, in.MaximumPageSize)

	return &listActivityTypesOutput{TypeInfos: infos, NextPageToken: nextPageToken}, nil
}

// --- DescribeActivityType ---

type activityTypeConfigOutput struct {
	DefaultTaskList                   *taskListRef `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority               string       `json:"defaultTaskPriority,omitempty"`
	DefaultTaskHeartbeatTimeout       string       `json:"defaultTaskHeartbeatTimeout,omitempty"`
	DefaultTaskScheduleToCloseTimeout string       `json:"defaultTaskScheduleToCloseTimeout,omitempty"`
	DefaultTaskScheduleToStartTimeout string       `json:"defaultTaskScheduleToStartTimeout,omitempty"`
	DefaultTaskStartToCloseTimeout    string       `json:"defaultTaskStartToCloseTimeout,omitempty"`
}

type describeActivityTypeOutput struct {
	Configuration activityTypeConfigOutput `json:"configuration"`
	TypeInfo      activityTypeInfoOutput   `json:"typeInfo"`
}

type handleDescribeActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

//nolint:dupl // ActivityType describe mirrors WorkflowType describe structure
func (h *Handler) handleDescribeActivityType(
	_ context.Context,
	in *handleDescribeActivityTypeInput,
) (*describeActivityTypeOutput, error) {
	at, err := h.Backend.DescribeActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version)
	if err != nil {
		return nil, err
	}
	cfg := activityTypeConfigOutput{
		DefaultTaskPriority:               at.Defaults.DefaultTaskPriority,
		DefaultTaskHeartbeatTimeout:       at.Defaults.DefaultTaskHeartbeatTimeout,
		DefaultTaskScheduleToCloseTimeout: at.Defaults.DefaultTaskScheduleToCloseTimeout,
		DefaultTaskScheduleToStartTimeout: at.Defaults.DefaultTaskScheduleToStartTimeout,
		DefaultTaskStartToCloseTimeout:    at.Defaults.DefaultTaskStartToCloseTimeout,
	}
	if at.Defaults.DefaultTaskList != "" {
		cfg.DefaultTaskList = &taskListRef{Name: at.Defaults.DefaultTaskList}
	}

	return &describeActivityTypeOutput{
		TypeInfo: activityTypeInfoOutput{
			ActivityType: &activityTypeRef{Name: at.Name, Version: at.Version},
			Status:       at.Status,
			Description:  at.Description,
			CreationDate: at.CreationDate,
		},
		Configuration: cfg,
	}, nil
}

// --- DeprecateActivityType ---

type deprecateActivityTypeOutput struct{}

type handleDeprecateActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

func (h *Handler) handleDeprecateActivityType(
	_ context.Context,
	in *handleDeprecateActivityTypeInput,
) (*deprecateActivityTypeOutput, error) {
	if err := h.Backend.DeprecateActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &deprecateActivityTypeOutput{}, nil
}

// --- UndeprecateActivityType ---

type undeprecateActivityTypeOutput struct{}

type handleUndeprecateActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

func (h *Handler) handleUndeprecateActivityType(
	_ context.Context,
	in *handleUndeprecateActivityTypeInput,
) (*undeprecateActivityTypeOutput, error) {
	if err := h.Backend.UndeprecateActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &undeprecateActivityTypeOutput{}, nil
}

// --- DeleteActivityType ---

type deleteActivityTypeOutput struct{}

type handleDeleteActivityTypeInput struct {
	Domain       string          `json:"domain"`
	ActivityType activityTypeRef `json:"activityType"`
}

func (h *Handler) handleDeleteActivityType(
	_ context.Context,
	in *handleDeleteActivityTypeInput,
) (*deleteActivityTypeOutput, error) {
	if err := h.Backend.DeleteActivityType(in.Domain, in.ActivityType.Name, in.ActivityType.Version); err != nil {
		return nil, err
	}

	return &deleteActivityTypeOutput{}, nil
}
