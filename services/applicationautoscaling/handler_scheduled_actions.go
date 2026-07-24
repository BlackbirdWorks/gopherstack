package applicationautoscaling

import (
	"context"
)

type scalableTargetActionInput struct {
	MinCapacity *int32 `json:"MinCapacity,omitempty"`
	MaxCapacity *int32 `json:"MaxCapacity,omitempty"`
}

type putScheduledActionInput struct {
	ScalableTargetAction *scalableTargetActionInput `json:"ScalableTargetAction,omitempty"`
	StartTime            *float64                   `json:"StartTime,omitempty"`
	EndTime              *float64                   `json:"EndTime,omitempty"`
	ServiceNamespace     string                     `json:"ServiceNamespace"`
	ResourceID           string                     `json:"ResourceId"`
	ScalableDimension    string                     `json:"ScalableDimension"`
	ScheduledActionName  string                     `json:"ScheduledActionName"`
	Schedule             string                     `json:"Schedule"`
	Timezone             string                     `json:"Timezone,omitempty"`
}

type putScheduledActionOutput struct {
	ScheduledActionARN string `json:"ScheduledActionARN"`
}

func (h *Handler) handlePutScheduledAction(
	_ context.Context,
	in *putScheduledActionInput,
) (*putScheduledActionOutput, error) {
	var sta *ScalableTargetAction
	if in.ScalableTargetAction != nil {
		sta = &ScalableTargetAction{
			MinCapacity: in.ScalableTargetAction.MinCapacity,
			MaxCapacity: in.ScalableTargetAction.MaxCapacity,
		}
	}

	// StartTime/EndTime are AWS JSON-protocol timestamps: the wire
	// representation is a JSON number of Unix epoch seconds, not an ISO8601
	// string, so in.StartTime/in.EndTime decode directly as *float64.
	startTime := parseEpochSeconds(in.StartTime)
	endTime := parseEpochSeconds(in.EndTime)

	a, err := h.Backend.PutScheduledAction(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension,
		in.ScheduledActionName, in.Schedule, in.Timezone,
		startTime, endTime, sta,
	)
	if err != nil {
		return nil, err
	}

	return &putScheduledActionOutput{ScheduledActionARN: a.ARN}, nil
}

type deleteScheduledActionInput struct {
	ServiceNamespace    string `json:"ServiceNamespace"`
	ResourceID          string `json:"ResourceId"`
	ScalableDimension   string `json:"ScalableDimension"`
	ScheduledActionName string `json:"ScheduledActionName"`
}

type deleteScheduledActionOutput struct{}

func (h *Handler) handleDeleteScheduledAction(
	_ context.Context,
	in *deleteScheduledActionInput,
) (*deleteScheduledActionOutput, error) {
	if err := h.Backend.DeleteScheduledAction(
		in.ServiceNamespace,
		in.ResourceID,
		in.ScalableDimension,
		in.ScheduledActionName,
	); err != nil {
		return nil, err
	}

	return &deleteScheduledActionOutput{}, nil
}

type describeScheduledActionsInput struct {
	ServiceNamespace     string   `json:"ServiceNamespace"`
	ResourceID           string   `json:"ResourceId,omitempty"`
	ScalableDimension    string   `json:"ScalableDimension,omitempty"`
	NextToken            string   `json:"NextToken,omitempty"`
	ScheduledActionNames []string `json:"ScheduledActionNames,omitempty"`
	MaxResults           int32    `json:"MaxResults,omitempty"`
}

type scalableTargetActionSummary struct {
	MinCapacity *int32 `json:"MinCapacity,omitempty"`
	MaxCapacity *int32 `json:"MaxCapacity,omitempty"`
}

type scheduledActionSummary struct {
	ScalableTargetAction *scalableTargetActionSummary `json:"ScalableTargetAction,omitempty"`
	CreationTime         *float64                     `json:"CreationTime,omitempty"`
	LastModifiedTime     *float64                     `json:"LastModifiedTime,omitempty"`
	StartTime            *float64                     `json:"StartTime,omitempty"`
	EndTime              *float64                     `json:"EndTime,omitempty"`
	ServiceNamespace     string                       `json:"ServiceNamespace"`
	ResourceID           string                       `json:"ResourceId"`
	ScalableDimension    string                       `json:"ScalableDimension"`
	ScheduledActionName  string                       `json:"ScheduledActionName"`
	Schedule             string                       `json:"Schedule"`
	ScheduledActionARN   string                       `json:"ScheduledActionARN"`
	Timezone             string                       `json:"Timezone,omitempty"`
}

type describeScheduledActionsOutput struct {
	NextToken        string                   `json:"NextToken,omitempty"`
	ScheduledActions []scheduledActionSummary `json:"ScheduledActions"`
}

func (h *Handler) handleDescribeScheduledActions(
	_ context.Context,
	in *describeScheduledActionsInput,
) (*describeScheduledActionsOutput, error) {
	actions, nextToken, err := h.Backend.DescribeScheduledActions(DescribeScheduledActionsFilter{
		ServiceNamespace:     in.ServiceNamespace,
		ResourceID:           in.ResourceID,
		ScalableDimension:    in.ScalableDimension,
		ScheduledActionNames: in.ScheduledActionNames,
		MaxResults:           in.MaxResults,
		NextToken:            in.NextToken,
	})
	if err != nil {
		return nil, err
	}

	items := make([]scheduledActionSummary, 0, len(actions))
	for _, a := range actions {
		item := scheduledActionSummary{
			ServiceNamespace:    a.ServiceNamespace,
			ResourceID:          a.ResourceID,
			ScalableDimension:   a.ScalableDimension,
			ScheduledActionName: a.ScheduledActionName,
			Schedule:            a.Schedule,
			Timezone:            a.Timezone,
			ScheduledActionARN:  a.ARN,
			CreationTime:        epochSecondsPtr(a.CreationTime),
			LastModifiedTime:    epochSecondsPtr(a.LastModifiedTime),
		}
		if a.StartTime != nil {
			item.StartTime = epochSecondsPtr(*a.StartTime)
		}

		if a.EndTime != nil {
			item.EndTime = epochSecondsPtr(*a.EndTime)
		}

		if a.ScalableTargetAction != nil {
			item.ScalableTargetAction = &scalableTargetActionSummary{
				MinCapacity: a.ScalableTargetAction.MinCapacity,
				MaxCapacity: a.ScalableTargetAction.MaxCapacity,
			}
		}

		items = append(items, item)
	}

	return &describeScheduledActionsOutput{ScheduledActions: items, NextToken: nextToken}, nil
}
