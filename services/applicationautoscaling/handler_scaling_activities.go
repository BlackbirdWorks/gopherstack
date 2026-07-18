package applicationautoscaling

import (
	"context"
	"fmt"
)

type describeScalingActivitiesInput struct {
	ServiceNamespace           string `json:"ServiceNamespace"`
	ResourceID                 string `json:"ResourceId,omitempty"`
	ScalableDimension          string `json:"ScalableDimension,omitempty"`
	NextToken                  string `json:"NextToken,omitempty"`
	MaxResults                 int32  `json:"MaxResults,omitempty"`
	IncludeNotScaledActivities bool   `json:"IncludeNotScaledActivities,omitempty"`
}

type scalingActivitySummary struct {
	StartTime         *float64 `json:"StartTime,omitempty"`
	EndTime           *float64 `json:"EndTime,omitempty"`
	ActivityID        string   `json:"ActivityId"`
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceID        string   `json:"ResourceId"`
	ScalableDimension string   `json:"ScalableDimension"`
	Description       string   `json:"Description"`
	Cause             string   `json:"Cause"`
	StatusCode        string   `json:"StatusCode"`
	StatusMessage     string   `json:"StatusMessage,omitempty"`
}

type describeScalingActivitiesOutput struct {
	NextToken         string                   `json:"NextToken,omitempty"`
	ScalingActivities []scalingActivitySummary `json:"ScalingActivities"`
}

func (h *Handler) handleDescribeScalingActivities(
	_ context.Context,
	in *describeScalingActivitiesInput,
) (*describeScalingActivitiesOutput, error) {
	if in.ServiceNamespace == "" {
		return nil, fmt.Errorf("%w: ServiceNamespace is required", ErrValidation)
	}

	activities, nextToken := h.Backend.DescribeScalingActivities(DescribeScalingActivitiesFilter{
		ServiceNamespace:  in.ServiceNamespace,
		ResourceID:        in.ResourceID,
		ScalableDimension: in.ScalableDimension,
		MaxResults:        in.MaxResults,
		NextToken:         in.NextToken,
	})

	items := make([]scalingActivitySummary, 0, len(activities))
	for _, a := range activities {
		items = append(items, scalingActivitySummary{
			ActivityID:        a.ActivityID,
			ServiceNamespace:  a.ServiceNamespace,
			ResourceID:        a.ResourceID,
			ScalableDimension: a.ScalableDimension,
			Description:       a.Description,
			Cause:             a.Cause,
			StatusCode:        a.StatusCode,
			StatusMessage:     a.StatusMessage,
			StartTime:         epochSecondsPtr(a.StartTime),
			EndTime:           epochSecondsPtr(a.EndTime),
		})
	}

	return &describeScalingActivitiesOutput{ScalingActivities: items, NextToken: nextToken}, nil
}
