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

type notScaledReasonSummary struct {
	CurrentCapacity *int32 `json:"CurrentCapacity,omitempty"`
	MaxCapacity     *int32 `json:"MaxCapacity,omitempty"`
	MinCapacity     *int32 `json:"MinCapacity,omitempty"`
	Code            string `json:"Code"`
}

type scalingActivitySummary struct {
	StartTime         *float64                 `json:"StartTime,omitempty"`
	EndTime           *float64                 `json:"EndTime,omitempty"`
	ActivityID        string                   `json:"ActivityId"`
	ServiceNamespace  string                   `json:"ServiceNamespace"`
	ResourceID        string                   `json:"ResourceId"`
	ScalableDimension string                   `json:"ScalableDimension"`
	Description       string                   `json:"Description"`
	Cause             string                   `json:"Cause"`
	StatusCode        string                   `json:"StatusCode"`
	StatusMessage     string                   `json:"StatusMessage,omitempty"`
	Details           string                   `json:"Details,omitempty"`
	NotScaledReasons  []notScaledReasonSummary `json:"NotScaledReasons,omitempty"`
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

	activities, nextToken, err := h.Backend.DescribeScalingActivities(DescribeScalingActivitiesFilter{
		ServiceNamespace:           in.ServiceNamespace,
		ResourceID:                 in.ResourceID,
		ScalableDimension:          in.ScalableDimension,
		MaxResults:                 in.MaxResults,
		NextToken:                  in.NextToken,
		IncludeNotScaledActivities: in.IncludeNotScaledActivities,
	})
	if err != nil {
		return nil, err
	}

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
			Details:           a.Details,
			NotScaledReasons:  notScaledReasonSummaries(a.NotScaledReasons),
		})
	}

	return &describeScalingActivitiesOutput{ScalingActivities: items, NextToken: nextToken}, nil
}

// notScaledReasonSummaries converts backend NotScaledReason values to their
// wire shape. Always receives an empty slice today (see the doc comment on
// [InMemoryBackend.DescribeScalingActivities]); implemented for wire
// completeness rather than left unmapped.
func notScaledReasonSummaries(reasons []NotScaledReason) []notScaledReasonSummary {
	if len(reasons) == 0 {
		return nil
	}

	out := make([]notScaledReasonSummary, 0, len(reasons))
	for _, r := range reasons {
		out = append(out, notScaledReasonSummary(r))
	}

	return out
}
