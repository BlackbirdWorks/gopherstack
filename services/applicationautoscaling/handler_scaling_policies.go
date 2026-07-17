package applicationautoscaling

import (
	"context"
)

type putScalingPolicyInput struct {
	TargetTrackingScalingPolicyConfiguration map[string]any `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	StepScalingPolicyConfiguration           map[string]any `json:"StepScalingPolicyConfiguration,omitempty"`
	ServiceNamespace                         string         `json:"ServiceNamespace"`
	ResourceID                               string         `json:"ResourceId"`
	ScalableDimension                        string         `json:"ScalableDimension"`
	PolicyName                               string         `json:"PolicyName"`
	PolicyType                               string         `json:"PolicyType"`
}

type putScalingPolicyOutput struct {
	PolicyARN string `json:"PolicyARN"`
}

func (h *Handler) handlePutScalingPolicy(
	_ context.Context,
	in *putScalingPolicyInput,
) (*putScalingPolicyOutput, error) {
	p, err := h.Backend.PutScalingPolicy(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension,
		in.PolicyName, in.PolicyType,
		in.TargetTrackingScalingPolicyConfiguration,
		in.StepScalingPolicyConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return &putScalingPolicyOutput{PolicyARN: p.ARN}, nil
}

type deleteScalingPolicyInput struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceID        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
	PolicyName        string `json:"PolicyName"`
}

type deleteScalingPolicyOutput struct{}

func (h *Handler) handleDeleteScalingPolicy(
	_ context.Context,
	in *deleteScalingPolicyInput,
) (*deleteScalingPolicyOutput, error) {
	if err := h.Backend.DeleteScalingPolicy(
		in.ServiceNamespace,
		in.ResourceID,
		in.ScalableDimension,
		in.PolicyName,
	); err != nil {
		return nil, err
	}

	return &deleteScalingPolicyOutput{}, nil
}

type describeScalingPoliciesInput struct {
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceID        string   `json:"ResourceId,omitempty"`
	ScalableDimension string   `json:"ScalableDimension,omitempty"`
	NextToken         string   `json:"NextToken,omitempty"`
	PolicyNames       []string `json:"PolicyNames,omitempty"`
	PolicyARNs        []string `json:"PolicyARNs,omitempty"`
	MaxResults        int32    `json:"MaxResults,omitempty"`
}

type scalingPolicySummary struct {
	TargetTrackingScalingPolicyConfiguration map[string]any `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	StepScalingPolicyConfiguration           map[string]any `json:"StepScalingPolicyConfiguration,omitempty"`
	CreationTime                             *float64       `json:"CreationTime,omitempty"`
	LastModifiedTime                         *float64       `json:"LastModifiedTime,omitempty"`
	ServiceNamespace                         string         `json:"ServiceNamespace"`
	ResourceID                               string         `json:"ResourceId"`
	ScalableDimension                        string         `json:"ScalableDimension"`
	PolicyName                               string         `json:"PolicyName"`
	PolicyType                               string         `json:"PolicyType"`
	PolicyARN                                string         `json:"PolicyARN"`
	Alarms                                   []alarmSummary `json:"Alarms,omitempty"`
}

// alarmSummary mirrors the CloudWatch alarm reference returned by AWS in policy descriptions.
type alarmSummary struct {
	AlarmARN  string `json:"AlarmARN"`
	AlarmName string `json:"AlarmName"`
}

type describeScalingPoliciesOutput struct {
	NextToken       string                 `json:"NextToken,omitempty"`
	ScalingPolicies []scalingPolicySummary `json:"ScalingPolicies"`
}

func (h *Handler) handleDescribeScalingPolicies(
	_ context.Context,
	in *describeScalingPoliciesInput,
) (*describeScalingPoliciesOutput, error) {
	policies, nextToken := h.Backend.DescribeScalingPolicies(DescribeScalingPoliciesFilter{
		ServiceNamespace:  in.ServiceNamespace,
		ResourceID:        in.ResourceID,
		ScalableDimension: in.ScalableDimension,
		PolicyNames:       in.PolicyNames,
		PolicyARNs:        in.PolicyARNs,
		MaxResults:        in.MaxResults,
		NextToken:         in.NextToken,
	})
	items := make([]scalingPolicySummary, 0, len(policies))
	for _, p := range policies {
		items = append(items, scalingPolicySummary{
			ServiceNamespace:                         p.ServiceNamespace,
			ResourceID:                               p.ResourceID,
			ScalableDimension:                        p.ScalableDimension,
			PolicyName:                               p.PolicyName,
			PolicyType:                               p.PolicyType,
			PolicyARN:                                p.ARN,
			CreationTime:                             epochSecondsPtr(p.CreationTime),
			LastModifiedTime:                         epochSecondsPtr(p.LastModifiedTime),
			TargetTrackingScalingPolicyConfiguration: p.TargetTrackingConfig,
			StepScalingPolicyConfiguration:           p.StepScalingConfig,
		})
	}

	return &describeScalingPoliciesOutput{ScalingPolicies: items, NextToken: nextToken}, nil
}
