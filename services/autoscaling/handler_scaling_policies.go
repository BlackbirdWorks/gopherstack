package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleDescribeAdjustmentTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeAdjustmentTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlAdjustmentType, 0, len(types))
	for _, t := range types {
		members = append(members, xmlAdjustmentType{AdjustmentType: t})
	}

	return &describeAdjustmentTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAdjustmentTypesResult{
			AdjustmentTypes: xmlAdjustmentTypeList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-adjustment-types"},
	}, nil
}

func (h *Handler) handleExecutePolicy(vals url.Values) (any, error) {
	input := ExecutePolicyInput{
		AutoScalingGroupName: vals.Get("AutoScalingGroupName"),
		PolicyName:           vals.Get("PolicyName"),
		HonorCooldown:        vals.Get("HonorCooldown") == formValueTrue,
	}

	if v := vals.Get("MetricValue"); v != "" {
		f, parseErr := strconv.ParseFloat(v, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid MetricValue", ErrInvalidParameter)
		}

		input.MetricValue = &f
	}

	if v := vals.Get("BreachThreshold"); v != "" {
		f, parseErr := strconv.ParseFloat(v, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid BreachThreshold", ErrInvalidParameter)
		}

		input.BreachThreshold = &f
	}

	if err := h.Backend.ExecutePolicy(input); err != nil {
		return nil, err
	}

	return &executePolicyResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-execute-policy"},
	}, nil
}

//nolint:gocognit,cyclop,funlen // parses many optional scaling policy fields
func (h *Handler) handlePutScalingPolicy(vals url.Values) (any, error) {
	scalingAdjustment, err := parseIntVal(vals.Get("ScalingAdjustment"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid ScalingAdjustment", ErrInvalidParameter)
	}

	minAdjustmentStep, err := parseIntVal(vals.Get("MinAdjustmentStep"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinAdjustmentStep", ErrInvalidParameter)
	}

	cooldown, err := parseIntVal(vals.Get("Cooldown"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Cooldown", ErrInvalidParameter)
	}

	estimatedWarmup, err := parseIntVal(vals.Get("TargetTrackingConfiguration.EstimatedInstanceWarmup"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid EstimatedInstanceWarmup", ErrInvalidParameter)
	}

	// Parse TargetTrackingConfiguration fields
	var targetValue float64
	if v := vals.Get("TargetTrackingConfiguration.TargetValue"); v != "" {
		tv, parseErr := strconv.ParseFloat(v, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid TargetTrackingConfiguration.TargetValue", ErrInvalidParameter)
		}

		targetValue = tv
	}

	metricType := vals.Get("TargetTrackingConfiguration.PredefinedMetricSpecification.PredefinedMetricType")
	disableScaleIn := vals.Get("TargetTrackingConfiguration.DisableScaleIn") == formValueTrue

	minAdjustmentMagnitude, err := parseIntVal(vals.Get("MinAdjustmentMagnitude"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinAdjustmentMagnitude", ErrInvalidParameter)
	}

	// Parse StepAdjustments.member.N.{ScalingAdjustment,MetricIntervalLowerBound,MetricIntervalUpperBound}
	var stepAdjustments []StepAdjustment
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("StepAdjustments.member.%d.", i)
		saStr := vals.Get(prefix + "ScalingAdjustment")
		if saStr == "" {
			break
		}

		sa, parseErr := parseIntVal(saStr)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid StepAdjustments.member.%d.ScalingAdjustment", ErrInvalidParameter, i)
		}

		adj := StepAdjustment{ScalingAdjustment: sa}
		if v := vals.Get(prefix + "MetricIntervalLowerBound"); v != "" {
			f, floatErr := strconv.ParseFloat(v, 64)
			if floatErr != nil {
				return nil, fmt.Errorf("%w: invalid MetricIntervalLowerBound", ErrInvalidParameter)
			}

			adj.MetricIntervalLowerBound = &f
		}

		if v := vals.Get(prefix + "MetricIntervalUpperBound"); v != "" {
			f, floatErr := strconv.ParseFloat(v, 64)
			if floatErr != nil {
				return nil, fmt.Errorf("%w: invalid MetricIntervalUpperBound", ErrInvalidParameter)
			}

			adj.MetricIntervalUpperBound = &f
		}

		stepAdjustments = append(stepAdjustments, adj)
	}

	input := ScalingPolicyInput{
		AutoScalingGroupName:   vals.Get("AutoScalingGroupName"),
		PolicyName:             vals.Get("PolicyName"),
		PolicyType:             vals.Get("PolicyType"),
		AdjustmentType:         vals.Get("AdjustmentType"),
		MetricAggregationType:  vals.Get("MetricAggregationType"),
		ScalingAdjustment:      scalingAdjustment,
		MinAdjustmentStep:      minAdjustmentStep,
		MinAdjustmentMagnitude: minAdjustmentMagnitude,
		StepAdjustments:        stepAdjustments,
		Cooldown:               cooldown,
		TargetValue:            targetValue,
		MetricType:             metricType,
		DisableScaleIn:         disableScaleIn,
		EstimatedWarmup:        estimatedWarmup,
	}

	policy, putErr := h.Backend.PutScalingPolicy(input)
	if putErr != nil {
		return nil, putErr
	}

	return &putScalingPolicyResponse{
		Xmlns: autoscalingXMLNS,
		Result: putScalingPolicyResult{
			PolicyARN: policy.PolicyARN,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-scaling-policy"},
	}, nil
}

func (h *Handler) handleDeletePolicy(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	policyName := vals.Get("PolicyName")

	if err := h.Backend.DeletePolicy(groupName, policyName); err != nil {
		return nil, err
	}

	return &deletePolicyResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-policy"},
	}, nil
}

func (h *Handler) handleDescribePolicies(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	policyNames := parseMembers(vals, "PolicyNames.member")

	policies, err := h.Backend.DescribePolicies(groupName, policyNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingPolicy, 0, len(policies))
	for _, p := range policies {
		xmlPolicy := xmlScalingPolicy{
			PolicyName:             p.PolicyName,
			PolicyARN:              p.PolicyARN,
			AutoScalingGroupName:   p.AutoScalingGroupName,
			PolicyType:             p.PolicyType,
			AdjustmentType:         p.AdjustmentType,
			MetricAggregationType:  p.MetricAggregationType,
			ScalingAdjustment:      p.ScalingAdjustment,
			MinAdjustmentStep:      p.MinAdjustmentStep,
			MinAdjustmentMagnitude: p.MinAdjustmentMagnitude,
			Cooldown:               p.Cooldown,
		}

		if len(p.StepAdjustments) > 0 {
			steps := make([]xmlStepAdjustment, 0, len(p.StepAdjustments))
			for _, s := range p.StepAdjustments {
				steps = append(steps, xmlStepAdjustment(s))
			}

			xmlPolicy.StepAdjustments = &xmlStepAdjustmentList{Members: steps}
		}

		if p.PolicyType == "TargetTrackingScaling" {
			ttc := &xmlTargetTrackingConfiguration{
				TargetValue:             p.TargetValue,
				DisableScaleIn:          p.DisableScaleIn,
				EstimatedInstanceWarmup: p.EstimatedWarmup,
			}

			if p.MetricType != "" {
				ttc.PredefinedMetricSpecification = &xmlPredefinedMetricSpecification{
					PredefinedMetricType: p.MetricType,
				}
			}

			xmlPolicy.TargetTrackingConfiguration = ttc
		}

		members = append(members, xmlPolicy)
	}

	return &describePoliciesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describePoliciesResult{
			ScalingPolicies: xmlScalingPolicyList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-policies"},
	}, nil
}

type xmlAdjustmentType struct {
	AdjustmentType string `xml:"AdjustmentType"`
}

type xmlAdjustmentTypeList struct {
	Members []xmlAdjustmentType `xml:"member"`
}

type describeAdjustmentTypesResult struct {
	AdjustmentTypes xmlAdjustmentTypeList `xml:"AdjustmentTypes"`
}

type describeAdjustmentTypesResponse struct {
	XMLName          xml.Name                      `xml:"DescribeAdjustmentTypesResponse"`
	Xmlns            string                        `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata           `xml:"ResponseMetadata"`
	Result           describeAdjustmentTypesResult `xml:"DescribeAdjustmentTypesResult"`
}

type executePolicyResponse struct {
	XMLName          xml.Name            `xml:"ExecutePolicyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type putScalingPolicyResult struct {
	PolicyARN string `xml:"PolicyARN"`
}

type putScalingPolicyResponse struct {
	XMLName          xml.Name               `xml:"PutScalingPolicyResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           putScalingPolicyResult `xml:"PutScalingPolicyResult"`
}

type deletePolicyResponse struct {
	XMLName          xml.Name            `xml:"DeletePolicyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlPredefinedMetricSpecification struct {
	PredefinedMetricType string `xml:"PredefinedMetricType"`
}

type xmlTargetTrackingConfiguration struct {
	PredefinedMetricSpecification *xmlPredefinedMetricSpecification `xml:"PredefinedMetricSpecification,omitempty"`
	TargetValue                   float64                           `xml:"TargetValue"`
	DisableScaleIn                bool                              `xml:"DisableScaleIn,omitempty"`
	EstimatedInstanceWarmup       int32                             `xml:"EstimatedInstanceWarmup,omitempty"`
}

type xmlStepAdjustment struct {
	MetricIntervalLowerBound *float64 `xml:"MetricIntervalLowerBound,omitempty"`
	MetricIntervalUpperBound *float64 `xml:"MetricIntervalUpperBound,omitempty"`
	ScalingAdjustment        int32    `xml:"ScalingAdjustment"`
}

type xmlStepAdjustmentList struct {
	Members []xmlStepAdjustment `xml:"member"`
}

// xmlScalingPolicy is the XML type for a scaling policy.
type xmlScalingPolicy struct {
	TargetTrackingConfiguration *xmlTargetTrackingConfiguration `xml:"TargetTrackingConfiguration,omitempty"`
	StepAdjustments             *xmlStepAdjustmentList          `xml:"StepAdjustments,omitempty"`
	PolicyName                  string                          `xml:"PolicyName"`
	PolicyARN                   string                          `xml:"PolicyARN"`
	AutoScalingGroupName        string                          `xml:"AutoScalingGroupName"`
	PolicyType                  string                          `xml:"PolicyType,omitempty"`
	AdjustmentType              string                          `xml:"AdjustmentType,omitempty"`
	MetricAggregationType       string                          `xml:"MetricAggregationType,omitempty"`
	ScalingAdjustment           int32                           `xml:"ScalingAdjustment,omitempty"`
	MinAdjustmentStep           int32                           `xml:"MinAdjustmentStep,omitempty"`
	MinAdjustmentMagnitude      int32                           `xml:"MinAdjustmentMagnitude,omitempty"`
	Cooldown                    int32                           `xml:"Cooldown,omitempty"`
}

type xmlScalingPolicyList struct {
	Members []xmlScalingPolicy `xml:"member"`
}

type describePoliciesResult struct {
	ScalingPolicies xmlScalingPolicyList `xml:"ScalingPolicies"`
}

type describePoliciesResponse struct {
	XMLName          xml.Name               `xml:"DescribePoliciesResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           describePoliciesResult `xml:"DescribePoliciesResult"`
}
