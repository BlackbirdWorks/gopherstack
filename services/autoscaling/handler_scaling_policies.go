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

// scalingPolicyIntFieldValues bundles every plain (top-level) optional int32
// field of a PutScalingPolicy request.
type scalingPolicyIntFieldValues struct {
	scalingAdjustment      int32
	minAdjustmentStep      int32
	cooldown               int32
	minAdjustmentMagnitude int32
}

// scalingPolicyIntFields parses every plain (top-level) optional int32 field of
// a PutScalingPolicy request.
func scalingPolicyIntFields(vals url.Values) (scalingPolicyIntFieldValues, error) {
	var v scalingPolicyIntFieldValues

	fields := []struct {
		dest  *int32
		param string
	}{
		{param: "ScalingAdjustment", dest: &v.scalingAdjustment},
		{param: "MinAdjustmentStep", dest: &v.minAdjustmentStep},
		{param: "Cooldown", dest: &v.cooldown},
		{param: "MinAdjustmentMagnitude", dest: &v.minAdjustmentMagnitude},
	}

	for _, f := range fields {
		n, parseErr := parseIntVal(vals.Get(f.param))
		if parseErr != nil {
			return v, fmt.Errorf("%w: invalid %s", ErrInvalidParameter, f.param)
		}

		*f.dest = n
	}

	return v, nil
}

// targetTrackingFields holds the parsed TargetTrackingConfiguration.* portion
// of a PutScalingPolicy request.
type targetTrackingFields struct {
	customizedMetricSpec *CustomizedMetricSpecification
	metricType           string
	targetValue          float64
	estimatedWarmup      int32
	disableScaleIn       bool
}

// parseTargetTrackingFields parses the TargetTrackingConfiguration.* form values.
func parseTargetTrackingFields(vals url.Values) (targetTrackingFields, error) {
	var f targetTrackingFields

	estimatedWarmup, err := parseIntVal(vals.Get("TargetTrackingConfiguration.EstimatedInstanceWarmup"))
	if err != nil {
		return f, fmt.Errorf("%w: invalid EstimatedInstanceWarmup", ErrInvalidParameter)
	}

	f.estimatedWarmup = estimatedWarmup

	if v := vals.Get("TargetTrackingConfiguration.TargetValue"); v != "" {
		tv, parseErr := strconv.ParseFloat(v, 64)
		if parseErr != nil {
			return f, fmt.Errorf("%w: invalid TargetTrackingConfiguration.TargetValue", ErrInvalidParameter)
		}

		f.targetValue = tv
	}

	f.metricType = vals.Get("TargetTrackingConfiguration.PredefinedMetricSpecification.PredefinedMetricType")
	f.disableScaleIn = vals.Get("TargetTrackingConfiguration.DisableScaleIn") == formValueTrue

	spec, err := parseCustomizedMetricSpecification(vals, "TargetTrackingConfiguration.CustomizedMetricSpecification.")
	if err != nil {
		return f, err
	}

	f.customizedMetricSpec = spec

	return f, nil
}

// parseMetricDimensions parses a Dimensions.member.N.{Name,Value} list shared
// by types.Metric and types.CustomizedMetricSpecification
// (serializers.go:5750, 5767).
func parseMetricDimensions(vals url.Values, prefix string) []MetricDimension {
	var dims []MetricDimension

	for i := 1; ; i++ {
		memberPrefix := fmt.Sprintf("%sDimensions.member.%d.", prefix, i)

		name := vals.Get(memberPrefix + "Name")
		value := vals.Get(memberPrefix + "Value")

		if name == "" && value == "" {
			break
		}

		dims = append(dims, MetricDimension{Name: name, Value: value})
	}

	return dims
}

// parseMetricRef parses the {MetricName,Namespace,Dimensions} shape of AWS
// types.Metric (serializers.go:5680). Returns nil if nothing was specified.
func parseMetricRef(vals url.Values, prefix string) *MetricRef {
	name := vals.Get(prefix + "MetricName")
	namespace := vals.Get(prefix + "Namespace")
	dims := parseMetricDimensions(vals, prefix)

	if name == "" && namespace == "" && len(dims) == 0 {
		return nil
	}

	return &MetricRef{MetricName: name, Namespace: namespace, Dimensions: dims}
}

// parseMetricDataStat parses the {Metric,Stat,Unit[,Period]} shape shared by
// types.MetricStat (serializers.go:5789, no Period) and
// types.TargetTrackingMetricStat (serializers.go:6559, has Period).
// withPeriod selects which variant's wire shape to parse.
func parseMetricDataStat(vals url.Values, prefix string, withPeriod bool) (*MetricDataStat, error) {
	metric := parseMetricRef(vals, prefix+"Metric.")
	stat := vals.Get(prefix + "Stat")
	unit := vals.Get(prefix + "Unit")
	periodStr := ""

	if withPeriod {
		periodStr = vals.Get(prefix + "Period")
	}

	if metric == nil && stat == "" && unit == "" && periodStr == "" {
		return nil, nil //nolint:nilnil // absent stat is a meaningful "not specified" state, not an error
	}

	ms := &MetricDataStat{Metric: metric, Stat: stat, Unit: unit}

	if periodStr != "" {
		n, parseErr := parseIntVal(periodStr)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid %sPeriod", ErrInvalidParameter, prefix)
		}

		ms.Period = &n
	}

	return ms, nil
}

// parseMetricDataQueries parses a metric-data-query list shared by
// types.MetricDataQuery (serializers.go:5704, no Period -- predictive
// scaling's customized metrics) and types.TargetTrackingMetricDataQuery
// (serializers.go:6508, has Period -- TargetTrackingConfiguration.
// CustomizedMetricSpecification.Metrics). withPeriod selects which variant's
// wire shape to parse. Id is required by AWS on every element, so its
// presence is the loop-continuation sentinel (matching parseStepAdjustments).
func parseMetricDataQueries(vals url.Values, prefix string, withPeriod bool) ([]MetricDataQuery, error) {
	var queries []MetricDataQuery

	for i := 1; ; i++ {
		memberPrefix := fmt.Sprintf("%smember.%d.", prefix, i)

		id := vals.Get(memberPrefix + "Id")
		if id == "" {
			break
		}

		stat, err := parseMetricDataStat(vals, memberPrefix+"MetricStat.", withPeriod)
		if err != nil {
			return nil, err
		}

		q := MetricDataQuery{
			ID:         id,
			Expression: vals.Get(memberPrefix + "Expression"),
			Label:      vals.Get(memberPrefix + "Label"),
			MetricStat: stat,
		}

		if v := vals.Get(memberPrefix + "ReturnData"); v != "" {
			b := v == formValueTrue
			q.ReturnData = &b
		}

		if withPeriod {
			if v := vals.Get(memberPrefix + "Period"); v != "" {
				n, parseErr := parseIntVal(v)
				if parseErr != nil {
					return nil, fmt.Errorf("%w: invalid %sPeriod", ErrInvalidParameter, memberPrefix)
				}

				q.Period = &n
			}
		}

		queries = append(queries, q)
	}

	return queries, nil
}

// parseCustomizedMetricSpecification parses
// TargetTrackingConfiguration.CustomizedMetricSpecification.* form values
// (types.CustomizedMetricSpecification, serializers.go:4985). Returns nil if
// nothing was specified.
func parseCustomizedMetricSpecification(vals url.Values, prefix string) (*CustomizedMetricSpecification, error) {
	metricName := vals.Get(prefix + "MetricName")
	namespace := vals.Get(prefix + "Namespace")
	statistic := vals.Get(prefix + "Statistic")
	unit := vals.Get(prefix + "Unit")
	periodStr := vals.Get(prefix + "Period")
	dims := parseMetricDimensions(vals, prefix)

	queries, err := parseMetricDataQueries(vals, prefix+"Metrics.", true)
	if err != nil {
		return nil, err
	}

	if metricName == "" && namespace == "" && statistic == "" && unit == "" &&
		periodStr == "" && len(dims) == 0 && len(queries) == 0 {
		return nil, nil //nolint:nilnil // absent spec means "not customized", not an error
	}

	spec := &CustomizedMetricSpecification{
		MetricName: metricName,
		Namespace:  namespace,
		Statistic:  statistic,
		Unit:       unit,
		Dimensions: dims,
		Metrics:    queries,
	}

	if periodStr != "" {
		n, parseErr := parseIntVal(periodStr)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid %sPeriod", ErrInvalidParameter, prefix)
		}

		spec.Period = &n
	}

	return spec, nil
}

// parsePredictiveScalingCustomizedMetric parses a Customized{Load,Scaling,
// Capacity}MetricSpecification.MetricDataQueries.member.N.* block, the
// {MetricDataQueries} shape shared by types.PredictiveScalingCustomized
// {Load,Scaling,Capacity}Metric (serializers.go:6001-6042). Returns nil if
// nothing was specified.
func parsePredictiveScalingCustomizedMetric(
	vals url.Values,
	prefix string,
) (*CustomMetricQueries, error) {
	queries, err := parseMetricDataQueries(vals, prefix+"MetricDataQueries.", false)
	if err != nil {
		return nil, err
	}

	if len(queries) == 0 {
		return nil, nil //nolint:nilnil // absent customized metric means "not specified", not an error
	}

	return &CustomMetricQueries{MetricDataQueries: queries}, nil
}

// parseStepAdjustmentBound parses a single optional float bound
// (MetricIntervalLowerBound/UpperBound) of a StepAdjustments.member.N entry.
func parseStepAdjustmentBound(vals url.Values, key string) (*float64, error) {
	v := vals.Get(key)
	if v == "" {
		return nil, nil //nolint:nilnil // absent bound is a meaningful "unbounded" state, not an error
	}

	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid %s", ErrInvalidParameter, key)
	}

	return &f, nil
}

// parseStepAdjustments parses StepAdjustments.member.N.{ScalingAdjustment,
// MetricIntervalLowerBound,MetricIntervalUpperBound} form values.
func parseStepAdjustments(vals url.Values) ([]StepAdjustment, error) {
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

		lower, err := parseStepAdjustmentBound(vals, prefix+"MetricIntervalLowerBound")
		if err != nil {
			return nil, err
		}

		adj.MetricIntervalLowerBound = lower

		upper, err := parseStepAdjustmentBound(vals, prefix+"MetricIntervalUpperBound")
		if err != nil {
			return nil, err
		}

		adj.MetricIntervalUpperBound = upper

		stepAdjustments = append(stepAdjustments, adj)
	}

	return stepAdjustments, nil
}

// parsePredictiveScalingConfiguration parses PredictiveScalingConfiguration.*
// form values into a PredictiveScalingConfiguration (see models.go for the
// field mapping to types.PredictiveScalingConfiguration, aws-sdk-go-v2/
// service/autoscaling/types/types.go:2558). Returns nil, nil if the
// top-level object was not specified.
func parsePredictiveScalingConfiguration(vals url.Values) (*PredictiveScalingConfiguration, error) {
	const prefix = "PredictiveScalingConfiguration."

	maxBehavior := vals.Get(prefix + "MaxCapacityBreachBehavior")
	mode := vals.Get(prefix + "Mode")
	maxBufferStr := vals.Get(prefix + "MaxCapacityBuffer")
	schedBufferStr := vals.Get(prefix + "SchedulingBufferTime")

	specs, err := parsePredictiveScalingMetricSpecifications(vals, prefix+"MetricSpecifications.member.")
	if err != nil {
		return nil, err
	}

	if maxBehavior == "" && mode == "" && maxBufferStr == "" && schedBufferStr == "" && len(specs) == 0 {
		return nil, nil //nolint:nilnil // absent config means "not a predictive policy", not an error
	}

	cfg := &PredictiveScalingConfiguration{
		MaxCapacityBreachBehavior: maxBehavior,
		Mode:                      mode,
		MetricSpecifications:      specs,
	}

	if maxBufferStr != "" {
		n, parseErr := parseIntVal(maxBufferStr)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid PredictiveScalingConfiguration.MaxCapacityBuffer", ErrInvalidParameter)
		}

		cfg.MaxCapacityBuffer = &n
	}

	if schedBufferStr != "" {
		n, parseErr := parseIntVal(schedBufferStr)
		if parseErr != nil {
			return nil, fmt.Errorf(
				"%w: invalid PredictiveScalingConfiguration.SchedulingBufferTime",
				ErrInvalidParameter,
			)
		}

		cfg.SchedulingBufferTime = &n
	}

	return cfg, nil
}

// parsePredictiveScalingMetricSpecifications parses
// MetricSpecifications.member.N.* form values. TargetValue is required by
// AWS on every element, so its presence (like ScalingAdjustment in
// parseStepAdjustments) is used as the loop-continuation sentinel; the
// Customized* checks are added defensively for the same reason the
// MixedInstancesPolicy override loop checks InstanceRequirements (see
// parseLaunchTemplateOverrides) -- a spec carrying only a Customized* metric
// must not be mistaken for "end of list".
func parsePredictiveScalingMetricSpecifications(
	vals url.Values, prefix string,
) ([]PredictiveScalingMetricSpecification, error) {
	var specs []PredictiveScalingMetricSpecification

	for i := 1; ; i++ {
		memberPrefix := fmt.Sprintf("%s%d.", prefix, i)

		targetStr := vals.Get(memberPrefix + "TargetValue")
		pair := parsePredefinedMetricRef(vals, memberPrefix+"PredefinedMetricPairSpecification.")
		load := parsePredefinedMetricRef(vals, memberPrefix+"PredefinedLoadMetricSpecification.")
		scalingMetric := parsePredefinedMetricRef(
			vals,
			memberPrefix+"PredefinedScalingMetricSpecification.",
		)

		customizedLoad, err := parsePredictiveScalingCustomizedMetric(
			vals, memberPrefix+"CustomizedLoadMetricSpecification.",
		)
		if err != nil {
			return nil, err
		}

		customizedScaling, err := parsePredictiveScalingCustomizedMetric(
			vals, memberPrefix+"CustomizedScalingMetricSpecification.",
		)
		if err != nil {
			return nil, err
		}

		customizedCapacity, err := parsePredictiveScalingCustomizedMetric(
			vals, memberPrefix+"CustomizedCapacityMetricSpecification.",
		)
		if err != nil {
			return nil, err
		}

		if targetStr == "" && pair == nil && load == nil && scalingMetric == nil &&
			customizedLoad == nil && customizedScaling == nil && customizedCapacity == nil {
			break
		}

		spec := PredictiveScalingMetricSpecification{
			PredefinedMetricPairSpecification:     pair,
			PredefinedLoadMetricSpecification:     load,
			PredefinedScalingMetricSpecification:  scalingMetric,
			CustomizedLoadMetricSpecification:     customizedLoad,
			CustomizedScalingMetricSpecification:  customizedScaling,
			CustomizedCapacityMetricSpecification: customizedCapacity,
		}

		if targetStr != "" {
			tv, parseErr := strconv.ParseFloat(targetStr, 64)
			if parseErr != nil {
				return nil, fmt.Errorf("%w: invalid %sTargetValue", ErrInvalidParameter, memberPrefix)
			}

			spec.TargetValue = tv
		}

		specs = append(specs, spec)
	}

	return specs, nil
}

// parsePredefinedMetricRef parses the {PredefinedMetricType,
// ResourceLabel} shape shared by PredefinedMetricPairSpecification,
// PredefinedLoadMetricSpecification, and PredefinedScalingMetricSpecification.
func parsePredefinedMetricRef(vals url.Values, prefix string) *PredefinedMetricRef {
	metricType := vals.Get(prefix + "PredefinedMetricType")
	resourceLabel := vals.Get(prefix + "ResourceLabel")

	if metricType == "" && resourceLabel == "" {
		return nil
	}

	return &PredefinedMetricRef{
		PredefinedMetricType: metricType,
		ResourceLabel:        resourceLabel,
	}
}

func (h *Handler) handlePutScalingPolicy(vals url.Values) (any, error) {
	intFields, err := scalingPolicyIntFields(vals)
	if err != nil {
		return nil, err
	}

	ttc, err := parseTargetTrackingFields(vals)
	if err != nil {
		return nil, err
	}

	stepAdjustments, err := parseStepAdjustments(vals)
	if err != nil {
		return nil, err
	}

	predictiveScaling, err := parsePredictiveScalingConfiguration(vals)
	if err != nil {
		return nil, err
	}

	input := ScalingPolicyInput{
		AutoScalingGroupName:           vals.Get("AutoScalingGroupName"),
		PolicyName:                     vals.Get("PolicyName"),
		PolicyType:                     vals.Get("PolicyType"),
		AdjustmentType:                 vals.Get("AdjustmentType"),
		MetricAggregationType:          vals.Get("MetricAggregationType"),
		ScalingAdjustment:              intFields.scalingAdjustment,
		MinAdjustmentStep:              intFields.minAdjustmentStep,
		MinAdjustmentMagnitude:         intFields.minAdjustmentMagnitude,
		StepAdjustments:                stepAdjustments,
		Cooldown:                       intFields.cooldown,
		TargetValue:                    ttc.targetValue,
		MetricType:                     ttc.metricType,
		DisableScaleIn:                 ttc.disableScaleIn,
		EstimatedWarmup:                ttc.estimatedWarmup,
		PredictiveScalingConfiguration: predictiveScaling,
		CustomizedMetricSpecification:  ttc.customizedMetricSpec,
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

			ttc.CustomizedMetricSpecification = toXMLCustomizedMetricSpecification(p.CustomizedMetricSpecification)

			xmlPolicy.TargetTrackingConfiguration = ttc
		}

		if p.PredictiveScalingConfiguration != nil {
			xmlPolicy.PredictiveScalingConfiguration = toXMLPredictiveScalingConfiguration(
				p.PredictiveScalingConfiguration,
			)
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
	CustomizedMetricSpecification *xmlCustomizedMetricSpecification `xml:"CustomizedMetricSpecification,omitempty"`
	TargetValue                   float64                           `xml:"TargetValue"`
	DisableScaleIn                bool                              `xml:"DisableScaleIn,omitempty"`
	EstimatedInstanceWarmup       int32                             `xml:"EstimatedInstanceWarmup,omitempty"`
}

type xmlMetricDimension struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type xmlMetricDimensionList struct {
	Members []xmlMetricDimension `xml:"member"`
}

// xmlMetricRef is the XML response projection of MetricRef, matching AWS
// types.Metric (deserializers.go:14890).
type xmlMetricRef struct {
	Dimensions *xmlMetricDimensionList `xml:"Dimensions,omitempty"`
	MetricName string                  `xml:"MetricName,omitempty"`
	Namespace  string                  `xml:"Namespace,omitempty"`
}

func toXMLMetricRef(m *MetricRef) *xmlMetricRef {
	if m == nil {
		return nil
	}

	return &xmlMetricRef{
		MetricName: m.MetricName,
		Namespace:  m.Namespace,
		Dimensions: toXMLMetricDimensionList(m.Dimensions),
	}
}

func toXMLMetricDimensionList(dims []MetricDimension) *xmlMetricDimensionList {
	if len(dims) == 0 {
		return nil
	}

	members := make([]xmlMetricDimension, 0, len(dims))
	for _, d := range dims {
		members = append(members, xmlMetricDimension(d))
	}

	return &xmlMetricDimensionList{Members: members}
}

// xmlMetricDataStat is the XML response projection of MetricDataStat,
// matching AWS types.MetricStat (deserializers.go:15487) and
// types.TargetTrackingMetricStat (deserializers.go:18997) -- both share this
// shape on the wire, differing only in whether Period is populated.
type xmlMetricDataStat struct {
	Metric *xmlMetricRef `xml:"Metric,omitempty"`
	Period *int32        `xml:"Period,omitempty"`
	Stat   string        `xml:"Stat,omitempty"`
	Unit   string        `xml:"Unit,omitempty"`
}

func toXMLMetricDataStat(s *MetricDataStat) *xmlMetricDataStat {
	if s == nil {
		return nil
	}

	return &xmlMetricDataStat{
		Metric: toXMLMetricRef(s.Metric),
		Period: s.Period,
		Stat:   s.Stat,
		Unit:   s.Unit,
	}
}

// xmlMetricDataQuery is the XML response projection of MetricDataQuery,
// matching AWS types.MetricDataQuery (deserializers.go:15143) and
// types.TargetTrackingMetricDataQuery (deserializers.go:18883).
type xmlMetricDataQuery struct {
	MetricStat *xmlMetricDataStat `xml:"MetricStat,omitempty"`
	Period     *int32             `xml:"Period,omitempty"`
	ReturnData *bool              `xml:"ReturnData,omitempty"`
	ID         string             `xml:"Id"`
	Expression string             `xml:"Expression,omitempty"`
	Label      string             `xml:"Label,omitempty"`
}

type xmlMetricDataQueryList struct {
	Members []xmlMetricDataQuery `xml:"member"`
}

func toXMLMetricDataQueryList(queries []MetricDataQuery) *xmlMetricDataQueryList {
	if len(queries) == 0 {
		return nil
	}

	members := make([]xmlMetricDataQuery, 0, len(queries))
	for _, q := range queries {
		members = append(members, xmlMetricDataQuery{
			ID:         q.ID,
			Expression: q.Expression,
			Label:      q.Label,
			MetricStat: toXMLMetricDataStat(q.MetricStat),
			Period:     q.Period,
			ReturnData: q.ReturnData,
		})
	}

	return &xmlMetricDataQueryList{Members: members}
}

// xmlCustomizedMetricSpecification is the XML response projection of
// CustomizedMetricSpecification, matching AWS
// types.CustomizedMetricSpecification (deserializers.go:10629).
type xmlCustomizedMetricSpecification struct {
	Dimensions *xmlMetricDimensionList `xml:"Dimensions,omitempty"`
	Metrics    *xmlMetricDataQueryList `xml:"Metrics,omitempty"`
	Period     *int32                  `xml:"Period,omitempty"`
	MetricName string                  `xml:"MetricName,omitempty"`
	Namespace  string                  `xml:"Namespace,omitempty"`
	Statistic  string                  `xml:"Statistic,omitempty"`
	Unit       string                  `xml:"Unit,omitempty"`
}

func toXMLCustomizedMetricSpecification(spec *CustomizedMetricSpecification) *xmlCustomizedMetricSpecification {
	if spec == nil {
		return nil
	}

	return &xmlCustomizedMetricSpecification{
		MetricName: spec.MetricName,
		Namespace:  spec.Namespace,
		Statistic:  spec.Statistic,
		Unit:       spec.Unit,
		Period:     spec.Period,
		Dimensions: toXMLMetricDimensionList(spec.Dimensions),
		Metrics:    toXMLMetricDataQueryList(spec.Metrics),
	}
}

// xmlCustomMetricQueries is the XML response projection of
// CustomMetricQueries, matching AWS
// types.PredictiveScalingCustomized{Capacity,Load,Scaling}Metric
// (deserializers.go:16235, 16277, 16319).
type xmlCustomMetricQueries struct {
	MetricDataQueries xmlMetricDataQueryList `xml:"MetricDataQueries"`
}

func toXMLCustomMetricQueries(
	m *CustomMetricQueries,
) *xmlCustomMetricQueries {
	if m == nil {
		return nil
	}

	queries := toXMLMetricDataQueryList(m.MetricDataQueries)
	if queries == nil {
		queries = &xmlMetricDataQueryList{}
	}

	return &xmlCustomMetricQueries{MetricDataQueries: *queries}
}

// toXMLPredictiveScalingConfiguration converts the stored config to its XML
// response shape, matching DescribePolicies' ScalingPolicy.
// PredictiveScalingConfiguration wire shape (deserializers.go:16133).
func toXMLPredictiveScalingConfiguration(cfg *PredictiveScalingConfiguration) *xmlPredictiveScalingConfiguration {
	specs := make([]xmlPredictiveScalingMetricSpecification, 0, len(cfg.MetricSpecifications))
	for _, s := range cfg.MetricSpecifications {
		specs = append(specs, xmlPredictiveScalingMetricSpecification{
			PredefinedMetricPairSpecification: toXMLPredefinedMetricRef(
				s.PredefinedMetricPairSpecification,
			),
			PredefinedLoadMetricSpecification: toXMLPredefinedMetricRef(
				s.PredefinedLoadMetricSpecification,
			),
			PredefinedScalingMetricSpecification: toXMLPredefinedMetricRef(
				s.PredefinedScalingMetricSpecification,
			),
			CustomizedLoadMetricSpecification: toXMLCustomMetricQueries(
				s.CustomizedLoadMetricSpecification,
			),
			CustomizedScalingMetricSpecification: toXMLCustomMetricQueries(
				s.CustomizedScalingMetricSpecification,
			),
			CustomizedCapacityMetricSpecification: toXMLCustomMetricQueries(
				s.CustomizedCapacityMetricSpecification,
			),
			TargetValue: s.TargetValue,
		})
	}

	return &xmlPredictiveScalingConfiguration{
		MaxCapacityBreachBehavior: cfg.MaxCapacityBreachBehavior,
		Mode:                      cfg.Mode,
		MaxCapacityBuffer:         cfg.MaxCapacityBuffer,
		SchedulingBufferTime:      cfg.SchedulingBufferTime,
		MetricSpecifications:      xmlPredictiveScalingMetricSpecificationList{Members: specs},
	}
}

func toXMLPredefinedMetricRef(
	m *PredefinedMetricRef,
) *xmlPredefinedMetricRef {
	if m == nil {
		return nil
	}

	return &xmlPredefinedMetricRef{
		PredefinedMetricType: m.PredefinedMetricType,
		ResourceLabel:        m.ResourceLabel,
	}
}

type xmlPredefinedMetricRef struct {
	PredefinedMetricType string `xml:"PredefinedMetricType,omitempty"`
	ResourceLabel        string `xml:"ResourceLabel,omitempty"`
}

type xmlPredictiveScalingMetricSpecification struct {
	PredefinedMetricPairSpecification     *xmlPredefinedMetricRef `xml:"PredefinedMetricPairSpecification,omitempty"`
	PredefinedLoadMetricSpecification     *xmlPredefinedMetricRef `xml:"PredefinedLoadMetricSpecification,omitempty"`
	PredefinedScalingMetricSpecification  *xmlPredefinedMetricRef `xml:"PredefinedScalingMetricSpecification,omitempty"`
	CustomizedLoadMetricSpecification     *xmlCustomMetricQueries `xml:"CustomizedLoadMetricSpecification,omitempty"`
	CustomizedScalingMetricSpecification  *xmlCustomMetricQueries `xml:"CustomizedScalingMetricSpecification,omitempty"`
	CustomizedCapacityMetricSpecification *xmlCustomMetricQueries `xml:"CustomizedCapacityMetricSpecification,omitempty"`
	TargetValue                           float64                 `xml:"TargetValue"`
}

type xmlPredictiveScalingMetricSpecificationList struct {
	Members []xmlPredictiveScalingMetricSpecification `xml:"member"`
}

type xmlPredictiveScalingConfiguration struct {
	MaxCapacityBuffer         *int32                                      `xml:"MaxCapacityBuffer,omitempty"`
	SchedulingBufferTime      *int32                                      `xml:"SchedulingBufferTime,omitempty"`
	MaxCapacityBreachBehavior string                                      `xml:"MaxCapacityBreachBehavior,omitempty"`
	Mode                      string                                      `xml:"Mode,omitempty"`
	MetricSpecifications      xmlPredictiveScalingMetricSpecificationList `xml:"MetricSpecifications"`
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
	TargetTrackingConfiguration    *xmlTargetTrackingConfiguration    `xml:"TargetTrackingConfiguration,omitempty"`
	PredictiveScalingConfiguration *xmlPredictiveScalingConfiguration `xml:"PredictiveScalingConfiguration,omitempty"`
	StepAdjustments                *xmlStepAdjustmentList             `xml:"StepAdjustments,omitempty"`
	PolicyName                     string                             `xml:"PolicyName"`
	PolicyARN                      string                             `xml:"PolicyARN"`
	AutoScalingGroupName           string                             `xml:"AutoScalingGroupName"`
	PolicyType                     string                             `xml:"PolicyType,omitempty"`
	AdjustmentType                 string                             `xml:"AdjustmentType,omitempty"`
	MetricAggregationType          string                             `xml:"MetricAggregationType,omitempty"`
	ScalingAdjustment              int32                              `xml:"ScalingAdjustment,omitempty"`
	MinAdjustmentStep              int32                              `xml:"MinAdjustmentStep,omitempty"`
	MinAdjustmentMagnitude         int32                              `xml:"MinAdjustmentMagnitude,omitempty"`
	Cooldown                       int32                              `xml:"Cooldown,omitempty"`
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
