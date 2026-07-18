package autoscaling

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func (h *Handler) handleCreateAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")
	lcName := vals.Get("LaunchConfigurationName")
	healthCheckType := vals.Get("HealthCheckType")

	minSize, err := parseIntVal(vals.Get("MinSize"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinSize", ErrInvalidParameter)
	}

	maxSize, err := parseIntVal(vals.Get("MaxSize"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MaxSize", ErrInvalidParameter)
	}

	desiredCapacity, err := parseIntVal(vals.Get("DesiredCapacity"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
	}

	// Enforce bounds on sizes to prevent excessive memory allocation when creating
	// the initial instances slice in the backend.
	if minSize < 0 || maxSize < 0 || desiredCapacity < 0 {
		return nil, fmt.Errorf("%w: sizes must be non-negative", ErrInvalidParameter)
	}

	if minSize > maxDesiredCapacity || maxSize > maxDesiredCapacity || desiredCapacity > maxDesiredCapacity {
		return nil, fmt.Errorf("%w: sizes must not exceed %d", ErrInvalidParameter, maxDesiredCapacity)
	}

	defaultCooldown, err := parseIntVal(vals.Get("DefaultCooldown"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DefaultCooldown", ErrInvalidParameter)
	}

	healthCheckGracePeriod, err := parseIntVal(vals.Get("HealthCheckGracePeriod"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckGracePeriod", ErrInvalidParameter)
	}

	maxInstanceLifetime, err := parseIntVal(vals.Get("MaxInstanceLifetime"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MaxInstanceLifetime", ErrInvalidParameter)
	}

	defaultInstanceWarmup, err := parseIntVal(vals.Get("DefaultInstanceWarmup"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DefaultInstanceWarmup", ErrInvalidParameter)
	}

	azs := parseMembers(vals, "AvailabilityZones.member")
	lbNames := parseMembers(vals, "LoadBalancerNames.member")
	targetGroupARNs := parseMembers(vals, "TargetGroupARNs.member")
	tags := parseTags(vals, "Tags.member")
	terminationPolicies := parseMembers(vals, "TerminationPolicies.member")
	lt := parseLaunchTemplate(vals, "LaunchTemplate")
	mip := parseMixedInstancesPolicy(vals)
	hooks := parseLifecycleHookSpecifications(vals)
	trafficSources := parseTrafficSources(vals)

	input := CreateAutoScalingGroupInput{
		AutoScalingGroupName:             name,
		LaunchConfigurationName:          lcName,
		LaunchTemplate:                   lt,
		MixedInstancesPolicy:             mip,
		VPCZoneIdentifier:                vals.Get("VPCZoneIdentifier"),
		PlacementGroup:                   vals.Get("PlacementGroup"),
		Context:                          vals.Get("Context"),
		DesiredCapacityType:              vals.Get("DesiredCapacityType"),
		MinSize:                          minSize,
		MaxSize:                          maxSize,
		DesiredCapacity:                  desiredCapacity,
		DefaultCooldown:                  defaultCooldown,
		HealthCheckType:                  healthCheckType,
		HealthCheckGracePeriod:           healthCheckGracePeriod,
		MaxInstanceLifetime:              maxInstanceLifetime,
		DefaultInstanceWarmup:            defaultInstanceWarmup,
		NewInstancesProtectedFromScaleIn: vals.Get("NewInstancesProtectedFromScaleIn") == formValueTrue,
		CapacityRebalance:                vals.Get("CapacityRebalance") == formValueTrue,
		AvailabilityZones:                azs,
		LoadBalancerNames:                lbNames,
		TargetGroupARNs:                  targetGroupARNs,
		TrafficSources:                   trafficSources,
		Tags:                             tags,
		TerminationPolicies:              terminationPolicies,
		LifecycleHookSpecificationList:   hooks,
	}

	_, createErr := h.Backend.CreateAutoScalingGroup(input)
	if createErr != nil {
		return nil, createErr
	}

	return &createAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-" + name},
	}, nil
}

const (
	// defaultMaxRecords is the default max records for paginated responses.
	defaultMaxRecords = int32(100)
	// maxMaxRecords is the maximum max records allowed.
	maxMaxRecords = int32(100)
)

func (h *Handler) handleDescribeAutoScalingGroups(vals url.Values) (any, error) {
	names := parseMembers(vals, "AutoScalingGroupNames.member")

	groups, err := h.Backend.DescribeAutoScalingGroups(names)
	if err != nil {
		return nil, err
	}

	// Parse MaxRecords (default 100, max 100)
	maxRecords := defaultMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(n, maxMaxRecords)
		}
	}

	// Apply NextToken cursor (base64-encoded last group name)
	nextToken := vals.Get("NextToken")
	if nextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(nextToken); decErr == nil {
			lastName := string(decoded)
			// Skip groups up to and including lastName
			for i, g := range groups {
				if g.AutoScalingGroupName == lastName {
					groups = groups[i+1:]

					break
				}
			}
		}
	}

	// Paginate
	var returnedNextToken string
	if int32(len(groups)) > maxRecords { //nolint:gosec // bounded by maxMaxRecords
		returnedNextToken = base64.StdEncoding.EncodeToString(
			[]byte(groups[maxRecords-1].AutoScalingGroupName),
		)
		groups = groups[:maxRecords]
	}

	members := make([]xmlAutoScalingGroup, 0, len(groups))
	for i := range groups {
		members = append(members, toXMLGroup(&groups[i]))
	}

	return &describeAutoScalingGroupsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAutoScalingGroupsResult{
			NextToken:         returnedNextToken,
			AutoScalingGroups: xmlAutoScalingGroupList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-groups"},
	}, nil
}

//nolint:gocognit,cyclop // Too complex to refactor given time constraints
func (h *Handler) handleUpdateAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")

	input := UpdateAutoScalingGroupInput{
		AutoScalingGroupName:    name,
		LaunchConfigurationName: vals.Get("LaunchConfigurationName"),
		HealthCheckType:         vals.Get("HealthCheckType"),
		VPCZoneIdentifier:       vals.Get("VPCZoneIdentifier"),
		PlacementGroup:          vals.Get("PlacementGroup"),
		Context:                 vals.Get("Context"),
		DesiredCapacityType:     vals.Get("DesiredCapacityType"),
		AvailabilityZones:       parseMembers(vals, "AvailabilityZones.member"),
		TerminationPolicies:     parseMembers(vals, "TerminationPolicies.member"),
		LaunchTemplate:          parseLaunchTemplate(vals, "LaunchTemplate"),
		MixedInstancesPolicy:    parseMixedInstancesPolicy(vals),
	}

	if v := vals.Get("MinSize"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid MinSize", ErrInvalidParameter)
		}

		input.MinSize = &n
	}

	if v := vals.Get("MaxSize"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid MaxSize", ErrInvalidParameter)
		}

		input.MaxSize = &n
	}

	if v := vals.Get("DesiredCapacity"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
		}

		input.DesiredCapacity = &n
	}

	if v := vals.Get("DefaultCooldown"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DefaultCooldown", ErrInvalidParameter)
		}

		input.DefaultCooldown = &n
	}

	if v := vals.Get("HealthCheckGracePeriod"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid HealthCheckGracePeriod", ErrInvalidParameter)
		}

		input.HealthCheckGracePeriod = &n
	}

	if v := vals.Get("MaxInstanceLifetime"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid MaxInstanceLifetime", ErrInvalidParameter)
		}

		input.MaxInstanceLifetime = &n
	}

	if v := vals.Get("DefaultInstanceWarmup"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DefaultInstanceWarmup", ErrInvalidParameter)
		}

		input.DefaultInstanceWarmup = &n
	}

	if v := vals.Get("NewInstancesProtectedFromScaleIn"); v != "" {
		b := v == formValueTrue
		input.NewInstancesProtectedFromScaleIn = &b
	}

	if v := vals.Get("CapacityRebalance"); v != "" {
		b := v == formValueTrue
		input.CapacityRebalance = &b
	}

	_, updateErr := h.Backend.UpdateAutoScalingGroup(input)
	if updateErr != nil {
		return nil, updateErr
	}

	return &updateAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-update-" + name},
	}, nil
}

func (h *Handler) handleDeleteAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")
	forceDelete := vals.Get("ForceDelete") == formValueTrue

	if err := h.Backend.DeleteAutoScalingGroup(name, forceDelete); err != nil {
		return nil, err
	}

	return &deleteAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-" + name},
	}, nil
}

func (h *Handler) handleSetDesiredCapacity(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	desired, parseErr := parseIntVal(vals.Get("DesiredCapacity"))
	if parseErr != nil {
		return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
	}

	if err := h.Backend.SetDesiredCapacity(groupName, desired); err != nil {
		return nil, err
	}

	return &setDesiredCapacityResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-set-desired-capacity"},
	}, nil
}

// toXMLGroup converts an AutoScalingGroup to the XML response type.
func toXMLGroup(g *AutoScalingGroup) xmlAutoScalingGroup { //nolint:funlen // XML projection inherently maps many fields
	azs := make([]xmlStringValue, 0, len(g.AvailabilityZones))
	for _, az := range g.AvailabilityZones {
		azs = append(azs, xmlStringValue{Value: az})
	}

	lbNames := make([]xmlStringValue, 0, len(g.LoadBalancerNames))
	for _, lb := range g.LoadBalancerNames {
		lbNames = append(lbNames, xmlStringValue{Value: lb})
	}

	tgARNs := make([]xmlStringValue, 0, len(g.TargetGroupARNs))
	for _, tg := range g.TargetGroupARNs {
		tgARNs = append(tgARNs, xmlStringValue{Value: tg})
	}

	tags := make([]xmlTag, 0, len(g.Tags))
	for _, t := range g.Tags {
		tags = append(tags, xmlTag{
			Key:               t.Key,
			Value:             t.Value,
			ResourceID:        g.AutoScalingGroupName,
			ResourceType:      resourceTypeAutoScalingGroup,
			PropagateAtLaunch: t.PropagateAtLaunch,
		})
	}

	instances := make([]xmlInstance, 0, len(g.Instances))
	for _, inst := range g.Instances {
		instances = append(instances, xmlInstance{
			InstanceID:              inst.InstanceID,
			AvailabilityZone:        inst.AvailabilityZone,
			LifecycleState:          inst.LifecycleState,
			HealthStatus:            inst.HealthStatus,
			InstanceType:            inst.InstanceType,
			LaunchConfigurationName: inst.LaunchConfigurationName,
			ProtectedFromScaleIn:    inst.ProtectedFromScaleIn,
		})
	}

	suspendedProcesses := make([]xmlSuspendedProcess, 0, len(g.SuspendedProcesses))
	for _, p := range g.SuspendedProcesses {
		suspendedProcesses = append(suspendedProcesses, xmlSuspendedProcess{ProcessName: p})
	}

	trafficSources := make([]xmlTrafficSource, 0, len(g.TrafficSources))
	for _, ts := range g.TrafficSources {
		trafficSources = append(trafficSources, xmlTrafficSource(ts))
	}

	terminationPolicies := make([]xmlStringValue, 0, len(g.TerminationPolicies))
	for _, tp := range g.TerminationPolicies {
		terminationPolicies = append(terminationPolicies, xmlStringValue{Value: tp})
	}

	enabledMetrics := make([]xmlEnabledMetric, 0, len(g.EnabledMetrics))
	for _, m := range g.EnabledMetrics {
		enabledMetrics = append(enabledMetrics, xmlEnabledMetric{Metric: m, Granularity: granularity1Minute})
	}

	var xmlLT *xmlLaunchTemplateSpecification
	if g.LaunchTemplate != nil {
		xmlLT = &xmlLaunchTemplateSpecification{
			LaunchTemplateID:   g.LaunchTemplate.LaunchTemplateID,
			LaunchTemplateName: g.LaunchTemplate.LaunchTemplateName,
			Version:            g.LaunchTemplate.Version,
		}
	}

	xmlMIP := toXMLMixedInstancesPolicy(g.MixedInstancesPolicy)

	return xmlAutoScalingGroup{
		AutoScalingGroupName:             g.AutoScalingGroupName,
		AutoScalingGroupARN:              g.AutoScalingGroupARN,
		LaunchConfigurationName:          g.LaunchConfigurationName,
		LaunchTemplate:                   xmlLT,
		MixedInstancesPolicy:             xmlMIP,
		VPCZoneIdentifier:                g.VPCZoneIdentifier,
		PlacementGroup:                   g.PlacementGroup,
		Context:                          g.Context,
		DesiredCapacityType:              g.DesiredCapacityType,
		MinSize:                          g.MinSize,
		MaxSize:                          g.MaxSize,
		DesiredCapacity:                  g.DesiredCapacity,
		DefaultCooldown:                  g.DefaultCooldown,
		HealthCheckType:                  g.HealthCheckType,
		HealthCheckGracePeriod:           g.HealthCheckGracePeriod,
		MaxInstanceLifetime:              g.MaxInstanceLifetime,
		DefaultInstanceWarmup:            g.DefaultInstanceWarmup,
		NewInstancesProtectedFromScaleIn: g.NewInstancesProtectedFromScaleIn,
		CapacityRebalance:                g.CapacityRebalance,
		CreatedTime:                      g.CreatedTime.UTC().Format(time.RFC3339),
		Status:                           g.Status,
		AvailabilityZones:                xmlStringValueList{Members: azs},
		LoadBalancerNames:                xmlStringValueList{Members: lbNames},
		TargetGroupARNs:                  xmlStringValueList{Members: tgARNs},
		TrafficSources:                   xmlTrafficSourceList{Members: trafficSources},
		Tags:                             xmlTagList{Members: tags},
		Instances:                        xmlInstanceList{Members: instances},
		SuspendedProcesses:               xmlSuspendedProcessList{Members: suspendedProcesses},
		TerminationPolicies:              xmlTerminationPoliciesList{Members: terminationPolicies},
		EnabledMetrics:                   xmlEnabledMetricList{Members: enabledMetrics},
		ServiceLinkedRoleARN: fmt.Sprintf(
			"arn:aws:iam::%s:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
			config.DefaultAccountID,
		),
	}
}

// toXMLMixedInstancesPolicy converts a MixedInstancesPolicy to its XML response
// projection, or nil when policy is nil (matches AWS: the element is entirely absent
// for ASGs that don't use a mixed instances policy).
func toXMLMixedInstancesPolicy(policy *MixedInstancesPolicy) *xmlMixedInstancesPolicy {
	if policy == nil {
		return nil
	}

	overrides := make([]xmlLaunchTemplateOverride, 0, len(policy.LaunchTemplate.Overrides))

	for _, o := range policy.LaunchTemplate.Overrides {
		xo := xmlLaunchTemplateOverride{
			InstanceType:     o.InstanceType,
			WeightedCapacity: o.WeightedCapacity,
		}

		if o.LaunchTemplateSpecification != nil {
			xo.LaunchTemplateSpecification = &xmlLaunchTemplateSpecification{
				LaunchTemplateID:   o.LaunchTemplateSpecification.LaunchTemplateID,
				LaunchTemplateName: o.LaunchTemplateSpecification.LaunchTemplateName,
				Version:            o.LaunchTemplateSpecification.Version,
			}
		}

		overrides = append(overrides, xo)
	}

	return &xmlMixedInstancesPolicy{
		LaunchTemplate: xmlMixedInstancesLaunchTemplate{
			LaunchTemplateSpecification: xmlLaunchTemplateSpecification{
				LaunchTemplateID:   policy.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateID,
				LaunchTemplateName: policy.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName,
				Version:            policy.LaunchTemplate.LaunchTemplateSpecification.Version,
			},
			Overrides: xmlLaunchTemplateOverrideList{Members: overrides},
		},
		InstancesDistribution: xmlInstancesDistribution{
			OnDemandAllocationStrategy:          policy.InstancesDistribution.OnDemandAllocationStrategy,
			SpotAllocationStrategy:              policy.InstancesDistribution.SpotAllocationStrategy,
			SpotMaxPrice:                        policy.InstancesDistribution.SpotMaxPrice,
			OnDemandBaseCapacity:                policy.InstancesDistribution.OnDemandBaseCapacity,
			OnDemandPercentageAboveBaseCapacity: policy.InstancesDistribution.OnDemandPercentageAboveBaseCapacity,
			SpotInstancePools:                   policy.InstancesDistribution.SpotInstancePools,
		},
	}
}

type createAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type updateAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"UpdateAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"DeleteAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlTag struct {
	Key               string `xml:"Key"`
	Value             string `xml:"Value"`
	ResourceID        string `xml:"ResourceId,omitempty"`
	ResourceType      string `xml:"ResourceType,omitempty"`
	PropagateAtLaunch bool   `xml:"PropagateAtLaunch,omitempty"`
}

type xmlTagList struct {
	Members []xmlTag `xml:"member"`
}

type xmlInstance struct {
	InstanceID              string `xml:"InstanceId"`
	AvailabilityZone        string `xml:"AvailabilityZone"`
	LifecycleState          string `xml:"LifecycleState"`
	HealthStatus            string `xml:"HealthStatus"`
	InstanceType            string `xml:"InstanceType,omitempty"`
	LaunchConfigurationName string `xml:"LaunchConfigurationName,omitempty"`
	ProtectedFromScaleIn    bool   `xml:"ProtectedFromScaleIn,omitempty"`
}

type xmlInstanceList struct {
	Members []xmlInstance `xml:"member"`
}

type xmlSuspendedProcess struct {
	ProcessName      string `xml:"ProcessName"`
	SuspensionReason string `xml:"SuspensionReason,omitempty"`
}

type xmlSuspendedProcessList struct {
	Members []xmlSuspendedProcess `xml:"member"`
}

type xmlEnabledMetric struct {
	Metric      string `xml:"Metric"`
	Granularity string `xml:"Granularity"`
}

type xmlEnabledMetricList struct {
	Members []xmlEnabledMetric `xml:"member"`
}

type xmlLaunchTemplateSpecification struct {
	LaunchTemplateID   string `xml:"LaunchTemplateId,omitempty"`
	LaunchTemplateName string `xml:"LaunchTemplateName,omitempty"`
	Version            string `xml:"Version,omitempty"`
}

type xmlLaunchTemplateOverride struct {
	LaunchTemplateSpecification *xmlLaunchTemplateSpecification `xml:"LaunchTemplateSpecification,omitempty"`
	InstanceType                string                          `xml:"InstanceType,omitempty"`
	WeightedCapacity            string                          `xml:"WeightedCapacity,omitempty"`
}

type xmlLaunchTemplateOverrideList struct {
	Members []xmlLaunchTemplateOverride `xml:"member"`
}

type xmlMixedInstancesLaunchTemplate struct {
	LaunchTemplateSpecification xmlLaunchTemplateSpecification `xml:"LaunchTemplateSpecification"`
	Overrides                   xmlLaunchTemplateOverrideList  `xml:"Overrides,omitempty"`
}

type xmlInstancesDistribution struct {
	OnDemandAllocationStrategy          string `xml:"OnDemandAllocationStrategy,omitempty"`
	SpotAllocationStrategy              string `xml:"SpotAllocationStrategy,omitempty"`
	SpotMaxPrice                        string `xml:"SpotMaxPrice,omitempty"`
	OnDemandBaseCapacity                int32  `xml:"OnDemandBaseCapacity,omitempty"`
	OnDemandPercentageAboveBaseCapacity int32  `xml:"OnDemandPercentageAboveBaseCapacity,omitempty"`
	SpotInstancePools                   int32  `xml:"SpotInstancePools,omitempty"`
}

type xmlMixedInstancesPolicy struct {
	LaunchTemplate        xmlMixedInstancesLaunchTemplate `xml:"LaunchTemplate"`
	InstancesDistribution xmlInstancesDistribution        `xml:"InstancesDistribution"`
}

type xmlTrafficSource struct {
	Identifier string `xml:"Identifier"`
	Type       string `xml:"Type,omitempty"`
}

type xmlTrafficSourceList struct {
	Members []xmlTrafficSource `xml:"member"`
}

type xmlTerminationPoliciesList struct {
	Members []xmlStringValue `xml:"member"`
}

type xmlAutoScalingGroup struct {
	LaunchTemplate                   *xmlLaunchTemplateSpecification `xml:"LaunchTemplate,omitempty"`
	MixedInstancesPolicy             *xmlMixedInstancesPolicy        `xml:"MixedInstancesPolicy,omitempty"`
	AutoScalingGroupARN              string                          `xml:"AutoScalingGroupARN"`
	Status                           string                          `xml:"Status,omitempty"`
	CreatedTime                      string                          `xml:"CreatedTime"`
	HealthCheckType                  string                          `xml:"HealthCheckType"`
	LaunchConfigurationName          string                          `xml:"LaunchConfigurationName,omitempty"`
	AutoScalingGroupName             string                          `xml:"AutoScalingGroupName"`
	VPCZoneIdentifier                string                          `xml:"VPCZoneIdentifier,omitempty"`
	PlacementGroup                   string                          `xml:"PlacementGroup,omitempty"`
	Context                          string                          `xml:"Context,omitempty"`
	DesiredCapacityType              string                          `xml:"DesiredCapacityType,omitempty"`
	ServiceLinkedRoleARN             string                          `xml:"ServiceLinkedRoleARN,omitempty"`
	TargetGroupARNs                  xmlStringValueList              `xml:"TargetGroupARNs"`
	Tags                             xmlTagList                      `xml:"Tags"`
	AvailabilityZones                xmlStringValueList              `xml:"AvailabilityZones"`
	LoadBalancerNames                xmlStringValueList              `xml:"LoadBalancerNames"`
	TrafficSources                   xmlTrafficSourceList            `xml:"TrafficSources"`
	SuspendedProcesses               xmlSuspendedProcessList         `xml:"SuspendedProcesses"`
	TerminationPolicies              xmlTerminationPoliciesList      `xml:"TerminationPolicies"`
	EnabledMetrics                   xmlEnabledMetricList            `xml:"EnabledMetrics"`
	Instances                        xmlInstanceList                 `xml:"Instances"`
	MaxSize                          int32                           `xml:"MaxSize"`
	DesiredCapacity                  int32                           `xml:"DesiredCapacity"`
	DefaultCooldown                  int32                           `xml:"DefaultCooldown"`
	HealthCheckGracePeriod           int32                           `xml:"HealthCheckGracePeriod"`
	MaxInstanceLifetime              int32                           `xml:"MaxInstanceLifetime,omitempty"`
	DefaultInstanceWarmup            int32                           `xml:"DefaultInstanceWarmup,omitempty"`
	MinSize                          int32                           `xml:"MinSize"`
	NewInstancesProtectedFromScaleIn bool                            `xml:"NewInstancesProtectedFromScaleIn,omitempty"`
	CapacityRebalance                bool                            `xml:"CapacityRebalance,omitempty"`
}

type xmlAutoScalingGroupList struct {
	Members []xmlAutoScalingGroup `xml:"member"`
}

type describeAutoScalingGroupsResult struct {
	NextToken         string                  `xml:"NextToken,omitempty"`
	AutoScalingGroups xmlAutoScalingGroupList `xml:"AutoScalingGroups"`
}

type describeAutoScalingGroupsResponse struct {
	XMLName          xml.Name                        `xml:"DescribeAutoScalingGroupsResponse"`
	Xmlns            string                          `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata             `xml:"ResponseMetadata"`
	Result           describeAutoScalingGroupsResult `xml:"DescribeAutoScalingGroupsResult"`
}

// parseLaunchTemplate parses a LaunchTemplateSpecification from form values.
// prefix is e.g. "LaunchTemplate" or "MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification".
// Returns nil if no LaunchTemplateId or LaunchTemplateName found.
func parseLaunchTemplate(vals url.Values, prefix string) *LaunchTemplateSpecification {
	id := vals.Get(prefix + ".LaunchTemplateId")
	name := vals.Get(prefix + ".LaunchTemplateName")
	version := vals.Get(prefix + ".Version")

	if id == "" && name == "" {
		return nil
	}

	return &LaunchTemplateSpecification{
		LaunchTemplateID:   id,
		LaunchTemplateName: name,
		Version:            version,
	}
}

// parseMixedInstancesPolicy parses a MixedInstancesPolicy from form values using the
// standard AWS query-protocol flattening: MixedInstancesPolicy.LaunchTemplate.*,
// MixedInstancesPolicy.LaunchTemplate.Overrides.member.N.*, and
// MixedInstancesPolicy.InstancesDistribution.*. Returns nil if neither a launch
// template nor an instances distribution was specified.
func parseMixedInstancesPolicy(vals url.Values) *MixedInstancesPolicy {
	const prefix = "MixedInstancesPolicy"

	lt := parseLaunchTemplate(vals, prefix+".LaunchTemplate.LaunchTemplateSpecification")
	overrides := parseLaunchTemplateOverrides(vals, prefix+".LaunchTemplate.Overrides.member")
	dist, hasDist := parseInstancesDistribution(vals, prefix+".InstancesDistribution.")

	if lt == nil && len(overrides) == 0 && !hasDist {
		return nil
	}

	policy := &MixedInstancesPolicy{InstancesDistribution: dist}
	if lt != nil {
		policy.LaunchTemplate.LaunchTemplateSpecification = *lt
	}

	policy.LaunchTemplate.Overrides = overrides

	return policy
}

// parseLaunchTemplateOverrides parses the LaunchTemplate.Overrides.member.N.* form
// values within a MixedInstancesPolicy.
func parseLaunchTemplateOverrides(vals url.Values, memberPrefix string) []LaunchTemplateOverride {
	var overrides []LaunchTemplateOverride

	for i := 1; ; i++ {
		op := fmt.Sprintf("%s.%d.", memberPrefix, i)
		instanceType := vals.Get(op + "InstanceType")
		weighted := vals.Get(op + "WeightedCapacity")
		olt := parseLaunchTemplate(vals, op+"LaunchTemplateSpecification")

		if instanceType == "" && weighted == "" && olt == nil {
			break
		}

		overrides = append(overrides, LaunchTemplateOverride{
			InstanceType:                instanceType,
			WeightedCapacity:            weighted,
			LaunchTemplateSpecification: olt,
		})
	}

	return overrides
}

// parseInstancesDistribution parses the InstancesDistribution.* form values within a
// MixedInstancesPolicy. The second return value reports whether any field was set.
func parseInstancesDistribution(vals url.Values, distPrefix string) (InstancesDistribution, bool) {
	dist := InstancesDistribution{}
	hasDist := false

	if v := vals.Get(distPrefix + "OnDemandAllocationStrategy"); v != "" {
		dist.OnDemandAllocationStrategy = v
		hasDist = true
	}

	if v := vals.Get(distPrefix + "OnDemandBaseCapacity"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			dist.OnDemandBaseCapacity = n
			hasDist = true
		}
	}

	if v := vals.Get(distPrefix + "OnDemandPercentageAboveBaseCapacity"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			dist.OnDemandPercentageAboveBaseCapacity = n
			hasDist = true
		}
	}

	if v := vals.Get(distPrefix + "SpotAllocationStrategy"); v != "" {
		dist.SpotAllocationStrategy = v
		hasDist = true
	}

	if v := vals.Get(distPrefix + "SpotInstancePools"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			dist.SpotInstancePools = n
			hasDist = true
		}
	}

	if v := vals.Get(distPrefix + "SpotMaxPrice"); v != "" {
		dist.SpotMaxPrice = v
		hasDist = true
	}

	return dist, hasDist
}

// parseLifecycleHookSpecifications parses the LifecycleHookSpecificationList form
// values used by CreateAutoScalingGroup to register hooks atomically with the group.
func parseLifecycleHookSpecifications(vals url.Values) []LifecycleHook {
	var result []LifecycleHook

	const prefix = "LifecycleHookSpecificationList.member"

	for i := 1; ; i++ {
		p := fmt.Sprintf("%s.%d.", prefix, i)
		name := vals.Get(p + "LifecycleHookName")

		if name == "" {
			break
		}

		hook := LifecycleHook{
			LifecycleHookName:     name,
			LifecycleTransition:   vals.Get(p + "LifecycleTransition"),
			DefaultResult:         vals.Get(p + "DefaultResult"),
			NotificationTargetARN: vals.Get(p + "NotificationTargetARN"),
			NotificationMetadata:  vals.Get(p + "NotificationMetadata"),
			RoleARN:               vals.Get(p + "RoleARN"),
		}

		if v := vals.Get(p + "HeartbeatTimeout"); v != "" {
			if n, err := parseIntVal(v); err == nil {
				hook.HeartbeatTimeout = n
			}
		}

		result = append(result, hook)
	}

	return result
}

type setDesiredCapacityResponse struct {
	XMLName          xml.Name            `xml:"SetDesiredCapacityResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// --- New handler implementations ---

func (h *Handler) handleDescribeAccountLimits(_ url.Values) (any, error) {
	limits, err := h.Backend.DescribeAccountLimits()
	if err != nil {
		return nil, err
	}

	return &describeAccountLimitsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAccountLimitsResult{
			MaxNumberOfAutoScalingGroups:    limits.MaxNumberOfAutoScalingGroups,
			MaxNumberOfLaunchConfigurations: limits.MaxNumberOfLaunchConfigurations,
			NumberOfAutoScalingGroups:       limits.NumberOfAutoScalingGroups,
			NumberOfLaunchConfigurations:    limits.NumberOfLaunchConfigurations,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-account-limits"},
	}, nil
}

func (h *Handler) handleDescribeScalingProcessTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeScalingProcessTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlProcessType, 0, len(types))
	for _, t := range types {
		members = append(members, xmlProcessType{ProcessName: t})
	}

	return &describeScalingProcessTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeScalingProcessTypesResult{
			Processes: xmlProcessTypeList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-scaling-process-types"},
	}, nil
}

func (h *Handler) handleDescribeTerminationPolicyTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeTerminationPolicyTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(types))
	for _, t := range types {
		members = append(members, xmlStringValue{Value: t})
	}

	return &describeTerminationPolicyTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeTerminationPolicyTypesResult{
			TerminationPolicyTypes: xmlStringValueList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-termination-policy-types"},
	}, nil
}

func (h *Handler) handleSuspendProcesses(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	processes := parseMembers(vals, "ScalingProcesses.member")

	if err := h.Backend.SuspendProcesses(groupName, processes); err != nil {
		return nil, err
	}

	return &suspendProcessesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-suspend-processes"},
	}, nil
}

func (h *Handler) handleResumeProcesses(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	processes := parseMembers(vals, "ScalingProcesses.member")

	if err := h.Backend.ResumeProcesses(groupName, processes); err != nil {
		return nil, err
	}

	return &resumeProcessesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-resume-processes"},
	}, nil
}

// --- New XML response types ---

type describeAccountLimitsResult struct {
	MaxNumberOfAutoScalingGroups    int32 `xml:"MaxNumberOfAutoScalingGroups"`
	MaxNumberOfLaunchConfigurations int32 `xml:"MaxNumberOfLaunchConfigurations"`
	NumberOfAutoScalingGroups       int32 `xml:"NumberOfAutoScalingGroups"`
	NumberOfLaunchConfigurations    int32 `xml:"NumberOfLaunchConfigurations"`
}

type describeAccountLimitsResponse struct {
	XMLName          xml.Name                    `xml:"DescribeAccountLimitsResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeAccountLimitsResult `xml:"DescribeAccountLimitsResult"`
}

type xmlProcessType struct {
	ProcessName string `xml:"ProcessName"`
}

type xmlProcessTypeList struct {
	Members []xmlProcessType `xml:"member"`
}

type describeScalingProcessTypesResult struct {
	Processes xmlProcessTypeList `xml:"Processes"`
}

type describeScalingProcessTypesResponse struct {
	XMLName          xml.Name                          `xml:"DescribeScalingProcessTypesResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
	Result           describeScalingProcessTypesResult `xml:"DescribeScalingProcessTypesResult"`
}

type describeTerminationPolicyTypesResult struct {
	TerminationPolicyTypes xmlStringValueList `xml:"TerminationPolicyTypes"`
}

type describeTerminationPolicyTypesResponse struct {
	XMLName          xml.Name                             `xml:"DescribeTerminationPolicyTypesResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           describeTerminationPolicyTypesResult `xml:"DescribeTerminationPolicyTypesResult"`
}

type suspendProcessesResponse struct {
	XMLName          xml.Name            `xml:"SuspendProcessesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type resumeProcessesResponse struct {
	XMLName          xml.Name            `xml:"ResumeProcessesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
