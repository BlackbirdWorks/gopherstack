package autoscaling

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// createASGSizeFields bundles every parsed int32 size/timing field of a
// CreateAutoScalingGroup request, so handleCreateAutoScalingGroup itself only
// has to deal with one parse call and one struct instead of seven individual
// parseIntVal/error-check pairs.
type createASGSizeFields struct {
	minSize                int32
	maxSize                int32
	desiredCapacity        int32
	defaultCooldown        int32
	healthCheckGracePeriod int32
	maxInstanceLifetime    int32
	defaultInstanceWarmup  int32
}

// parseCreateASGSizeFields parses and bounds-checks every int32 size/timing
// field of a CreateAutoScalingGroup request.
func parseCreateASGSizeFields(vals url.Values) (createASGSizeFields, error) {
	var f createASGSizeFields

	intFields := []struct {
		dest  *int32
		param string
	}{
		{param: "MinSize", dest: &f.minSize},
		{param: "MaxSize", dest: &f.maxSize},
		{param: "DesiredCapacity", dest: &f.desiredCapacity},
		{param: "DefaultCooldown", dest: &f.defaultCooldown},
		{param: "HealthCheckGracePeriod", dest: &f.healthCheckGracePeriod},
		{param: "MaxInstanceLifetime", dest: &f.maxInstanceLifetime},
		{param: "DefaultInstanceWarmup", dest: &f.defaultInstanceWarmup},
	}

	for _, field := range intFields {
		n, err := parseIntVal(vals.Get(field.param))
		if err != nil {
			return f, fmt.Errorf("%w: invalid %s", ErrInvalidParameter, field.param)
		}

		*field.dest = n
	}

	// Enforce bounds on sizes to prevent excessive memory allocation when creating
	// the initial instances slice in the backend.
	if f.minSize < 0 || f.maxSize < 0 || f.desiredCapacity < 0 {
		return f, fmt.Errorf("%w: sizes must be non-negative", ErrInvalidParameter)
	}

	if f.minSize > maxDesiredCapacity || f.maxSize > maxDesiredCapacity || f.desiredCapacity > maxDesiredCapacity {
		return f, fmt.Errorf("%w: sizes must not exceed %d", ErrInvalidParameter, maxDesiredCapacity)
	}

	return f, nil
}

func (h *Handler) handleCreateAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")
	lcName := vals.Get("LaunchConfigurationName")
	healthCheckType := vals.Get("HealthCheckType")

	sizes, err := parseCreateASGSizeFields(vals)
	if err != nil {
		return nil, err
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
		MinSize:                          sizes.minSize,
		MaxSize:                          sizes.maxSize,
		DesiredCapacity:                  sizes.desiredCapacity,
		DefaultCooldown:                  sizes.defaultCooldown,
		HealthCheckType:                  healthCheckType,
		HealthCheckGracePeriod:           sizes.healthCheckGracePeriod,
		MaxInstanceLifetime:              sizes.maxInstanceLifetime,
		DefaultInstanceWarmup:            sizes.defaultInstanceWarmup,
		NewInstancesProtectedFromScaleIn: vals.Get("NewInstancesProtectedFromScaleIn") == formValueTrue,
		CapacityRebalance:                vals.Get("CapacityRebalance") == formValueTrue,
		AvailabilityZones:                azs,
		LoadBalancerNames:                lbNames,
		TargetGroupARNs:                  targetGroupARNs,
		TrafficSources:                   trafficSources,
		Tags:                             tags,
		TerminationPolicies:              terminationPolicies,
		LifecycleHookSpecificationList:   hooks,
		AvailabilityZoneDistribution:     parseAvailabilityZoneDistribution(vals),
		AvailabilityZoneImpairmentPolicy: parseAvailabilityZoneImpairmentPolicy(vals),
		CapacityReservationSpecification: parseCapacityReservationSpecification(vals),
		DeletionProtection:               vals.Get("DeletionProtection"),
		InstanceLifecyclePolicy:          parseInstanceLifecyclePolicy(vals),
		InstanceMaintenancePolicy:        parseInstanceMaintenancePolicy(vals),
		SkipZonalShiftValidation:         vals.Get("SkipZonalShiftValidation") == formValueTrue,
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
	filters := parseTagFilters(vals)

	groups, err := h.Backend.DescribeAutoScalingGroups(names, filters)
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

func (h *Handler) handleUpdateAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")

	input := UpdateAutoScalingGroupInput{
		AutoScalingGroupName:             name,
		LaunchConfigurationName:          vals.Get("LaunchConfigurationName"),
		HealthCheckType:                  vals.Get("HealthCheckType"),
		VPCZoneIdentifier:                vals.Get("VPCZoneIdentifier"),
		PlacementGroup:                   formStringOrNil(vals, "PlacementGroup"),
		Context:                          vals.Get("Context"),
		DesiredCapacityType:              vals.Get("DesiredCapacityType"),
		DeletionProtection:               vals.Get("DeletionProtection"),
		AvailabilityZones:                parseMembers(vals, "AvailabilityZones.member"),
		TerminationPolicies:              parseMembers(vals, "TerminationPolicies.member"),
		LaunchTemplate:                   parseLaunchTemplate(vals, "LaunchTemplate"),
		MixedInstancesPolicy:             parseMixedInstancesPolicy(vals),
		AvailabilityZoneDistribution:     parseAvailabilityZoneDistribution(vals),
		AvailabilityZoneImpairmentPolicy: parseAvailabilityZoneImpairmentPolicy(vals),
		CapacityReservationSpecification: parseCapacityReservationSpecification(vals),
		InstanceLifecyclePolicy:          parseInstanceLifecyclePolicy(vals),
		InstanceMaintenancePolicy:        parseInstanceMaintenancePolicy(vals),
	}

	if err := applyUpdateASGSizeFields(vals, &input); err != nil {
		return nil, err
	}

	applyUpdateASGBoolFields(vals, &input)

	_, updateErr := h.Backend.UpdateAutoScalingGroup(input)
	if updateErr != nil {
		return nil, updateErr
	}

	return &updateAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-update-" + name},
	}, nil
}

// formStringOrNil distinguishes an omitted form value (nil, "unchanged") from
// one explicitly sent empty (pointer to "", a real clear) -- vals.Get alone
// returns "" for both cases.
func formStringOrNil(vals url.Values, param string) *string {
	if !vals.Has(param) {
		return nil
	}

	v := vals.Get(param)

	return &v
}

// updateASGIntField binds a single optional int32 form value (by AWS param name)
// to a *int32 destination on an UpdateAutoScalingGroupInput, returning a
// ValidationError wrapping the param name on a parse failure. A blank form value
// leaves dest untouched (nil), matching AWS's "omitted means unchanged" semantics.
func updateASGIntField(vals url.Values, param string, dest **int32) error {
	v := vals.Get(param)
	if v == "" {
		return nil
	}

	n, err := parseIntVal(v)
	if err != nil {
		return fmt.Errorf("%w: invalid %s", ErrInvalidParameter, param)
	}

	*dest = &n

	return nil
}

// applyUpdateASGSizeFields parses every optional int32 field of
// UpdateAutoScalingGroupInput, stopping at the first invalid value.
func applyUpdateASGSizeFields(vals url.Values, input *UpdateAutoScalingGroupInput) error {
	fields := []struct {
		dest  **int32
		param string
	}{
		{param: "MinSize", dest: &input.MinSize},
		{param: "MaxSize", dest: &input.MaxSize},
		{param: "DesiredCapacity", dest: &input.DesiredCapacity},
		{param: "DefaultCooldown", dest: &input.DefaultCooldown},
		{param: "HealthCheckGracePeriod", dest: &input.HealthCheckGracePeriod},
		{param: "MaxInstanceLifetime", dest: &input.MaxInstanceLifetime},
		{param: "DefaultInstanceWarmup", dest: &input.DefaultInstanceWarmup},
	}

	for _, f := range fields {
		if err := updateASGIntField(vals, f.param, f.dest); err != nil {
			return err
		}
	}

	return nil
}

// applyUpdateASGBoolFields parses every optional bool field of
// UpdateAutoScalingGroupInput. A blank form value leaves the destination
// pointer nil (unchanged); a present value is always parsed (AWS booleans in
// the query protocol are never invalid, just true/anything-else).
func applyUpdateASGBoolFields(vals url.Values, input *UpdateAutoScalingGroupInput) {
	fields := []struct {
		dest  **bool
		param string
	}{
		{param: "NewInstancesProtectedFromScaleIn", dest: &input.NewInstancesProtectedFromScaleIn},
		{param: "CapacityRebalance", dest: &input.CapacityRebalance},
		{param: "SkipZonalShiftValidation", dest: &input.SkipZonalShiftValidation},
	}

	for _, f := range fields {
		if v := vals.Get(f.param); v != "" {
			b := v == formValueTrue
			*f.dest = &b
		}
	}
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

// xmlGroupLists bundles every list-shaped projection of an AutoScalingGroup so
// toXMLGroup can assemble the final response without ballooning past a
// reasonable function length.
type xmlGroupLists struct {
	AvailabilityZones   xmlStringValueList
	LoadBalancerNames   xmlStringValueList
	TargetGroupARNs     xmlStringValueList
	TrafficSources      xmlTrafficSourceList
	Tags                xmlTagList
	Instances           xmlInstanceList
	SuspendedProcesses  xmlSuspendedProcessList
	TerminationPolicies xmlTerminationPoliciesList
	EnabledMetrics      xmlEnabledMetricList
}

// buildXMLGroupLists projects every slice-valued field of g into its XML
// response shape.
func buildXMLGroupLists(g *AutoScalingGroup) xmlGroupLists {
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

	return xmlGroupLists{
		AvailabilityZones:   xmlStringValueList{Members: azs},
		LoadBalancerNames:   xmlStringValueList{Members: lbNames},
		TargetGroupARNs:     xmlStringValueList{Members: tgARNs},
		TrafficSources:      xmlTrafficSourceList{Members: trafficSources},
		Tags:                xmlTagList{Members: tags},
		Instances:           xmlInstanceList{Members: instances},
		SuspendedProcesses:  xmlSuspendedProcessList{Members: suspendedProcesses},
		TerminationPolicies: xmlTerminationPoliciesList{Members: terminationPolicies},
		EnabledMetrics:      xmlEnabledMetricList{Members: enabledMetrics},
	}
}

// toXMLGroup converts an AutoScalingGroup to the XML response type.
func toXMLGroup(g *AutoScalingGroup) xmlAutoScalingGroup {
	lists := buildXMLGroupLists(g)

	var xmlLT *xmlLaunchTemplateSpecification
	if g.LaunchTemplate != nil {
		xmlLT = &xmlLaunchTemplateSpecification{
			LaunchTemplateID:   g.LaunchTemplate.LaunchTemplateID,
			LaunchTemplateName: g.LaunchTemplate.LaunchTemplateName,
			Version:            g.LaunchTemplate.Version,
		}
	}

	return xmlAutoScalingGroup{
		AutoScalingGroupName:             g.AutoScalingGroupName,
		AutoScalingGroupARN:              g.AutoScalingGroupARN,
		LaunchConfigurationName:          g.LaunchConfigurationName,
		LaunchTemplate:                   xmlLT,
		MixedInstancesPolicy:             toXMLMixedInstancesPolicy(g.MixedInstancesPolicy),
		VPCZoneIdentifier:                g.VPCZoneIdentifier,
		PlacementGroup:                   g.PlacementGroup,
		Context:                          g.Context,
		DesiredCapacityType:              g.DesiredCapacityType,
		DeletionProtection:               g.DeletionProtection,
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
		AvailabilityZones:                lists.AvailabilityZones,
		LoadBalancerNames:                lists.LoadBalancerNames,
		TargetGroupARNs:                  lists.TargetGroupARNs,
		TrafficSources:                   lists.TrafficSources,
		Tags:                             lists.Tags,
		Instances:                        lists.Instances,
		SuspendedProcesses:               lists.SuspendedProcesses,
		TerminationPolicies:              lists.TerminationPolicies,
		EnabledMetrics:                   lists.EnabledMetrics,
		AvailabilityZoneDistribution:     toXMLAvailabilityZoneDistribution(g.AvailabilityZoneDistribution),
		AvailabilityZoneImpairmentPolicy: toXMLAvailabilityZoneImpairmentPolicy(g.AvailabilityZoneImpairmentPolicy),
		CapacityReservationSpecification: toXMLCapacityReservationSpecification(g.CapacityReservationSpecification),
		InstanceLifecyclePolicy:          toXMLInstanceLifecyclePolicy(g.InstanceLifecyclePolicy),
		InstanceMaintenancePolicy:        toXMLInstanceMaintenancePolicy(g.InstanceMaintenancePolicy),
		ServiceLinkedRoleARN: fmt.Sprintf(
			"arn:aws:iam::%s:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
			config.DefaultAccountID,
		),
	}
}

// toXMLAvailabilityZoneDistribution converts an AvailabilityZoneDistribution to
// its XML projection, or nil when policy is nil.
func toXMLAvailabilityZoneDistribution(v *AvailabilityZoneDistribution) *xmlAvailabilityZoneDistribution {
	if v == nil {
		return nil
	}

	return &xmlAvailabilityZoneDistribution{CapacityDistributionStrategy: v.CapacityDistributionStrategy}
}

// toXMLAvailabilityZoneImpairmentPolicy converts an AvailabilityZoneImpairmentPolicy
// to its XML projection, or nil when policy is nil.
func toXMLAvailabilityZoneImpairmentPolicy(v *AvailabilityZoneImpairmentPolicy) *xmlAZImpairmentPolicy {
	if v == nil {
		return nil
	}

	return &xmlAZImpairmentPolicy{
		ImpairedZoneHealthCheckBehavior: v.ImpairedZoneHealthCheckBehavior,
		ZonalShiftEnabled:               v.ZonalShiftEnabled,
	}
}

// toXMLCapacityReservationSpecification converts a CapacityReservationSpecification
// to its XML projection, or nil when spec is nil.
func toXMLCapacityReservationSpecification(spec *CapacityReservationSpecification) *xmlCapacityReservationSpec {
	if spec == nil {
		return nil
	}

	out := &xmlCapacityReservationSpec{CapacityReservationPreference: spec.CapacityReservationPreference}

	if t := spec.CapacityReservationTarget; t != nil {
		ids := make([]xmlStringValue, 0, len(t.CapacityReservationIDs))
		for _, id := range t.CapacityReservationIDs {
			ids = append(ids, xmlStringValue{Value: id})
		}

		arns := make([]xmlStringValue, 0, len(t.CapacityReservationResourceGroupARNs))
		for _, arn := range t.CapacityReservationResourceGroupARNs {
			arns = append(arns, xmlStringValue{Value: arn})
		}

		out.CapacityReservationTarget = &xmlCapacityReservationTarget{
			CapacityReservationIDs:               xmlStringValueList{Members: ids},
			CapacityReservationResourceGroupArns: xmlStringValueList{Members: arns},
		}
	}

	return out
}

// toXMLInstanceLifecyclePolicy converts an InstanceLifecyclePolicy to its XML
// projection, or nil when policy is nil.
func toXMLInstanceLifecyclePolicy(policy *InstanceLifecyclePolicy) *xmlInstanceLifecyclePolicy {
	if policy == nil {
		return nil
	}

	out := &xmlInstanceLifecyclePolicy{}
	if policy.RetentionTriggers != nil {
		out.RetentionTriggers = &xmlRetentionTriggers{
			TerminateHookAbandon: policy.RetentionTriggers.TerminateHookAbandon,
		}
	}

	return out
}

// toXMLInstanceMaintenancePolicy converts an InstanceMaintenancePolicy to its
// XML projection, or nil when policy is nil.
func toXMLInstanceMaintenancePolicy(policy *InstanceMaintenancePolicy) *xmlInstanceMaintenancePolicy {
	if policy == nil {
		return nil
	}

	return &xmlInstanceMaintenancePolicy{
		MinHealthyPercentage: policy.MinHealthyPercentage,
		MaxHealthyPercentage: policy.MaxHealthyPercentage,
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

		xo.InstanceRequirements = toXMLInstanceRequirements(o.InstanceRequirements)

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

// toXMLInstanceRequirements converts an InstanceRequirements to its XML
// response projection, or nil when ir is nil (matches AWS: the element is
// entirely absent for overrides that select by InstanceType instead).
func toXMLInstanceRequirements(ir *InstanceRequirements) *xmlInstanceRequirements {
	if ir == nil {
		return nil
	}

	return &xmlInstanceRequirements{
		MemoryMiB:                 toXMLIntRangeRequest(ir.MemoryMiB),
		VCpuCount:                 toXMLIntRangeRequest(ir.VCpuCount),
		AcceleratorCount:          toXMLIntRangeRequest(ir.AcceleratorCount),
		AcceleratorTotalMemoryMiB: toXMLIntRangeRequest(ir.AcceleratorTotalMemoryMiB),
		BaselineEbsBandwidthMbps:  toXMLIntRangeRequest(ir.BaselineEbsBandwidthMbps),
		NetworkInterfaceCount:     toXMLIntRangeRequest(ir.NetworkInterfaceCount),
		MemoryGiBPerVCpu:          toXMLFloatRangeRequest(ir.MemoryGiBPerVCpu),
		NetworkBandwidthGbps:      toXMLFloatRangeRequest(ir.NetworkBandwidthGbps),
		TotalLocalStorageGB:       toXMLFloatRangeRequest(ir.TotalLocalStorageGB),
		MaxSpotPriceAsPercentageOfOptimalOnDemandPrice: ir.MaxSpotPriceAsPercentageOfOptimalOnDemandPrice,
		OnDemandMaxPricePercentageOverLowestPrice:      ir.OnDemandMaxPricePercentageOverLowestPrice,
		SpotMaxPricePercentageOverLowestPrice:          ir.SpotMaxPricePercentageOverLowestPrice,
		RequireHibernateSupport:                        ir.RequireHibernateSupport,
		AcceleratorManufacturers:                       toXMLStringValueList(ir.AcceleratorManufacturers),
		AcceleratorNames:                               toXMLStringValueList(ir.AcceleratorNames),
		AcceleratorTypes:                               toXMLStringValueList(ir.AcceleratorTypes),
		AllowedInstanceTypes:                           toXMLStringValueList(ir.AllowedInstanceTypes),
		CPUManufacturers:                               toXMLStringValueList(ir.CPUManufacturers),
		ExcludedInstanceTypes:                          toXMLStringValueList(ir.ExcludedInstanceTypes),
		InstanceGenerations:                            toXMLStringValueList(ir.InstanceGenerations),
		LocalStorageTypes:                              toXMLStringValueList(ir.LocalStorageTypes),
		BareMetal:                                      ir.BareMetal,
		BurstablePerformance:                           ir.BurstablePerformance,
		LocalStorage:                                   ir.LocalStorage,
		BaselinePerformanceFactors:                     toXMLBaselinePerformanceFactors(ir.BaselinePerformanceFactors),
	}
}

// toXMLPerformanceFactorReferenceList converts a []PerformanceFactorReference
// to its XML response shape. The wrapper element is "item", not "member" --
// see parsePerformanceFactorReferences for the citation.
func toXMLPerformanceFactorReferenceList(refs []PerformanceFactorReference) *xmlPerformanceFactorReferenceList {
	if len(refs) == 0 {
		return nil
	}

	members := make([]xmlPerformanceFactorReference, 0, len(refs))
	for _, r := range refs {
		members = append(members, xmlPerformanceFactorReference(r))
	}

	return &xmlPerformanceFactorReferenceList{Items: members}
}

func toXMLBaselinePerformanceFactors(bpf *BaselinePerformanceFactors) *xmlBaselinePerformanceFactors {
	if bpf == nil || bpf.CPU == nil {
		return nil
	}

	refs := toXMLPerformanceFactorReferenceList(bpf.CPU.References)
	if refs == nil {
		return nil
	}

	return &xmlBaselinePerformanceFactors{CPU: &xmlCPUPerformanceFactor{Reference: *refs}}
}

func toXMLIntRangeRequest(r *IntRangeRequest) *xmlIntRangeRequest {
	if r == nil {
		return nil
	}

	return &xmlIntRangeRequest{Min: r.Min, Max: r.Max}
}

func toXMLFloatRangeRequest(r *FloatRangeRequest) *xmlFloatRangeRequest {
	if r == nil {
		return nil
	}

	return &xmlFloatRangeRequest{Min: r.Min, Max: r.Max}
}

// toXMLStringValueList converts a []string to the shared <member> list wire
// shape used throughout this handler for query-protocol string lists.
func toXMLStringValueList(vs []string) xmlStringValueList {
	members := make([]xmlStringValue, 0, len(vs))
	for _, v := range vs {
		members = append(members, xmlStringValue{Value: v})
	}

	return xmlStringValueList{Members: members}
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
	InstanceRequirements        *xmlInstanceRequirements        `xml:"InstanceRequirements,omitempty"`
	InstanceType                string                          `xml:"InstanceType,omitempty"`
	WeightedCapacity            string                          `xml:"WeightedCapacity,omitempty"`
}

type xmlIntRangeRequest struct {
	Min *int32 `xml:"Min,omitempty"`
	Max *int32 `xml:"Max,omitempty"`
}

type xmlFloatRangeRequest struct {
	Min *float64 `xml:"Min,omitempty"`
	Max *float64 `xml:"Max,omitempty"`
}

// xmlInstanceRequirements is the XML response projection of InstanceRequirements
// (models.go), matching DescribeAutoScalingGroups' LaunchTemplateOverrides.
// InstanceRequirements wire shape (deserializers.go:12592).
type xmlInstanceRequirements struct {
	RequireHibernateSupport    *bool                          `xml:"RequireHibernateSupport,omitempty"`
	MemoryGiBPerVCpu           *xmlFloatRangeRequest          `xml:"MemoryGiBPerVCpu,omitempty"`
	AcceleratorCount           *xmlIntRangeRequest            `xml:"AcceleratorCount,omitempty"`
	AcceleratorTotalMemoryMiB  *xmlIntRangeRequest            `xml:"AcceleratorTotalMemoryMiB,omitempty"`
	BaselineEbsBandwidthMbps   *xmlIntRangeRequest            `xml:"BaselineEbsBandwidthMbps,omitempty"`
	NetworkInterfaceCount      *xmlIntRangeRequest            `xml:"NetworkInterfaceCount,omitempty"`
	BaselinePerformanceFactors *xmlBaselinePerformanceFactors `xml:"BaselinePerformanceFactors,omitempty"`
	// omitempty is redundant on *int32 (encoding/xml always skips a nil
	// pointer field); dropped from the three tags below to fit the lll limit.
	SpotMaxPricePercentageOverLowestPrice *int32                `xml:"SpotMaxPricePercentageOverLowestPrice"`
	NetworkBandwidthGbps                  *xmlFloatRangeRequest `xml:"NetworkBandwidthGbps,omitempty"`
	TotalLocalStorageGB                   *xmlFloatRangeRequest `xml:"TotalLocalStorageGB,omitempty"`

	MaxSpotPriceAsPercentageOfOptimalOnDemandPrice *int32 `xml:"MaxSpotPriceAsPercentageOfOptimalOnDemandPrice"`

	VCpuCount                                 *xmlIntRangeRequest `xml:"VCpuCount,omitempty"`
	OnDemandMaxPricePercentageOverLowestPrice *int32              `xml:"OnDemandMaxPricePercentageOverLowestPrice"`
	MemoryMiB                                 *xmlIntRangeRequest `xml:"MemoryMiB,omitempty"`
	BareMetal                                 string              `xml:"BareMetal,omitempty"`
	BurstablePerformance                      string              `xml:"BurstablePerformance,omitempty"`
	LocalStorage                              string              `xml:"LocalStorage,omitempty"`
	AcceleratorManufacturers                  xmlStringValueList  `xml:"AcceleratorManufacturers,omitempty"`
	AcceleratorNames                          xmlStringValueList  `xml:"AcceleratorNames,omitempty"`
	AcceleratorTypes                          xmlStringValueList  `xml:"AcceleratorTypes,omitempty"`
	AllowedInstanceTypes                      xmlStringValueList  `xml:"AllowedInstanceTypes,omitempty"`
	CPUManufacturers                          xmlStringValueList  `xml:"CpuManufacturers,omitempty"`
	ExcludedInstanceTypes                     xmlStringValueList  `xml:"ExcludedInstanceTypes,omitempty"`
	InstanceGenerations                       xmlStringValueList  `xml:"InstanceGenerations,omitempty"`
	LocalStorageTypes                         xmlStringValueList  `xml:"LocalStorageTypes,omitempty"`
}

// xmlPerformanceFactorReference is the XML response projection of
// PerformanceFactorReference, matching AWS types.PerformanceFactorReferenceRequest
// (deserializers.go:15954).
type xmlPerformanceFactorReference struct {
	InstanceFamily string `xml:"InstanceFamily,omitempty"`
}

// xmlPerformanceFactorReferenceList wraps its members in "item", not
// "member" -- see parsePerformanceFactorReferences for the citation
// (deserializers.go:16003).
type xmlPerformanceFactorReferenceList struct {
	Items []xmlPerformanceFactorReference `xml:"item"`
}

// xmlCPUPerformanceFactor is the XML response projection of
// CPUPerformanceFactor, matching AWS types.CpuPerformanceFactorRequest
// (deserializers.go:10587). The field is named "Reference" (singular) on the
// wire, not "References".
type xmlCPUPerformanceFactor struct {
	Reference xmlPerformanceFactorReferenceList `xml:"Reference"`
}

// xmlBaselinePerformanceFactors is the XML response projection of
// BaselinePerformanceFactors, matching AWS types.BaselinePerformanceFactorsRequest
// (deserializers.go:9834).
type xmlBaselinePerformanceFactors struct {
	CPU *xmlCPUPerformanceFactor `xml:"Cpu,omitempty"`
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

type xmlAvailabilityZoneDistribution struct {
	CapacityDistributionStrategy string `xml:"CapacityDistributionStrategy,omitempty"`
}

type xmlAZImpairmentPolicy struct {
	ImpairedZoneHealthCheckBehavior string `xml:"ImpairedZoneHealthCheckBehavior,omitempty"`
	ZonalShiftEnabled               bool   `xml:"ZonalShiftEnabled,omitempty"`
}

type xmlCapacityReservationTarget struct {
	CapacityReservationIDs               xmlStringValueList `xml:"CapacityReservationIds,omitempty"`
	CapacityReservationResourceGroupArns xmlStringValueList `xml:"CapacityReservationResourceGroupArns,omitempty"`
}

type xmlCapacityReservationSpec struct {
	CapacityReservationTarget     *xmlCapacityReservationTarget `xml:"CapacityReservationTarget,omitempty"`
	CapacityReservationPreference string                        `xml:"CapacityReservationPreference,omitempty"`
}

type xmlRetentionTriggers struct {
	TerminateHookAbandon string `xml:"TerminateHookAbandon,omitempty"`
}

type xmlInstanceLifecyclePolicy struct {
	RetentionTriggers *xmlRetentionTriggers `xml:"RetentionTriggers,omitempty"`
}

type xmlInstanceMaintenancePolicy struct {
	MinHealthyPercentage *int32 `xml:"MinHealthyPercentage,omitempty"`
	MaxHealthyPercentage *int32 `xml:"MaxHealthyPercentage,omitempty"`
}

type xmlAutoScalingGroup struct {
	LaunchTemplate                   *xmlLaunchTemplateSpecification  `xml:"LaunchTemplate,omitempty"`
	MixedInstancesPolicy             *xmlMixedInstancesPolicy         `xml:"MixedInstancesPolicy,omitempty"`
	AvailabilityZoneDistribution     *xmlAvailabilityZoneDistribution `xml:"AvailabilityZoneDistribution,omitempty"`
	AvailabilityZoneImpairmentPolicy *xmlAZImpairmentPolicy           `xml:"AvailabilityZoneImpairmentPolicy,omitempty"`
	CapacityReservationSpecification *xmlCapacityReservationSpec      `xml:"CapacityReservationSpecification,omitempty"`
	InstanceLifecyclePolicy          *xmlInstanceLifecyclePolicy      `xml:"InstanceLifecyclePolicy,omitempty"`
	InstanceMaintenancePolicy        *xmlInstanceMaintenancePolicy    `xml:"InstanceMaintenancePolicy,omitempty"`
	AutoScalingGroupARN              string                           `xml:"AutoScalingGroupARN"`
	Status                           string                           `xml:"Status,omitempty"`
	CreatedTime                      string                           `xml:"CreatedTime"`
	HealthCheckType                  string                           `xml:"HealthCheckType"`
	LaunchConfigurationName          string                           `xml:"LaunchConfigurationName,omitempty"`
	AutoScalingGroupName             string                           `xml:"AutoScalingGroupName"`
	VPCZoneIdentifier                string                           `xml:"VPCZoneIdentifier,omitempty"`
	PlacementGroup                   string                           `xml:"PlacementGroup,omitempty"`
	Context                          string                           `xml:"Context,omitempty"`
	DesiredCapacityType              string                           `xml:"DesiredCapacityType,omitempty"`
	DeletionProtection               string                           `xml:"DeletionProtection,omitempty"`
	ServiceLinkedRoleARN             string                           `xml:"ServiceLinkedRoleARN,omitempty"`
	TargetGroupARNs                  xmlStringValueList               `xml:"TargetGroupARNs"`
	Tags                             xmlTagList                       `xml:"Tags"`
	AvailabilityZones                xmlStringValueList               `xml:"AvailabilityZones"`
	LoadBalancerNames                xmlStringValueList               `xml:"LoadBalancerNames"`
	TrafficSources                   xmlTrafficSourceList             `xml:"TrafficSources"`
	SuspendedProcesses               xmlSuspendedProcessList          `xml:"SuspendedProcesses"`
	TerminationPolicies              xmlTerminationPoliciesList       `xml:"TerminationPolicies"`
	EnabledMetrics                   xmlEnabledMetricList             `xml:"EnabledMetrics"`
	Instances                        xmlInstanceList                  `xml:"Instances"`
	MaxSize                          int32                            `xml:"MaxSize"`
	DesiredCapacity                  int32                            `xml:"DesiredCapacity"`
	DefaultCooldown                  int32                            `xml:"DefaultCooldown"`
	HealthCheckGracePeriod           int32                            `xml:"HealthCheckGracePeriod"`
	MaxInstanceLifetime              int32                            `xml:"MaxInstanceLifetime,omitempty"`
	DefaultInstanceWarmup            int32                            `xml:"DefaultInstanceWarmup,omitempty"`
	MinSize                          int32                            `xml:"MinSize"`
	NewInstancesProtectedFromScaleIn bool                             `xml:"NewInstancesProtectedFromScaleIn,omitempty"`
	CapacityRebalance                bool                             `xml:"CapacityRebalance,omitempty"`
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
// values within a MixedInstancesPolicy. An override commonly carries
// InstanceRequirements with no InstanceType at all (attribute-based instance
// type selection), so InstanceRequirements presence must also gate the
// loop-continuation check below -- otherwise such an override is silently
// dropped as if the member list had ended.
func parseLaunchTemplateOverrides(vals url.Values, memberPrefix string) []LaunchTemplateOverride {
	var overrides []LaunchTemplateOverride

	for i := 1; ; i++ {
		op := fmt.Sprintf("%s.%d.", memberPrefix, i)
		instanceType := vals.Get(op + "InstanceType")
		weighted := vals.Get(op + "WeightedCapacity")
		olt := parseLaunchTemplate(vals, op+"LaunchTemplateSpecification")
		instanceReq := parseInstanceRequirements(vals, op+"InstanceRequirements.")

		if instanceType == "" && weighted == "" && olt == nil && instanceReq == nil {
			break
		}

		overrides = append(overrides, LaunchTemplateOverride{
			InstanceType:                instanceType,
			WeightedCapacity:            weighted,
			LaunchTemplateSpecification: olt,
			InstanceRequirements:        instanceReq,
		})
	}

	return overrides
}

// parseIntRangeRequest parses the {Min,Max} shape shared by several
// InstanceRequirements range sub-fields (e.g. types.VCpuCountRequest,
// types.MemoryMiBRequest; types.go various). Returns nil if neither is set.
func parseIntRangeRequest(vals url.Values, prefix string) *IntRangeRequest {
	minStr := vals.Get(prefix + "Min")
	maxStr := vals.Get(prefix + "Max")

	if minStr == "" && maxStr == "" {
		return nil
	}

	r := &IntRangeRequest{}

	if minStr != "" {
		if n, err := parseIntVal(minStr); err == nil {
			r.Min = &n
		}
	}

	if maxStr != "" {
		if n, err := parseIntVal(maxStr); err == nil {
			r.Max = &n
		}
	}

	return r
}

// parseFloatRangeRequest parses the {Min,Max} shape shared by
// types.MemoryGiBPerVCpuRequest, NetworkBandwidthGbpsRequest, and
// TotalLocalStorageGBRequest. Returns nil if neither is set.
func parseFloatRangeRequest(vals url.Values, prefix string) *FloatRangeRequest {
	minStr := vals.Get(prefix + "Min")
	maxStr := vals.Get(prefix + "Max")

	if minStr == "" && maxStr == "" {
		return nil
	}

	r := &FloatRangeRequest{}

	if minStr != "" {
		if f, err := strconv.ParseFloat(minStr, 64); err == nil {
			r.Min = &f
		}
	}

	if maxStr != "" {
		if f, err := strconv.ParseFloat(maxStr, 64); err == nil {
			r.Max = &f
		}
	}

	return r
}

// parseInstanceRequirements parses InstanceRequirements.* form values (see
// models.go InstanceRequirements for the field mapping to
// types.InstanceRequirements, aws-sdk-go-v2/service/autoscaling/types/
// types.go:1267). Returns nil if nothing was specified.
func parseInstanceRequirements(vals url.Values, prefix string) *InstanceRequirements {
	ir := &InstanceRequirements{}

	hasRanges := parseInstanceRequirementRanges(vals, prefix, ir)
	hasLists := parseInstanceRequirementLists(vals, prefix, ir)
	hasScalars := parseInstanceRequirementScalars(vals, prefix, ir)

	if !hasRanges && !hasLists && !hasScalars {
		return nil
	}

	return ir
}

// parseInstanceRequirementRanges parses the six *IntRangeRequest and three
// *FloatRangeRequest {Min,Max} sub-fields of InstanceRequirements into ir,
// returning whether any was set.
func parseInstanceRequirementRanges(vals url.Values, prefix string, ir *InstanceRequirements) bool {
	hasAny := false

	intRanges := []struct {
		dest **IntRangeRequest
		sub  string
	}{
		{&ir.MemoryMiB, "MemoryMiB"},
		{&ir.VCpuCount, "VCpuCount"},
		{&ir.AcceleratorCount, "AcceleratorCount"},
		{&ir.AcceleratorTotalMemoryMiB, "AcceleratorTotalMemoryMiB"},
		{&ir.BaselineEbsBandwidthMbps, "BaselineEbsBandwidthMbps"},
		{&ir.NetworkInterfaceCount, "NetworkInterfaceCount"},
	}
	for _, f := range intRanges {
		if r := parseIntRangeRequest(vals, prefix+f.sub+"."); r != nil {
			*f.dest = r
			hasAny = true
		}
	}

	floatRanges := []struct {
		dest **FloatRangeRequest
		sub  string
	}{
		{&ir.MemoryGiBPerVCpu, "MemoryGiBPerVCpu"},
		{&ir.NetworkBandwidthGbps, "NetworkBandwidthGbps"},
		{&ir.TotalLocalStorageGB, "TotalLocalStorageGB"},
	}
	for _, f := range floatRanges {
		if r := parseFloatRangeRequest(vals, prefix+f.sub+"."); r != nil {
			*f.dest = r
			hasAny = true
		}
	}

	return hasAny
}

// parseInstanceRequirementLists parses the eight .member.N-flattened string
// list sub-fields of InstanceRequirements into ir, returning whether any was
// set.
func parseInstanceRequirementLists(vals url.Values, prefix string, ir *InstanceRequirements) bool {
	hasAny := false

	stringLists := []struct {
		dest *[]string
		sub  string
	}{
		{&ir.AcceleratorManufacturers, "AcceleratorManufacturers"},
		{&ir.AcceleratorNames, "AcceleratorNames"},
		{&ir.AcceleratorTypes, "AcceleratorTypes"},
		{&ir.AllowedInstanceTypes, "AllowedInstanceTypes"},
		{&ir.CPUManufacturers, "CpuManufacturers"},
		{&ir.ExcludedInstanceTypes, "ExcludedInstanceTypes"},
		{&ir.InstanceGenerations, "InstanceGenerations"},
		{&ir.LocalStorageTypes, "LocalStorageTypes"},
	}
	for _, f := range stringLists {
		if members := parseMembers(vals, prefix+f.sub+".member"); len(members) > 0 {
			*f.dest = members
			hasAny = true
		}
	}

	return hasAny
}

// parseInstanceRequirementScalars parses the remaining plain-string,
// *int32, *bool, and BaselinePerformanceFactors sub-fields of
// InstanceRequirements into ir, returning whether any was set.
func parseInstanceRequirementScalars(vals url.Values, prefix string, ir *InstanceRequirements) bool {
	hasAny := false

	strFields := []struct {
		dest *string
		sub  string
	}{
		{&ir.BareMetal, "BareMetal"},
		{&ir.BurstablePerformance, "BurstablePerformance"},
		{&ir.LocalStorage, "LocalStorage"},
	}
	for _, f := range strFields {
		if v := vals.Get(prefix + f.sub); v != "" {
			*f.dest = v
			hasAny = true
		}
	}

	intFields := []struct {
		dest **int32
		sub  string
	}{
		{&ir.MaxSpotPriceAsPercentageOfOptimalOnDemandPrice, "MaxSpotPriceAsPercentageOfOptimalOnDemandPrice"},
		{&ir.OnDemandMaxPricePercentageOverLowestPrice, "OnDemandMaxPricePercentageOverLowestPrice"},
		{&ir.SpotMaxPricePercentageOverLowestPrice, "SpotMaxPricePercentageOverLowestPrice"},
	}
	for _, f := range intFields {
		if v := vals.Get(prefix + f.sub); v != "" {
			if n, err := parseIntVal(v); err == nil {
				*f.dest = &n
				hasAny = true
			}
		}
	}

	if v := vals.Get(prefix + "RequireHibernateSupport"); v != "" {
		b := v == formValueTrue
		ir.RequireHibernateSupport = &b
		hasAny = true
	}

	if bpf := parseBaselinePerformanceFactors(vals, prefix+"BaselinePerformanceFactors."); bpf != nil {
		ir.BaselinePerformanceFactors = bpf
		hasAny = true
	}

	return hasAny
}

// parsePerformanceFactorReferences parses a
// BaselinePerformanceFactors.Cpu.Reference.item.N.InstanceFamily list.
// Verified against serializers.go:5918
// (awsAwsquery_serializeDocumentPerformanceFactorReferenceSetRequest): unlike
// every other list in this handler, the wrapper element is "item", not
// "member" -- and the field itself is serialized under the singular key
// "Reference", not "References" (serializers.go:4971).
func parsePerformanceFactorReferences(vals url.Values, prefix string) []PerformanceFactorReference {
	var refs []PerformanceFactorReference

	for i := 1; ; i++ {
		key := fmt.Sprintf("%sitem.%d.InstanceFamily", prefix, i)

		v := vals.Get(key)
		if v == "" {
			break
		}

		refs = append(refs, PerformanceFactorReference{InstanceFamily: v})
	}

	return refs
}

// parseBaselinePerformanceFactors parses InstanceRequirements.
// BaselinePerformanceFactors.* form values (types.BaselinePerformanceFactorsRequest,
// serializers.go:4826). Returns nil if nothing was specified.
func parseBaselinePerformanceFactors(vals url.Values, prefix string) *BaselinePerformanceFactors {
	refs := parsePerformanceFactorReferences(vals, prefix+"Cpu.Reference.")
	if len(refs) == 0 {
		return nil
	}

	return &BaselinePerformanceFactors{CPU: &CPUPerformanceFactor{References: refs}}
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

// parseAvailabilityZoneDistribution parses AvailabilityZoneDistribution.* form
// values. Returns nil if not specified.
func parseAvailabilityZoneDistribution(vals url.Values) *AvailabilityZoneDistribution {
	v := vals.Get("AvailabilityZoneDistribution.CapacityDistributionStrategy")
	if v == "" {
		return nil
	}

	return &AvailabilityZoneDistribution{CapacityDistributionStrategy: v}
}

// parseAvailabilityZoneImpairmentPolicy parses AvailabilityZoneImpairmentPolicy.*
// form values. Returns nil if neither field was specified.
func parseAvailabilityZoneImpairmentPolicy(vals url.Values) *AvailabilityZoneImpairmentPolicy {
	const prefix = "AvailabilityZoneImpairmentPolicy."

	behavior := vals.Get(prefix + "ImpairedZoneHealthCheckBehavior")
	enabled := vals.Get(prefix + "ZonalShiftEnabled")

	if behavior == "" && enabled == "" {
		return nil
	}

	return &AvailabilityZoneImpairmentPolicy{
		ImpairedZoneHealthCheckBehavior: behavior,
		ZonalShiftEnabled:               enabled == formValueTrue,
	}
}

// parseCapacityReservationTarget parses the CapacityReservationTarget.* form
// values nested under prefix. Returns nil if neither list was specified.
func parseCapacityReservationTarget(vals url.Values, prefix string) *CapacityReservationTarget {
	ids := parseMembers(vals, prefix+".CapacityReservationIds.member")
	arns := parseMembers(vals, prefix+".CapacityReservationResourceGroupArns.member")

	if len(ids) == 0 && len(arns) == 0 {
		return nil
	}

	return &CapacityReservationTarget{
		CapacityReservationIDs:               ids,
		CapacityReservationResourceGroupARNs: arns,
	}
}

// parseCapacityReservationSpecification parses CapacityReservationSpecification.*
// form values. Returns nil if not specified.
func parseCapacityReservationSpecification(vals url.Values) *CapacityReservationSpecification {
	const prefix = "CapacityReservationSpecification"

	pref := vals.Get(prefix + ".CapacityReservationPreference")
	target := parseCapacityReservationTarget(vals, prefix+".CapacityReservationTarget")

	if pref == "" && target == nil {
		return nil
	}

	return &CapacityReservationSpecification{
		CapacityReservationPreference: pref,
		CapacityReservationTarget:     target,
	}
}

// parseInstanceLifecyclePolicy parses InstanceLifecyclePolicy.* form values.
// Returns nil if not specified.
func parseInstanceLifecyclePolicy(vals url.Values) *InstanceLifecyclePolicy {
	v := vals.Get("InstanceLifecyclePolicy.RetentionTriggers.TerminateHookAbandon")
	if v == "" {
		return nil
	}

	return &InstanceLifecyclePolicy{RetentionTriggers: &RetentionTriggers{TerminateHookAbandon: v}}
}

// parseInstanceMaintenancePolicy parses InstanceMaintenancePolicy.* form values.
// Returns nil if neither field was specified. Uses *int32 (not the shared
// parseIntVal-into-plain-int32 pattern) so a valid "-1 clears the value"
// sentinel round-trips distinctly from "field omitted".
func parseInstanceMaintenancePolicy(vals url.Values) *InstanceMaintenancePolicy {
	const prefix = "InstanceMaintenancePolicy."

	minStr := vals.Get(prefix + "MinHealthyPercentage")
	maxStr := vals.Get(prefix + "MaxHealthyPercentage")

	if minStr == "" && maxStr == "" {
		return nil
	}

	policy := &InstanceMaintenancePolicy{}

	if minStr != "" {
		if n, err := parseIntVal(minStr); err == nil {
			policy.MinHealthyPercentage = &n
		}
	}

	if maxStr != "" {
		if n, err := parseIntVal(maxStr); err == nil {
			policy.MaxHealthyPercentage = &n
		}
	}

	return policy
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
