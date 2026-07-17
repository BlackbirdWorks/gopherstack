package codedeploy

import (
	"context"
	"fmt"
)

// tagFilterEntry is the wire format for a tag filter (with Type field).
type tagFilterEntry struct {
	Key   string `json:"Key,omitempty"`
	Value string `json:"Value,omitempty"`
	Type  string `json:"Type,omitempty"`
}

// autoScalingGroupEntry is the wire format for an auto scaling group reference.
type autoScalingGroupEntry struct {
	Name string `json:"name,omitempty"`
	Hook string `json:"hook,omitempty"`
}

// elbInfoEntry is the wire format for an ELB reference.
type elbInfoEntry struct {
	Name string `json:"name,omitempty"`
}

// targetGroupInfoEntry is the wire format for a target group reference.
type targetGroupInfoEntry struct {
	Name string `json:"name,omitempty"`
}

// trafficRouteEntry is the wire format for a traffic route.
type trafficRouteEntry struct {
	ListenerArns []string `json:"listenerArns,omitempty"`
}

// targetGroupPairInfoEntry is the wire format for a target group pair.
type targetGroupPairInfoEntry struct {
	ProdTrafficRoute *trafficRouteEntry     `json:"prodTrafficRoute,omitempty"`
	TestTrafficRoute *trafficRouteEntry     `json:"testTrafficRoute,omitempty"`
	TargetGroups     []targetGroupInfoEntry `json:"targetGroups,omitempty"`
}

// loadBalancerInfoEntry is the wire format for load balancer info.
type loadBalancerInfoEntry struct {
	ElbInfoList             []elbInfoEntry             `json:"elbInfoList,omitempty"`
	TargetGroupInfoList     []targetGroupInfoEntry     `json:"targetGroupInfoList,omitempty"`
	TargetGroupPairInfoList []targetGroupPairInfoEntry `json:"targetGroupPairInfoList,omitempty"`
}

// deploymentStyleEntry is the wire format for deployment style.
type deploymentStyleEntry struct {
	DeploymentType   string `json:"deploymentType,omitempty"`
	DeploymentOption string `json:"deploymentOption,omitempty"`
}

// terminateBlueEntry is the wire format for blue-instance termination config.
type terminateBlueEntry struct {
	Action                       string `json:"action,omitempty"`
	TerminationWaitTimeInMinutes int    `json:"terminationWaitTimeInMinutes,omitempty"`
}

// deploymentReadyEntry is the wire format for deployment ready option.
type deploymentReadyEntry struct {
	ActionOnTimeout   string `json:"actionOnTimeout,omitempty"`
	WaitTimeInMinutes int    `json:"waitTimeInMinutes,omitempty"`
}

// greenFleetEntry is the wire format for green fleet provisioning option.
type greenFleetEntry struct {
	Action string `json:"action,omitempty"`
}

// blueGreenConfigEntry is the wire format for blue/green deployment configuration.
type blueGreenConfigEntry struct {
	TerminateBlueInstancesOnDeploymentSuccess *terminateBlueEntry   `json:"terminateBlueInstancesOnDeploymentSuccess,omitempty"` //nolint:lll // long AWS name
	DeploymentReadyOption                     *deploymentReadyEntry `json:"deploymentReadyOption,omitempty"`
	GreenFleetProvisioningOption              *greenFleetEntry      `json:"greenFleetProvisioningOption,omitempty"`
}

// alarmEntry is the wire format for a CloudWatch alarm reference.
type alarmEntry struct {
	Name string `json:"name,omitempty"`
}

// alarmConfigEntry is the wire format for alarm configuration.
type alarmConfigEntry struct {
	Alarms                 []alarmEntry `json:"alarms,omitempty"`
	Enabled                bool         `json:"enabled,omitempty"`
	IgnorePollAlarmFailure bool         `json:"ignorePollAlarmFailure,omitempty"`
}

// autoRollbackConfigEntry is the wire format for auto-rollback configuration.
type autoRollbackConfigEntry struct {
	Events  []string `json:"events,omitempty"`
	Enabled bool     `json:"enabled,omitempty"`
}

// triggerConfigEntry is the wire format for SNS trigger configuration.
type triggerConfigEntry struct {
	TriggerName      string   `json:"triggerName,omitempty"`
	TriggerTargetArn string   `json:"triggerTargetArn,omitempty"`
	TriggerEvents    []string `json:"triggerEvents,omitempty"`
}

// ec2TagSetEntry is the wire format for an EC2 tag set.
type ec2TagSetEntry struct {
	Ec2TagSetList [][]tagFilterEntry `json:"ec2TagSetList,omitempty"`
}

// onPremTagSetEntry is the wire format for an on-premises tag set.
type onPremTagSetEntry struct {
	OnPremisesTagSetList [][]tagFilterEntry `json:"onPremisesTagSetList,omitempty"`
}

// ecsServiceEntry is the wire format for an ECS service reference.
type ecsServiceEntry struct {
	ServiceName string `json:"serviceName,omitempty"`
	ClusterName string `json:"clusterName,omitempty"`
}

// deploymentGroupInfoOutput is the full wire format for a deployment group (get/batch responses).
type deploymentGroupInfoOutput struct {
	BlueGreenDeploymentConfiguration *blueGreenConfigEntry    `json:"blueGreenDeploymentConfiguration,omitempty"`
	AlarmConfiguration               *alarmConfigEntry        `json:"alarmConfiguration,omitempty"`
	AutoRollbackConfiguration        *autoRollbackConfigEntry `json:"autoRollbackConfiguration,omitempty"`
	LoadBalancerInfo                 *loadBalancerInfoEntry   `json:"loadBalancerInfo,omitempty"`
	DeploymentStyle                  *deploymentStyleEntry    `json:"deploymentStyle,omitempty"`
	Ec2TagSet                        *ec2TagSetEntry          `json:"ec2TagSet,omitempty"`
	OnPremisesTagSet                 *onPremTagSetEntry       `json:"onPremisesTagSet,omitempty"`
	ApplicationName                  string                   `json:"applicationName"`
	DeploymentGroupID                string                   `json:"deploymentGroupId"`
	DeploymentGroupName              string                   `json:"deploymentGroupName"`
	ServiceRoleArn                   string                   `json:"serviceRoleArn"`
	DeploymentConfigName             string                   `json:"deploymentConfigName"`
	ComputePlatform                  string                   `json:"computePlatform,omitempty"`
	OutdatedInstancesStrategy        string                   `json:"outdatedInstancesStrategy,omitempty"`
	Ec2TagFilters                    []tagFilterEntry         `json:"ec2TagFilters,omitempty"`
	OnPremisesInstanceTagFilters     []tagFilterEntry         `json:"onPremisesInstanceTagFilters,omitempty"`
	AutoScalingGroups                []autoScalingGroupEntry  `json:"autoScalingGroups,omitempty"`
	TriggerConfigurations            []triggerConfigEntry     `json:"triggerConfigurations,omitempty"`
	ECSServices                      []ecsServiceEntry        `json:"ecsServices,omitempty"`
	TerminationHookEnabled           bool                     `json:"terminationHookEnabled,omitempty"`
}

// dgToOutput converts a backend DeploymentGroup to the wire output format.
//
//nolint:gocognit,cyclop,funlen // wire-format conversion requires many field mappings
func dgToOutput(dg *DeploymentGroup) deploymentGroupInfoOutput {
	out := deploymentGroupInfoOutput{
		ApplicationName:           dg.ApplicationName,
		DeploymentGroupID:         dg.DeploymentGroupID,
		DeploymentGroupName:       dg.DeploymentGroupName,
		ServiceRoleArn:            dg.ServiceRoleArn,
		DeploymentConfigName:      dg.DeploymentConfigName,
		ComputePlatform:           dg.ComputePlatform,
		OutdatedInstancesStrategy: dg.OutdatedInstancesStrategy,
		TerminationHookEnabled:    dg.TerminationHookEnabled,
	}

	for _, f := range dg.Ec2TagFilters {
		out.Ec2TagFilters = append(out.Ec2TagFilters, tagFilterEntry(f))
	}

	for _, f := range dg.OnPremisesInstanceTagFilters {
		out.OnPremisesInstanceTagFilters = append(out.OnPremisesInstanceTagFilters,
			tagFilterEntry(f))
	}

	for _, asg := range dg.AutoScalingGroups {
		out.AutoScalingGroups = append(out.AutoScalingGroups, autoScalingGroupEntry(asg))
	}

	for _, tc := range dg.TriggerConfigurations {
		out.TriggerConfigurations = append(out.TriggerConfigurations, triggerConfigEntry(tc))
	}

	for _, svc := range dg.ECSServices {
		out.ECSServices = append(out.ECSServices, ecsServiceEntry(svc))
	}

	if dg.LoadBalancerInfo != nil {
		lbi := &loadBalancerInfoEntry{}
		for _, e := range dg.LoadBalancerInfo.ElbInfoList {
			lbi.ElbInfoList = append(lbi.ElbInfoList, elbInfoEntry(e))
		}
		for _, tg := range dg.LoadBalancerInfo.TargetGroupInfoList {
			lbi.TargetGroupInfoList = append(lbi.TargetGroupInfoList, targetGroupInfoEntry(tg))
		}
		for _, pair := range dg.LoadBalancerInfo.TargetGroupPairInfoList {
			p := targetGroupPairInfoEntry{}
			if pair.ProdTrafficRoute != nil {
				p.ProdTrafficRoute = &trafficRouteEntry{ListenerArns: pair.ProdTrafficRoute.ListenerArns}
			}
			if pair.TestTrafficRoute != nil {
				p.TestTrafficRoute = &trafficRouteEntry{ListenerArns: pair.TestTrafficRoute.ListenerArns}
			}
			for _, tg := range pair.TargetGroups {
				p.TargetGroups = append(p.TargetGroups, targetGroupInfoEntry(tg))
			}
			lbi.TargetGroupPairInfoList = append(lbi.TargetGroupPairInfoList, p)
		}
		out.LoadBalancerInfo = lbi
	}

	if dg.DeploymentStyle != nil {
		out.DeploymentStyle = &deploymentStyleEntry{
			DeploymentType:   dg.DeploymentStyle.DeploymentType,
			DeploymentOption: dg.DeploymentStyle.DeploymentOption,
		}
	}

	if dg.BlueGreenDeploymentConfiguration != nil {
		bgc := &blueGreenConfigEntry{}
		if dg.BlueGreenDeploymentConfiguration.TerminateBlueInstancesOnDeploymentSuccess != nil {
			tb := dg.BlueGreenDeploymentConfiguration.TerminateBlueInstancesOnDeploymentSuccess
			bgc.TerminateBlueInstancesOnDeploymentSuccess = &terminateBlueEntry{
				Action:                       tb.Action,
				TerminationWaitTimeInMinutes: tb.TerminationWaitTimeInMinutes,
			}
		}
		if dg.BlueGreenDeploymentConfiguration.DeploymentReadyOption != nil {
			dr := dg.BlueGreenDeploymentConfiguration.DeploymentReadyOption
			bgc.DeploymentReadyOption = &deploymentReadyEntry{
				ActionOnTimeout:   dr.ActionOnTimeout,
				WaitTimeInMinutes: dr.WaitTimeInMinutes,
			}
		}
		if dg.BlueGreenDeploymentConfiguration.GreenFleetProvisioningOption != nil {
			bgc.GreenFleetProvisioningOption = &greenFleetEntry{
				Action: dg.BlueGreenDeploymentConfiguration.GreenFleetProvisioningOption.Action,
			}
		}
		out.BlueGreenDeploymentConfiguration = bgc
	}

	if dg.AlarmConfiguration != nil {
		ac := &alarmConfigEntry{
			Enabled:                dg.AlarmConfiguration.Enabled,
			IgnorePollAlarmFailure: dg.AlarmConfiguration.IgnorePollAlarmFailure,
		}
		for _, a := range dg.AlarmConfiguration.Alarms {
			ac.Alarms = append(ac.Alarms, alarmEntry(a))
		}
		out.AlarmConfiguration = ac
	}

	if dg.AutoRollbackConfiguration != nil {
		out.AutoRollbackConfiguration = &autoRollbackConfigEntry{
			Events:  dg.AutoRollbackConfiguration.Events,
			Enabled: dg.AutoRollbackConfiguration.Enabled,
		}
	}

	if dg.Ec2TagSet != nil {
		ets := &ec2TagSetEntry{}
		for _, group := range dg.Ec2TagSet.Ec2TagSetList {
			row := make([]tagFilterEntry, 0, len(group))
			for _, f := range group {
				row = append(row, tagFilterEntry(f))
			}
			ets.Ec2TagSetList = append(ets.Ec2TagSetList, row)
		}
		out.Ec2TagSet = ets
	}

	if dg.OnPremisesTagSet != nil {
		opts := &onPremTagSetEntry{}
		for _, group := range dg.OnPremisesTagSet.OnPremisesTagSetList {
			row := make([]tagFilterEntry, 0, len(group))
			for _, f := range group {
				row = append(row, tagFilterEntry(f))
			}
			opts.OnPremisesTagSetList = append(opts.OnPremisesTagSetList, row)
		}
		out.OnPremisesTagSet = opts
	}

	return out
}

// dgInputFromWire converts wire-format deployment group input fields to backend DeploymentGroupInput.
//
//nolint:gocognit,cyclop,funlen // wire-format conversion requires many field mappings
func dgInputFromWire(
	serviceRoleArn, deploymentConfigName, outdatedInstancesStrategy string,
	terminationHookEnabled bool,
	ec2TagFilters []tagFilterEntry,
	onPremTagFilters []tagFilterEntry,
	autoScalingGroups []autoScalingGroupEntry,
	triggerConfigs []triggerConfigEntry,
	ecsServices []ecsServiceEntry,
	lbi *loadBalancerInfoEntry,
	style *deploymentStyleEntry,
	bgConfig *blueGreenConfigEntry,
	alarmConfig *alarmConfigEntry,
	autoRollback *autoRollbackConfigEntry,
	ec2TagSet *ec2TagSetEntry,
	onPremTagSet *onPremTagSetEntry,
) DeploymentGroupInput {
	input := DeploymentGroupInput{
		ServiceRoleArn:            serviceRoleArn,
		DeploymentConfigName:      deploymentConfigName,
		OutdatedInstancesStrategy: outdatedInstancesStrategy,
		TerminationHookEnabled:    terminationHookEnabled,
	}

	for _, f := range ec2TagFilters {
		input.Ec2TagFilters = append(input.Ec2TagFilters, TagFilter(f))
	}
	for _, f := range onPremTagFilters {
		input.OnPremisesInstanceTagFilters = append(input.OnPremisesInstanceTagFilters,
			TagFilter(f))
	}
	for _, asg := range autoScalingGroups {
		input.AutoScalingGroups = append(input.AutoScalingGroups, AutoScalingGroup(asg))
	}
	for _, tc := range triggerConfigs {
		input.TriggerConfigurations = append(input.TriggerConfigurations, TriggerConfiguration(tc))
	}
	for _, svc := range ecsServices {
		input.ECSServices = append(input.ECSServices, ECSService(svc))
	}

	if lbi != nil {
		lb := &LoadBalancerInfo{}
		for _, e := range lbi.ElbInfoList {
			lb.ElbInfoList = append(lb.ElbInfoList, ElbInfo(e))
		}
		for _, tg := range lbi.TargetGroupInfoList {
			lb.TargetGroupInfoList = append(lb.TargetGroupInfoList, TargetGroupInfo(tg))
		}
		for _, pair := range lbi.TargetGroupPairInfoList {
			p := TargetGroupPairInfo{}
			if pair.ProdTrafficRoute != nil {
				p.ProdTrafficRoute = &TrafficRoute{ListenerArns: pair.ProdTrafficRoute.ListenerArns}
			}
			if pair.TestTrafficRoute != nil {
				p.TestTrafficRoute = &TrafficRoute{ListenerArns: pair.TestTrafficRoute.ListenerArns}
			}
			for _, tg := range pair.TargetGroups {
				p.TargetGroups = append(p.TargetGroups, TargetGroupInfo(tg))
			}
			lb.TargetGroupPairInfoList = append(lb.TargetGroupPairInfoList, p)
		}
		input.LoadBalancerInfo = lb
	}

	if style != nil {
		input.DeploymentStyle = &DeploymentStyle{
			DeploymentType:   style.DeploymentType,
			DeploymentOption: style.DeploymentOption,
		}
	}

	if bgConfig != nil {
		bgc := &BlueGreenDeploymentConfiguration{}
		if bgConfig.TerminateBlueInstancesOnDeploymentSuccess != nil {
			tb := bgConfig.TerminateBlueInstancesOnDeploymentSuccess
			bgc.TerminateBlueInstancesOnDeploymentSuccess = &TerminateBlueInstancesOnDeploymentSuccess{
				Action:                       tb.Action,
				TerminationWaitTimeInMinutes: tb.TerminationWaitTimeInMinutes,
			}
		}
		if bgConfig.DeploymentReadyOption != nil {
			bgc.DeploymentReadyOption = &DeploymentReadyOption{
				ActionOnTimeout:   bgConfig.DeploymentReadyOption.ActionOnTimeout,
				WaitTimeInMinutes: bgConfig.DeploymentReadyOption.WaitTimeInMinutes,
			}
		}
		if bgConfig.GreenFleetProvisioningOption != nil {
			bgc.GreenFleetProvisioningOption = &GreenFleetProvisioningOption{
				Action: bgConfig.GreenFleetProvisioningOption.Action,
			}
		}
		input.BlueGreenDeploymentConfiguration = bgc
	}

	if alarmConfig != nil {
		ac := &AlarmConfiguration{
			Enabled:                alarmConfig.Enabled,
			IgnorePollAlarmFailure: alarmConfig.IgnorePollAlarmFailure,
		}
		for _, a := range alarmConfig.Alarms {
			ac.Alarms = append(ac.Alarms, Alarm(a))
		}
		input.AlarmConfiguration = ac
	}

	if autoRollback != nil {
		input.AutoRollbackConfiguration = &AutoRollbackConfiguration{
			Events:  autoRollback.Events,
			Enabled: autoRollback.Enabled,
		}
	}

	if ec2TagSet != nil {
		ets := &Ec2TagSet{}
		for _, group := range ec2TagSet.Ec2TagSetList {
			row := make([]TagFilter, 0, len(group))
			for _, f := range group {
				row = append(row, TagFilter(f))
			}
			ets.Ec2TagSetList = append(ets.Ec2TagSetList, row)
		}
		input.Ec2TagSet = ets
	}

	if onPremTagSet != nil {
		opts := &TagSet{}
		for _, group := range onPremTagSet.OnPremisesTagSetList {
			row := make([]TagFilter, 0, len(group))
			for _, f := range group {
				row = append(row, TagFilter(f))
			}
			opts.OnPremisesTagSetList = append(opts.OnPremisesTagSetList, row)
		}
		input.OnPremisesTagSet = opts
	}

	return input
}

type createDeploymentGroupInput struct {
	DeploymentStyle                  *deploymentStyleEntry    `json:"deploymentStyle"`
	OnPremisesTagSet                 *onPremTagSetEntry       `json:"onPremisesTagSet"`
	LoadBalancerInfo                 *loadBalancerInfoEntry   `json:"loadBalancerInfo"`
	Ec2TagSet                        *ec2TagSetEntry          `json:"ec2TagSet"`
	BlueGreenDeploymentConfiguration *blueGreenConfigEntry    `json:"blueGreenDeploymentConfiguration"`
	AlarmConfiguration               *alarmConfigEntry        `json:"alarmConfiguration"`
	AutoRollbackConfiguration        *autoRollbackConfigEntry `json:"autoRollbackConfiguration"`
	ServiceRoleArn                   string                   `json:"serviceRoleArn"`
	DeploymentGroupName              string                   `json:"deploymentGroupName"`
	ApplicationName                  string                   `json:"applicationName"`
	DeploymentConfigName             string                   `json:"deploymentConfigName"`
	OutdatedInstancesStrategy        string                   `json:"outdatedInstancesStrategy"`
	Tags                             []tagEntry               `json:"tags"`
	Ec2TagFilters                    []tagFilterEntry         `json:"ec2TagFilters"`
	OnPremisesInstanceTagFilters     []tagFilterEntry         `json:"onPremisesInstanceTagFilters"`
	AutoScalingGroups                []autoScalingGroupEntry  `json:"autoScalingGroups"`
	TriggerConfigurations            []triggerConfigEntry     `json:"triggerConfigurations"`
	ECSServices                      []ecsServiceEntry        `json:"ecsServices"`
	TerminationHookEnabled           bool                     `json:"terminationHookEnabled"`
}

type createDeploymentGroupOutput struct {
	DeploymentGroupID string `json:"deploymentGroupId"`
}

func (h *Handler) handleCreateDeploymentGroup(
	_ context.Context,
	in *createDeploymentGroupInput,
) (*createDeploymentGroupOutput, error) {
	if in.ApplicationName == "" || in.DeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and deploymentGroupName are required", errInvalidRequest)
	}

	input := dgInputFromWire(
		in.ServiceRoleArn, in.DeploymentConfigName, in.OutdatedInstancesStrategy,
		in.TerminationHookEnabled,
		in.Ec2TagFilters, in.OnPremisesInstanceTagFilters,
		in.AutoScalingGroups, in.TriggerConfigurations, in.ECSServices,
		in.LoadBalancerInfo, in.DeploymentStyle,
		in.BlueGreenDeploymentConfiguration, in.AlarmConfiguration,
		in.AutoRollbackConfiguration, in.Ec2TagSet, in.OnPremisesTagSet,
	)

	dg, err := h.Backend.CreateDeploymentGroup(
		in.ApplicationName, in.DeploymentGroupName,
		input,
		tagEntriesToMap(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createDeploymentGroupOutput{DeploymentGroupID: dg.DeploymentGroupID}, nil
}

type getDeploymentGroupInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
}

type getDeploymentGroupOutput struct {
	DeploymentGroupInfo deploymentGroupInfoOutput `json:"deploymentGroupInfo"`
}

func (h *Handler) handleGetDeploymentGroup(
	_ context.Context,
	in *getDeploymentGroupInput,
) (*getDeploymentGroupOutput, error) {
	if in.ApplicationName == "" || in.DeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and deploymentGroupName are required", errInvalidRequest)
	}

	dg, err := h.Backend.GetDeploymentGroup(in.ApplicationName, in.DeploymentGroupName)
	if err != nil {
		return nil, err
	}

	return &getDeploymentGroupOutput{DeploymentGroupInfo: dgToOutput(dg)}, nil
}

type listDeploymentGroupsInput struct {
	ApplicationName string `json:"applicationName"`
}

type listDeploymentGroupsOutput struct {
	ApplicationName  string   `json:"applicationName"`
	DeploymentGroups []string `json:"deploymentGroups"`
}

func (h *Handler) handleListDeploymentGroups(
	_ context.Context,
	in *listDeploymentGroupsInput,
) (*listDeploymentGroupsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	names, err := h.Backend.ListDeploymentGroups(in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &listDeploymentGroupsOutput{
		ApplicationName:  in.ApplicationName,
		DeploymentGroups: names,
	}, nil
}

type deleteDeploymentGroupInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
}

type deleteDeploymentGroupOutput struct{}

func (h *Handler) handleDeleteDeploymentGroup(
	_ context.Context,
	in *deleteDeploymentGroupInput,
) (*deleteDeploymentGroupOutput, error) {
	if err := h.Backend.DeleteDeploymentGroup(in.ApplicationName, in.DeploymentGroupName); err != nil {
		return nil, err
	}

	return &deleteDeploymentGroupOutput{}, nil
}

type updateDeploymentGroupInput struct {
	AutoRollbackConfiguration        *autoRollbackConfigEntry `json:"autoRollbackConfiguration"`
	OnPremisesTagSet                 *onPremTagSetEntry       `json:"onPremisesTagSet"`
	Ec2TagSet                        *ec2TagSetEntry          `json:"ec2TagSet"`
	DeploymentStyle                  *deploymentStyleEntry    `json:"deploymentStyle"`
	LoadBalancerInfo                 *loadBalancerInfoEntry   `json:"loadBalancerInfo"`
	BlueGreenDeploymentConfiguration *blueGreenConfigEntry    `json:"blueGreenDeploymentConfiguration"`
	AlarmConfiguration               *alarmConfigEntry        `json:"alarmConfiguration"`
	DeploymentConfigName             string                   `json:"deploymentConfigName"`
	ApplicationName                  string                   `json:"applicationName"`
	ServiceRoleArn                   string                   `json:"serviceRoleArn"`
	NewDeploymentGroupName           string                   `json:"newDeploymentGroupName"`
	CurrentDeploymentGroupName       string                   `json:"currentDeploymentGroupName"`
	OutdatedInstancesStrategy        string                   `json:"outdatedInstancesStrategy"`
	Ec2TagFilters                    []tagFilterEntry         `json:"ec2TagFilters"`
	OnPremisesInstanceTagFilters     []tagFilterEntry         `json:"onPremisesInstanceTagFilters"`
	AutoScalingGroups                []autoScalingGroupEntry  `json:"autoScalingGroups"`
	TriggerConfigurations            []triggerConfigEntry     `json:"triggerConfigurations"`
	ECSServices                      []ecsServiceEntry        `json:"ecsServices"`
	TerminationHookEnabled           bool                     `json:"terminationHookEnabled"`
}

type updateDeploymentGroupOutput struct {
	HooksNotCleanedUp bool `json:"hooksNotCleanedUp,omitempty"`
}

func (h *Handler) handleUpdateDeploymentGroup(
	_ context.Context,
	in *updateDeploymentGroupInput,
) (*updateDeploymentGroupOutput, error) {
	if in.ApplicationName == "" || in.CurrentDeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and currentDeploymentGroupName are required", errInvalidRequest)
	}

	input := dgInputFromWire(
		in.ServiceRoleArn, in.DeploymentConfigName, in.OutdatedInstancesStrategy,
		in.TerminationHookEnabled,
		in.Ec2TagFilters, in.OnPremisesInstanceTagFilters,
		in.AutoScalingGroups, in.TriggerConfigurations, in.ECSServices,
		in.LoadBalancerInfo, in.DeploymentStyle,
		in.BlueGreenDeploymentConfiguration, in.AlarmConfiguration,
		in.AutoRollbackConfiguration, in.Ec2TagSet, in.OnPremisesTagSet,
	)

	hooks, err := h.Backend.UpdateDeploymentGroup(
		in.ApplicationName, in.CurrentDeploymentGroupName, in.NewDeploymentGroupName,
		input,
	)
	if err != nil {
		return nil, err
	}

	return &updateDeploymentGroupOutput{HooksNotCleanedUp: hooks}, nil
}

type batchGetDeploymentGroupsInput struct {
	ApplicationName      string   `json:"applicationName"`
	DeploymentGroupNames []string `json:"deploymentGroupNames"`
}

type batchGetDeploymentGroupsOutput struct {
	ErrorMessage         string                      `json:"errorMessage,omitempty"`
	DeploymentGroupsInfo []deploymentGroupInfoOutput `json:"deploymentGroupsInfo"`
}

func (h *Handler) handleBatchGetDeploymentGroups(
	_ context.Context,
	in *batchGetDeploymentGroupsInput,
) (*batchGetDeploymentGroupsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	dgs, err := h.Backend.BatchGetDeploymentGroups(in.ApplicationName, in.DeploymentGroupNames)
	if err != nil {
		return nil, err
	}

	infos := make([]deploymentGroupInfoOutput, 0, len(dgs))
	for _, dg := range dgs {
		infos = append(infos, dgToOutput(dg))
	}

	return &batchGetDeploymentGroupsOutput{DeploymentGroupsInfo: infos}, nil
}
