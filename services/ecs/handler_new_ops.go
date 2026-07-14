package ecs

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ----- Capacity provider view types -----

type managedScalingView struct {
	Status                    string `json:"status,omitempty"`
	TargetCapacityPercent     int    `json:"targetCapacityPercent,omitempty"`
	MinimumScalingStepSize    int    `json:"minimumScalingStepSize,omitempty"`
	MaximumScalingStepSize    int    `json:"maximumScalingStepSize,omitempty"`
	InstanceWarmupPeriod      int    `json:"instanceWarmupPeriod,omitempty"`
	TargetCapacityUtilization int    `json:"targetCapacityUtilization,omitempty"`
}

type autoScalingGroupProviderView struct {
	AutoScalingGroupArn          string              `json:"autoScalingGroupArn"`
	ManagedScaling               *managedScalingView `json:"managedScaling,omitempty"`
	ManagedTerminationProtection string              `json:"managedTerminationProtection,omitempty"`
	ManagedDraining              string              `json:"managedDraining,omitempty"`
}

type capacityProviderView struct {
	CapacityProviderArn      string                        `json:"capacityProviderArn"`
	Name                     string                        `json:"name"`
	Status                   string                        `json:"status"`
	UpdateStatus             string                        `json:"updateStatus,omitempty"`
	UpdateStatusReason       string                        `json:"updateStatusReason,omitempty"`
	AutoScalingGroupProvider *autoScalingGroupProviderView `json:"autoScalingGroupProvider,omitempty"`
	Tags                     []Tag                         `json:"tags,omitempty"`
	CreatedAt                float64                       `json:"createdAt"`
}

func toAutoScalingGroupProviderView(asg *AutoScalingGroupProvider) *autoScalingGroupProviderView {
	if asg == nil {
		return nil
	}

	v := &autoScalingGroupProviderView{
		AutoScalingGroupArn:          asg.AutoScalingGroupArn,
		ManagedTerminationProtection: asg.ManagedTerminationProtection,
		ManagedDraining:              asg.ManagedDraining,
	}

	if asg.ManagedScaling != nil {
		v.ManagedScaling = &managedScalingView{
			Status:                    asg.ManagedScaling.Status,
			TargetCapacityPercent:     asg.ManagedScaling.TargetCapacityPercent,
			MinimumScalingStepSize:    asg.ManagedScaling.MinimumScalingStepSize,
			MaximumScalingStepSize:    asg.ManagedScaling.MaximumScalingStepSize,
			InstanceWarmupPeriod:      asg.ManagedScaling.InstanceWarmupPeriod,
			TargetCapacityUtilization: asg.ManagedScaling.TargetCapacityUtilization,
		}
	}

	return v
}

func toCapacityProviderView(cp CapacityProvider) capacityProviderView {
	return capacityProviderView{
		CapacityProviderArn:      cp.CapacityProviderArn,
		Name:                     cp.Name,
		Status:                   cp.Status,
		UpdateStatus:             cp.UpdateStatus,
		UpdateStatusReason:       cp.UpdateStatusReason,
		AutoScalingGroupProvider: toAutoScalingGroupProviderView(cp.AutoScalingGroupProvider),
		Tags:                     cp.Tags,
		CreatedAt:                float64(cp.CreatedAt.Unix()),
	}
}

// ----- Handler: CreateCapacityProvider -----

type managedScalingInput struct {
	Status                    string `json:"status,omitempty"`
	TargetCapacityPercent     int    `json:"targetCapacityPercent,omitempty"`
	MinimumScalingStepSize    int    `json:"minimumScalingStepSize,omitempty"`
	MaximumScalingStepSize    int    `json:"maximumScalingStepSize,omitempty"`
	InstanceWarmupPeriod      int    `json:"instanceWarmupPeriod,omitempty"`
	TargetCapacityUtilization int    `json:"targetCapacityUtilization,omitempty"`
}

type autoScalingGroupProviderInput struct {
	AutoScalingGroupArn          string               `json:"autoScalingGroupArn"`
	ManagedScaling               *managedScalingInput `json:"managedScaling,omitempty"`
	ManagedTerminationProtection string               `json:"managedTerminationProtection,omitempty"`
	ManagedDraining              string               `json:"managedDraining,omitempty"`
}

type createCapacityProviderInput struct {
	Name                     string                         `json:"name"`
	AutoScalingGroupProvider *autoScalingGroupProviderInput `json:"autoScalingGroupProvider,omitempty"`
	Tags                     []tagInput                     `json:"tags,omitempty"`
}

type tagInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type createCapacityProviderOutput struct {
	CapacityProvider capacityProviderView `json:"capacityProvider"`
}

func toAutoScalingGroupProvider(in *autoScalingGroupProviderInput) *AutoScalingGroupProvider {
	if in == nil {
		return nil
	}

	asg := &AutoScalingGroupProvider{
		AutoScalingGroupArn:          in.AutoScalingGroupArn,
		ManagedTerminationProtection: in.ManagedTerminationProtection,
		ManagedDraining:              in.ManagedDraining,
	}

	if in.ManagedScaling != nil {
		asg.ManagedScaling = &ManagedScaling{
			Status:                    in.ManagedScaling.Status,
			TargetCapacityPercent:     in.ManagedScaling.TargetCapacityPercent,
			MinimumScalingStepSize:    in.ManagedScaling.MinimumScalingStepSize,
			MaximumScalingStepSize:    in.ManagedScaling.MaximumScalingStepSize,
			InstanceWarmupPeriod:      in.ManagedScaling.InstanceWarmupPeriod,
			TargetCapacityUtilization: in.ManagedScaling.TargetCapacityUtilization,
		}
	}

	return asg
}

func (h *Handler) handleCreateCapacityProvider(
	_ context.Context,
	in *createCapacityProviderInput,
) (*createCapacityProviderOutput, error) {
	tags := make([]Tag, 0, len(in.Tags))
	for _, t := range in.Tags {
		tags = append(tags, Tag(t))
	}

	cp, err := h.Backend.CreateCapacityProvider(CreateCapacityProviderInput{
		Name:                     in.Name,
		AutoScalingGroupProvider: toAutoScalingGroupProvider(in.AutoScalingGroupProvider),
		Tags:                     tags,
	})
	if err != nil {
		return nil, err
	}

	return &createCapacityProviderOutput{CapacityProvider: toCapacityProviderView(*cp)}, nil
}

// ----- Handler: DeleteCapacityProvider -----

type deleteCapacityProviderInput struct {
	CapacityProvider string `json:"capacityProvider"`
}

type deleteCapacityProviderOutput struct {
	CapacityProvider capacityProviderView `json:"capacityProvider"`
}

func (h *Handler) handleDeleteCapacityProvider(
	_ context.Context,
	in *deleteCapacityProviderInput,
) (*deleteCapacityProviderOutput, error) {
	cp, err := h.Backend.DeleteCapacityProvider(in.CapacityProvider)
	if err != nil {
		return nil, err
	}

	return &deleteCapacityProviderOutput{CapacityProvider: toCapacityProviderView(*cp)}, nil
}

// ----- Handler: DescribeCapacityProviders -----

type describeCapacityProvidersInput struct {
	NextToken         string   `json:"nextToken,omitempty"`
	CapacityProviders []string `json:"capacityProviders,omitempty"`
	MaxResults        int      `json:"maxResults,omitempty"`
}

type describeCapacityProvidersOutput struct {
	NextToken         string                 `json:"nextToken,omitempty"`
	CapacityProviders []capacityProviderView `json:"capacityProviders"`
	Failures          []failureView          `json:"failures"`
}

func (h *Handler) handleDescribeCapacityProviders(
	_ context.Context,
	in *describeCapacityProvidersInput,
) (*describeCapacityProvidersOutput, error) {
	providers, failures, err := h.Backend.DescribeCapacityProviders(in.CapacityProviders)
	if err != nil {
		return nil, err
	}

	views := make([]capacityProviderView, 0, len(providers))
	for _, cp := range providers {
		views = append(views, toCapacityProviderView(cp))
	}

	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })

	p := page.New(views, in.NextToken, in.MaxResults, defaultECSMaxResults)

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeCapacityProvidersOutput{
		CapacityProviders: p.Data,
		NextToken:         p.Next,
		Failures:          failViews,
	}, nil
}

// ----- Handler: DeleteAccountSetting -----

type deleteAccountSettingInput struct {
	Name         string `json:"name"`
	PrincipalArn string `json:"principalArn,omitempty"`
}

type accountSettingView struct {
	Name         string `json:"name"`
	Value        string `json:"value,omitempty"`
	PrincipalArn string `json:"principalArn,omitempty"`
}

type deleteAccountSettingOutput struct {
	Setting accountSettingView `json:"setting"`
}

func (h *Handler) handleDeleteAccountSetting(
	_ context.Context,
	in *deleteAccountSettingInput,
) (*deleteAccountSettingOutput, error) {
	setting, err := h.Backend.DeleteAccountSetting(in.Name, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	return &deleteAccountSettingOutput{Setting: accountSettingView{
		Name:         setting.Name,
		Value:        setting.Value,
		PrincipalArn: setting.PrincipalArn,
	}}, nil
}

// ----- Handler: DeleteAttributes -----

type attributeInput struct {
	Name       string `json:"name"`
	Value      string `json:"value,omitempty"`
	TargetID   string `json:"targetId,omitempty"`
	TargetType string `json:"targetType,omitempty"`
}

type deleteAttributesInput struct {
	Cluster    string           `json:"cluster,omitempty"`
	Attributes []attributeInput `json:"attributes"`
}

type deleteAttributesOutput struct {
	Attributes []attributeInput `json:"attributes"`
}

func (h *Handler) handleDeleteAttributes(
	_ context.Context,
	in *deleteAttributesInput,
) (*deleteAttributesOutput, error) {
	attrs := make([]Attribute, 0, len(in.Attributes))
	for _, a := range in.Attributes {
		attrs = append(attrs, Attribute(a))
	}

	deleted, err := h.Backend.DeleteAttributes(in.Cluster, attrs)
	if err != nil {
		return nil, err
	}

	views := make([]attributeInput, 0, len(deleted))
	for _, a := range deleted {
		views = append(views, attributeInput(a))
	}

	return &deleteAttributesOutput{Attributes: views}, nil
}

// ----- Handler: DeleteTaskDefinitions -----

type deleteTaskDefinitionsInput struct {
	TaskDefinitions []string `json:"taskDefinitions"`
}

type failureView struct {
	Arn    string `json:"arn,omitempty"`
	Detail string `json:"detail,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type deleteTaskDefinitionsOutput struct {
	TaskDefinitions []taskDefinitionView `json:"taskDefinitions"`
	Failures        []failureView        `json:"failures"`
}

func (h *Handler) handleDeleteTaskDefinitions(
	_ context.Context,
	in *deleteTaskDefinitionsInput,
) (*deleteTaskDefinitionsOutput, error) {
	deleted, failures, err := h.Backend.DeleteTaskDefinitions(in.TaskDefinitions)
	if err != nil {
		return nil, err
	}

	tdViews := make([]taskDefinitionView, 0, len(deleted))
	for _, td := range deleted {
		tdViews = append(tdViews, toTaskDefinitionView(td))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &deleteTaskDefinitionsOutput{
		TaskDefinitions: tdViews,
		Failures:        failViews,
	}, nil
}

// ----- Handler: DescribeServiceDeployments -----

type describeServiceDeploymentsInput struct {
	ServiceDeploymentArns []string `json:"serviceDeploymentArns"`
}

type serviceDeploymentView struct {
	ServiceDeploymentArn string  `json:"serviceDeploymentArn"`
	ClusterArn           string  `json:"clusterArn"`
	ServiceArn           string  `json:"serviceArn"`
	Status               string  `json:"status"`
	StatusReason         string  `json:"statusReason,omitempty"`
	CreatedAt            float64 `json:"createdAt,omitempty"`
	UpdatedAt            float64 `json:"updatedAt,omitempty"`
}

type describeServiceDeploymentsOutput struct {
	ServiceDeployments []serviceDeploymentView `json:"serviceDeployments"`
	Failures           []failureView           `json:"failures"`
}

func toServiceDeploymentView(sd ServiceDeployment) serviceDeploymentView {
	v := serviceDeploymentView{
		ServiceDeploymentArn: sd.ServiceDeploymentArn,
		ClusterArn:           sd.ClusterArn,
		ServiceArn:           sd.ServiceArn,
		Status:               sd.Status,
		StatusReason:         sd.StatusReason,
	}

	if sd.CreatedAt != nil {
		v.CreatedAt = float64(sd.CreatedAt.Unix())
	}

	if sd.UpdatedAt != nil {
		v.UpdatedAt = float64(sd.UpdatedAt.Unix())
	}

	return v
}

func (h *Handler) handleDescribeServiceDeployments(
	_ context.Context,
	in *describeServiceDeploymentsInput,
) (*describeServiceDeploymentsOutput, error) {
	deployments, failures, err := h.Backend.DescribeServiceDeployments(in.ServiceDeploymentArns)
	if err != nil {
		return nil, err
	}

	views := make([]serviceDeploymentView, 0, len(deployments))
	for _, sd := range deployments {
		views = append(views, toServiceDeploymentView(sd))
	}

	failViews := make([]failureView, 0, len(failures))
	for _, f := range failures {
		failViews = append(failViews, failureView(f))
	}

	return &describeServiceDeploymentsOutput{
		ServiceDeployments: views,
		Failures:           failViews,
	}, nil
}

// ----- ExpressGatewayService view types -----

type expressGatewayServiceView struct {
	ServiceArn            string  `json:"serviceArn"`
	ServiceName           string  `json:"serviceName"`
	Cluster               string  `json:"cluster"`
	Status                string  `json:"status"`
	ExecutionRoleArn      string  `json:"executionRoleArn"`
	InfrastructureRoleArn string  `json:"infrastructureRoleArn"`
	Tags                  []Tag   `json:"tags,omitempty"`
	CreatedAt             float64 `json:"createdAt"`
}

func toExpressGatewayServiceView(svc ExpressGatewayService) expressGatewayServiceView {
	return expressGatewayServiceView{
		ServiceArn:            svc.ServiceArn,
		ServiceName:           svc.ServiceName,
		Cluster:               svc.Cluster,
		Status:                svc.Status,
		ExecutionRoleArn:      svc.ExecutionRoleArn,
		InfrastructureRoleArn: svc.InfrastructureRoleArn,
		Tags:                  svc.Tags,
		CreatedAt:             float64(svc.CreatedAt.Unix()),
	}
}

// ----- Handler: CreateExpressGatewayService -----

type createExpressGatewayServiceInput struct {
	ExecutionRoleArn      string     `json:"executionRoleArn"`
	InfrastructureRoleArn string     `json:"infrastructureRoleArn"`
	Cluster               string     `json:"cluster,omitempty"`
	ServiceName           string     `json:"serviceName,omitempty"`
	Tags                  []tagInput `json:"tags,omitempty"`
}

type createExpressGatewayServiceOutput struct {
	Service expressGatewayServiceView `json:"service"`
}

func (h *Handler) handleCreateExpressGatewayService(
	_ context.Context,
	in *createExpressGatewayServiceInput,
) (*createExpressGatewayServiceOutput, error) {
	tags := make([]Tag, 0, len(in.Tags))
	for _, t := range in.Tags {
		tags = append(tags, Tag(t))
	}

	svc, err := h.Backend.CreateExpressGatewayService(CreateExpressGatewayServiceInput{
		ExecutionRoleArn:      in.ExecutionRoleArn,
		InfrastructureRoleArn: in.InfrastructureRoleArn,
		Cluster:               in.Cluster,
		ServiceName:           in.ServiceName,
		Tags:                  tags,
	})
	if err != nil {
		return nil, err
	}

	return &createExpressGatewayServiceOutput{Service: toExpressGatewayServiceView(*svc)}, nil
}

// ----- Handler: DeleteExpressGatewayService -----

type deleteExpressGatewayServiceInput struct {
	ServiceArn string `json:"serviceArn"`
}

type deleteExpressGatewayServiceOutput struct {
	Service expressGatewayServiceView `json:"service"`
}

func (h *Handler) handleDeleteExpressGatewayService(
	_ context.Context,
	in *deleteExpressGatewayServiceInput,
) (*deleteExpressGatewayServiceOutput, error) {
	svc, err := h.Backend.DeleteExpressGatewayService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &deleteExpressGatewayServiceOutput{Service: toExpressGatewayServiceView(*svc)}, nil
}

// ----- Handler: DescribeExpressGatewayService -----

type describeExpressGatewayServiceInput struct {
	ServiceArn string `json:"serviceArn"`
}

type describeExpressGatewayServiceOutput struct {
	Service expressGatewayServiceView `json:"service"`
}

func (h *Handler) handleDescribeExpressGatewayService(
	_ context.Context,
	in *describeExpressGatewayServiceInput,
) (*describeExpressGatewayServiceOutput, error) {
	svc, err := h.Backend.DescribeExpressGatewayService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &describeExpressGatewayServiceOutput{Service: toExpressGatewayServiceView(*svc)}, nil
}
