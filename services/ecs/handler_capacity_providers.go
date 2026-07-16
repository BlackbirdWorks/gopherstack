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

// tagInput is a small generic {key,value} tag wire shape shared by
// CreateCapacityProvider (its first/primary caller here) and
// CreateExpressGatewayService (handler_express_gateway.go, cross-file).
type tagInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type createCapacityProviderInput struct {
	Name                     string                         `json:"name"`
	AutoScalingGroupProvider *autoScalingGroupProviderInput `json:"autoScalingGroupProvider,omitempty"`
	Tags                     []tagInput                     `json:"tags,omitempty"`
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

// ----- UpdateCapacityProvider -----

type updateCapacityProviderInput struct {
	Name                     string                         `json:"name"`
	Status                   string                         `json:"status,omitempty"`
	AutoScalingGroupProvider *autoScalingGroupProviderInput `json:"autoScalingGroupProvider,omitempty"`
	Tags                     []Tag                          `json:"tags,omitempty"`
}

type updateCapacityProviderOutput struct {
	CapacityProvider capacityProviderView `json:"capacityProvider"`
}

func (h *Handler) handleUpdateCapacityProvider(
	_ context.Context,
	in *updateCapacityProviderInput,
) (*updateCapacityProviderOutput, error) {
	cp, err := h.Backend.UpdateCapacityProvider(UpdateCapacityProviderInput{
		Name:                     in.Name,
		Status:                   in.Status,
		AutoScalingGroupProvider: toAutoScalingGroupProvider(in.AutoScalingGroupProvider),
		Tags:                     in.Tags,
	})
	if err != nil {
		return nil, err
	}

	return &updateCapacityProviderOutput{CapacityProvider: toCapacityProviderView(*cp)}, nil
}
