package ecs

import (
	"context"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// describeCapacityProviderIncludeTags is the AWS-defined `include` value that
// requests resource tags be returned alongside each CapacityProvider (see also
// describeClusterIncludeTags for the equivalent DescribeClusters option).
const describeCapacityProviderIncludeTags = "TAGS"

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
	Cluster           string   `json:"cluster,omitempty"`
	CapacityProviders []string `json:"capacityProviders,omitempty"`
	Include           []string `json:"include,omitempty"`
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
	providers, failures, err := h.Backend.DescribeCapacityProviders(in.CapacityProviders, in.Cluster)
	if err != nil {
		return nil, err
	}

	wantTags := false

	for _, opt := range in.Include {
		if strings.EqualFold(opt, describeCapacityProviderIncludeTags) {
			wantTags = true

			break
		}
	}

	views := make([]capacityProviderView, 0, len(providers))
	for _, cp := range providers {
		v := toCapacityProviderView(cp)
		v.Tags = nil

		if wantTags {
			tags, tagErr := h.Backend.ListTagsForResource(cp.CapacityProviderArn)
			if tagErr != nil {
				return nil, tagErr
			}

			v.Tags = tags
		}

		views = append(views, v)
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
//
// The real UpdateCapacityProviderRequest has only name, cluster,
// autoScalingGroupProvider, and managedInstancesProvider -- no status (only
// transitions via Create/DeleteCapacityProvider) or tags (via
// TagResource/UntagResource). autoScalingGroupProvider omits autoScalingGroupArn
// too, since the ASG can't be swapped after creation. managedInstancesProvider
// is not modeled by this backend.

type autoScalingGroupProviderUpdateInput struct {
	ManagedScaling               *managedScalingInput `json:"managedScaling,omitempty"`
	ManagedTerminationProtection string               `json:"managedTerminationProtection,omitempty"`
	ManagedDraining              string               `json:"managedDraining,omitempty"`
}

type updateCapacityProviderInput struct {
	AutoScalingGroupProvider *autoScalingGroupProviderUpdateInput `json:"autoScalingGroupProvider,omitempty"`
	Name                     string                               `json:"name"`
}

type updateCapacityProviderOutput struct {
	CapacityProvider capacityProviderView `json:"capacityProvider"`
}

func toAutoScalingGroupProviderUpdate(
	in *autoScalingGroupProviderUpdateInput,
) *AutoScalingGroupProviderUpdate {
	if in == nil {
		return nil
	}

	upd := &AutoScalingGroupProviderUpdate{
		ManagedTerminationProtection: in.ManagedTerminationProtection,
		ManagedDraining:              in.ManagedDraining,
	}

	if in.ManagedScaling != nil {
		upd.ManagedScaling = &ManagedScaling{
			Status:                    in.ManagedScaling.Status,
			TargetCapacityPercent:     in.ManagedScaling.TargetCapacityPercent,
			MinimumScalingStepSize:    in.ManagedScaling.MinimumScalingStepSize,
			MaximumScalingStepSize:    in.ManagedScaling.MaximumScalingStepSize,
			InstanceWarmupPeriod:      in.ManagedScaling.InstanceWarmupPeriod,
			TargetCapacityUtilization: in.ManagedScaling.TargetCapacityUtilization,
		}
	}

	return upd
}

func (h *Handler) handleUpdateCapacityProvider(
	_ context.Context,
	in *updateCapacityProviderInput,
) (*updateCapacityProviderOutput, error) {
	cp, err := h.Backend.UpdateCapacityProvider(UpdateCapacityProviderInput{
		Name:                     in.Name,
		AutoScalingGroupProvider: toAutoScalingGroupProviderUpdate(in.AutoScalingGroupProvider),
	})
	if err != nil {
		return nil, err
	}

	return &updateCapacityProviderOutput{CapacityProvider: toCapacityProviderView(*cp)}, nil
}
