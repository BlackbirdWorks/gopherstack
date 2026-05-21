package ecs

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ----- UpdateCluster -----

type updateClusterInput struct {
	Cluster                         string                `json:"cluster"`
	CapacityProviders               []string              `json:"capacityProviders,omitempty"`
	DefaultCapacityProviderStrategy []cpStrategyItemInput `json:"defaultCapacityProviderStrategy,omitempty"`
	Settings                        []clusterSettingView  `json:"settings,omitempty"`
}

type updateClusterOutput struct {
	Cluster clusterView `json:"cluster"`
}

func (h *Handler) handleUpdateCluster(
	_ context.Context,
	in *updateClusterInput,
) (*updateClusterOutput, error) {
	input := UpdateClusterInput{
		Cluster: in.Cluster,
	}

	if in.CapacityProviders != nil {
		input.CapacityProviders = in.CapacityProviders
	}

	for _, item := range in.DefaultCapacityProviderStrategy {
		input.DefaultCapacityProviderStrategy = append(
			input.DefaultCapacityProviderStrategy,
			CapacityProviderStrategyItem(item),
		)
	}

	for _, s := range in.Settings {
		input.Settings = append(input.Settings, ClusterSetting(s))
	}

	c, err := h.Backend.UpdateCluster(input)
	if err != nil {
		return nil, err
	}

	return &updateClusterOutput{Cluster: toClusterView(*c)}, nil
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

// ----- ListTaskDefinitionFamilies -----

type listTaskDefinitionFamiliesInput struct {
	FamilyPrefix string `json:"familyPrefix,omitempty"`
	Status       string `json:"status,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
	MaxResults   int    `json:"maxResults,omitempty"`
}

type listTaskDefinitionFamiliesOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	Families  []string `json:"families"`
}

func (h *Handler) handleListTaskDefinitionFamilies(
	_ context.Context,
	in *listTaskDefinitionFamiliesInput,
) (*listTaskDefinitionFamiliesOutput, error) {
	families, err := h.Backend.ListTaskDefinitionFamilies(in.FamilyPrefix, in.Status)
	if err != nil {
		return nil, err
	}

	sort.Strings(families)

	p := page.New(families, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listTaskDefinitionFamiliesOutput{Families: p.Data, NextToken: p.Next}, nil
}

// ----- StartTask -----

type startTaskInput struct {
	Cluster            string   `json:"cluster,omitempty"`
	TaskDefinition     string   `json:"taskDefinition"`
	Group              string   `json:"group,omitempty"`
	StartedBy          string   `json:"startedBy,omitempty"`
	ContainerInstances []string `json:"containerInstances"`
}

type startTaskOutput struct {
	Failures []failureView `json:"failures"`
	Tasks    []taskView    `json:"tasks"`
}

func (h *Handler) handleStartTask(
	_ context.Context,
	in *startTaskInput,
) (*startTaskOutput, error) {
	tasks, err := h.Backend.StartTask(StartTaskInput{
		Cluster:            in.Cluster,
		TaskDefinition:     in.TaskDefinition,
		ContainerInstances: in.ContainerInstances,
		Group:              in.Group,
		StartedBy:          in.StartedBy,
	})
	if err != nil {
		return nil, err
	}

	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, toTaskView(t))
	}

	return &startTaskOutput{Tasks: views, Failures: []failureView{}}, nil
}

// ----- ListServicesByNamespace -----

type listServicesByNamespaceInput struct {
	Namespace  string `json:"namespace,omitempty"`
	Cluster    string `json:"cluster,omitempty"`
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listServicesByNamespaceOutput struct {
	NextToken   string   `json:"nextToken,omitempty"`
	ServiceArns []string `json:"serviceArns"`
}

func (h *Handler) handleListServicesByNamespace(
	_ context.Context,
	in *listServicesByNamespaceInput,
) (*listServicesByNamespaceOutput, error) {
	arns, err := h.Backend.ListServicesByNamespace(in.Cluster, in.Namespace)
	if err != nil {
		return nil, err
	}

	p := page.New(arns, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listServicesByNamespaceOutput{ServiceArns: p.Data, NextToken: p.Next}, nil
}

// ----- TagResource -----

type tagResourceInput struct {
	ResourceArn string `json:"resourceArn"`
	Tags        []Tag  `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

// ----- UntagResource -----

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// ----- ListTagsForResource -----

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

// ----- ListServiceDeployments -----

type listServiceDeploymentsInput struct {
	Cluster    string `json:"cluster,omitempty"`
	Service    string `json:"service,omitempty"`
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listServiceDeploymentsOutput struct {
	NextToken             string   `json:"nextToken,omitempty"`
	ServiceDeploymentArns []string `json:"serviceDeploymentArns"`
}

func (h *Handler) handleListServiceDeployments(
	_ context.Context,
	in *listServiceDeploymentsInput,
) (*listServiceDeploymentsOutput, error) {
	arns, err := h.Backend.ListServiceDeployments(in.Cluster, in.Service)
	if err != nil {
		return nil, err
	}

	p := page.New(arns, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listServiceDeploymentsOutput{ServiceDeploymentArns: p.Data, NextToken: p.Next}, nil
}

// ----- StopServiceDeployment -----

type stopServiceDeploymentInput struct {
	ServiceDeploymentArn string `json:"serviceDeploymentArn"`
}

type stopServiceDeploymentOutput struct {
	ServiceDeployment serviceDeploymentView `json:"serviceDeployment"`
}

func (h *Handler) handleStopServiceDeployment(
	_ context.Context,
	in *stopServiceDeploymentInput,
) (*stopServiceDeploymentOutput, error) {
	sd, err := h.Backend.StopServiceDeployment(in.ServiceDeploymentArn)
	if err != nil {
		return nil, err
	}

	return &stopServiceDeploymentOutput{ServiceDeployment: toServiceDeploymentView(*sd)}, nil
}
