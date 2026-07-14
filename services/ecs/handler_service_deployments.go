package ecs

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

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

// ----- ContinueServiceDeployment -----

type continueServiceDeploymentInput struct {
	HookID               string `json:"hookId"`
	ServiceDeploymentArn string `json:"serviceDeploymentArn"`
	Action               string `json:"action,omitempty"`
}

type continueServiceDeploymentOutput struct {
	ServiceDeploymentArn string `json:"serviceDeploymentArn"`
}

func (h *Handler) handleContinueServiceDeployment(
	_ context.Context,
	in *continueServiceDeploymentInput,
) (*continueServiceDeploymentOutput, error) {
	sd, err := h.Backend.ContinueServiceDeployment(in.ServiceDeploymentArn, in.HookID, in.Action)
	if err != nil {
		return nil, err
	}

	return &continueServiceDeploymentOutput{ServiceDeploymentArn: sd.ServiceDeploymentArn}, nil
}

// ----- DescribeServiceDeployments -----

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
