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

// serviceDeploymentBriefView mirrors types.ServiceDeploymentBrief. Alarms,
// DeploymentCircuitBreaker and DeploymentConfiguration are absent: this
// backend doesn't model deployment alarms/circuit-breaker/configuration on a
// ServiceDeployment record, so there is nothing honest to source them from.
type serviceDeploymentBriefView struct {
	ClusterArn               string  `json:"clusterArn,omitempty"`
	ServiceArn               string  `json:"serviceArn,omitempty"`
	ServiceDeploymentArn     string  `json:"serviceDeploymentArn"`
	Status                   string  `json:"status,omitempty"`
	StatusReason             string  `json:"statusReason,omitempty"`
	TargetServiceRevisionArn string  `json:"targetServiceRevisionArn,omitempty"`
	CreatedAt                float64 `json:"createdAt,omitempty"`
	StartedAt                float64 `json:"startedAt,omitempty"`
	FinishedAt               float64 `json:"finishedAt,omitempty"`
}

func toServiceDeploymentBriefView(sd ServiceDeployment) serviceDeploymentBriefView {
	v := serviceDeploymentBriefView{
		ServiceDeploymentArn:     sd.ServiceDeploymentArn,
		ClusterArn:               sd.ClusterArn,
		ServiceArn:               sd.ServiceArn,
		Status:                   sd.Status,
		StatusReason:             sd.StatusReason,
		TargetServiceRevisionArn: sd.TargetServiceRevisionArn,
	}

	if sd.CreatedAt != nil {
		// The real full ServiceDeployment type has no separate "started"
		// timestamp either -- only CreatedAt/FinishedAt -- so Brief.StartedAt
		// carries the same moment this backend tracks as CreatedAt.
		v.CreatedAt = float64(sd.CreatedAt.Unix())
		v.StartedAt = v.CreatedAt
	}

	if isTerminalServiceDeploymentStatus(sd.Status) && sd.UpdatedAt != nil {
		v.FinishedAt = float64(sd.UpdatedAt.Unix())
	}

	return v
}

func isTerminalServiceDeploymentStatus(status string) bool {
	switch status {
	case "SUCCESSFUL", statusStopped:
		return true
	default:
		return false
	}
}

type listServiceDeploymentsOutput struct {
	NextToken          string                       `json:"nextToken,omitempty"`
	ServiceDeployments []serviceDeploymentBriefView `json:"serviceDeployments"`
}

func (h *Handler) handleListServiceDeployments(
	_ context.Context,
	in *listServiceDeploymentsInput,
) (*listServiceDeploymentsOutput, error) {
	deployments, err := h.Backend.ListServiceDeployments(in.Cluster, in.Service)
	if err != nil {
		return nil, err
	}

	p := page.New(deployments, in.NextToken, in.MaxResults, defaultECSMaxResults)

	views := make([]serviceDeploymentBriefView, 0, len(p.Data))
	for _, sd := range p.Data {
		views = append(views, toServiceDeploymentBriefView(sd))
	}

	return &listServiceDeploymentsOutput{ServiceDeployments: views, NextToken: p.Next}, nil
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
