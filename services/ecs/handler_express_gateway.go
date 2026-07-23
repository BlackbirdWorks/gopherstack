package ecs

import (
	"context"
	"strings"
)

// describeExpressGatewayIncludeTags is the AWS-defined `include` value that
// requests resource tags be returned alongside the Express service (see also
// describeClusterIncludeTags for the equivalent DescribeClusters option).
const describeExpressGatewayIncludeTags = "TAGS"

// ----- Handler: UpdateExpressGatewayService -----

type updateExpressGatewayServiceInput struct {
	ServiceArn            string `json:"serviceArn"`
	ExecutionRoleArn      string `json:"executionRoleArn,omitempty"`
	InfrastructureRoleArn string `json:"infrastructureRoleArn,omitempty"`
}

type updateExpressGatewayServiceOutput struct {
	Service expressGatewayServiceView `json:"service"`
}

func (h *Handler) handleUpdateExpressGatewayService(
	_ context.Context,
	in *updateExpressGatewayServiceInput,
) (*updateExpressGatewayServiceOutput, error) {
	svc, err := h.Backend.UpdateExpressGatewayService(UpdateExpressGatewayServiceInput{
		ServiceArn:            in.ServiceArn,
		ExecutionRoleArn:      in.ExecutionRoleArn,
		InfrastructureRoleArn: in.InfrastructureRoleArn,
	})
	if err != nil {
		return nil, err
	}

	return &updateExpressGatewayServiceOutput{Service: toExpressGatewayServiceView(*svc)}, nil
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
	ServiceArn string   `json:"serviceArn"`
	Include    []string `json:"include,omitempty"`
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

	view := toExpressGatewayServiceView(*svc)

	wantTags := false

	for _, opt := range in.Include {
		if strings.EqualFold(opt, describeExpressGatewayIncludeTags) {
			wantTags = true

			break
		}
	}

	if !wantTags {
		view.Tags = nil
	}

	return &describeExpressGatewayServiceOutput{Service: view}, nil
}
