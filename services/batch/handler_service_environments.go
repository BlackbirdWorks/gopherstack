package batch

import (
	"context"
	"fmt"
)

// --- ServiceEnvironment handlers ---

type createServiceEnvironmentInput struct {
	Tags                   map[string]string `json:"tags"`
	ServiceEnvironmentName string            `json:"serviceEnvironmentName"`
	ServiceEnvironmentType string            `json:"serviceEnvironmentType"`
	State                  string            `json:"state"`
}

type createServiceEnvironmentOutput struct {
	ServiceEnvironmentArn  string `json:"serviceEnvironmentArn"`
	ServiceEnvironmentName string `json:"serviceEnvironmentName"`
}

func (h *Handler) handleCreateServiceEnvironment(
	ctx context.Context,
	in *createServiceEnvironmentInput,
) (*createServiceEnvironmentOutput, error) {
	if in.ServiceEnvironmentName == "" {
		return nil, fmt.Errorf("%w: serviceEnvironmentName is required", ErrValidation)
	}

	if in.ServiceEnvironmentType == "" {
		return nil, fmt.Errorf("%w: serviceEnvironmentType is required", ErrValidation)
	}

	se, err := h.Backend.CreateServiceEnvironment(
		ctx,
		in.ServiceEnvironmentName,
		in.ServiceEnvironmentType,
		in.State,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createServiceEnvironmentOutput{
		ServiceEnvironmentArn:  se.ServiceEnvironmentArn,
		ServiceEnvironmentName: se.ServiceEnvironmentName,
	}, nil
}

type deleteServiceEnvironmentInput struct {
	ServiceEnvironment string `json:"serviceEnvironment"`
}

func (h *Handler) handleDeleteServiceEnvironment(
	ctx context.Context,
	in *deleteServiceEnvironmentInput,
) (*emptyOutput, error) {
	if in.ServiceEnvironment == "" {
		return nil, fmt.Errorf("%w: serviceEnvironment is required", ErrValidation)
	}

	if err := h.Backend.DeleteServiceEnvironment(ctx, in.ServiceEnvironment); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- DescribeServiceEnvironments handler ---

type describeServiceEnvironmentsInput struct {
	ServiceEnvironments []string `json:"serviceEnvironments"`
}

type describeServiceEnvironmentsOutput struct {
	ServiceEnvironments []*ServiceEnvironment `json:"serviceEnvironments"`
}

func (h *Handler) handleDescribeServiceEnvironments(
	ctx context.Context,
	in *describeServiceEnvironmentsInput,
) (*describeServiceEnvironmentsOutput, error) {
	list := h.Backend.DescribeServiceEnvironments(ctx, in.ServiceEnvironments)

	return &describeServiceEnvironmentsOutput{ServiceEnvironments: list}, nil
}

// --- UpdateServiceEnvironment handler ---

type updateServiceEnvironmentInput struct {
	ServiceEnvironment string `json:"serviceEnvironment"`
	State              string `json:"state"`
}

type updateServiceEnvironmentOutput struct {
	ServiceEnvironmentArn  string `json:"serviceEnvironmentArn"`
	ServiceEnvironmentName string `json:"serviceEnvironmentName"`
}

func (h *Handler) handleUpdateServiceEnvironment(
	ctx context.Context,
	in *updateServiceEnvironmentInput,
) (*updateServiceEnvironmentOutput, error) {
	if in.ServiceEnvironment == "" {
		return nil, fmt.Errorf("%w: serviceEnvironment is required", ErrValidation)
	}

	se, err := h.Backend.UpdateServiceEnvironment(ctx, in.ServiceEnvironment, in.State)
	if err != nil {
		return nil, err
	}

	return &updateServiceEnvironmentOutput{
		ServiceEnvironmentArn:  se.ServiceEnvironmentArn,
		ServiceEnvironmentName: se.ServiceEnvironmentName,
	}, nil
}
