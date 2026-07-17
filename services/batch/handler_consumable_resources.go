package batch

import (
	"context"
	"fmt"
)

// --- ConsumableResource handlers ---

type createConsumableResourceInput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType"`
	TotalQuantity          int64             `json:"totalQuantity"`
}

type createConsumableResourceOutput struct {
	ConsumableResourceArn  string `json:"consumableResourceArn"`
	ConsumableResourceName string `json:"consumableResourceName"`
}

func (h *Handler) handleCreateConsumableResource(
	ctx context.Context,
	in *createConsumableResourceInput,
) (*createConsumableResourceOutput, error) {
	if in.ConsumableResourceName == "" {
		return nil, fmt.Errorf("%w: consumableResourceName is required", ErrValidation)
	}

	cr, err := h.Backend.CreateConsumableResource(
		ctx,
		in.ConsumableResourceName,
		in.ResourceType,
		in.TotalQuantity,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
	}, nil
}

type deleteConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
}

func (h *Handler) handleDeleteConsumableResource(
	ctx context.Context,
	in *deleteConsumableResourceInput,
) (*emptyOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	if err := h.Backend.DeleteConsumableResource(ctx, in.ConsumableResource); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type describeConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
}

type describeConsumableResourceOutput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceArn  string            `json:"consumableResourceArn"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType,omitempty"`
	CreatedAt              int64             `json:"createdAt"`
	TotalQuantity          int64             `json:"totalQuantity"`
	AvailableQuantity      int64             `json:"availableQuantity"`
	InUseQuantity          int64             `json:"inUseQuantity"`
}

func (h *Handler) handleDescribeConsumableResource(
	ctx context.Context,
	in *describeConsumableResourceInput,
) (*describeConsumableResourceOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	cr, err := h.Backend.DescribeConsumableResource(ctx, in.ConsumableResource)
	if err != nil {
		return nil, err
	}

	return &describeConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
		ResourceType:           cr.ResourceType,
		Tags:                   tagsOrEmpty(cr.Tags),
		CreatedAt:              cr.CreatedAt,
		TotalQuantity:          cr.TotalQuantity,
		AvailableQuantity:      cr.AvailableQuantity,
		InUseQuantity:          cr.InUseQuantity,
	}, nil
}

// --- UpdateConsumableResource handler ---

type updateConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
	Operation          string `json:"operation"`
	Quantity           int64  `json:"quantity"`
}

type updateConsumableResourceOutput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceArn  string            `json:"consumableResourceArn"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType,omitempty"`
	CreatedAt              int64             `json:"createdAt"`
	TotalQuantity          int64             `json:"totalQuantity"`
	AvailableQuantity      int64             `json:"availableQuantity"`
	InUseQuantity          int64             `json:"inUseQuantity"`
}

func (h *Handler) handleUpdateConsumableResource(
	ctx context.Context,
	in *updateConsumableResourceInput,
) (*updateConsumableResourceOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	cr, err := h.Backend.UpdateConsumableResource(ctx, in.ConsumableResource, in.Operation, in.Quantity)
	if err != nil {
		return nil, err
	}

	return &updateConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
		ResourceType:           cr.ResourceType,
		Tags:                   tagsOrEmpty(cr.Tags),
		CreatedAt:              cr.CreatedAt,
		TotalQuantity:          cr.TotalQuantity,
		AvailableQuantity:      cr.AvailableQuantity,
		InUseQuantity:          cr.InUseQuantity,
	}, nil
}

// --- ListConsumableResources handler ---

type listConsumableResourcesOutput struct {
	ConsumableResourceSummaryList []*ConsumableResource `json:"consumableResourceSummaryList"`
}

func (h *Handler) handleListConsumableResources(
	ctx context.Context,
	_ *struct{},
) (*listConsumableResourcesOutput, error) {
	list := h.Backend.ListConsumableResources(ctx)

	return &listConsumableResourcesOutput{ConsumableResourceSummaryList: list}, nil
}

type listJobsByConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
}

type listJobsByConsumableResourceOutput struct {
	Jobs []*Job `json:"jobs"`
}

func (h *Handler) handleListJobsByConsumableResource(
	ctx context.Context,
	in *listJobsByConsumableResourceInput,
) (*listJobsByConsumableResourceOutput, error) {
	jobs, err := h.Backend.ListJobsByConsumableResource(ctx, in.ConsumableResource)
	if err != nil {
		return nil, err
	}

	return &listJobsByConsumableResourceOutput{Jobs: jobs}, nil
}
