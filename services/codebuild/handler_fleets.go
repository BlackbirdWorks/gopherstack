package codebuild

import (
	"context"
	"fmt"
)

type createFleetInput struct {
	Tags                 map[string]string     `json:"tags"`
	ComputeConfiguration *ComputeConfiguration `json:"computeConfiguration"`
	ProxyConfiguration   *ProxyConfiguration   `json:"proxyConfiguration"`
	VpcConfig            *VpcConfig            `json:"vpcConfig"`
	ScalingConfiguration *ScalingConfiguration `json:"scalingConfiguration"`
	Name                 string                `json:"name"`
	ComputeType          string                `json:"computeType"`
	EnvironmentType      string                `json:"environmentType"`
	OverflowBehavior     string                `json:"overflowBehavior"`
	ImageID              string                `json:"imageId"`
	FleetServiceRole     string                `json:"fleetServiceRole"`
	BaseCapacity         int32                 `json:"baseCapacity"`
}

type createFleetOutput struct {
	Fleet *Fleet `json:"fleet"`
}

func (h *Handler) handleCreateFleet(
	_ context.Context,
	in *createFleetInput,
) (*createFleetOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	f, err := h.Backend.CreateFleet(in.Name, in.BaseCapacity, CreateFleetOptions{
		ComputeType:          in.ComputeType,
		EnvironmentType:      in.EnvironmentType,
		OverflowBehavior:     in.OverflowBehavior,
		ImageID:              in.ImageID,
		FleetServiceRole:     in.FleetServiceRole,
		ComputeConfiguration: in.ComputeConfiguration,
		ProxyConfiguration:   in.ProxyConfiguration,
		VpcConfig:            in.VpcConfig,
		ScalingConfiguration: in.ScalingConfiguration,
		Tags:                 in.Tags,
	})
	if err != nil {
		return nil, err
	}

	return &createFleetOutput{Fleet: f}, nil
}

type batchGetFleetsInput struct {
	Names []string `json:"names"`
}

type batchGetFleetsOutput struct {
	Fleets         []*Fleet `json:"fleets"`
	FleetsNotFound []string `json:"fleetsNotFound"`
}

func (h *Handler) handleBatchGetFleets(
	_ context.Context,
	in *batchGetFleetsInput,
) (*batchGetFleetsOutput, error) {
	found, notFound := h.Backend.BatchGetFleets(in.Names)

	return &batchGetFleetsOutput{
		Fleets:         found,
		FleetsNotFound: notFound,
	}, nil
}

type deleteFleetInput struct {
	Arn string `json:"arn"`
}

type deleteFleetOutput struct{}

func (h *Handler) handleDeleteFleet(_ context.Context, in *deleteFleetInput) (*deleteFleetOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteFleet(in.Arn); err != nil {
		return nil, err
	}

	return &deleteFleetOutput{}, nil
}

type listFleetsInput struct {
	NextToken  string `json:"nextToken"`
	SortBy     string `json:"sortBy"`
	SortOrder  string `json:"sortOrder"`
	MaxResults int32  `json:"maxResults"`
}

type listFleetsOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	Fleets    []string `json:"fleets"`
}

func (h *Handler) handleListFleets(_ context.Context, in *listFleetsInput) (*listFleetsOutput, error) {
	arns := h.Backend.ListFleetsSortedBy(in.SortBy)

	pg, err := paginateIDs(arns, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listFleetsOutput{Fleets: pg.Data, NextToken: pg.Next}, nil
}

type updateFleetInput struct {
	ComputeConfiguration *ComputeConfiguration `json:"computeConfiguration"`
	ProxyConfiguration   *ProxyConfiguration   `json:"proxyConfiguration"`
	VpcConfig            *VpcConfig            `json:"vpcConfig"`
	ScalingConfiguration *ScalingConfiguration `json:"scalingConfiguration"`
	Arn                  string                `json:"arn"`
	ComputeType          string                `json:"computeType"`
	EnvironmentType      string                `json:"environmentType"`
	OverflowBehavior     string                `json:"overflowBehavior"`
	ImageID              string                `json:"imageId"`
	FleetServiceRole     string                `json:"fleetServiceRole"`
	BaseCapacity         int32                 `json:"baseCapacity"`
}

type updateFleetOutput struct {
	Fleet *Fleet `json:"fleet"`
}

func (h *Handler) handleUpdateFleet(_ context.Context, in *updateFleetInput) (*updateFleetOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", errInvalidRequest)
	}

	f, err := h.Backend.UpdateFleet(in.Arn, in.BaseCapacity, UpdateFleetOptions{
		ComputeType:          in.ComputeType,
		EnvironmentType:      in.EnvironmentType,
		OverflowBehavior:     in.OverflowBehavior,
		ImageID:              in.ImageID,
		FleetServiceRole:     in.FleetServiceRole,
		ComputeConfiguration: in.ComputeConfiguration,
		ProxyConfiguration:   in.ProxyConfiguration,
		VpcConfig:            in.VpcConfig,
		ScalingConfiguration: in.ScalingConfiguration,
	})
	if err != nil {
		return nil, err
	}

	return &updateFleetOutput{Fleet: f}, nil
}
