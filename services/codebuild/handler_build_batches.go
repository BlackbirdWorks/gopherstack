package codebuild

import (
	"context"
	"fmt"
)

type batchGetBuildBatchesInput struct {
	IDs []string `json:"ids"`
}

type batchGetBuildBatchesOutput struct {
	BuildBatches         []*BuildBatch `json:"buildBatches"`
	BuildBatchesNotFound []string      `json:"buildBatchesNotFound"`
}

func (h *Handler) handleBatchGetBuildBatches(
	_ context.Context,
	in *batchGetBuildBatchesInput,
) (*batchGetBuildBatchesOutput, error) {
	found, notFound := h.Backend.BatchGetBuildBatches(in.IDs)

	return &batchGetBuildBatchesOutput{
		BuildBatches:         found,
		BuildBatchesNotFound: notFound,
	}, nil
}

type deleteBuildBatchInput struct {
	ID string `json:"id"`
}

type deleteBuildBatchOutput struct {
	StatusCode string `json:"statusCode"`
}

func (h *Handler) handleDeleteBuildBatch(
	_ context.Context,
	in *deleteBuildBatchInput,
) (*deleteBuildBatchOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteBuildBatch(in.ID); err != nil {
		return nil, err
	}

	return &deleteBuildBatchOutput{StatusCode: buildStatusSucceeded}, nil
}

type listBuildBatchesInput struct{}

type listBuildBatchesOutput struct {
	IDs []string `json:"ids"`
}

func (h *Handler) handleListBuildBatches(_ context.Context, _ *listBuildBatchesInput) (*listBuildBatchesOutput, error) {
	return &listBuildBatchesOutput{IDs: h.Backend.ListBuildBatches()}, nil
}

type listBuildBatchesForProjectInput struct {
	ProjectName string `json:"projectName"`
}

type listBuildBatchesForProjectOutput struct {
	IDs []string `json:"ids"`
}

func (h *Handler) handleListBuildBatchesForProject(
	_ context.Context,
	in *listBuildBatchesForProjectInput,
) (*listBuildBatchesForProjectOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	ids, err := h.Backend.ListBuildBatchesForProject(in.ProjectName)
	if err != nil {
		return nil, err
	}

	return &listBuildBatchesForProjectOutput{IDs: ids}, nil
}

type retryBuildBatchInput struct {
	ID string `json:"id"`
}

type retryBuildBatchOutput struct {
	BuildBatch *BuildBatch `json:"buildBatch"`
}

func (h *Handler) handleRetryBuildBatch(_ context.Context, in *retryBuildBatchInput) (*retryBuildBatchOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	bb, err := h.Backend.RetryBuildBatch(in.ID)
	if err != nil {
		return nil, err
	}

	return &retryBuildBatchOutput{BuildBatch: bb}, nil
}

type startBuildBatchInput struct {
	ProjectName string `json:"projectName"`
}

type startBuildBatchOutput struct {
	BuildBatch *BuildBatch `json:"buildBatch"`
}

func (h *Handler) handleStartBuildBatch(_ context.Context, in *startBuildBatchInput) (*startBuildBatchOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	bb, err := h.Backend.StartBuildBatch(in.ProjectName)
	if err != nil {
		return nil, err
	}

	return &startBuildBatchOutput{BuildBatch: bb}, nil
}

type stopBuildBatchInput struct {
	ID string `json:"id"`
}

type stopBuildBatchOutput struct {
	BuildBatch *BuildBatch `json:"buildBatch"`
}

func (h *Handler) handleStopBuildBatch(_ context.Context, in *stopBuildBatchInput) (*stopBuildBatchOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	bb, err := h.Backend.StopBuildBatch(in.ID)
	if err != nil {
		return nil, err
	}

	return &stopBuildBatchOutput{BuildBatch: bb}, nil
}
