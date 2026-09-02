package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createModelRequest is the request body for CreateModel.
type createModelRequest struct {
	PrimaryContainer         *ContainerDefinition      `json:"PrimaryContainer"`
	VpcConfig                *VpcConfig                `json:"VpcConfig,omitempty"`
	InferenceExecutionConfig *InferenceExecutionConfig `json:"InferenceExecutionConfig,omitempty"`
	ModelName                string                    `json:"ModelName"`
	ExecutionRoleArn         string                    `json:"ExecutionRoleArn"`
	Tags                     []tagObject               `json:"Tags"`
	Containers               []ContainerDefinition     `json:"Containers"`
	EnableNetworkIsolation   bool                      `json:"EnableNetworkIsolation,omitempty"`
}

// modelSummary is a summary of a SageMaker model for list responses.
type modelSummary struct {
	ModelName    string  `json:"ModelName"`
	ModelArn     string  `json:"ModelArn"`
	CreationTime float64 `json:"CreationTime"`
}

// describeModelResponse is the response body for DescribeModel.
type describeModelResponse struct {
	PrimaryContainer         *ContainerDefinition      `json:"PrimaryContainer,omitempty"`
	VpcConfig                *VpcConfig                `json:"VpcConfig,omitempty"`
	InferenceExecutionConfig *InferenceExecutionConfig `json:"InferenceExecutionConfig,omitempty"`
	ModelArn                 string                    `json:"ModelArn"`
	ModelName                string                    `json:"ModelName"`
	ExecutionRoleArn         string                    `json:"ExecutionRoleArn"`
	Containers               []ContainerDefinition     `json:"Containers,omitempty"`
	CreationTime             float64                   `json:"CreationTime"`
	EnableNetworkIsolation   bool                      `json:"EnableNetworkIsolation,omitempty"`
}

func (h *Handler) handleCreateModel(ctx context.Context, body []byte) ([]byte, error) {
	var req createModelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	if req.PrimaryContainer != nil && len(req.Containers) > 0 {
		return nil, fmt.Errorf(
			"%w: provide either PrimaryContainer or Containers, not both",
			errInvalidRequest,
		)
	}

	tags := fromTagObjects(req.Tags)

	m, err := h.Backend.CreateModel(
		ctx,
		req.ModelName,
		req.ExecutionRoleArn,
		req.PrimaryContainer,
		req.Containers,
		tags,
	)
	if err != nil {
		return nil, err
	}

	if req.VpcConfig != nil || req.EnableNetworkIsolation || req.InferenceExecutionConfig != nil {
		if extErr := h.Backend.SetModelExtras(
			ctx,
			req.ModelName,
			req.VpcConfig,
			req.EnableNetworkIsolation,
			req.InferenceExecutionConfig,
		); extErr != nil {
			return nil, extErr
		}
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: created model", "name", m.ModelName, "arn", m.ModelARN)

	return json.Marshal(map[string]string{"ModelArn": m.ModelARN})
}

// describeModelRequest is the request body for DescribeModel.
type describeModelRequest struct {
	ModelName string `json:"ModelName"`
}

func (h *Handler) handleDescribeModel(ctx context.Context, body []byte) ([]byte, error) {
	var req describeModelRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	m, err := h.Backend.DescribeModel(ctx, req.ModelName)
	if err != nil {
		return nil, err
	}

	resp := describeModelResponse{
		ModelName:                m.ModelName,
		ModelArn:                 m.ModelARN,
		ExecutionRoleArn:         m.ExecutionRoleARN,
		CreationTime:             epochSeconds(m.CreationTime),
		PrimaryContainer:         m.PrimaryContainer,
		Containers:               m.Containers,
		VpcConfig:                m.VpcConfig,
		EnableNetworkIsolation:   m.EnableNetworkIsolation,
		InferenceExecutionConfig: m.InferenceExecutionConfig,
	}

	if len(resp.Containers) == 0 {
		resp.Containers = nil
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListModels(ctx context.Context, body []byte) ([]byte, error) {
	var req nameTimeListRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	filter := req.toFilter()
	// ListModelsInput.CreationTimeAfter's own doc: "a creation time greater
	// than or equal to the specified time" -- inclusive, unlike this
	// family's shared default.
	filter.AfterInclusive = true

	models, nextToken := h.Backend.ListModels(ctx, req.NextToken, filter)
	summaries := make([]modelSummary, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, modelSummary{
			ModelName:    m.ModelName,
			ModelArn:     m.ModelARN,
			CreationTime: epochSeconds(m.CreationTime),
		})
	}

	resp := map[string]any{"Models": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deleteModelRequest is the request body for DeleteModel.
type deleteModelRequest struct {
	ModelName string `json:"ModelName"`
}

func (h *Handler) handleDeleteModel(ctx context.Context, body []byte) error {
	var req deleteModelRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelName == "" {
		return fmt.Errorf("%w: ModelName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteModel(ctx, req.ModelName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted model", "name", req.ModelName)

	return nil
}
