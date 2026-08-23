package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// modelCardExportOpsSupported returns the real stateful operations
// implemented in this file (ModelCard export job family).
func modelCardExportOpsSupported() []string {
	return []string{
		"CreateModelCardExportJob",
		"DescribeModelCardExportJob",
	}
}

// dispatchModelCardExportOps dispatches the ModelCard export job family of
// real stateful operations.
func (h *Handler) dispatchModelCardExportOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateModelCardExportJob":
		r, err := h.handleCreateModelCardExportJob(ctx, body)

		return r, true, err
	case "DescribeModelCardExportJob":
		r, err := h.handleDescribeModelCardExportJob(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

// createModelCardExportJobInput mirrors CreateModelCardExportJobInput
// (api_op_CreateModelCardExportJob.go): ModelCardExportJobName/ModelCardName/
// OutputConfig are required, ModelCardVersion optional. ModelCardName and
// OutputConfig.S3OutputPath are validated required in the backend
// (CreateModelCardExportJob, modelcard_export.go), not duplicated here.
type createModelCardExportJobInput struct {
	ModelCardExportJobName string `json:"ModelCardExportJobName"`
	ModelCardName          string `json:"ModelCardName"`
	OutputConfig           struct {
		S3OutputPath string `json:"S3OutputPath"`
	} `json:"OutputConfig"`
	ModelCardVersion int `json:"ModelCardVersion"`
}

func (h *Handler) handleCreateModelCardExportJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createModelCardExportJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardExportJobName == "" {
		return nil, fmt.Errorf("%w: ModelCardExportJobName is required", errInvalidRequest)
	}

	job, err := h.Backend.CreateModelCardExportJob(
		ctx, req.ModelCardExportJobName, req.ModelCardName, req.ModelCardVersion, req.OutputConfig.S3OutputPath,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardExportJobArn: job.ModelCardExportJobArn})
}

// describeModelCardExportJobInput mirrors DescribeModelCardExportJobInput
// (api_op_DescribeModelCardExportJob.go): ModelCardExportJobArn is its sole,
// required member.
type describeModelCardExportJobInput struct {
	ModelCardExportJobArn string `json:"ModelCardExportJobArn"`
}

func (h *Handler) handleDescribeModelCardExportJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeModelCardExportJobInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardExportJobArn == "" {
		return nil, fmt.Errorf("%w: ModelCardExportJobArn is required", errInvalidRequest)
	}

	job, err := h.Backend.DescribeModelCardExportJob(ctx, req.ModelCardExportJobArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"CreatedAt":              epochSeconds(job.CreatedAt),
		"LastModifiedAt":         epochSeconds(job.LastModifiedAt),
		keyModelCardExportJobArn: job.ModelCardExportJobArn,
		"ModelCardExportJobName": job.ModelCardExportJobName,
		keyModelCardNameField:    job.ModelCardName,
		keyModelCardVersion:      job.ModelCardVersion,
		keyStatus:                job.Status,
		keyOutputConfig:          map[string]any{"S3OutputPath": job.S3OutputPath},
		"ExportArtifacts":        map[string]any{"S3ExportArtifacts": job.S3ExportArtifacts},
	})
}
