package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Pipeline version history + related execution-definition/update operations.
// ---------------------------------------------------------------------------

func (h *Handler) dispatchPipelineVersionOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "ListPipelineVersions":
		r, err := h.handleListPipelineVersions(ctx, body)

		return r, true, err
	case "UpdatePipelineVersion":
		r, err := h.handleUpdatePipelineVersion(ctx, body)

		return r, true, err
	case "DescribePipelineDefinitionForExecution":
		r, err := h.handleDescribePipelineDefinitionForExecution(ctx, body)

		return r, true, err
	case "UpdatePipelineExecution":
		r, err := h.handleUpdatePipelineExecution(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) handleListPipelineVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineName string `json:"PipelineName"`
		NextToken    string `json:"NextToken,omitempty"`
		MaxResults   int32  `json:"MaxResults,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	versions, nextToken, err := h.Backend.ListPipelineVersions(ctx, req.PipelineName, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(versions))

	for _, v := range versions {
		summary := map[string]any{
			keyPipelineArn:      v.PipelineArn,
			"PipelineVersionId": v.PipelineVersionID,
			keyCreationTime:     epochSeconds(v.CreationTime),
		}
		if v.PipelineVersionDisplayName != "" {
			summary["PipelineVersionDisplayName"] = v.PipelineVersionDisplayName
		}
		if v.PipelineVersionDescription != "" {
			summary["PipelineVersionDescription"] = v.PipelineVersionDescription
		}
		if v.LastExecutionPipelineExecutionArn != "" {
			summary["LastExecutionPipelineExecutionArn"] = v.LastExecutionPipelineExecutionArn
		}

		summaries = append(summaries, summary)
	}

	return listResp("PipelineVersionSummaries", summaries, nextToken)
}

func (h *Handler) handleUpdatePipelineVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineArn                string `json:"PipelineArn"`
		PipelineVersionDescription string `json:"PipelineVersionDescription,omitempty"`
		PipelineVersionDisplayName string `json:"PipelineVersionDisplayName,omitempty"`
		PipelineVersionID          int64  `json:"PipelineVersionId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineArn == "" {
		return nil, fmt.Errorf("%w: PipelineArn is required", errInvalidRequest)
	}

	if req.PipelineVersionID == 0 {
		return nil, fmt.Errorf("%w: PipelineVersionId is required", errInvalidRequest)
	}

	v, err := h.Backend.UpdatePipelineVersion(
		ctx, req.PipelineArn, req.PipelineVersionID, req.PipelineVersionDescription, req.PipelineVersionDisplayName,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyPipelineArn:      v.PipelineArn,
		"PipelineVersionId": v.PipelineVersionID,
	})
}

func (h *Handler) handleDescribePipelineDefinitionForExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	definition, creationTime, err := h.Backend.DescribePipelineDefinitionForExecution(ctx, req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"PipelineDefinition": definition,
		keyCreationTime:      epochSeconds(creationTime),
	})
}

func (h *Handler) handleUpdatePipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ParallelismConfiguration     *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
		PipelineExecutionArn         string                    `json:"PipelineExecutionArn"`
		PipelineExecutionDescription string                    `json:"PipelineExecutionDescription,omitempty"`
		PipelineExecutionDisplayName string                    `json:"PipelineExecutionDisplayName,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	pe, err := h.Backend.UpdatePipelineExecution(
		ctx,
		req.PipelineExecutionArn,
		req.PipelineExecutionDescription,
		req.PipelineExecutionDisplayName,
		req.ParallelismConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyPipelineExecutionArn: pe.PipelineExecutionArn})
}
