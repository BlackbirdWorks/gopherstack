package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// InferenceExperiment handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name    string      `json:"Name"`
		Type    string      `json:"Type"`
		RoleArn string      `json:"RoleArn"`
		Tags    []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateInferenceExperiment(ctx, req.Name, req.Type, req.RoleArn, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

func (h *Handler) handleDescribeInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeInferenceExperiment(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleStopInferenceExperiment(ctx context.Context, body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	return h.Backend.StopInferenceExperiment(ctx, req.Name)
}

func (h *Handler) handleDeleteInferenceExperiment(ctx context.Context, body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	return h.Backend.DeleteInferenceExperiment(ctx, req.Name)
}

func (h *Handler) handleStartInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.StartInferenceExperiment(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

func (h *Handler) handleUpdateInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateInferenceExperiment(ctx, req.Name, req.Description)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

func (h *Handler) handleListInferenceExperiments(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	exps, nextToken := h.Backend.ListInferenceExperiments(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(exps))
	for _, e := range exps {
		entry := map[string]any{
			keyGenericName:      e.Name,
			"Arn":               e.Arn,
			keyStatus:           e.Status,
			keyCreationTime:     epochSeconds(e.CreationTime),
			keyLastModifiedTime: epochSeconds(e.LastModifiedTime),
		}
		if e.Type != "" {
			entry["Type"] = e.Type
		}

		items = append(items, entry)
	}

	return listResp("InferenceExperiments", items, nextToken)
}
