package sagemaker

import (
	"context"
	"encoding/json"
)

// ---------------------------------------------------------------------------
// InferenceComponent handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags          map[string]string `json:"Tags"`
		RuntimeConfig *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"RuntimeConfig"`
		InferenceComponentName string `json:"InferenceComponentName"`
		EndpointName           string `json:"EndpointName"`
		VariantName            string `json:"VariantName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.RuntimeConfig != nil {
		copyCount = req.RuntimeConfig.CopyCount
	}

	c, err := h.Backend.CreateInferenceComponent(ctx, CreateInferenceComponentOptions{
		InferenceComponentName: req.InferenceComponentName,
		EndpointName:           req.EndpointName,
		VariantName:            req.VariantName,
		CopyCount:              copyCount,
		Tags:                   req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

func (h *Handler) handleDescribeInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		InferenceComponentName string `json:"InferenceComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(c)
}

func (h *Handler) handleListInferenceComponents(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		EndpointName string `json:"EndpointNameEquals"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	components, next := h.Backend.ListInferenceComponents(ctx, req.EndpointName, req.NextToken)

	items := make([]map[string]any, 0, len(components))
	for _, c := range components {
		items = append(items, map[string]any{
			"InferenceComponentName":   c.InferenceComponentName,
			keyInferenceComponentArn:   c.InferenceComponentArn,
			"EndpointName":             c.EndpointName,
			"InferenceComponentStatus": c.InferenceComponentStatus,
			keyCreationTime:            c.CreationTime,
			keyLastModifiedTime:        c.LastModifiedTime,
		})
	}

	return listResp("InferenceComponents", items, next)
}

func (h *Handler) handleUpdateInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		RuntimeConfig *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"RuntimeConfig"`
		InferenceComponentName string `json:"InferenceComponentName"`
		VariantName            string `json:"VariantName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.RuntimeConfig != nil {
		copyCount = req.RuntimeConfig.CopyCount
	}

	if err := h.Backend.UpdateInferenceComponent(
		ctx,
		req.InferenceComponentName,
		req.VariantName,
		copyCount,
	); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

func (h *Handler) handleUpdateInferenceComponentRuntimeConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DesiredRuntimeConfig *struct {
			CopyCount int `json:"CopyCount"`
		} `json:"DesiredRuntimeConfig"`
		InferenceComponentName string `json:"InferenceComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	copyCount := 0
	if req.DesiredRuntimeConfig != nil {
		copyCount = req.DesiredRuntimeConfig.CopyCount
	}

	if err := h.Backend.UpdateInferenceComponentRuntimeConfig(ctx, req.InferenceComponentName, copyCount); err != nil {
		return nil, err
	}

	c, err := h.Backend.DescribeInferenceComponent(ctx, req.InferenceComponentName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyInferenceComponentArn: c.InferenceComponentArn,
	})
}

func (h *Handler) handleDeleteInferenceComponent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		InferenceComponentName string `json:"InferenceComponentName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteInferenceComponent(ctx, req.InferenceComponentName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
