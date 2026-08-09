package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// PartnerApp handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags              []tagObject     `json:"Tags"`
		Name              string          `json:"Name"`
		Type              string          `json:"Type"`
		ExecutionRoleArn  string          `json:"ExecutionRoleArn,omitempty"`
		AuthType          string          `json:"AuthType,omitempty"`
		Tier              string          `json:"Tier,omitempty"`
		ApplicationConfig json.RawMessage `json:"ApplicationConfig,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreatePartnerApp(ctx, CreatePartnerAppOptions{
		Name:              req.Name,
		Type:              req.Type,
		ExecutionRoleArn:  req.ExecutionRoleArn,
		AuthType:          req.AuthType,
		Tier:              req.Tier,
		ApplicationConfig: req.ApplicationConfig,
		Tags:              fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

// describePartnerAppResponse is the response body for DescribePartnerApp.
type describePartnerAppResponse struct {
	Arn               string          `json:"Arn"`
	Name              string          `json:"Name"`
	Status            string          `json:"Status"`
	Type              string          `json:"Type,omitempty"`
	ExecutionRoleArn  string          `json:"ExecutionRoleArn,omitempty"`
	AuthType          string          `json:"AuthType,omitempty"`
	Tier              string          `json:"Tier,omitempty"`
	ApplicationConfig json.RawMessage `json:"ApplicationConfig,omitempty"`
	CreationTime      float64         `json:"CreationTime"`
	LastModifiedTime  float64         `json:"LastModifiedTime"`
}

func (h *Handler) handleDescribePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribePartnerApp(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(describePartnerAppResponse{
		Arn:               result.Arn,
		Name:              result.Name,
		Status:            result.Status,
		Type:              result.Type,
		ExecutionRoleArn:  result.ExecutionRoleArn,
		AuthType:          result.AuthType,
		Tier:              result.Tier,
		ApplicationConfig: result.ApplicationConfig,
		CreationTime:      epochSeconds(result.CreationTime),
		LastModifiedTime:  epochSeconds(result.LastModifiedTime),
	})
}

func (h *Handler) handleDeletePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePartnerApp(ctx, req.Arn); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyGenericArn: req.Arn})
}

func (h *Handler) handleUpdatePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn               string          `json:"Arn"`
		Tier              string          `json:"Tier,omitempty"`
		ApplicationConfig json.RawMessage `json:"ApplicationConfig,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdatePartnerApp(ctx, UpdatePartnerAppOptions{
		Arn:               req.Arn,
		Tier:              req.Tier,
		ApplicationConfig: req.ApplicationConfig,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

func (h *Handler) handleListPartnerApps(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	apps, nextToken := h.Backend.ListPartnerApps(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(apps))
	for _, p := range apps {
		items = append(items, map[string]any{
			keyGenericArn:   p.Arn,
			keyGenericName:  p.Name,
			keyStatus:       p.Status,
			"Type":          p.Type,
			keyCreationTime: epochSeconds(p.CreationTime),
		})
	}

	return listResp("Summaries", items, nextToken)
}

func (h *Handler) handleCreatePartnerAppPresignedURL(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePartnerAppPresignedURL(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyURL: url})
}
