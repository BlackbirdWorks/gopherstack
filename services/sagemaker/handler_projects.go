package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Project handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		ProjectName        string            `json:"ProjectName"`
		ProjectDescription string            `json:"ProjectDescription"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateProject(ctx, req.ProjectName, req.ProjectDescription, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ProjectArn": result.ProjectArn,
		"ProjectId":  result.ProjectID,
	})
}

func (h *Handler) handleDescribeProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ProjectName string `json:"ProjectName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeProject(ctx, req.ProjectName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteProject(ctx context.Context, body []byte) error {
	var req struct {
		ProjectName string `json:"ProjectName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	return h.Backend.DeleteProject(ctx, req.ProjectName)
}

func (h *Handler) handleListProjects(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListProjects(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, p := range items {
		summaries = append(summaries, map[string]any{
			"ProjectName":   p.ProjectName,
			"ProjectArn":    p.ProjectArn,
			"ProjectId":     p.ProjectID,
			"ProjectStatus": p.ProjectStatus,
			keyCreationTime: p.CreationTime,
		})
	}

	return json.Marshal(map[string]any{
		"ProjectSummaryList": summaries,
		keyNextToken:         next,
	})
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ProjectName        string      `json:"ProjectName"`
		ProjectDescription string      `json:"ProjectDescription,omitempty"`
		Tags               []tagObject `json:"Tags,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProjectName == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdateProject(ctx, req.ProjectName, req.ProjectDescription, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyProjectArn: p.ProjectArn})
}
