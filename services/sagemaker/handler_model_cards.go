package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// ModelCard handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string      `json:"ModelCardName"`
		Content       string      `json:"Content"`
		Tags          []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelCard(ctx, req.ModelCardName, req.Content, fromTagObjects(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

func (h *Handler) handleDescribeModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleUpdateModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
		Content       string `json:"Content"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateModelCard(ctx, req.ModelCardName, req.Content)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

func (h *Handler) handleDeleteModelCard(ctx context.Context, body []byte) error {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelCard(ctx, req.ModelCardName)
}

// ---------------------------------------------------------------------------
// ModelCard list handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListModelCards(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cards, nextToken := h.Backend.ListModelCards(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(cards))
	for _, c := range cards {
		items = append(items, map[string]any{
			"ModelCardName":     c.ModelCardName,
			"ModelCardArn":      c.ModelCardArn,
			"ModelCardStatus":   c.ModelCardStatus,
			"ModelCardVersion":  c.ModelCardVersion,
			keyCreationTime:     epochSeconds(c.CreationTime),
			keyLastModifiedTime: epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("ModelCardSummaries", items, nextToken)
}

func (h *Handler) handleListModelCardVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
		NextToken     string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	card, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName)
	if err != nil {
		return nil, err
	}

	summaries := []map[string]any{
		{
			"ModelCardName":     card.ModelCardName,
			"ModelCardArn":      card.ModelCardArn,
			"ModelCardStatus":   card.ModelCardStatus,
			"ModelCardVersion":  card.ModelCardVersion,
			keyCreationTime:     epochSeconds(card.CreationTime),
			keyLastModifiedTime: epochSeconds(card.LastModifiedTime),
		},
	}

	return json.Marshal(map[string]any{"ModelCardVersionSummaryList": summaries})
}

func (h *Handler) handleListModelCardExportJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName                  string `json:"ModelCardName"`
		ModelCardExportJobNameContains string `json:"ModelCardExportJobNameContains"`
		StatusEquals                   string `json:"StatusEquals"`
		NextToken                      string `json:"NextToken"`
		MaxResults                     int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName != "" {
		if _, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName); err != nil {
			return nil, err
		}
	}

	jobs, next := h.Backend.ListModelCardExportJobs(
		ctx, req.ModelCardName, req.ModelCardExportJobNameContains, req.StatusEquals, req.NextToken, req.MaxResults,
	)

	summaries := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, map[string]any{
			"CreatedAt":              epochSeconds(j.CreatedAt),
			"LastModifiedAt":         epochSeconds(j.LastModifiedAt),
			keyModelCardExportJobArn: j.ModelCardExportJobArn,
			"ModelCardExportJobName": j.ModelCardExportJobName,
			keyModelCardNameField:    j.ModelCardName,
			keyModelCardVersion:      j.ModelCardVersion,
			keyStatus:                j.Status,
		})
	}

	return listResp("ModelCardExportJobSummaries", summaries, next)
}
