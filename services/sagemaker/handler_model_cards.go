package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// ModelCard handlers
// ---------------------------------------------------------------------------

// createModelCardInput mirrors CreateModelCardInput (api_op_CreateModelCard.go:32-66):
// Content/ModelCardName/ModelCardStatus are all required; SecurityConfig/Tags
// are optional.
type createModelCardInput struct {
	ModelCardName   string                   `json:"ModelCardName"`
	Content         string                   `json:"Content"`
	ModelCardStatus string                   `json:"ModelCardStatus"`
	SecurityConfig  *ModelCardSecurityConfig `json:"SecurityConfig"`
	Tags            []tagObject              `json:"Tags"`
}

func (h *Handler) handleCreateModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req createModelCardInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelCard(ctx, CreateModelCardOptions{
		Name:           req.ModelCardName,
		Content:        req.Content,
		Status:         req.ModelCardStatus,
		SecurityConfig: req.SecurityConfig,
		Tags:           fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

// describeModelCardInput mirrors DescribeModelCardInput
// (api_op_DescribeModelCard.go:36-59): ModelCardName is required;
// IncludedData/ModelCardVersion are optional.
type describeModelCardInput struct {
	ModelCardName    string `json:"ModelCardName"`
	IncludedData     string `json:"IncludedData"`
	ModelCardVersion int32  `json:"ModelCardVersion"`
}

func (h *Handler) handleDescribeModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req describeModelCardInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName, req.ModelCardVersion, req.IncludedData)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// updateModelCardInput mirrors UpdateModelCardInput (api_op_UpdateModelCard.go:31-59):
// ModelCardName is required; Content/ModelCardStatus are optional and
// mutually exclusive (enforced by [InMemoryBackend.UpdateModelCard]).
type updateModelCardInput struct {
	ModelCardName   string `json:"ModelCardName"`
	Content         string `json:"Content"`
	ModelCardStatus string `json:"ModelCardStatus"`
}

func (h *Handler) handleUpdateModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req updateModelCardInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateModelCard(ctx, req.ModelCardName, req.Content, req.ModelCardStatus)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

// deleteModelCardInput mirrors DeleteModelCardInput (api_op_DeleteModelCard.go:27-33):
// its sole member is required.
type deleteModelCardInput struct {
	ModelCardName string `json:"ModelCardName"`
}

func (h *Handler) handleDeleteModelCard(ctx context.Context, body []byte) error {
	var req deleteModelCardInput

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

// listModelCardsInput mirrors ListModelCardsInput (api_op_ListModelCards.go:30-59):
// every member is optional.
type listModelCardsInput struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	ModelCardStatus    string   `json:"ModelCardStatus"`
	NameContains       string   `json:"NameContains"`
	NextToken          string   `json:"NextToken"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListModelCards(ctx context.Context, body []byte) ([]byte, error) {
	var req listModelCardsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cards, nextToken := h.Backend.ListModelCards(ctx, ListModelCardsParams{
		CreationTimeAfter:  epochPtr(req.CreationTimeAfter),
		CreationTimeBefore: epochPtr(req.CreationTimeBefore),
		ModelCardStatus:    req.ModelCardStatus,
		NameContains:       req.NameContains,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		NextToken:          req.NextToken,
		MaxResults:         req.MaxResults,
	})

	// ModelCardSummary (types/types.go) has no ModelCardVersion member — a
	// previous version of this handler fabricated one anyway; it has no
	// wire counterpart and is not emitted here.
	items := make([]map[string]any, 0, len(cards))
	for _, c := range cards {
		items = append(items, map[string]any{
			"ModelCardName":     c.ModelCardName,
			"ModelCardArn":      c.ModelCardArn,
			"ModelCardStatus":   c.ModelCardStatus,
			keyCreationTime:     epochSeconds(c.CreationTime),
			keyLastModifiedTime: epochSeconds(c.LastModifiedTime),
		})
	}

	return listResp("ModelCardSummaries", items, nextToken)
}

// listModelCardVersionsInput mirrors ListModelCardVersionsInput
// (api_op_ListModelCardVersions.go:30-59): ModelCardName is required, every
// other member optional.
type listModelCardVersionsInput struct {
	ModelCardName      string   `json:"ModelCardName"`
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	ModelCardStatus    string   `json:"ModelCardStatus"`
	NextToken          string   `json:"NextToken"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

// handleListModelCardVersions returns at most one version summary: this
// backend keeps no historical per-version snapshot, only the card's current
// state (see [InMemoryBackend.DescribeModelCard]'s doc comment), so
// ModelCardStatus/CreationTimeAfter/CreationTimeBefore are honored as
// filters over that single synthetic entry rather than genuinely selecting
// among multiple stored versions. SortBy/SortOrder/MaxResults are
// disclosed no-ops for the same reason: there is never more than one row to
// sort or page.
func (h *Handler) handleListModelCardVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req listModelCardVersionsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	card, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName, 0, "")
	if err != nil {
		return nil, err
	}

	summaries := []map[string]any{}
	if modelCardVersionMatchesFilter(card, req) {
		summaries = append(summaries, map[string]any{
			"ModelCardName":     card.ModelCardName,
			"ModelCardArn":      card.ModelCardArn,
			"ModelCardStatus":   card.ModelCardStatus,
			"ModelCardVersion":  card.ModelCardVersion,
			keyCreationTime:     epochSeconds(card.CreationTime),
			keyLastModifiedTime: epochSeconds(card.LastModifiedTime),
		})
	}

	return json.Marshal(map[string]any{"ModelCardVersionSummaryList": summaries})
}

func modelCardVersionMatchesFilter(card *ModelCard, req listModelCardVersionsInput) bool {
	if req.ModelCardStatus != "" && card.ModelCardStatus != req.ModelCardStatus {
		return false
	}

	if after := epochPtr(req.CreationTimeAfter); after != nil && !card.CreationTime.After(*after) {
		return false
	}

	if before := epochPtr(req.CreationTimeBefore); before != nil && !card.CreationTime.Before(*before) {
		return false
	}

	return true
}

// listModelCardExportJobsInput mirrors ListModelCardExportJobsInput
// (api_op_ListModelCardExportJobs.go:29-61): ModelCardName is required,
// every other member optional.
type listModelCardExportJobsInput struct {
	ModelCardName                  string   `json:"ModelCardName"`
	CreationTimeAfter              *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore             *float64 `json:"CreationTimeBefore"`
	ModelCardExportJobNameContains string   `json:"ModelCardExportJobNameContains"`
	StatusEquals                   string   `json:"StatusEquals"`
	NextToken                      string   `json:"NextToken"`
	SortBy                         string   `json:"SortBy"`
	SortOrder                      string   `json:"SortOrder"`
	MaxResults                     int32    `json:"MaxResults"`
	ModelCardVersion               int32    `json:"ModelCardVersion"`
}

func (h *Handler) handleListModelCardExportJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listModelCardExportJobsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName != "" {
		if _, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName, 0, ""); err != nil {
			return nil, err
		}
	}

	jobs, next := h.Backend.ListModelCardExportJobs(ctx, ListModelCardExportJobsParams{
		CreationTimeAfter:  epochPtr(req.CreationTimeAfter),
		CreationTimeBefore: epochPtr(req.CreationTimeBefore),
		ModelCardName:      req.ModelCardName,
		NameContains:       req.ModelCardExportJobNameContains,
		StatusEquals:       req.StatusEquals,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		NextToken:          req.NextToken,
		ModelCardVersion:   req.ModelCardVersion,
		MaxResults:         req.MaxResults,
	})

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
