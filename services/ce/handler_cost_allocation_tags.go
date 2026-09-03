package ce

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type listCostAllocationTagBackfillHistoryInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

// backfillRequest mirrors aws-sdk-go-v2/service/costexplorer/types'
// CostAllocationTagBackfillRequest exactly. BackfillJob (the backend's
// internal/persisted model) carries lowerCamelCase JSON tags for storage;
// emitting it directly on the wire under those tags was a real bug under
// this service's case-sensitive JSON-RPC 1.1 protocol -- a real client's
// typed BackfillFrom/BackfillStatus/CompletedAt/LastUpdatedAt/RequestedAt
// were all nil/empty regardless of backend state, on both
// ListCostAllocationTagBackfillHistory and StartCostAllocationTagBackfill.
type backfillRequest struct {
	BackfillFrom   string `json:"BackfillFrom,omitempty"`
	BackfillStatus string `json:"BackfillStatus,omitempty"`
	CompletedAt    string `json:"CompletedAt,omitempty"`
	LastUpdatedAt  string `json:"LastUpdatedAt,omitempty"`
	RequestedAt    string `json:"RequestedAt,omitempty"`
}

func toBackfillRequest(j *BackfillJob) backfillRequest {
	if j == nil {
		return backfillRequest{}
	}

	return backfillRequest{
		BackfillFrom:   j.BackfillFrom,
		BackfillStatus: j.BackfillStatus,
		CompletedAt:    j.CompletedAt,
		LastUpdatedAt:  j.LastUpdatedAt,
		RequestedAt:    j.RequestedAt,
	}
}

type listCostAllocationTagBackfillHistoryOutput struct {
	NextToken        string            `json:"NextToken,omitempty"`
	BackfillRequests []backfillRequest `json:"BackfillRequests"`
}

func (h *Handler) handleListCostAllocationTagBackfillHistory(
	_ context.Context,
	in *listCostAllocationTagBackfillHistoryInput,
) (*listCostAllocationTagBackfillHistoryOutput, error) {
	jobs := h.Backend.ListBackfillHistory()

	// paginateOrdered, not paginateList: jobs is already in
	// most-recently-requested-first order, which re-sorting ascending by
	// BackfillID would discard.
	page, nextToken := paginateOrdered(jobs, in.MaxResults, in.NextToken,
		func(j *BackfillJob) string { return j.BackfillID })

	items := make([]backfillRequest, 0, len(page))
	for _, j := range page {
		items = append(items, toBackfillRequest(j))
	}

	return &listCostAllocationTagBackfillHistoryOutput{
		BackfillRequests: items,
		NextToken:        nextToken,
	}, nil
}

type listCostAllocationTagsInput struct {
	Status     string   `json:"Status"`
	Type       string   `json:"Type"`
	NextToken  string   `json:"NextToken"`
	TagKeys    []string `json:"TagKeys"`
	MaxResults int      `json:"MaxResults"`
}

type costAllocationTagEntry struct {
	TagKey          string `json:"TagKey"`
	Status          string `json:"Status"`
	Type            string `json:"Type"`
	LastUpdatedDate string `json:"LastUpdatedDate,omitempty"`
}

type listCostAllocationTagsOutput struct {
	NextToken          string                   `json:"NextToken,omitempty"`
	CostAllocationTags []costAllocationTagEntry `json:"CostAllocationTags"`
}

func (h *Handler) handleListCostAllocationTags(
	_ context.Context,
	in *listCostAllocationTagsInput,
) (*listCostAllocationTagsOutput, error) {
	tags := h.Backend.ListCostAllocationTags(in.Status, in.Type, in.TagKeys)

	// ListCostAllocationTags already sorts ascending by the unique TagKey, so
	// paginateList's own re-sort by the same key is a no-op -- the established
	// paginateList pattern applies directly here, unlike ops with an
	// independent SortBy.
	page, nextToken := paginateList(tags, in.MaxResults, in.NextToken,
		func(t *CostAllocationTag) string { return t.TagKey })

	entries := make([]costAllocationTagEntry, 0, len(page))
	for _, t := range page {
		entries = append(entries, costAllocationTagEntry{
			TagKey:          t.TagKey,
			Status:          t.Status,
			Type:            t.Type,
			LastUpdatedDate: t.LastUpdatedDate,
		})
	}

	if entries == nil {
		entries = []costAllocationTagEntry{}
	}

	return &listCostAllocationTagsOutput{
		CostAllocationTags: entries,
		NextToken:          nextToken,
	}, nil
}

type startCostAllocationTagBackfillInput struct {
	BackfillFrom string `json:"BackfillFrom"`
}

type startCostAllocationTagBackfillOutput struct {
	BackfillRequest *backfillRequest `json:"BackfillRequest,omitempty"`
}

func (h *Handler) handleStartCostAllocationTagBackfill(
	_ context.Context,
	in *startCostAllocationTagBackfillInput,
) (*startCostAllocationTagBackfillOutput, error) {
	if in.BackfillFrom == "" {
		return nil, fmt.Errorf("%w: BackfillFrom is required", ErrValidation)
	}

	job := h.Backend.CreateBackfillJob(in.BackfillFrom)
	req := toBackfillRequest(job)

	return &startCostAllocationTagBackfillOutput{
		BackfillRequest: &req,
	}, nil
}

type updateCostAllocationTagsStatusInput struct {
	CostAllocationTagsStatus []CostAllocationTagStatusEntry `json:"CostAllocationTagsStatus"`
}

type updateCostAllocationTagsStatusOutput struct {
	Errors []CostAllocationTagError `json:"Errors"`
}

func (h *Handler) handleUpdateCostAllocationTagsStatus(
	_ context.Context,
	in *updateCostAllocationTagsStatusInput,
) (*updateCostAllocationTagsStatusOutput, error) {
	errs := h.Backend.UpdateCostAllocationTagsStatus(in.CostAllocationTagsStatus)

	return &updateCostAllocationTagsStatusOutput{
		Errors: errs,
	}, nil
}

// buildCostAllocationTagOps returns the cost-allocation-tag-family op dispatch entries.
func (h *Handler) buildCostAllocationTagOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ListCostAllocationTagBackfillHistory": service.WrapOp(
			h.handleListCostAllocationTagBackfillHistory,
		),
		"ListCostAllocationTags": service.WrapOp(
			h.handleListCostAllocationTags,
		),
		"StartCostAllocationTagBackfill": service.WrapOp(
			h.handleStartCostAllocationTagBackfill,
		),
		"UpdateCostAllocationTagsStatus": service.WrapOp(
			h.handleUpdateCostAllocationTagsStatus,
		),
	}
}
