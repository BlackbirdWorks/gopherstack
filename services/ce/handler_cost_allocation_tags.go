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

type listCostAllocationTagBackfillHistoryOutput struct {
	NextToken        string         `json:"NextToken,omitempty"`
	BackfillRequests []*BackfillJob `json:"BackfillRequests"`
}

func (h *Handler) handleListCostAllocationTagBackfillHistory(
	_ context.Context,
	_ *listCostAllocationTagBackfillHistoryInput,
) (*listCostAllocationTagBackfillHistoryOutput, error) {
	jobs := h.Backend.ListBackfillHistory()

	return &listCostAllocationTagBackfillHistoryOutput{
		BackfillRequests: jobs,
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

	entries := make([]costAllocationTagEntry, 0, len(tags))
	for _, t := range tags {
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
	}, nil
}

type startCostAllocationTagBackfillInput struct {
	BackfillFrom string `json:"BackfillFrom"`
}

type startCostAllocationTagBackfillOutput struct {
	BackfillRequest *BackfillJob `json:"BackfillRequest,omitempty"`
}

func (h *Handler) handleStartCostAllocationTagBackfill(
	_ context.Context,
	in *startCostAllocationTagBackfillInput,
) (*startCostAllocationTagBackfillOutput, error) {
	if in.BackfillFrom == "" {
		return nil, fmt.Errorf("%w: BackfillFrom is required", errInvalidRequest)
	}

	job := h.Backend.CreateBackfillJob(in.BackfillFrom)

	return &startCostAllocationTagBackfillOutput{
		BackfillRequest: job,
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
