package dms

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type batchStartRecommendationsInput struct {
	Data []any `json:"Data"`
}

type batchStartRecommendationsErrorEntryJSON struct {
	Code       string `json:"Code"`
	Message    string `json:"Message"`
	DatabaseID string `json:"DatabaseId"`
}

type batchStartRecommendationsOutput struct {
	ErrorEntries []batchStartRecommendationsErrorEntryJSON `json:"ErrorEntries"`
}

func (h *Handler) handleBatchStartRecommendations(
	ctx context.Context, _ *batchStartRecommendationsInput,
) (*batchStartRecommendationsOutput, error) {
	if err := h.Backend.BatchStartRecommendations(ctx); err != nil {
		return nil, err
	}

	return &batchStartRecommendationsOutput{
		ErrorEntries: []batchStartRecommendationsErrorEntryJSON{},
	}, nil
}

type describeRecommendationLimitationsInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeRecommendationLimitationsOutput struct {
	NextToken   *string          `json:"NextToken,omitempty"`
	Limitations []map[string]any `json:"Limitations"`
}

func (h *Handler) handleDescribeRecommendationLimitations(
	_ context.Context, _ *describeRecommendationLimitationsInput,
) (*describeRecommendationLimitationsOutput, error) {
	return &describeRecommendationLimitationsOutput{Limitations: []map[string]any{}}, nil
}

type describeRecommendationsInput struct {
	NextToken  *string       `json:"NextToken"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeRecommendationsOutput struct {
	NextToken       *string          `json:"NextToken,omitempty"`
	Recommendations []map[string]any `json:"Recommendations"`
}

func (h *Handler) handleDescribeRecommendations(
	ctx context.Context, in *describeRecommendationsInput,
) (*describeRecommendationsOutput, error) {
	list, err := h.Backend.DescribeRecommendations(ctx)
	if err != nil {
		return nil, err
	}

	dbFilter := extractFilterValue(in.Filters, "database-id")
	engineFilter := extractFilterValue(in.Filters, "engine-name")

	recs := make([]map[string]any, 0, len(list))
	for _, r := range list {
		if dbFilter != "" && r.DatabaseID != dbFilter {
			continue
		}

		if engineFilter != "" && r.EngineName != engineFilter {
			continue
		}

		recs = append(recs, map[string]any{
			"DatabaseId": r.DatabaseID,
			"EngineName": r.EngineName,
			"Status":     r.Status,
		})
	}

	data, nextMarker := dmsPaginate(recs, in.NextToken, in.MaxRecords)

	return &describeRecommendationsOutput{Recommendations: data, NextToken: nextMarker}, nil
}

type startRecommendationsInput struct {
	DatabaseID *string        `json:"DatabaseId"`
	Settings   map[string]any `json:"Settings"`
}

type startRecommendationsOutput struct{}

func (h *Handler) handleStartRecommendations(
	ctx context.Context, in *startRecommendationsInput,
) (*startRecommendationsOutput, error) {
	if err := h.Backend.StartRecommendation(ctx, ptrconv.String(in.DatabaseID)); err != nil {
		return nil, err
	}

	return &startRecommendationsOutput{}, nil
}

// opsRecommendations returns the dispatch-table entries for the recommendations operation family.
func (h *Handler) opsRecommendations() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"BatchStartRecommendations": service.WrapOp(
			h.handleBatchStartRecommendations,
		),
		opDescribeRecommendationLimitations: service.WrapOp(
			h.handleDescribeRecommendationLimitations,
		),
		opDescribeRecommendations: service.WrapOp(
			h.handleDescribeRecommendations,
		),
		opStartRecommendations: service.WrapOp(
			h.handleStartRecommendations,
		),
	}
}
