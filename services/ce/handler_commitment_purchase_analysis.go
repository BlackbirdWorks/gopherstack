package ce

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type getCommitmentPurchaseAnalysisInput struct {
	AnalysisID string `json:"AnalysisId"`
}

// getCommitmentPurchaseAnalysisOutput's CommitmentPurchaseAnalysisConfiguration
// echo (not the previously invented "EstimatedSavings", which named no real
// GetCommitmentPurchaseAnalysisOutput member and was never populated) is
// field-diffed against real AWS CE (api_op_GetCommitmentPurchaseAnalysis.go).
// AnalysisDetails (the real op's computed-savings member, nested under
// SavingsPlansPurchaseAnalysisDetails) is disclosed-not-modeled: this backend
// doesn't simulate commitment analysis internals, so there is no non-fabricated
// value to source it from.
type getCommitmentPurchaseAnalysisOutput struct {
	CommitmentPurchaseAnalysisConfiguration any    `json:"CommitmentPurchaseAnalysisConfiguration,omitempty"`
	AnalysisID                              string `json:"AnalysisId,omitempty"`
	AnalysisStatus                          string `json:"AnalysisStatus,omitempty"`
	AnalysisStartedTime                     string `json:"AnalysisStartedTime,omitempty"`
	EstimatedCompletionTime                 string `json:"EstimatedCompletionTime,omitempty"`
	ErrorCode                               string `json:"ErrorCode,omitempty"`
}

func (h *Handler) handleGetCommitmentPurchaseAnalysis(
	_ context.Context,
	in *getCommitmentPurchaseAnalysisInput,
) (*getCommitmentPurchaseAnalysisOutput, error) {
	if in.AnalysisID == "" {
		return nil, fmt.Errorf("%w: AnalysisId is required", ErrValidation)
	}

	a, err := h.Backend.GetCommitmentAnalysis(in.AnalysisID)
	if err != nil {
		return nil, err
	}

	return &getCommitmentPurchaseAnalysisOutput{
		AnalysisID:                              a.AnalysisID,
		AnalysisStatus:                          a.AnalysisStatus,
		AnalysisStartedTime:                     a.AnalysisStartedTime,
		EstimatedCompletionTime:                 a.EstimatedCompletionTime,
		ErrorCode:                               a.ErrorCode,
		CommitmentPurchaseAnalysisConfiguration: a.Configuration,
	}, nil
}

type listCommitmentPurchaseAnalysesInput struct {
	NextPageToken  string `json:"NextPageToken"`
	AnalysisStatus string `json:"AnalysisStatus"`
	PageSize       int    `json:"PageSize"`
}

// analysisSummary mirrors aws-sdk-go-v2/service/costexplorer/types'
// AnalysisSummary exactly. CommitmentAnalysis (the backend's internal/
// persisted model) carries lowerCamelCase JSON tags for storage; emitting it
// directly on the wire under those tags was a real bug under this service's
// case-sensitive JSON-RPC 1.1 protocol -- a real client's typed
// AnalysisId/AnalysisStatus/AnalysisStartedTime/EstimatedCompletionTime/
// ErrorCode were all nil/empty on every item, regardless of backend state.
// AnalysisCompletionTime is disclosed-not-modeled: this backend's analyses
// never leave PROCESSING, so there is no real completion time to emit.
type analysisSummary struct {
	CommitmentPurchaseAnalysisConfiguration any    `json:"CommitmentPurchaseAnalysisConfiguration,omitempty"`
	AnalysisID                              string `json:"AnalysisId,omitempty"`
	AnalysisStatus                          string `json:"AnalysisStatus,omitempty"`
	AnalysisStartedTime                     string `json:"AnalysisStartedTime,omitempty"`
	EstimatedCompletionTime                 string `json:"EstimatedCompletionTime,omitempty"`
	ErrorCode                               string `json:"ErrorCode,omitempty"`
}

func toAnalysisSummary(a *CommitmentAnalysis) analysisSummary {
	if a == nil {
		return analysisSummary{}
	}

	return analysisSummary{
		AnalysisID:                              a.AnalysisID,
		AnalysisStatus:                          a.AnalysisStatus,
		AnalysisStartedTime:                     a.AnalysisStartedTime,
		EstimatedCompletionTime:                 a.EstimatedCompletionTime,
		ErrorCode:                               a.ErrorCode,
		CommitmentPurchaseAnalysisConfiguration: a.Configuration,
	}
}

type listCommitmentPurchaseAnalysesOutput struct {
	NextPageToken       string            `json:"NextPageToken,omitempty"`
	AnalysisSummaryList []analysisSummary `json:"AnalysisSummaryList"`
}

func (h *Handler) handleListCommitmentPurchaseAnalyses(
	_ context.Context,
	in *listCommitmentPurchaseAnalysesInput,
) (*listCommitmentPurchaseAnalysesOutput, error) {
	analyses := h.Backend.ListCommitmentAnalyses(in.AnalysisStatus)

	// paginateOrdered, not paginateList: analyses is already in
	// most-recently-started-first order, which re-sorting ascending by
	// AnalysisID would discard.
	page, nextToken := paginateOrdered(analyses, in.PageSize, in.NextPageToken,
		func(a *CommitmentAnalysis) string { return a.AnalysisID })

	items := make([]analysisSummary, 0, len(page))
	for _, a := range page {
		items = append(items, toAnalysisSummary(a))
	}

	return &listCommitmentPurchaseAnalysesOutput{
		AnalysisSummaryList: items,
		NextPageToken:       nextToken,
	}, nil
}

// startCommitmentPurchaseAnalysisInput's CommitmentPurchaseAnalysisConfiguration
// is a required real member (api_op_StartCommitmentPurchaseAnalysis.go) that a
// prior revision discarded entirely (handler signature took `_
// *startCommitmentPurchaseAnalysisInput`) -- never validated, never stored,
// never echoed back. Fixed to require it and round-trip it verbatim, matching
// this op's sibling StartCostAllocationTagBackfill/
// StartSavingsPlansPurchaseRecommendationGeneration, which already validate
// their own required inputs.
type startCommitmentPurchaseAnalysisInput struct {
	CommitmentPurchaseAnalysisConfiguration any `json:"CommitmentPurchaseAnalysisConfiguration"`
}

// startCommitmentPurchaseAnalysisOutput has no Configuration echo -- real
// StartCommitmentPurchaseAnalysisOutput only carries AnalysisId/
// AnalysisStartedTime/EstimatedCompletionTime
// (api_op_StartCommitmentPurchaseAnalysis.go); the configuration is only
// echoed back on Get/List.
type startCommitmentPurchaseAnalysisOutput struct {
	AnalysisID              string `json:"AnalysisId,omitempty"`
	AnalysisStartedTime     string `json:"AnalysisStartedTime,omitempty"`
	EstimatedCompletionTime string `json:"EstimatedCompletionTime,omitempty"`
}

func (h *Handler) handleStartCommitmentPurchaseAnalysis(
	_ context.Context,
	in *startCommitmentPurchaseAnalysisInput,
) (*startCommitmentPurchaseAnalysisOutput, error) {
	if in.CommitmentPurchaseAnalysisConfiguration == nil {
		return nil, fmt.Errorf("%w: CommitmentPurchaseAnalysisConfiguration is required", ErrValidation)
	}

	a := h.Backend.CreateCommitmentAnalysis(in.CommitmentPurchaseAnalysisConfiguration)

	return &startCommitmentPurchaseAnalysisOutput{
		AnalysisID:              a.AnalysisID,
		AnalysisStartedTime:     a.AnalysisStartedTime,
		EstimatedCompletionTime: a.EstimatedCompletionTime,
	}, nil
}

// buildCommitmentOps returns the commitment-purchase-analysis-family op dispatch entries.
func (h *Handler) buildCommitmentOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetCommitmentPurchaseAnalysis": service.WrapOp(
			h.handleGetCommitmentPurchaseAnalysis,
		),
		"ListCommitmentPurchaseAnalyses": service.WrapOp(
			h.handleListCommitmentPurchaseAnalyses,
		),
		"StartCommitmentPurchaseAnalysis": service.WrapOp(
			h.handleStartCommitmentPurchaseAnalysis,
		),
	}
}
