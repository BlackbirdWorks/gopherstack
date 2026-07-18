package ce

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type getCommitmentPurchaseAnalysisInput struct {
	AnalysisID string `json:"AnalysisId"`
}

type getCommitmentPurchaseAnalysisOutput struct {
	EstimatedSavings        any    `json:"EstimatedSavings,omitempty"`
	AnalysisID              string `json:"AnalysisId,omitempty"`
	AnalysisStatus          string `json:"AnalysisStatus,omitempty"`
	AnalysisStartedTime     string `json:"AnalysisStartedTime,omitempty"`
	EstimatedCompletionTime string `json:"EstimatedCompletionTime,omitempty"`
	ErrorCode               string `json:"ErrorCode,omitempty"`
}

func (h *Handler) handleGetCommitmentPurchaseAnalysis(
	_ context.Context,
	in *getCommitmentPurchaseAnalysisInput,
) (*getCommitmentPurchaseAnalysisOutput, error) {
	if in.AnalysisID == "" {
		return nil, fmt.Errorf("%w: AnalysisId is required", errInvalidRequest)
	}

	a, err := h.Backend.GetCommitmentAnalysis(in.AnalysisID)
	if err != nil {
		return nil, err
	}

	return &getCommitmentPurchaseAnalysisOutput{
		AnalysisID:              a.AnalysisID,
		AnalysisStatus:          a.AnalysisStatus,
		AnalysisStartedTime:     a.AnalysisStartedTime,
		EstimatedCompletionTime: a.EstimatedCompletionTime,
		ErrorCode:               a.ErrorCode,
	}, nil
}

type listCommitmentPurchaseAnalysesInput struct {
	NextPageToken  string `json:"NextPageToken"`
	AnalysisStatus string `json:"AnalysisStatus"`
	PageSize       int    `json:"PageSize"`
}

type listCommitmentPurchaseAnalysesOutput struct {
	NextPageToken       string                `json:"NextPageToken,omitempty"`
	AnalysisSummaryList []*CommitmentAnalysis `json:"AnalysisSummaryList"`
}

func (h *Handler) handleListCommitmentPurchaseAnalyses(
	_ context.Context,
	_ *listCommitmentPurchaseAnalysesInput,
) (*listCommitmentPurchaseAnalysesOutput, error) {
	analyses := h.Backend.ListCommitmentAnalyses()

	return &listCommitmentPurchaseAnalysesOutput{
		AnalysisSummaryList: analyses,
	}, nil
}

type startCommitmentPurchaseAnalysisInput struct {
	CommitmentPurchaseAnalysisConfiguration any `json:"CommitmentPurchaseAnalysisConfiguration"`
}

type startCommitmentPurchaseAnalysisOutput struct {
	AnalysisID              string `json:"AnalysisId,omitempty"`
	AnalysisStartedTime     string `json:"AnalysisStartedTime,omitempty"`
	EstimatedCompletionTime string `json:"EstimatedCompletionTime,omitempty"`
}

func (h *Handler) handleStartCommitmentPurchaseAnalysis(
	_ context.Context,
	_ *startCommitmentPurchaseAnalysisInput,
) (*startCommitmentPurchaseAnalysisOutput, error) {
	a := h.Backend.CreateCommitmentAnalysis()

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
