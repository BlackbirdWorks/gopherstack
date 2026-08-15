package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommitmentAnalysis_MultipleStartsListed verifies multi-analysis list.
func TestCommitmentAnalysis_MultipleStartsListed(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

	analysisIDs := make([]string, 0, 3)

	for range 3 {
		rec := doRequest(t, h, "StartCommitmentPurchaseAnalysis", map[string]any{
			"CommitmentPurchaseAnalysisConfiguration": map[string]any{},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			AnalysisID string `json:"AnalysisId"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		require.NotEmpty(t, out.AnalysisID)
		analysisIDs = append(analysisIDs, out.AnalysisID)
	}

	// All distinct IDs
	seen := make(map[string]struct{})
	for _, id := range analysisIDs {
		seen[id] = struct{}{}
	}
	assert.Len(t, seen, 3, "each analysis must have a unique ID")

	listRec := doRequest(t, h, "ListCommitmentPurchaseAnalyses", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		AnalysisSummaryList []struct {
			AnalysisID string `json:"AnalysisId"`
		} `json:"AnalysisSummaryList"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	require.Len(t, listOut.AnalysisSummaryList, 3)

	listedIDs := make(map[string]struct{}, 3)
	for _, item := range listOut.AnalysisSummaryList {
		require.NotEmpty(t, item.AnalysisID, "AnalysisId must be emitted under its real PascalCase wire key")
		listedIDs[item.AnalysisID] = struct{}{}
	}
	assert.Equal(t, seen, listedIDs)
}

func TestCommitmentPurchaseAnalysis_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start analysis
	rec := doRequest(t, h, "StartCommitmentPurchaseAnalysis", map[string]any{
		"CommitmentPurchaseAnalysisConfiguration": map[string]any{
			"SavingsPlansPurchaseAnalysisConfiguration": map[string]string{
				"SavingsPlansType": "COMPUTE_SP",
				"TermInYears":      "ONE_YEAR",
				"PaymentOption":    "NO_UPFRONT",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut struct {
		AnalysisID              string `json:"AnalysisId"`
		AnalysisStartedTime     string `json:"AnalysisStartedTime"`
		EstimatedCompletionTime string `json:"EstimatedCompletionTime"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&startOut))
	require.NotEmpty(t, startOut.AnalysisID)
	assert.NotEmpty(t, startOut.AnalysisStartedTime)
	assert.NotEmpty(t, startOut.EstimatedCompletionTime)

	// Get analysis
	getRec := doRequest(t, h, "GetCommitmentPurchaseAnalysis", map[string]any{
		"AnalysisId": startOut.AnalysisID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		AnalysisID     string `json:"AnalysisId"`
		AnalysisStatus string `json:"AnalysisStatus"`
	}
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
	assert.Equal(t, startOut.AnalysisID, getOut.AnalysisID)
	assert.Equal(t, "PROCESSING", getOut.AnalysisStatus)

	// List analyses
	listRec := doRequest(t, h, "ListCommitmentPurchaseAnalyses", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		AnalysisSummaryList []struct {
			AnalysisID string `json:"AnalysisId"`
		} `json:"AnalysisSummaryList"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	require.Len(t, listOut.AnalysisSummaryList, 1)
	assert.Equal(t, startOut.AnalysisID, listOut.AnalysisSummaryList[0].AnalysisID)
}

func TestSnapshotRestore_IncludesCommitmentAnalyses(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an analysis
	rec := doRequest(t, h, "StartCommitmentPurchaseAnalysis", map[string]any{
		"CommitmentPurchaseAnalysisConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut struct {
		AnalysisID string `json:"AnalysisId"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&startOut))

	// Snapshot + restore
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// The restored handler should have the analysis
	getRec := doRequest(t, fresh, "GetCommitmentPurchaseAnalysis", map[string]any{
		"AnalysisId": startOut.AnalysisID,
	})
	assert.Equal(t, http.StatusOK, getRec.Code)
}

func TestHandler_GetCommitmentPurchaseAnalysis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name:           "not_found_for_unknown_analysis",
			body:           map[string]any{"AnalysisId": "analysis-123"},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "missing_analysis_id_returns_400",
			body:           map[string]any{},
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCommitmentPurchaseAnalysis", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}
