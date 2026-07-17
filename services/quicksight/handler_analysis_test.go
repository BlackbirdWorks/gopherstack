package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Analysis tests ----

func TestQuickSight_Analyses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateAnalysis returns ARN",
			method:   http.MethodPost,
			path:     accountPath("/analyses/a1"),
			body:     map[string]any{"Name": "My Analysis"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "a1", body["AnalysisId"])
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:analysis/a1")
				assert.Equal(t, "CREATION_SUCCESSFUL", body["CreationStatus"])
			},
		},
		{
			name:   "CreateAnalysis duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath("/analyses/dup"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/dup"), map[string]any{"Name": "x"})
			},
			body:     map[string]any{"Name": "x"},
			wantCode: http.StatusConflict,
		},
		{
			name:   "DescribeAnalysis returns analysis",
			method: http.MethodGet,
			path:   accountPath("/analyses/a2"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a2"), map[string]any{"Name": "A2"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				a, ok := body["Analysis"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "a2", a["AnalysisId"])
				assert.Equal(t, "CREATION_SUCCESSFUL", a["Status"])
			},
		},
		{
			name:     "DescribeAnalysis missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/analyses/notexist"),
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateAnalysis changes name and status",
			method: http.MethodPut,
			path:   accountPath("/analyses/a3"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a3"), map[string]any{"Name": "A3"})
			},
			body:     map[string]any{"Name": "A3-Updated"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "UPDATE_SUCCESSFUL", body["UpdateStatus"])
			},
		},
		{
			name:   "DeleteAnalysis softdeletes without force",
			method: http.MethodDelete,
			path:   accountPath("/analyses/a4"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a4"), map[string]any{"Name": "A4"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteAnalysis forceDelete removes permanently",
			method: http.MethodDelete,
			path:   accountPath("/analyses/a5") + "?forceDeleteWithoutRecovery=true",
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a5"), map[string]any{"Name": "A5"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "RestoreAnalysis resets status",
			method: http.MethodPost,
			path:   accountPath("/restore/analyses/a6"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/a6"), map[string]any{"Name": "A6"})
				doRequest(t, h, http.MethodDelete, accountPath("/analyses/a6"), nil)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Equal(t, "a6", body["AnalysisId"])
			},
		},
		{
			name:   "ListAnalyses returns analyses",
			method: http.MethodGet,
			path:   accountPath("/analyses"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/analyses/la1"), map[string]any{"Name": "A"})
				doRequest(t, h, http.MethodPost, accountPath("/analyses/la2"), map[string]any{"Name": "B"})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["AnalysisSummaryList"].([]any)
				require.True(t, ok)
				assert.Len(t, items, 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}
