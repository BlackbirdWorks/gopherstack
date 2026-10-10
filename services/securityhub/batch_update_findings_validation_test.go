package securityhub_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchUpdateFindings_Validation(t *testing.T) {
	t.Parallel()

	idents := func(n int) []any {
		out := make([]any, n)
		for i := range out {
			out[i] = map[string]any{
				"Id":         fmt.Sprintf("f-%d", i),
				"ProductArn": "arn:aws:securityhub:us-east-1::product/x/y",
			}
		}

		return out
	}

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{name: "hundred_findings", body: map[string]any{"FindingIdentifiers": idents(100)}, wantStatus: http.StatusOK},
		{
			name:       "too_many_findings",
			body:       map[string]any{"FindingIdentifiers": idents(101)},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "confidence_high",
			body:       map[string]any{"FindingIdentifiers": idents(1), "Confidence": 101},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "criticality_negative",
			body:       map[string]any{"FindingIdentifiers": idents(1), "Criticality": -1},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "normalized_high",
			body:       map[string]any{"FindingIdentifiers": idents(1), "Severity": map[string]any{"Normalized": 101}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "bad_label",
			body:       map[string]any{"FindingIdentifiers": idents(1), "Severity": map[string]any{"Label": "SEVERE"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "in_range",
			body: map[string]any{
				"FindingIdentifiers": idents(1),
				"Confidence":         100,
				"Severity":           map[string]any{"Label": "LOW"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPatch, "/findings/batchupdate", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusBadRequest {
				assert.Equal(t, "InvalidInputException", rec.Header().Get("X-Amzn-Errortype"))
			}
		})
	}
}
