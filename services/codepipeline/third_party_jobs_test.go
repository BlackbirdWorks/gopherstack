package codepipeline_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestHandler_AcknowledgeThirdPartyJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		wantStatus string
		httpStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{ID: "tp-job-1", Nonce: "tp-nonce", Status: "Created"})
			},
			input: map[string]any{
				"jobId":       "tp-job-1",
				"nonce":       "tp-nonce",
				"clientToken": "token-abc",
			},
			httpStatus: http.StatusOK,
			wantStatus: "InProgress",
		},
		{
			name:       "missing_clientToken",
			setup:      nil,
			input:      map[string]any{"jobId": "x", "nonce": "y"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "not_found",
			setup:      nil,
			input:      map[string]any{"jobId": "no-such", "nonce": "n", "clientToken": "t"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "AcknowledgeThirdPartyJob", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantStatus, out["status"])
			}
		})
	}
}

func TestHandler_ThirdPartyJobResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	h.Backend.AddJobInternal(&codepipeline.Job{
		ID:    "tp-job-001",
		Nonce: "nonce-tp-001",
	})

	// Acknowledge third party job
	rec := doRequest(t, h, "AcknowledgeThirdPartyJob", map[string]any{
		"jobId":       "tp-job-001",
		"nonce":       "nonce-tp-001",
		"clientToken": "token",
	})
	assert.Equal(t, 200, rec.Code)

	// Missing jobId
	rec = doRequest(t, h, "AcknowledgeThirdPartyJob", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Put third party job success
	rec = doRequest(t, h, "PutThirdPartyJobSuccessResult", map[string]any{
		"jobId":       "tp-job-001",
		"clientToken": "token",
	})
	assert.Equal(t, 200, rec.Code)

	// Missing jobId
	rec = doRequest(t, h, "PutThirdPartyJobSuccessResult", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	h.Backend.AddJobInternal(&codepipeline.Job{
		ID:    "tp-job-002",
		Nonce: "nonce-tp-002",
	})

	// Put third party job failure
	rec = doRequest(t, h, "PutThirdPartyJobFailureResult", map[string]any{
		"jobId":       "tp-job-002",
		"clientToken": "token",
		"failureDetails": map[string]any{
			"message": "failed",
			"type":    "JobFailed",
		},
	})
	assert.Equal(t, 200, rec.Code)

	// Missing jobId
	rec = doRequest(t, h, "PutThirdPartyJobFailureResult", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Action revision and approval ----
