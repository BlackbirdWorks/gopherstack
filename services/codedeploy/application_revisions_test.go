package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

func TestHandler_BatchGetApplicationRevisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
			},
			input: map[string]any{
				"applicationName": "my-app",
				"revisions": []map[string]any{
					{"revisionType": "S3"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_application_name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app_not_found",
			input: map[string]any{
				"applicationName": "nonexistent",
				"revisions":       []map[string]any{},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "BatchGetApplicationRevisions", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-app", resp["applicationName"])
			}
		})
	}
}

func TestApplicationRevisions_MaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)

	// Build 26 revisions (> maxBatchRevisions=25)
	revisions := make([]map[string]string, 26)
	for i := range revisions {
		revisions[i] = map[string]string{"revisionType": "S3"}
	}

	rec := doRequest(t, h, "BatchGetApplicationRevisions", map[string]any{
		"applicationName": "my-app",
		"revisions":       revisions,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
