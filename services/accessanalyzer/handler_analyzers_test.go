package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestServiceLinkedAnalyzerLifecycle verifies Create/Delete service-linked analyzer.
func TestServiceLinkedAnalyzerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
		name string
	}{
		{
			name: "create_service_linked_analyzer",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPut, "/service-linked-analyzer", map[string]any{
					"type": "ACCOUNT_UNUSED_ACCESS",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.True(t, strings.HasPrefix(resp["arn"], "arn:aws:access-analyzer:"))
			},
		},
		{
			name: "delete_service_linked_analyzer",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				mustAnalyzer(t, b, "sla-to-delete")

				rec := doRequest(t, h, http.MethodDelete, "/service-linked-analyzer/sla-to-delete", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_missing_analyzer",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodDelete, "/service-linked-analyzer/no-such", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}

// TestUpdateAnalyzer verifies PUT /analyzer/{name} updates an analyzer.
func TestUpdateAnalyzer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn    func(b *accessanalyzer.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "update_existing",
			setupFn: func(b *accessanalyzer.InMemoryBackend) string {
				mustAnalyzer(t, b, "upd-analyzer")

				return "upd-analyzer"
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update_missing",
			setupFn: func(_ *accessanalyzer.InMemoryBackend) string {
				return "no-such-analyzer"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			name := tt.setupFn(b)

			rec := doRequest(t, h, http.MethodPut, "/analyzer/"+name, map[string]any{
				"configuration": map[string]any{},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
