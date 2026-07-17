package mediastore_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/mediastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DeletePolicy_NotSet verifies that all four delete-policy operations
// return the correct AWS not-found error when no policy is set. Moved (and
// de-prefixed) from the former parity_audit1_test.go's
// TestParity_DeletePolicy_NotSet.
func TestHandler_DeletePolicy_NotSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		op          string
		wantErrType string
		wantStatus  int
	}{
		{
			name:        "DeleteContainerPolicy_no_policy",
			op:          "DeleteContainerPolicy",
			wantErrType: "PolicyNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "DeleteCorsPolicy_no_policy",
			op:          "DeleteCorsPolicy",
			wantErrType: "CorsPolicyNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "DeleteLifecyclePolicy_no_policy",
			op:          "DeleteLifecyclePolicy",
			wantErrType: "PolicyNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "DeleteMetricPolicy_no_policy",
			op:          "DeleteMetricPolicy",
			wantErrType: "PolicyNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestContainer(t, h, "parity-container")

			rec := doRequest(t, h, tt.op, map[string]any{"ContainerName": "parity-container"})

			assert.Equal(t, tt.wantStatus, rec.Code)
			m := unmarshalBody(t, rec)
			assert.Equal(t, tt.wantErrType, m["__type"])
		})
	}
}

// TestHandler_DeletePolicy_AfterSet verifies that all four delete-policy operations
// succeed (200) when a policy is set, and return not-found on a second delete.
// Moved (and de-prefixed) from the former parity_audit1_test.go's
// TestParity_DeletePolicy_AfterSet.
func TestHandler_DeletePolicy_AfterSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, h *mediastore.Handler)
		name        string
		deleteOp    string
		wantErrType string
	}{
		{
			name:     "DeleteContainerPolicy_idempotent_second_delete",
			deleteOp: "DeleteContainerPolicy",
			setup: func(t *testing.T, h *mediastore.Handler) {
				t.Helper()
				rec := doRequest(t, h, "PutContainerPolicy", map[string]any{
					"ContainerName": "del-test",
					"Policy":        `{"Version":"2012-10-17","Statement":[]}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErrType: "PolicyNotFoundException",
		},
		{
			name:     "DeleteCorsPolicy_idempotent_second_delete",
			deleteOp: "DeleteCorsPolicy",
			setup: func(t *testing.T, h *mediastore.Handler) {
				t.Helper()
				rec := doRequest(t, h, "PutCorsPolicy", map[string]any{
					"ContainerName": "del-test",
					"CorsPolicy": []any{
						map[string]any{
							"AllowedOrigins": []any{"https://example.com"},
							"AllowedHeaders": []any{"*"},
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErrType: "CorsPolicyNotFoundException",
		},
		{
			name:     "DeleteLifecyclePolicy_idempotent_second_delete",
			deleteOp: "DeleteLifecyclePolicy",
			setup: func(t *testing.T, h *mediastore.Handler) {
				t.Helper()
				rec := doRequest(t, h, "PutLifecyclePolicy", map[string]any{
					"ContainerName":   "del-test",
					"LifecyclePolicy": `{"rules":[]}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErrType: "PolicyNotFoundException",
		},
		{
			name:     "DeleteMetricPolicy_idempotent_second_delete",
			deleteOp: "DeleteMetricPolicy",
			setup: func(t *testing.T, h *mediastore.Handler) {
				t.Helper()
				rec := doRequest(t, h, "PutMetricPolicy", map[string]any{
					"ContainerName": "del-test",
					"MetricPolicy":  map[string]any{"ContainerLevelMetrics": "ENABLED"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErrType: "PolicyNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestContainer(t, h, "del-test")
			tt.setup(t, h)

			// First delete succeeds.
			rec := doRequest(t, h, tt.deleteOp, map[string]any{"ContainerName": "del-test"})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Second delete returns not-found.
			rec = doRequest(t, h, tt.deleteOp, map[string]any{"ContainerName": "del-test"})
			assert.Equal(t, http.StatusNotFound, rec.Code)
			m := unmarshalBody(t, rec)
			assert.Equal(t, tt.wantErrType, m["__type"])
		})
	}
}
