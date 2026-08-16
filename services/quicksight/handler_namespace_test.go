package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// ---- Namespace tests ----

func TestQuickSight_Namespaces(t *testing.T) {
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
			name:     "CreateNamespace returns ARN",
			method:   http.MethodPost,
			path:     accountPath(""),
			body:     map[string]any{"Namespace": "my-ns", "CapacityRegion": "us-east-1"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:namespace/my-ns")
				assert.Equal(t, "CREATION_SUCCESSFUL", body["CreationStatus"])
				assert.Equal(t, "QUICKSIGHT", body["IdentityStore"])
			},
		},
		{
			name:   "CreateNamespace duplicate returns 409",
			method: http.MethodPost,
			path:   accountPath(""),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath(""), map[string]any{"Namespace": "dup-ns"})
			},
			body:     map[string]any{"Namespace": "dup-ns"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "DescribeNamespace returns default",
			method:   http.MethodGet,
			path:     accountPath("/namespaces/default"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				ns, ok := body["Namespace"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "default", ns["Name"])
				assert.Equal(t, "CREATION_SUCCESSFUL", ns["CreationStatus"])
			},
		},
		{
			name:     "DescribeNamespace missing returns 404",
			method:   http.MethodGet,
			path:     accountPath("/namespaces/nonexistent"),
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListNamespaces includes default",
			method:   http.MethodGet,
			path:     accountPath("/namespaces"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				items, ok := body["Namespaces"].([]any)
				require.True(t, ok)
				assert.NotEmpty(t, items)
			},
		},
		{
			name:     "DeleteNamespace default returns 400",
			method:   http.MethodDelete,
			path:     accountPath("/namespaces/default"),
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DeleteNamespace non-default succeeds",
			method: http.MethodDelete,
			path:   accountPath("/namespaces/to-delete"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath(""), map[string]any{"Namespace": "to-delete"})
			},
			wantCode: http.StatusOK,
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
