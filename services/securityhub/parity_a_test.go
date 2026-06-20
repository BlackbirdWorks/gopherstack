package securityhub_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// enableHub is a helper that enables SecurityHub on a fresh handler.
func enableHub(t *testing.T, h *securityhub.Handler) {
	t.Helper()
	rec := doRequest(t, h, http.MethodPost, "/accounts", map[string]any{
		"EnableDefaultStandards": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestParity_CreateInsightRequiresNameAndGroupByAttribute verifies that
// CreateInsight rejects requests with a missing Name or GroupByAttribute.
// Real AWS returns 400 for both; the emulator previously silently stored
// insights with empty required fields.
func TestParity_CreateInsightRequiresNameAndGroupByAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "absent_name_rejected",
			body: map[string]any{
				"GroupByAttribute": "ProductName",
				"Filters":          map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_name_rejected",
			body: map[string]any{
				"Name":             "",
				"GroupByAttribute": "ProductName",
				"Filters":          map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "absent_group_by_attribute_rejected",
			body: map[string]any{
				"Name":    "my-insight",
				"Filters": map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_insight_accepted",
			body: map[string]any{
				"Name":             "my-insight",
				"GroupByAttribute": "ProductName",
				"Filters":          map[string]any{},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			enableHub(t, h)
			rec := doRequest(t, h, http.MethodPost, "/insights", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateInsight status for case %q", tt.name)
		})
	}
}
