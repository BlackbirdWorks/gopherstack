package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestUsageAndManagedIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_usage_statistics",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/usage/statistics", map[string]any{
					"maxResults": 25,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				records, _ := resp["records"].([]any)
				assert.Empty(t, records)
			},
		},
		{
			name: "get_usage_totals",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/usage", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				totals, _ := resp["usageTotals"].([]any)
				assert.NotEmpty(t, totals)
			},
		},
		{
			name: "list_managed_data_identifiers",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/managed-data-identifiers/list", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items, _ := resp["items"].([]any)
				assert.NotEmpty(t, items)

				// Verify structure of first item
				first, _ := items[0].(map[string]any)
				assert.NotEmpty(t, first["id"])
				assert.NotEmpty(t, first["category"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}
