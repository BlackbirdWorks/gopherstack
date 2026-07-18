package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverageReporting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "ListCoverage returns empty covered resources",
			method: http.MethodPost,
			path:   "/coverage/list",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["coveredResources"]
				assert.True(t, ok)
			},
		},
		{
			name:   "ListCoverageStatistics returns totals",
			method: http.MethodPost,
			path:   "/coverage/statistics/list",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["totalCounts"]
				assert.True(t, ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}
