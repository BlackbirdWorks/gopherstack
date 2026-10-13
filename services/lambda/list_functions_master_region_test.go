package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListFunctions_MasterRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantCode  int
		wantCount int
	}{
		{name: "no_filter", query: "", wantCode: http.StatusOK, wantCount: 1},
		{
			name:     "region_with_all_versions",
			query:    "?FunctionVersion=ALL&MasterRegion=us-east-1",
			wantCode: http.StatusOK,
		},
		{name: "region_without_all", query: "?MasterRegion=us-east-1", wantCode: http.StatusBadRequest},
		{
			name:      "region_with_latest_only",
			query:     "?FunctionVersion=ALL&MasterRegion=ALL",
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "edge-fn")

			rec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions"+tt.query, "")
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantCode != http.StatusOK {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")

				return
			}

			var out struct {
				Functions []json.RawMessage `json:"Functions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Functions, tt.wantCount)
		})
	}
}
