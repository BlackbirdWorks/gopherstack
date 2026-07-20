package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfiguredTables_Handlers(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)

	// Create a configured table
	res := doRequest(t, e, "POST", "/configuredTables", map[string]any{
		"name": "ct-test", "description": "desc",
		"tableReference": map[string]any{"glue": map[string]any{"databaseName": "db", "tableName": "t"}},
		"allowedColumns": []string{"id"}, "analysisMethod": "DIRECT_QUERY",
	})
	require.Equal(t, http.StatusOK, res.Code)

	var ctResp map[string]any
	json.Unmarshal(res.Body.Bytes(), &ctResp)
	ct := ctResp["configuredTable"].(map[string]any)
	id := ct["id"].(string)

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "GetConfiguredTable",
			method:     "GET",
			path:       "/configuredTables/" + id,
			body:       nil,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetConfiguredTable NotFound",
			method:     "GET",
			path:       "/configuredTables/invalid",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetAnalysisRule NotFound",
			method:     "GET",
			path:       "/configuredTables/" + id + "/analysisRule/AGGREGATION",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdateAnalysisRule NotFound",
			method:     "PATCH",
			path:       "/configuredTables/" + id + "/analysisRule/AGGREGATION",
			body:       map[string]any{"analysisRulePolicy": map[string]any{"v1": map[string]any{}}},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := doRequest(t, e, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, r.Code)
		})
	}
}
