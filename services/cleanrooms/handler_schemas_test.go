package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemas_Handlers(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)

	// Create a collab to get an ID
	res := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "collab-schemas", "creatorDisplayName": "Me",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, res.Code)

	var colResp map[string]any
	json.Unmarshal(res.Body.Bytes(), &colResp)
	collab := colResp["collaboration"].(map[string]any)
	collabID := collab["id"].(string)

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "ListSchemas",
			method:     "GET",
			path:       "/collaborations/" + collabID + "/schemas",
			body:       nil,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetSchema NotFound",
			method:     "GET",
			path:       "/collaborations/" + collabID + "/schemas/not-found",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetSchemaAnalysisRule NotFound",
			method:     "GET",
			path:       "/collaborations/" + collabID + "/schemas/not-found/analysisRule/AGGREGATION",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "BatchGetSchema",
			method:     "POST",
			path:       "/collaborations/" + collabID + "/batch-schema",
			body:       map[string]any{"names": []string{"invalid"}},
			wantStatus: http.StatusOK, // BatchGet returns 200 with errors array
		},
		{
			name:   "BatchGetSchemaAnalysisRule",
			method: "POST",
			path:   "/collaborations/" + collabID + "/batch-schema-analysis-rule",
			body: map[string]any{
				"schemaAnalysisRuleRequests": []map[string]any{{"name": "invalid", "type": "AGGREGATION"}},
			},
			wantStatus: http.StatusOK,
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
