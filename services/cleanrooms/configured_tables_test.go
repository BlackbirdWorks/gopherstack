package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createConfiguredTable(t *testing.T, e *echo.Echo) string {
	t.Helper()
	rec := doRequest(t, e, "POST", "/configuredTables", map[string]any{
		"name":           "id-test-table",
		"allowedColumns": []string{"col1"},
		"analysisMethod": "DIRECT_QUERY",
		"tableReference": map[string]any{"glue": map[string]any{
			"databaseName": "db", "tableName": "tbl",
		}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["configuredTable"].(map[string]any)["id"].(string)
}

func TestConfiguredTables_Create(t *testing.T) {
	t.Parallel()

	type args struct {
		body map[string]any
	}
	type wants struct {
		status int
	}

	tests := []struct {
		args  args
		name  string
		wants wants
	}{
		{
			name: "valid_create",
			args: args{
				body: map[string]any{
					"name":           "id-test-table",
					"allowedColumns": []string{"col1"},
					"analysisMethod": "DIRECT_QUERY",
					"tableReference": map[string]any{"glue": map[string]any{
						"databaseName": "db", "tableName": "tbl",
					}},
				},
			},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)

			rec := doRequest(t, e, "POST", "/configuredTables", tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			ct := resp["configuredTable"].(map[string]any)

			id, hasID := ct["id"]
			_, hasInvented := ct["configuredTableIdentifier"]

			assert.True(t, hasID, "configuredTable must have 'id' key (AWS canonical)")
			assert.False(t, hasInvented, "configuredTable must not have invented 'configuredTableIdentifier' key")
			assert.NotEmpty(t, id)
		})
	}
}

func TestConfiguredTableAnalysisRules_Create(t *testing.T) {
	t.Parallel()

	type args struct {
		body map[string]any
	}
	type wants struct {
		status int
	}

	tests := []struct {
		args  args
		name  string
		wants wants
	}{
		{
			name: "valid_create",
			args: args{
				body: map[string]any{
					"type":   "LIST",
					"policy": map[string]any{"v1": map[string]any{"list": map[string]any{}}},
				},
			},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			ctID := createConfiguredTable(t, e)

			rec := doRequest(t, e, "POST", "/configuredTables/"+ctID+"/analysisRule", tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code, rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			ar := resp["analysisRule"].(map[string]any)

			assert.Contains(t, ar, "configuredTableId", "analysisRule must have canonical 'configuredTableId' key")
			assert.Equal(t, ctID, ar["configuredTableId"])
		})
	}
}

func TestConfiguredTableAnalysisRules_Delete(t *testing.T) {
	t.Parallel()

	type args struct {
		ruleType string
	}
	type wants struct {
		status int
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "valid_delete",
			args: args{
				ruleType: "AGGREGATION",
			},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			ctID := createConfiguredTable(t, e)

			ruleRec := doRequest(t, e, "POST", "/configuredTables/"+ctID+"/analysisRule", map[string]any{
				"analysisRuleType": "AGGREGATION",
				"analysisRulePolicy": map[string]any{
					"v1": map[string]any{
						"aggregation": map[string]any{
							"aggregateColumns":  []any{},
							"joinColumns":       []any{},
							"dimensionColumns":  []any{},
							"scalarFunctions":   []any{},
							"outputConstraints": []any{},
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, ruleRec.Code)

			delRec := doRequest(t, e, "DELETE", "/configuredTables/"+ctID+"/analysisRule/"+tt.args.ruleType, nil)
			require.Equal(t, tt.wants.status, delRec.Code)
		})
	}
}
