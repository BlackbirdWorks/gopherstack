package redshiftdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

func TestAudit_ResultFormatControlsResultAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		resultFormat  string
		getOperation  string
		wantResultFmt string
		wantStatus    int
	}{
		{
			name:         "default_json_allows_json_result",
			getOperation: "GetStatementResult",
			wantStatus:   http.StatusOK,
		},
		{
			name:          "csv_allows_v2_result",
			resultFormat:  "CSV",
			getOperation:  "GetStatementResultV2",
			wantStatus:    http.StatusOK,
			wantResultFmt: "CSV",
		},
		{
			name:         "default_json_rejects_v2_result",
			getOperation: "GetStatementResultV2",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "csv_rejects_json_result",
			resultFormat: "CSV",
			getOperation: "GetStatementResult",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"Sql": "SELECT 1", "Database": "dev"}
			if tt.resultFormat != "" {
				body["ResultFormat"] = tt.resultFormat
			}

			rec := doRequest(t, h, "ExecuteStatement", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var execResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &execResp))

			rec = doRequest(t, h, tt.getOperation, map[string]any{"Id": execResp["Id"]})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResultFmt != "" {
				var resultResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resultResp))
				assert.Equal(t, tt.wantResultFmt, resultResp["ResultFormat"])
			}
		})
	}
}

func TestAudit_ResultFormatMetadataAndValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		format     string
		wantStatus int
	}{
		{name: "default_json", wantStatus: http.StatusOK},
		{name: "csv", format: "CSV", wantStatus: http.StatusOK},
		{name: "unknown_rejected", format: "XML", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ExecuteStatement", map[string]any{
				"Sql":          "SELECT 1",
				"Database":     "dev",
				"ResultFormat": tt.format,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus != http.StatusOK {
				return
			}

			var execResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &execResp))

			rec = doRequest(t, h, "DescribeStatement", map[string]any{"Id": execResp["Id"]})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			wantFormat := tt.format
			if wantFormat == "" {
				wantFormat = "JSON"
			}
			assert.Equal(t, wantFormat, descResp["ResultFormat"])
		})
	}
}

func TestAudit_ListStatementsFilters(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b)
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql": "SELECT 1", "Database": "alpha", "StatementName": "daily-one",
	})
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql": "SELECT 2", "Database": "beta", "StatementName": "weekly-one",
	})
	redshiftdata.AddStatementInternal(b, testRegion, "started-alpha", "SELECT 3", "alpha", "STARTED", true)

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "default_finished_only", body: map[string]any{}, wantCount: 2},
		{name: "all_statuses", body: map[string]any{"Status": "ALL"}, wantCount: 3},
		{name: "database", body: map[string]any{"Database": "alpha"}, wantCount: 1},
		{name: "statement_name_prefix", body: map[string]any{"StatementName": "daily"}, wantCount: 1},
		{name: "started", body: map[string]any{"Status": "STARTED"}, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListStatements", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["Statements"], tt.wantCount)
		})
	}
}

func TestAudit_ListStatementsNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 3 {
		doRequest(t, h, "ExecuteStatement", map[string]any{
			"Sql": "SELECT " + string(rune('1'+i)), "Database": "dev",
		})
	}

	first := doRequest(t, h, "ListStatements", map[string]any{"MaxResults": 1})
	require.Equal(t, http.StatusOK, first.Code)

	var firstResp map[string]any
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &firstResp))
	token, ok := firstResp["NextToken"].(string)
	require.True(t, ok)

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{name: "continuation", token: token, wantStatus: http.StatusOK},
		{name: "invalid_token", token: "invalid", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListStatements", map[string]any{
				"MaxResults": 1, "NextToken": tt.token,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
