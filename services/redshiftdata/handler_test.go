package redshiftdata_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "000000000000"
)

func newTestHandler(t *testing.T) *redshiftdata.Handler {
	t.Helper()

	backend := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	return redshiftdata.NewHandler(backend)
}

func doRequest(t *testing.T, h *redshiftdata.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "RedshiftData."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "RedshiftData", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "ExecuteStatement")
	assert.Contains(t, ops, "BatchExecuteStatement")
	assert.Contains(t, ops, "DescribeStatement")
	assert.Contains(t, ops, "GetStatementResult")
	assert.Contains(t, ops, "ListStatements")
	assert.Contains(t, ops, "CancelStatement")
	assert.Contains(t, ops, "ListDatabases")
	assert.Contains(t, ops, "ListSchemas")
	assert.Contains(t, ops, "ListTables")
	assert.Contains(t, ops, "DescribeTable")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "redshift-data", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Contains(t, ops, "ExecuteStatement")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	assert.Equal(t, []string{testRegion}, regions)
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches_ExecuteStatement",
			target:    "RedshiftData.ExecuteStatement",
			wantMatch: true,
		},
		{
			name:      "matches_DescribeStatement",
			target:    "RedshiftData.DescribeStatement",
			wantMatch: true,
		},
		{
			name:      "no_match_wrong_prefix",
			target:    "AWSOrganizations.Something",
			wantMatch: false,
		},
		{
			name:      "no_match_empty",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			got := matcher(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "extract_ExecuteStatement",
			target: "RedshiftData.ExecuteStatement",
			want:   "ExecuteStatement",
		},
		{
			name:   "extract_DescribeStatement",
			target: "RedshiftData.DescribeStatement",
			want:   "DescribeStatement",
		},
		{
			name:   "extract_ListStatements",
			target: "RedshiftData.ListStatements",
			want:   "ListStatements",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.ExtractOperation(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "success",
			body: map[string]any{
				"Sql":               "SELECT 1",
				"ClusterIdentifier": "my-cluster",
				"Database":          "testdb",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name: "success_with_workgroup",
			body: map[string]any{
				"Sql":           "SELECT 2",
				"WorkgroupName": "my-workgroup",
				"Database":      "testdb",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name:       "missing_sql",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_json",
			body: nil,
			// empty body is treated as invalid
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ExecuteStatement", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["Id"])
			}
		})
	}
}

func TestHandler_BatchExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "success",
			body: map[string]any{
				"Sqls":              []string{"SELECT 1", "SELECT 2"},
				"ClusterIdentifier": "my-cluster",
				"Database":          "testdb",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name:       "missing_sqls",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "BatchExecuteStatement", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["Id"])
			}
		})
	}
}

func TestHandler_DescribeStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*redshiftdata.Handler) string
		name        string
		requestID   string
		wantStatus2 string
		wantStatus  int
	}{
		{
			name: "existing_statement",
			setup: func(h *redshiftdata.Handler) string {
				rec := doRequest(t, h, "ExecuteStatement", map[string]any{
					"Sql":               "SELECT 1",
					"ClusterIdentifier": "my-cluster",
					"Database":          "testdb",
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["Id"].(string)
			},
			wantStatus:  http.StatusOK,
			wantStatus2: "FINISHED",
		},
		{
			name:       "not_found",
			setup:      func(_ *redshiftdata.Handler) string { return "nonexistent-id" },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_id",
			setup:      func(_ *redshiftdata.Handler) string { return "" },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			body := map[string]any{"Id": id}

			if id == "" {
				body = map[string]any{}
			}

			rec := doRequest(t, h, "DescribeStatement", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus2 != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantStatus2, resp["Status"])
			}
		})
	}
}

func TestHandler_GetStatementResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*redshiftdata.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "existing_statement",
			setup: func(h *redshiftdata.Handler) string {
				rec := doRequest(t, h, "ExecuteStatement", map[string]any{
					"Sql":               "SELECT 1",
					"ClusterIdentifier": "my-cluster",
					"Database":          "testdb",
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["Id"].(string)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			setup:      func(_ *redshiftdata.Handler) string { return "nonexistent-id" },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doRequest(t, h, "GetStatementResult", map[string]any{"Id": id})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["Records"])
			}
		})
	}
}

func TestHandler_ListStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		stmts      int
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty_list",
			stmts:      0,
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "with_statements",
			stmts:      2,
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.stmts {
				doRequest(t, h, "ExecuteStatement", map[string]any{
					"Sql":               "SELECT " + string(rune('0'+i)),
					"ClusterIdentifier": "my-cluster",
					"Database":          "testdb",
				})
			}

			rec := doRequest(t, h, "ListStatements", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			stmts, ok := resp["Statements"].([]any)
			require.True(t, ok)
			assert.Len(t, stmts, tt.wantCount)
		})
	}
}

func TestHandler_CancelStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*redshiftdata.Handler) string
		name       string
		wantStatus int
	}{
		{
			name:       "not_found",
			setup:      func(_ *redshiftdata.Handler) string { return "nonexistent-id" },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_id",
			setup:      func(_ *redshiftdata.Handler) string { return "" },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)

			var body map[string]any
			if id != "" {
				body = map[string]any{"Id": id}
			} else {
				body = map[string]any{}
			}

			rec := doRequest(t, h, "CancelStatement", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListDatabases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Databases"])
}

func TestHandler_ListSchemas(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListSchemas", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Schemas"])
}

func TestHandler_ListTables(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTables", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Tables"])
}

func TestHandler_DescribeTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeTable", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["ColumnList"])
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &redshiftdata.Provider{}
	assert.Equal(t, "RedshiftData", p.Name())
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	assert.Equal(t, testRegion, b.Region())
}

func TestHandler_CancelStatement_AlreadyFinished(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create and immediately finish a statement.
	rec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 1",
		"ClusterIdentifier": "my-cluster",
		"Database":          "testdb",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	id := resp["Id"].(string)

	// Cancelling a FINISHED statement should return an error.
	cancelRec := doRequest(t, h, "CancelStatement", map[string]any{"Id": id})
	assert.Equal(t, http.StatusBadRequest, cancelRec.Code)
}

func TestHandler_ListStatements_WithFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create statements for two different clusters.
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 1",
		"ClusterIdentifier": "cluster-a",
		"Database":          "testdb",
	})
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 2",
		"ClusterIdentifier": "cluster-b",
		"Database":          "testdb",
	})

	// Filter by cluster-a.
	rec := doRequest(t, h, "ListStatements", map[string]any{
		"ClusterIdentifier": "cluster-a",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts := resp["Statements"].([]any)
	assert.Len(t, stmts, 1)
}

func TestHandler_ListStatements_WithWorkgroupFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":           "SELECT 1",
		"WorkgroupName": "wg-a",
		"Database":      "testdb",
	})
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":           "SELECT 2",
		"WorkgroupName": "wg-b",
		"Database":      "testdb",
	})

	rec := doRequest(t, h, "ListStatements", map[string]any{
		"WorkgroupName": "wg-a",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts := resp["Statements"].([]any)
	assert.Len(t, stmts, 1)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		want string
		body []byte
	}{
		{
			name: "with_id",
			body: []byte(`{"Id": "test-id-123"}`),
			want: "test-id-123",
		},
		{
			name: "without_id",
			body: []byte(`{}`),
			want: "",
		},
		{
			name: "invalid_json",
			body: []byte(`not-json`),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "RedshiftData.DescribeStatement")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			got := h.ExtractResource(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_DescribeStatement_AllFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a statement with all optional fields set.
	rec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 1",
		"ClusterIdentifier": "my-cluster",
		"WorkgroupName":     "my-workgroup",
		"Database":          "testdb",
		"DbUser":            "myuser",
		"SecretArn":         "arn:aws:secretsmanager:us-east-1:000000000000:secret:mysecret",
		"StatementName":     "my-statement",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	id := createResp["Id"].(string)

	descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

	assert.Equal(t, id, resp["Id"])
	assert.Equal(t, "FINISHED", resp["Status"])
	assert.Equal(t, "my-cluster", resp["ClusterIdentifier"])
	assert.Equal(t, "my-workgroup", resp["WorkgroupName"])
	assert.Equal(t, "testdb", resp["Database"])
	assert.Equal(t, "myuser", resp["DbUser"])
	assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:mysecret", resp["SecretArn"])
	assert.Equal(t, "my-statement", resp["StatementName"])
}

func TestHandler_BatchExecuteStatement_AllFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
		"Sqls":              []string{"SELECT 1", "SELECT 2"},
		"ClusterIdentifier": "my-cluster",
		"Database":          "testdb",
		"StatementName":     "my-batch",
		"SecretArn":         "arn:aws:secretsmanager:us-east-1:000000000000:secret:mysecret",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	id := createResp["Id"].(string)

	descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

	assert.Equal(t, id, resp["Id"])
	assert.Equal(t, true, resp["IsBatchStatement"])
	queryStrings, ok := resp["QueryStrings"].([]any)
	require.True(t, ok)
	assert.Len(t, queryStrings, 2)
}

func TestHandler_ListStatements_WithSecretARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":           "SELECT 1",
		"Database":      "testdb",
		"SecretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:mysecret",
		"StatementName": "named-stmt",
	})

	rec := doRequest(t, h, "ListStatements", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts := resp["Statements"].([]any)
	require.Len(t, stmts, 1)

	stmt := stmts[0].(map[string]any)
	assert.Equal(t, "named-stmt", stmt["StatementName"])
	assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:mysecret", stmt["SecretArn"])
}
