package rdsdata_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

func newTestHandler(t *testing.T) *rdsdata.Handler {
	t.Helper()

	return rdsdata.NewHandler(rdsdata.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRDSDataRequest(t *testing.T, h *rdsdata.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	return doRDSDataRawRequest(t, h, path, bodyBytes)
}

func doRDSDataRawRequest(t *testing.T, h *rdsdata.Handler, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/rds-data/aws4_request")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "RDSData", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "ExecuteStatement")
	assert.Contains(t, ops, "BatchExecuteStatement")
	assert.Contains(t, ops, "BeginTransaction")
	assert.Contains(t, ops, "CommitTransaction")
	assert.Contains(t, ops, "RollbackTransaction")
	assert.Contains(t, ops, "ExecuteSql")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 87, h.MatchPriority())
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "rds-data", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		authService string
		want        bool
	}{
		{
			name:        "matches_execute",
			path:        "/Execute",
			authService: "rds-data",
			want:        true,
		},
		{
			name:        "matches_batch_execute",
			path:        "/BatchExecute",
			authService: "rds-data",
			want:        true,
		},
		{
			name:        "matches_begin_transaction",
			path:        "/BeginTransaction",
			authService: "rds-data",
			want:        true,
		},
		{
			name:        "matches_commit_transaction",
			path:        "/CommitTransaction",
			authService: "rds-data",
			want:        true,
		},
		{
			name:        "matches_rollback_transaction",
			path:        "/RollbackTransaction",
			authService: "rds-data",
			want:        true,
		},
		{
			name:        "matches_execute_sql",
			path:        "/ExecuteSql",
			authService: "rds-data",
			want:        true,
		},
		{
			name:        "no_match_wrong_service",
			path:        "/Execute",
			authService: "s3",
			want:        false,
		},
		{
			name:        "no_match_unrelated_path",
			path:        "/api/v1/other",
			authService: "rds-data",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)

			if tt.authService != "" {
				req.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.authService+"/aws4_request")
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		wantOp string
	}{
		{
			name:   "execute_statement",
			path:   "/Execute",
			wantOp: "ExecuteStatement",
		},
		{
			name:   "batch_execute",
			path:   "/BatchExecute",
			wantOp: "BatchExecuteStatement",
		},
		{
			name:   "begin_transaction",
			path:   "/BeginTransaction",
			wantOp: "BeginTransaction",
		},
		{
			name:   "commit_transaction",
			path:   "/CommitTransaction",
			wantOp: "CommitTransaction",
		},
		{
			name:   "rollback_transaction",
			path:   "/RollbackTransaction",
			wantOp: "RollbackTransaction",
		},
		{
			name:   "execute_sql",
			path:   "/ExecuteSql",
			wantOp: "ExecuteSql",
		},
		{
			name:   "unknown_path",
			path:   "/Unknown",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/Execute", nil)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Empty(t, h.ExtractResource(c))
}

func TestHandler_RequiredFields(t *testing.T) {
	t.Parallel()

	const (
		resourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"
		secretARN   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret"
	)

	tests := []struct {
		name string
		body map[string]any
		path string
	}{
		{
			name: "execute_statement_secret_arn",
			path: "/Execute",
			body: map[string]any{"resourceArn": resourceARN, "sql": "SELECT 1"},
		},
		{
			name: "batch_execute_statement_secret_arn",
			path: "/BatchExecute",
			body: map[string]any{"resourceArn": resourceARN, "sql": "SELECT 1"},
		},
		{
			name: "begin_transaction_secret_arn",
			path: "/BeginTransaction",
			body: map[string]any{"resourceArn": resourceARN},
		},
		{
			name: "commit_transaction_resource_arn",
			path: "/CommitTransaction",
			body: map[string]any{"secretArn": secretARN, "transactionId": "txn-000001"},
		},
		{
			name: "commit_transaction_secret_arn",
			path: "/CommitTransaction",
			body: map[string]any{"resourceArn": resourceARN, "transactionId": "txn-000001"},
		},
		{
			name: "rollback_transaction_resource_arn",
			path: "/RollbackTransaction",
			body: map[string]any{"secretArn": secretARN, "transactionId": "txn-000001"},
		},
		{
			name: "rollback_transaction_secret_arn",
			path: "/RollbackTransaction",
			body: map[string]any{"resourceArn": resourceARN, "transactionId": "txn-000001"},
		},
		{
			name: "execute_sql_aws_secret_store_arn",
			path: "/ExecuteSql",
			body: map[string]any{"dbClusterOrInstanceArn": resourceARN, "sqlStatements": "SELECT 1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRDSDataRequest(t, newTestHandler(t), tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "missing")
		})
	}
}

func TestHandler_DispatchInvalidJSON_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Bad JSON on a known path exercises the errInvalidRequest branch of
	// dispatch, since the RouteMatcher guards unregistered paths and an
	// unknown-op branch is otherwise unreachable via HTTP.
	rec := doRDSDataRawRequest(t, h, "/Execute", []byte("{bad json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var b rdsdata.StorageBackend = rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)
	assert.NotNil(t, h)
}

// TestHandler_ConcurrentRequests_Race exercises concurrent requests to detect data races.
func TestHandler_ConcurrentRequests_Race(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	const n = 20

	done := make(chan struct{}, n)

	for i := range n {
		go func(i int) {
			defer func() { done <- struct{}{} }()

			body, _ := json.Marshal(map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:c",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:s",
				"sql":         "SELECT " + string(rune('0'+i%10)),
			})

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/Execute", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/rds-data/aws4_request")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			_ = h.Handler()(c)
		}(i)
	}

	for range n {
		<-done
	}

	assert.Equal(t, n, rdsdata.ExecutedStatementCount(b))
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &rdsdata.Provider{}
	assert.Equal(t, "RDSData", p.Name())

	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestProvider_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &rdsdata.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, rdsdata.ErrNilAppContext)
}
