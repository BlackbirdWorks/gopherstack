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

func TestHandler_ExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sql":         "SELECT 1",
			},
			wantStatus: http.StatusOK,
			wantBody:   "records",
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_resource_arn",
			body: map[string]any{
				"sql": "SELECT 1",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_sql",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doRDSDataRawRequest(t, h, "/Execute", tt.bodyRaw)
			} else {
				rec = doRDSDataRequest(t, h, "/Execute", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_BatchExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name: "success_with_params",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sql":         "INSERT INTO test VALUES (:val)",
				"parameterSets": []any{
					[]any{map[string]any{"name": "val", "value": map[string]any{"stringValue": "a"}}},
				},
			},
			wantStatus: http.StatusOK,
			wantBody:   "updateResults",
		},
		{
			name: "success_empty_params",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sql":         "INSERT INTO test VALUES (:val)",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_resource_arn",
			body: map[string]any{
				"sql": "INSERT INTO test VALUES (:val)",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_sql_batch",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_transaction_id",
			body: map[string]any{
				"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sql":           "INSERT INTO test VALUES (:val)",
				"transactionId": "txn-does-not-exist",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doRDSDataRawRequest(t, h, "/BatchExecute", tt.bodyRaw)
			} else {
				rec = doRDSDataRequest(t, h, "/BatchExecute", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_BeginTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_resource_arn",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doRDSDataRawRequest(t, h, "/BeginTransaction", tt.bodyRaw)
			} else {
				rec = doRDSDataRequest(t, h, "/BeginTransaction", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if rec.Code == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "transactionId")
			}
		})
	}
}

func TestHandler_CommitTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		bodyRaw    []byte
		wantStatus int
		startTxn   bool
	}{
		{
			name:       "success",
			startTxn:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_transaction_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "transaction_not_found",
			body: map[string]any{
				"transactionId": "txn-does-not-exist",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch {
			case tt.startTxn:
				beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
					"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
					"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				})
				require.Equal(t, http.StatusOK, beginRec.Code)

				var beginResp map[string]any
				require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
				txID := beginResp["transactionId"].(string)

				rec = doRDSDataRequest(t, h, "/CommitTransaction", map[string]any{
					"transactionId": txID,
				})
			case tt.bodyRaw != nil:
				rec = doRDSDataRawRequest(t, h, "/CommitTransaction", tt.bodyRaw)
			default:
				rec = doRDSDataRequest(t, h, "/CommitTransaction", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if rec.Code == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "transactionStatus")
			}
		})
	}
}

func TestHandler_RollbackTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		bodyRaw    []byte
		wantStatus int
		startTxn   bool
	}{
		{
			name:       "success",
			startTxn:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_transaction_id",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "transaction_not_found",
			body: map[string]any{
				"transactionId": "txn-does-not-exist",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder

			switch {
			case tt.startTxn:
				beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
					"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
					"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				})
				require.Equal(t, http.StatusOK, beginRec.Code)

				var beginResp map[string]any
				require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
				txID := beginResp["transactionId"].(string)

				rec = doRDSDataRequest(t, h, "/RollbackTransaction", map[string]any{
					"transactionId": txID,
				})
			case tt.bodyRaw != nil:
				rec = doRDSDataRawRequest(t, h, "/RollbackTransaction", tt.bodyRaw)
			default:
				rec = doRDSDataRequest(t, h, "/RollbackTransaction", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if rec.Code == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "transactionStatus")
			}
		})
	}
}

func TestHandler_ExecuteStatement_WithTransaction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
	})
	require.Equal(t, http.StatusOK, beginRec.Code)

	var beginResp map[string]any
	require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
	txID := beginResp["transactionId"].(string)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":           "INSERT INTO test VALUES (1)",
		"transactionId": txID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ExecuteStatement_InvalidTransaction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":           "INSERT INTO test VALUES (1)",
		"transactionId": "txn-does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestBackend_ListExecutedStatements(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	_, _, _, err := b.ExecuteStatement("arn:aws:rds:us-east-1:000000000000:cluster:test", "SELECT 1", "")
	require.NoError(t, err)

	stmts := b.ListExecutedStatements()
	require.Len(t, stmts, 1)
	assert.Equal(t, "SELECT 1", stmts[0].SQL)
}

func TestBackend_ListTransactions(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	txID, err := b.BeginTransaction("arn:aws:rds:us-east-1:000000000000:cluster:test")
	require.NoError(t, err)

	txns := b.ListTransactions()
	assert.Contains(t, txns, txID)
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &rdsdata.Provider{}
	assert.Equal(t, "RDSData", p.Name())

	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}
