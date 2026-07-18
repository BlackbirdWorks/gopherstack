package rdsdata_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

func TestHandler_ExecuteSql(t *testing.T) {
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
				"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"awsSecretStoreArn":      "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sqlStatements":          "SELECT 1; SELECT 2",
			},
			wantStatus: http.StatusOK,
			wantBody:   "sqlStatementResults",
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_db_cluster_arn",
			body: map[string]any{
				"awsSecretStoreArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sqlStatements":     "SELECT 1",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_sql_statements",
			body: map[string]any{
				"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"awsSecretStoreArn":      "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
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
				rec = doRDSDataRawRequest(t, h, "/ExecuteSql", tt.bodyRaw)
			} else {
				rec = doRDSDataRequest(t, h, "/ExecuteSql", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestHandler_ExecuteSQL_MissingSecretArn verifies generated SDK required fields are enforced.
func TestHandler_ExecuteSQL_MissingSecretArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/ExecuteSql", map[string]any{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"sqlStatements":          "SELECT 1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ExecuteSQL_TracksStatement verifies ExecuteSQL records the statement.
func TestHandler_ExecuteSQL_TracksStatement(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	rec := doRDSDataRequest(t, h, "/ExecuteSql", map[string]any{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"awsSecretStoreArn":      "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sqlStatements":          "SELECT 99",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, rdsdata.ExecutedStatementCount(b))

	stmts := b.ListExecutedStatements(context.Background())
	require.Len(t, stmts, 1)
	assert.Equal(t, "SELECT 99", stmts[0].SQL)
}

// TestHandler_ExecuteSQL_Response verifies the response contains sqlStatementResults array.
func TestHandler_ExecuteSQL_Response(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/ExecuteSql", map[string]any{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"awsSecretStoreArn":      "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sqlStatements":          "CREATE TABLE t (id INT)",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["sqlStatementResults"]
	require.True(t, ok)
	resultsArr, ok := results.([]any)
	require.True(t, ok)
	require.Len(t, resultsArr, 1)
}

// TestHandler_ExecuteSQL_Response_NotNull verifies sqlStatementResults is not null.
func TestHandler_ExecuteSQL_Response_NotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/ExecuteSql", map[string]any{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"awsSecretStoreArn":      "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sqlStatements":          "DROP TABLE IF EXISTS t",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["sqlStatementResults"])
}
