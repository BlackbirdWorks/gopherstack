package rdsdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsdatasdk "github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

const (
	columnOriginResourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:column-origin-cluster"
	columnOriginSecretARN   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:column-origin-secret"
)

// TestExecuteStatement_ColumnMetadata_TableOrigin verifies SchemaName/
// TableName/IsAutoIncrement are honestly populated for a single-table SELECT
// against a real table (via modernc.org/sqlite@v1.58.0's conn.ColumnInfo --
// see engine.go's columnOriginInfo), and stay empty/false for a computed
// column with no source table, matching real AWS's own "empty if the column
// does not resolve to an unambiguous reference to a single database column"
// contract for this accessor.
func TestExecuteStatement_ColumnMetadata_TableOrigin(t *testing.T) {
	t.Parallel()

	backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, rdsdata.NewHandler(backend))

	_, err := client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
		ResourceArn: aws.String(columnOriginResourceARN),
		SecretArn:   aws.String(columnOriginSecretARN),
		Sql:         aws.String("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)"),
	})
	require.NoError(t, err)

	out, err := client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
		ResourceArn:           aws.String(columnOriginResourceARN),
		SecretArn:             aws.String(columnOriginSecretARN),
		Sql:                   aws.String("SELECT id, name FROM widgets"),
		IncludeResultMetadata: true,
	})
	require.NoError(t, err)
	require.Len(t, out.ColumnMetadata, 2)

	idCol, nameCol := out.ColumnMetadata[0], out.ColumnMetadata[1]

	assert.Equal(t, "widgets", aws.ToString(idCol.TableName))
	assert.Equal(t, "main", aws.ToString(idCol.SchemaName))
	assert.True(t, idCol.IsAutoIncrement, "sole INTEGER PRIMARY KEY column is a SQLite rowid alias")

	assert.Equal(t, "widgets", aws.ToString(nameCol.TableName))
	assert.False(t, nameCol.IsAutoIncrement)

	out, err = client.ExecuteStatement(t.Context(), &rdsdatasdk.ExecuteStatementInput{
		ResourceArn:           aws.String(columnOriginResourceARN),
		SecretArn:             aws.String(columnOriginSecretARN),
		Sql:                   aws.String("SELECT 1 + 1"),
		IncludeResultMetadata: true,
	})
	require.NoError(t, err)
	require.Len(t, out.ColumnMetadata, 1)

	assert.Empty(t, aws.ToString(out.ColumnMetadata[0].TableName), "a computed column has no source table")
	assert.Empty(t, aws.ToString(out.ColumnMetadata[0].SchemaName))
	assert.False(t, out.ColumnMetadata[0].IsAutoIncrement)
	assert.Zero(t, out.ColumnMetadata[0].ArrayBaseColumnType, "arrays are unsupported -- always 0")
}

// TestExecuteStatement_ColumnMetadata_TableOrigin_InsideTransaction verifies
// that a statement run inside a BeginTransaction transaction leaves
// SchemaName/TableName/IsAutoIncrement at their zero value: *sql.Tx has no
// equivalent to *sql.Conn.Raw, so engine.go's columnOriginInfo can't recover
// the underlying driver connection to call ColumnInfo -- see PARITY.md.
//
// This drives the handler in-process (doRDSDataRequest), like this
// package's other multi-call transaction tests (transactions_test.go),
// rather than through a live newRoundTripClient server round trip: a real
// http.Server cancels each request's Context() once that request finishes,
// and sqlEngine.beginTx (engine.go) opens the engine-side *sql.Tx against
// the BeginTransaction call's own request context, so database/sql
// auto-rolls back the transaction as soon as that first HTTP request
// completes -- confirmed independently while writing this test, filed
// separately as gopherstack-fdle is scoped to typeHint/ColumnMetadata/array
// parameters, not transaction-context lifetime. Not a defect this test
// exists to ratify: doRDSDataRequest's httptest.NewRequest carries a
// never-canceled context.Background(), sidestepping it entirely so this
// test exercises only the ColumnMetadata behavior it's named for.
func TestExecuteStatement_ColumnMetadata_TableOrigin_InsideTransaction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn": columnOriginResourceARN,
		"secretArn":   columnOriginSecretARN,
		"sql":         "CREATE TABLE gizmos (id INTEGER PRIMARY KEY, name TEXT)",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
		"resourceArn": columnOriginResourceARN,
		"secretArn":   columnOriginSecretARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var begin map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &begin))
	txID, ok := begin["transactionId"].(string)
	require.True(t, ok)

	rec = doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":           columnOriginResourceARN,
		"secretArn":             columnOriginSecretARN,
		"sql":                   "SELECT id FROM gizmos",
		"transactionId":         txID,
		"includeResultMetadata": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cols, ok := resp["columnMetadata"].([]any)
	require.True(t, ok)
	require.Len(t, cols, 1)

	col, ok := cols[0].(map[string]any)
	require.True(t, ok)
	assert.Empty(t, col["tableName"])
	assert.Empty(t, col["schemaName"])
	assert.Equal(t, false, col["isAutoIncrement"])

	rec = doRDSDataRequest(t, h, "/CommitTransaction", map[string]any{
		"resourceArn":   columnOriginResourceARN,
		"secretArn":     columnOriginSecretARN,
		"transactionId": txID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}
