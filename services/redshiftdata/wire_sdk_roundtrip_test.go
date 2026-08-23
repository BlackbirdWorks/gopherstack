package redshiftdata_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	redshiftdatasdk "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	redshiftdatatypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// newTestRedshiftDataSDKClient stands up the real aws-sdk-go-v2 redshiftdata
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production -- so a
// fix is verified by the real client's own deserializer, not gopherstack's
// own JSON tags.
func newTestRedshiftDataSDKClient(t *testing.T, h *redshiftdata.Handler) *redshiftdatasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return redshiftdatasdk.NewFromConfig(cfg, func(o *redshiftdatasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestExecuteStatement_Status_SDKRoundTrip proves ExecuteStatementOutput.Status
// and .HasResultSet survive the real SDK client. Confirmed against
// aws-sdk-go-v2/service/redshiftdata@v1.43.4's
// awsAwsjson11_deserializeOpDocumentExecuteStatementOutput (deserializers.go),
// whose case list includes "Status" (types.StatementStatusString) and
// "HasResultSet" (*bool) -- statementCreateResponse (handler_statements.go)
// never emitted either key, so a real typed client would see a zero-value
// Status "" and a nil HasResultSet after every ExecuteStatement call, even
// though this backend completes every statement to FINISHED synchronously.
func TestExecuteStatement_Status_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(backend)
	client := newTestRedshiftDataSDKClient(t, h)

	out, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
		Sql:               aws.String("SELECT 1"),
		Database:          aws.String("testdb"),
		ClusterIdentifier: aws.String("my-cluster"),
	})
	require.NoError(t, err)

	assert.Equal(t, redshiftdatatypes.StatementStatusStringFinished, out.Status)
	require.NotNil(t, out.HasResultSet)
	assert.True(t, *out.HasResultSet)
}

// TestBatchExecuteStatement_Status_SDKRoundTrip is the BatchExecuteStatement
// sibling of TestExecuteStatement_Status_SDKRoundTrip -- confirmed against
// awsAwsjson11_deserializeOpDocumentBatchExecuteStatementOutput, which has
// the identical "Status"/"HasResultSet" case list.
func TestBatchExecuteStatement_Status_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(backend)
	client := newTestRedshiftDataSDKClient(t, h)

	out, err := client.BatchExecuteStatement(t.Context(), &redshiftdatasdk.BatchExecuteStatementInput{
		Sqls:              []string{"SELECT 1", "SELECT 2"},
		Database:          aws.String("testdb"),
		ClusterIdentifier: aws.String("my-cluster"),
	})
	require.NoError(t, err)

	assert.Equal(t, redshiftdatatypes.StatementStatusStringFinished, out.Status)
	require.NotNil(t, out.HasResultSet)
}

// TestDescribeStatement_NoFabricatedFields proves DescribeStatement never
// leaks IsBatchStatement/StatementName/QueryStrings/WithEvent on the wire.
// Confirmed against aws-sdk-go-v2/service/redshiftdata@v1.43.4's
// DescribeStatementOutput struct (api_op_DescribeStatement.go) and its
// deserializer's case list (deserializers.go): none of these four keys
// exist on the real DescribeStatementOutput at all -- IsBatchStatement/
// StatementName/QueryStrings are real members of the wider StatementData
// (the ListStatements item shape) instead, and WithEvent is a
// request-only field on ExecuteStatementInput/BatchExecuteStatementInput
// with no response counterpart anywhere in the SDK. A real typed client
// silently drops unknown keys (this is not a (c)-class hard failure), so
// this is a raw-body assertion, matching the precedent set by
// TestListStatements_NoFabricatedFields in handler_statements_semantics_test.go.
func TestDescribeStatement_NoFabricatedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body  map[string]any
		op    string
		field string
	}{
		{op: "ExecuteStatement", field: "IsBatchStatement", body: map[string]any{
			"Sql": "SELECT * FROM orders", "Database": "testdb", "ClusterIdentifier": "my-cluster",
		}},
		{op: "ExecuteStatement", field: "StatementName", body: map[string]any{
			"Sql": "SELECT * FROM orders", "Database": "testdb", "ClusterIdentifier": "my-cluster",
			"StatementName": "my-statement",
		}},
		{op: "ExecuteStatement", field: "WithEvent", body: map[string]any{
			"Sql": "SELECT * FROM orders", "Database": "testdb", "ClusterIdentifier": "my-cluster",
			"WithEvent": true,
		}},
		{op: "BatchExecuteStatement", field: "QueryStrings", body: map[string]any{
			"Sqls": []string{"SELECT 1", "SELECT 2"}, "Database": "testdb", "ClusterIdentifier": "my-cluster",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			execRec := doRequest(t, h, tt.op, tt.body)
			require.Equal(t, http.StatusOK, execRec.Code)

			var execResp struct {
				ID string `json:"Id"`
			}
			require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))

			rec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": execResp.ID})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotContains(t, resp, tt.field)
		})
	}
}
