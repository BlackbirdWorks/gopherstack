package dynamodb_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// sdkRouteOps is the authoritative operation list for DynamoDB, taken from
// the api_op_*.go filenames in dynamodb@v1.63.1 (one file per real op) and
// cross-checked against the X-Amz-Target header literal each op's
// awsAwsjson10_serializeOp<Op> writes via
// httpBindingEncoder.SetHeader("X-Amz-Target").String("DynamoDB_20120810.<Op>")
// in serializers.go. DynamoDB is JSON-RPC: there is no path/method routing
// to drift, the op name IS the wire value, so ExtractOperation cannot
// misroute on its own -- the risk this table guards is Handler()'s dispatch
// switch (handler.go dispatch/dispatchTableOps/dispatchItemOps/...) silently
// missing a case and falling through to the UnknownOperationException
// sentinel branch.
//
// Regenerate by listing api_op_*.go in the pinned dynamodb module.
func sdkRouteOps() []string {
	return []string{
		"BatchExecuteStatement",
		"BatchGetItem",
		"BatchWriteItem",
		"CreateBackup",
		"CreateGlobalTable",
		"CreateTable",
		"DeleteBackup",
		"DeleteItem",
		"DeleteResourcePolicy",
		"DeleteTable",
		"DescribeBackup",
		"DescribeContinuousBackups",
		"DescribeContributorInsights",
		"DescribeEndpoints",
		"DescribeExport",
		"DescribeGlobalTable",
		"DescribeGlobalTableSettings",
		"DescribeImport",
		"DescribeKinesisStreamingDestination",
		"DescribeLimits",
		"DescribeTable",
		"DescribeTableReplicaAutoScaling",
		"DescribeTimeToLive",
		"DisableKinesisStreamingDestination",
		"EnableKinesisStreamingDestination",
		"ExecuteStatement",
		"ExecuteTransaction",
		"ExportTableToPointInTime",
		"GetItem",
		"GetResourcePolicy",
		"ImportTable",
		"ListBackups",
		"ListContributorInsights",
		"ListExports",
		"ListGlobalTables",
		"ListImports",
		"ListTables",
		"ListTagsOfResource",
		"PutItem",
		"PutResourcePolicy",
		"Query",
		"RestoreTableFromBackup",
		"RestoreTableToPointInTime",
		"Scan",
		"SearchVectors",
		"TagResource",
		"TransactGetItems",
		"TransactWriteItems",
		"UntagResource",
		"UpdateContinuousBackups",
		"UpdateContributorInsights",
		"UpdateGlobalTable",
		"UpdateGlobalTableSettings",
		"UpdateItem",
		"UpdateKinesisStreamingDestination",
		"UpdateTable",
		"UpdateTableReplicaAutoScaling",
		"UpdateTimeToLive",
	}
}

// TestExtractOperation_SDKRouteTable drives every real DynamoDB operation's
// authoritative X-Amz-Target header through ExtractOperation and the real
// Handler(), asserting the response never falls through to the
// UnknownOperationException sentinel handler.go's dispatch default case
// emits. A minimal "{}" body is enough: DynamoDB's JSON-RPC dispatch
// resolves purely on the header, and an empty/zero-value input is expected
// to surface as a normal validation error from the real backend method, not
// as the unknown-operation branch.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(db)

	for _, op := range sdkRouteOps() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got, "ExtractOperation mismatch for target DynamoDB_20120810.%s", op)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"op=%s: dispatched to the unknown-operation handler", op)
		})
	}
}
