package redshiftdata_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// Redshift Data API operation, extracted from
// redshiftdata@v1.43.4/serializers.go's
// awsAwsjson11_serializeOp<Op>.HandleSerialize calls to
// SetHeader("X-Amz-Target").String("RedshiftData.<Op>"), always POSTing to
// "/" (JSON-RPC 1.1, services/_PROTOCOLS.md).
//
// All 12 real ops are covered. GetSupportedOperations() and the dispatch()
// switch are both hand-written literals (neither built by ranging over the
// other), so this is a genuinely independent diff.
//
// redshiftdata's Handler embeds a live *Janitor (a background goroutine
// started via WithJanitor/StartWorker) alongside its StorageBackend -- one
// of the two services flagged early in this campaign for a value struct
// embedding a live handle. That flag has now twice proved irrelevant to
// routing (apigatewaymanagementapi, dax): the janitor is a lifecycle
// concern (background TTL sweeps) never read by ExtractOperation or
// dispatch, and this table confirms the pattern a third time -- NewHandler
// alone (no WithJanitor/StartWorker call, as below) is sufficient to
// exercise every route.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"BatchExecuteStatement", "RedshiftData.BatchExecuteStatement"},
		{"CancelStatement", "RedshiftData.CancelStatement"},
		{"DescribeStatement", "RedshiftData.DescribeStatement"},
		{"DescribeTable", "RedshiftData.DescribeTable"},
		{"ExecuteStatement", "RedshiftData.ExecuteStatement"},
		{"GetStatementResult", "RedshiftData.GetStatementResult"},
		{"GetStatementResultV2", "RedshiftData.GetStatementResultV2"},
		{"ListDatabases", "RedshiftData.ListDatabases"},
		{"ListSchemas", "RedshiftData.ListSchemas"},
		{"ListSessions", "RedshiftData.ListSessions"},
		{"ListStatements", "RedshiftData.ListStatements"},
		{"ListTables", "RedshiftData.ListTables"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Redshift Data
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), confirming the header resolves to the right op name and that
// dispatch does not fall through to dispatch()'s single unmatched-route
// return (errUnknownAction, handler.go:247-249).
//
// This asserts on MESSAGE TEXT ("unknown action"), not wire type --
// handleError maps errUnknownAction to the same "ValidationException" type
// shared with ErrTerminalState, ErrValidation and ErrNoResultSet
// (handler.go:264-277), so a type assertion would not distinguish an
// unmatched route from a legitimate validation error on the deliberately
// minimal "{}" request body this test sends. errUnknownAction's message
// ("unknown action: <action>") has exactly one production call site
// (grepped) and is not produced by any other error path, so asserting on
// message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := redshiftdata.NewHandler(redshiftdata.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
