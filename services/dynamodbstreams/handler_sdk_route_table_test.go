package dynamodbstreams_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// DynamoDB Streams operation, extracted from
// dynamodbstreams@v1.36.4/serializers.go's
// awsAwsjson10_serializeOp<Op>.HandleSerialize calls to
// SetHeader("X-Amz-Target").String("DynamoDBStreams_20120810.<Op>"),
// always POSTing to "/" (JSON-RPC 1.0, services/_PROTOCOLS.md). This is
// the REAL DynamoDBStreams_ prefix -- distinct from the DynamoDB_ prefix
// used by services/dynamodb's own (dead) copy of these same four ops,
// see gopherstack-tsj5.
//
// All 4 real ops are covered: DescribeStream, GetRecords, GetShardIterator,
// ListStreams. GetSupportedOperations() and the dispatch switch in
// handler.go's dispatch() are both hand-written literals (neither built by
// ranging over the other), so this is a genuinely independent diff.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"DescribeStream", "DynamoDBStreams_20120810.DescribeStream"},
		{"GetRecords", "DynamoDBStreams_20120810.GetRecords"},
		{"GetShardIterator", "DynamoDBStreams_20120810.GetShardIterator"},
		{"ListStreams", "DynamoDBStreams_20120810.ListStreams"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real DynamoDB Streams
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), confirming the header resolves to the right op name and that
// dispatch does not fall through to the unmatched-route path.
//
// The sentinel for an unmatched route is __type
// "com.amazon.coral.service#UnknownOperationException" (handler.go's
// handleError, gated on strings.HasPrefix(err.Error(),
// "UnknownOperationException:")). This is distinct from every real
// DynamoDB Streams error, which is rewritten onto the
// "com.amazonaws.dynamodbstreams.v20120810#" namespace -- so asserting on
// __type is safe here, unlike services where the unmatched-route type is
// shared with a legitimate validation error.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := dynamodbstreams.NewHandler(ddbbackend.NewInMemoryDB())

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
