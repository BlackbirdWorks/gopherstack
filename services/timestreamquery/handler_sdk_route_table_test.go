package timestreamquery_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// Timestream Query operation, extracted from
// timestreamquery@v1.39.4/serializers.go's
// awsAwsjson10_serializeOp<Op>.HandleSerialize calls to
// SetHeader("X-Amz-Target").String("Timestream_20181101.<Op>"), always
// POSTing to "/" (JSON-RPC 1.0, services/_PROTOCOLS.md).
//
// All 15 real ops are covered, including TagResource, UntagResource and
// ListTagsForResource. Those 3 ARE real TimestreamQuery ops per the API
// model (confirmed present in this SDK's serializers.go) and this table's
// Handler()-level test drives them successfully -- but production routing
// never reaches them here: RouteMatcher explicitly declines them
// (writeServiceTagOps(), handler.go:39-45) because timestreamwrite@v1.38.4
// serializers.go emits the IDENTICAL prefix "Timestream_20181101." for
// these same 3 ops (confirmed directly), so gopherstack defers to a single
// unified tag store on the TimestreamWrite handler rather than splitting
// tags across two backends. This differs from a structurally-unreachable
// op (e.g. s3's ListDirectoryBuckets): the two services ARE distinguishable
// here, by operation name, not by target string -- gopherstack just chooses
// to route by name to one owner rather than duplicating tag storage.
//
// GetSupportedOperations() and the 3-deep dispatch/dispatchScheduledQueryAndTagOps/
// dispatchAccountOps switch chain are both hand-written literals (neither
// built by ranging over the other), so this is a genuinely independent diff.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CancelQuery", "Timestream_20181101.CancelQuery"},
		{"CreateScheduledQuery", "Timestream_20181101.CreateScheduledQuery"},
		{"DeleteScheduledQuery", "Timestream_20181101.DeleteScheduledQuery"},
		{"DescribeAccountSettings", "Timestream_20181101.DescribeAccountSettings"},
		{"DescribeEndpoints", "Timestream_20181101.DescribeEndpoints"},
		{"DescribeScheduledQuery", "Timestream_20181101.DescribeScheduledQuery"},
		{"ExecuteScheduledQuery", "Timestream_20181101.ExecuteScheduledQuery"},
		{"ListScheduledQueries", "Timestream_20181101.ListScheduledQueries"},
		{"ListTagsForResource", "Timestream_20181101.ListTagsForResource"},
		{"PrepareQuery", "Timestream_20181101.PrepareQuery"},
		{"Query", "Timestream_20181101.Query"},
		{"TagResource", "Timestream_20181101.TagResource"},
		{"UntagResource", "Timestream_20181101.UntagResource"},
		{"UpdateAccountSettings", "Timestream_20181101.UpdateAccountSettings"},
		{"UpdateScheduledQuery", "Timestream_20181101.UpdateScheduledQuery"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Timestream Query
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), confirming the header resolves to the right op name and that
// dispatch does not fall through to dispatchAccountOps's single
// unmatched-route return (ErrUnknownOperation, handler.go:249-259).
//
// This asserts on MESSAGE TEXT ("unknown operation"), not wire type --
// handleError maps ErrUnknownOperation to the same "ValidationException"
// type shared with ErrValidation, which is returned by many legitimate
// required-field checks (e.g. "QueryString is required") on the
// deliberately minimal "{}" request body this test sends
// (handler.go:281-284). ErrUnknownOperation's message ("unknown operation:
// <op>") has exactly one production call site (grepped) and is not
// produced by any other error path, so asserting on message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := timestreamquery.NewHandler(timestreamquery.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
