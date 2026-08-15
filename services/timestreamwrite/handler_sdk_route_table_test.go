package timestreamwrite_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Timestream
// Write operation, extracted from timestreamwrite@v1.38.4 serializers.go:
// each op's awsAwsjson10_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("Timestream_20181101.<Op>")
// and always POSTs to "/" -- Timestream Write is JSON-RPC 1.0
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header.
// ExtractOperation and Handler() (via h.dispatch's h.ops flat map, built
// once by buildOps()) both derive the action the same way (TrimPrefix on
// "Timestream_20181101."), so the class of bug this table catches is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case -- Timestream Write is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This service also has a CBOR path (handleCBOR, for smithy rpc-v2-cbor
// clients) that this table does not exercise -- it is a separate encoding
// of the same target/action, gated by service.IsCBORRequest before falling
// through to the JSON path this table drives, and shares the same h.dispatch
// map so it is covered by construction rather than needing its own table.
//
// The X-Amz-Target prefix is shared with the sibling TimestreamQuery
// service; RouteMatcher additionally checks h.supportedOps (built from
// GetSupportedOperations()) so requests for TimestreamQuery's own actions
// are not claimed here -- confirmed in handler.go's RouteMatcher doc
// comment, not asserted directly by this table since it targets
// ExtractOperation/Handler, not routing between services.
//
// This table covers all 19 real Timestream Write ops (timestreamwrite@v1.38.4)
// -- confirmed by diffing this SDK-extracted list against both
// GetSupportedOperations() (a hand-written literal) and the actual
// buildOps() dispatch map (also a hand-written literal, not built by
// ranging over anything): zero mismatches in either direction, no dead or
// excluded keys. The two diffs are genuinely independent -- neither is
// derived from the other.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Timestream_20181101.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateBatchLoadTask", "Timestream_20181101.CreateBatchLoadTask"},
		{"CreateDatabase", "Timestream_20181101.CreateDatabase"},
		{"CreateTable", "Timestream_20181101.CreateTable"},
		{"DeleteDatabase", "Timestream_20181101.DeleteDatabase"},
		{"DeleteTable", "Timestream_20181101.DeleteTable"},
		{"DescribeBatchLoadTask", "Timestream_20181101.DescribeBatchLoadTask"},
		{"DescribeDatabase", "Timestream_20181101.DescribeDatabase"},
		{"DescribeEndpoints", "Timestream_20181101.DescribeEndpoints"},
		{"DescribeTable", "Timestream_20181101.DescribeTable"},
		{"ListBatchLoadTasks", "Timestream_20181101.ListBatchLoadTasks"},
		{"ListDatabases", "Timestream_20181101.ListDatabases"},
		{"ListTables", "Timestream_20181101.ListTables"},
		{"ListTagsForResource", "Timestream_20181101.ListTagsForResource"},
		{"ResumeBatchLoadTask", "Timestream_20181101.ResumeBatchLoadTask"},
		{"TagResource", "Timestream_20181101.TagResource"},
		{"UntagResource", "Timestream_20181101.UntagResource"},
		{"UpdateDatabase", "Timestream_20181101.UpdateDatabase"},
		{"UpdateTable", "Timestream_20181101.UpdateTable"},
		{"WriteRecords", "Timestream_20181101.WriteRecords"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Timestream Write
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to h.dispatch's single unmatched-route
// return (fmt.Errorf("%w: %s", errUnknownAction, action), handler.go's
// dispatch() single production call site).
//
// This asserts on MESSAGE TEXT ("unknown action"), not wire type --
// handleError maps errUnknownAction to "ValidationException", the SAME wire
// type shared by errInvalidRequest, awserr.ErrInvalidParameter, and any
// JSON syntax/type-decode error (handler.go:287-295), so asserting on
// __type would be structurally unsafe here. errUnknownAction's message
// ("unknown action: <action>") has exactly one production call site
// (grepped) and is not produced by any other error path, so asserting on
// message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := timestreamwrite.NewHandler(timestreamwrite.NewInMemoryBackend())

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
