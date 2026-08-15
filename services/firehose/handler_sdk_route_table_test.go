package firehose_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Kinesis
// Data Firehose operation, extracted from firehose@v1.46.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("Firehose_20150804.<Op>")
// and always POSTs to "/" -- Firehose is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation and Handler()
// (via buildOps()'s map, dispatched through h.dispatch) both derive the
// action the same way (TrimPrefix on "Firehose_20150804."), so the class of
// bug this table catches is a dispatch-table key that doesn't exactly match
// the real op name (typo, wrong case -- Firehose is case-sensitive
// JSON-RPC), not a route-template mismatch.
//
// This table covers all 12 real Firehose ops (firehose@v1.46.4) --
// confirmed by diffing both GetSupportedOperations() and the actual
// buildOps() dispatch map against this exact list: zero mismatches in
// either direction, no dead or excluded keys.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Firehose_20150804.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateDeliveryStream", "Firehose_20150804.CreateDeliveryStream"},
		{"DeleteDeliveryStream", "Firehose_20150804.DeleteDeliveryStream"},
		{"DescribeDeliveryStream", "Firehose_20150804.DescribeDeliveryStream"},
		{"ListDeliveryStreams", "Firehose_20150804.ListDeliveryStreams"},
		{"ListTagsForDeliveryStream", "Firehose_20150804.ListTagsForDeliveryStream"},
		{"PutRecord", "Firehose_20150804.PutRecord"},
		{"PutRecordBatch", "Firehose_20150804.PutRecordBatch"},
		{"StartDeliveryStreamEncryption", "Firehose_20150804.StartDeliveryStreamEncryption"},
		{"StopDeliveryStreamEncryption", "Firehose_20150804.StopDeliveryStreamEncryption"},
		{"TagDeliveryStream", "Firehose_20150804.TagDeliveryStream"},
		{"UntagDeliveryStream", "Firehose_20150804.UntagDeliveryStream"},
		{"UpdateDestination", "Firehose_20150804.UpdateDestination"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Firehose operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the "UnknownOperationException" sentinel
// (errUnknownAction, handler.go's dispatch() single production call site)
// that a dispatch-table key mismatch would produce. "UnknownOperationException"
// is not reused by any other error path in this service (grepped: ErrNotFound
// maps to ResourceNotFoundException, ErrAlreadyExists to ResourceInUseException,
// validation/syntax/type errors to InvalidArgumentException), so asserting on
// the wire type is safe here -- unlike workmail/transfer, where the
// dispatch-miss sentinel shares its wire type with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := firehose.NewInMemoryBackend("111122223333", "us-east-1")
			h := firehose.NewHandler(b)

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
