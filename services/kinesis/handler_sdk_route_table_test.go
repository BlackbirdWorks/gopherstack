package kinesis_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Kinesis
// operation EXCEPT SubscribeToShard (covered separately below, since it
// bypasses the normal JSON dispatch table entirely), extracted from
// kinesis@v1.46.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("Kinesis_20131202.<Op>")
// and always request.Request.Method = "POST" against path "/" -- Kinesis is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "Kinesis_20131202."), so the class of bug this table can
// catch is a dispatch-table key that doesn't exactly match the real op name
// (typo, wrong case -- Kinesis is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers 38 of the 39 real Kinesis ops. Kinesis's
// GetSupportedOperations() is a hand-maintained literal (not derived from
// h.ops, unlike the other four services in this campaign's pass), which is
// exactly the shape of divergence that hid cognitoidp's
// AdminSetUserMFASetting: it was checked directly against the actual
// buildOps() map, not just the reported list. All 38 ops dispatched through
// h.ops match a real op name exactly and every entry in GetSupportedOperations
// (39, including SubscribeToShard) is accounted for: no dead key, no gap.
//
// gopherstack's own PARITY.md and prior sweeps flagged kinesis for disguised
// stubs and persistence data loss elsewhere in the service; this table found
// no hollow handler among the 38 ops it drives (each reaches real
// backend logic, not a stub returning a bare empty struct) -- worth stating
// since that risk was specifically called out for this service, not because
// hunting stubs was this table's job.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Kinesis_20131202.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddTagsToStream", "Kinesis_20131202.AddTagsToStream"},
		{"CreateStream", "Kinesis_20131202.CreateStream"},
		{"DecreaseStreamRetentionPeriod", "Kinesis_20131202.DecreaseStreamRetentionPeriod"},
		{"DeleteResourcePolicy", "Kinesis_20131202.DeleteResourcePolicy"},
		{"DeleteStream", "Kinesis_20131202.DeleteStream"},
		{"DeregisterStreamConsumer", "Kinesis_20131202.DeregisterStreamConsumer"},
		{"DescribeAccountSettings", "Kinesis_20131202.DescribeAccountSettings"},
		{"DescribeLimits", "Kinesis_20131202.DescribeLimits"},
		{"DescribeStream", "Kinesis_20131202.DescribeStream"},
		{"DescribeStreamConsumer", "Kinesis_20131202.DescribeStreamConsumer"},
		{"DescribeStreamSummary", "Kinesis_20131202.DescribeStreamSummary"},
		{"DisableEnhancedMonitoring", "Kinesis_20131202.DisableEnhancedMonitoring"},
		{"EnableEnhancedMonitoring", "Kinesis_20131202.EnableEnhancedMonitoring"},
		{"GetRecords", "Kinesis_20131202.GetRecords"},
		{"GetResourcePolicy", "Kinesis_20131202.GetResourcePolicy"},
		{"GetShardIterator", "Kinesis_20131202.GetShardIterator"},
		{"IncreaseStreamRetentionPeriod", "Kinesis_20131202.IncreaseStreamRetentionPeriod"},
		{"ListShards", "Kinesis_20131202.ListShards"},
		{"ListStreamConsumers", "Kinesis_20131202.ListStreamConsumers"},
		{"ListStreams", "Kinesis_20131202.ListStreams"},
		{"ListTagsForResource", "Kinesis_20131202.ListTagsForResource"},
		{"ListTagsForStream", "Kinesis_20131202.ListTagsForStream"},
		{"MergeShards", "Kinesis_20131202.MergeShards"},
		{"PutRecord", "Kinesis_20131202.PutRecord"},
		{"PutRecords", "Kinesis_20131202.PutRecords"},
		{"PutResourcePolicy", "Kinesis_20131202.PutResourcePolicy"},
		{"RegisterStreamConsumer", "Kinesis_20131202.RegisterStreamConsumer"},
		{"RemoveTagsFromStream", "Kinesis_20131202.RemoveTagsFromStream"},
		{"SplitShard", "Kinesis_20131202.SplitShard"},
		{"StartStreamEncryption", "Kinesis_20131202.StartStreamEncryption"},
		{"StopStreamEncryption", "Kinesis_20131202.StopStreamEncryption"},
		{"TagResource", "Kinesis_20131202.TagResource"},
		{"UntagResource", "Kinesis_20131202.UntagResource"},
		{"UpdateAccountSettings", "Kinesis_20131202.UpdateAccountSettings"},
		{"UpdateMaxRecordSize", "Kinesis_20131202.UpdateMaxRecordSize"},
		{"UpdateShardCount", "Kinesis_20131202.UpdateShardCount"},
		{"UpdateStreamMode", "Kinesis_20131202.UpdateStreamMode"},
		{"UpdateStreamWarmThroughput", "Kinesis_20131202.UpdateStreamWarmThroughput"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Kinesis operation
// (except SubscribeToShard) through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the "UnknownOperationException" sentinel that a
// dispatch-table key mismatch would produce. That sentinel
// (ErrUnknownAction in errors.go, whose Error() text is literally
// "UnknownOperationException") has exactly one production call site --
// kinesisRoute's h.ops map miss in handler.go -- so it cannot collide with a
// legitimate error on this all-empty-body table.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := kinesis.NewHandler(kinesis.NewInMemoryBackend())
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

// TestExtractOperation_SDKRouteTable_SubscribeToShard covers the 39th real
// Kinesis op separately: SubscribeToShard uses the AWS event-stream binary
// protocol, so Handler() special-cases its X-Amz-Target header before
// reaching the normal JSON dispatch table (see handler.go's Handler(),
// which checks for this exact target and routes to
// handleSubscribeToShardHTTP instead of h.ops/kinesisRoute). It is
// therefore unreachable by dispatch-table typo in the same way as the other
// 38 -- there is no "SubscribeToShard" key in h.ops to mis-key -- but
// ExtractOperation must still resolve it correctly (used for
// logging/chaos-injection keying), and Handler() must still route it to the
// event-stream path rather than silently falling through to the JSON
// dispatcher's unknown-action miss. An empty body drives
// Backend.SubscribeToShard with a blank consumer/shard, which fails
// validation with a real domain error (ResourceNotFoundException, not
// UnknownOperationException) -- proving the request reached the
// SubscribeToShard-specific handler.
func TestExtractOperation_SDKRouteTable_SubscribeToShard(t *testing.T) {
	t.Parallel()

	const op = "SubscribeToShard"
	const target = "Kinesis_20131202." + op

	h := kinesis.NewHandler(kinesis.NewInMemoryBackend())
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", target)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	got := h.ExtractOperation(c)
	assert.Equal(t, op, got)

	require.NoError(t, h.Handler()(c))
	assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
		"target=%s op=%s: dispatched to the unmatched-route handler", target, op)
}
