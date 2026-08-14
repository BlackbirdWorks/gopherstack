package sqs_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// sdkRouteOps is the authoritative operation list for SQS, taken from the
// api_op_*.go filenames in sqs@v1.46.4 (one file per real op) and
// cross-checked against the X-Amz-Target header literal each op's
// awsAwsjson10_serializeOp<Op> writes via
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AmazonSQS.<Op>") in
// serializers.go. SQS is JSON-RPC: the op name IS the wire value (no
// path/method to drift), so ExtractOperation cannot misroute on its own --
// the risk this table guards is handler.go's sqsDispatchTable() silently
// missing an entry and falling through to the InvalidAction sentinel
// (ErrUnknownAction, handler.go:349-351).
//
// Regenerate by listing api_op_*.go in the pinned sqs module.
func sdkRouteOps() []string {
	return []string{
		"AddPermission",
		"CancelMessageMoveTask",
		"ChangeMessageVisibility",
		"ChangeMessageVisibilityBatch",
		"CreateQueue",
		"DeleteMessage",
		"DeleteMessageBatch",
		"DeleteQueue",
		"GetQueueAttributes",
		"GetQueueUrl",
		"ListDeadLetterSourceQueues",
		"ListMessageMoveTasks",
		"ListQueues",
		"ListQueueTags",
		"PurgeQueue",
		"ReceiveMessage",
		"RemovePermission",
		"SendMessage",
		"SendMessageBatch",
		"SetQueueAttributes",
		"StartMessageMoveTask",
		"TagQueue",
		"UntagQueue",
	}
}

// TestExtractOperation_SDKRouteTable drives every real SQS operation's
// authoritative X-Amz-Target header through ExtractOperation and the real
// Handler(), asserting the response never falls through to the
// com.amazonaws.sqs#InvalidAction sentinel that ErrUnknownAction produces.
// A minimal "{}" body unmarshals cleanly for every op (all fields are
// optional on the wire struct); the real backend method is then expected to
// surface a normal validation/not-found error on the zero-value input, not
// the unknown-action branch.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)
	h := sqs.NewHandler(b)

	for _, op := range sdkRouteOps() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", "AmazonSQS."+op)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got, "ExtractOperation mismatch for target AmazonSQS.%s", op)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "com.amazonaws.sqs#InvalidAction",
				"op=%s: dispatched to the unknown-action handler", op)
		})
	}
}
