package sns_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteOps is the authoritative operation list for SNS, taken from the
// api_op_*.go filenames in sns@v1.42.4 (one file per real op) and
// cross-checked against the Action form field each op's
// awsAwsquery_serializeOp<Op> writes via body.Key("Action").String("<Op>")
// in serializers.go. SNS is AWS Query protocol: the Action value IS the
// wire op name (no path/method to drift), so ExtractOperation cannot
// misroute on its own -- the risk this table guards is Handler()'s
// dispatch table (buildActions, handler.go) silently missing an entry for
// a real op and falling through to the InvalidAction sentinel
// (handler.go:259, the only "InvalidAction" site in the package, unlike
// IAM where the same error code is reused for ordinary validation errors).
//
// Regenerate by listing api_op_*.go in the pinned sns module.
func sdkRouteOps() []string {
	return []string{
		"AddPermission",
		"CheckIfPhoneNumberIsOptedOut",
		"ConfirmSubscription",
		"CreatePlatformApplication",
		"CreatePlatformEndpoint",
		"CreateSMSSandboxPhoneNumber",
		"CreateTopic",
		"DeleteEndpoint",
		"DeletePlatformApplication",
		"DeleteSMSSandboxPhoneNumber",
		"DeleteTopic",
		"GetDataProtectionPolicy",
		"GetEndpointAttributes",
		"GetPlatformApplicationAttributes",
		"GetSMSAttributes",
		"GetSMSSandboxAccountStatus",
		"GetSubscriptionAttributes",
		"GetTopicAttributes",
		"ListEndpointsByPlatformApplication",
		"ListOriginationNumbers",
		"ListPhoneNumbersOptedOut",
		"ListPlatformApplications",
		"ListSMSSandboxPhoneNumbers",
		"ListSubscriptions",
		"ListSubscriptionsByTopic",
		"ListTagsForResource",
		"ListTopics",
		"OptInPhoneNumber",
		"Publish",
		"PublishBatch",
		"PutDataProtectionPolicy",
		"RemovePermission",
		"SetEndpointAttributes",
		"SetPlatformApplicationAttributes",
		"SetSMSAttributes",
		"SetSubscriptionAttributes",
		"SetTopicAttributes",
		"Subscribe",
		"TagResource",
		"Unsubscribe",
		"UntagResource",
		"VerifySMSSandboxPhoneNumber",
	}
}

// TestExtractOperation_SDKRouteTable drives every real SNS operation's
// authoritative Action form field through ExtractOperation and the real
// Handler(), asserting the response never falls through to the
// InvalidAction sentinel dispatch() emits for an unmapped action. A bare
// "Action=<Op>" body is enough: missing required parameters are expected
// to surface as ordinary validation/not-found errors from the real handler
// function, not the unknown-action branch.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteOps() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+op))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got, "ExtractOperation mismatch for Action=%s", op)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidAction",
				"op=%s: dispatched to the invalid-action handler", op)
		})
	}
}
