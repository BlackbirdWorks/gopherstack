package cloudcontrol_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS Cloud
// Control API operation, extracted from
// cloudcontrol@v1.32.4/serializers.go's
// awsAwsjson10_serializeOp<Op>.HandleSerialize calls to
// SetHeader("X-Amz-Target").String("CloudApiService.<Op>"), always POSTing
// to "/" (JSON-RPC 1.0, services/_PROTOCOLS.md). "CloudApiService" is Cloud
// Control's real internal AWS codename -- unrelated to the "cloudcontrol"
// directory name or the "AWS Cloud Control API" public branding, confirmed
// directly from serializers.go, not guessed.
//
// All 8 real ops are covered. GetSupportedOperations() and
// buildDispatchTable()'s map are both hand-written literals (neither built
// by ranging over the other), so this is a genuinely independent diff.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CancelResourceRequest", "CloudApiService.CancelResourceRequest"},
		{"CreateResource", "CloudApiService.CreateResource"},
		{"DeleteResource", "CloudApiService.DeleteResource"},
		{"GetResource", "CloudApiService.GetResource"},
		{"GetResourceRequestStatus", "CloudApiService.GetResourceRequestStatus"},
		{"ListResourceRequests", "CloudApiService.ListResourceRequests"},
		{"ListResources", "CloudApiService.ListResources"},
		{"UpdateResource", "CloudApiService.UpdateResource"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Cloud Control
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), confirming the header resolves to the right op name and that
// dispatch does not fall through to h.dispatch's single unmatched-route
// return (errUnknownAction, handler.go's dispatch() single production call
// site).
//
// This asserts on MESSAGE TEXT ("unknown action"), not wire type --
// handleError maps both errUnknownAction and ErrValidation to the same
// "InvalidRequestException" type (handler.go:159-162), so a type assertion
// would not distinguish an unmatched route from a legitimate missing-field
// error on the deliberately minimal "{}" request body this test sends.
// errUnknownAction's message ("unknown action: <action>") has exactly one
// production call site (grepped) and is not produced by any validation
// error message (all of which name a missing field, e.g. "TypeName is
// required"), so asserting on message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := cloudcontrol.NewHandler(cloudcontrol.NewInMemoryBackend("000000000000", "us-east-1"))

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
