package apprunner_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real App Runner
// operation, extracted from apprunner@v1.42.4 serializers.go: each op's
// awsAwsjson10_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AppRunner.<Op>") and
// always POSTs to "/" -- App Runner is JSON-RPC 1.0 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. The target has NO version
// suffix ("AppRunner.", not "AppRunner_YYYYMMDD.").
//
// ExtractOperation (TrimPrefix on "AppRunner.") and Handler() (via
// pkgs/service.HandleTarget splitting on "." and taking parts[1], then
// dispatch()'s h.ops flat map lookup) both resolve to the identical action
// string, so the class of bug this table catches is a dispatch-table key
// that doesn't exactly match the real op name (typo, wrong case), not a
// route-template or splitting mismatch.
//
// This table covers all 37 real App Runner ops (apprunner@v1.42.4) --
// confirmed by diffing both GetSupportedOperations() and buildOps()'s
// h.ops map keys against this exact list: zero mismatches in either
// direction, no dead or excluded keys. The two diffs are genuinely
// independent -- both are separately-typed string literals (unlike fsx's
// shared op<Name> constants), so a typo in either location would show up
// against the other.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AppRunner.` and pulling the suffix
// after the dot.
func sdkRouteCases() []string {
	return []string{
		"AssociateCustomDomain",
		"CreateAutoScalingConfiguration",
		"CreateConnection",
		"CreateObservabilityConfiguration",
		"CreateService",
		"CreateVpcConnector",
		"CreateVpcIngressConnection",
		"DeleteAutoScalingConfiguration",
		"DeleteConnection",
		"DeleteObservabilityConfiguration",
		"DeleteService",
		"DeleteVpcConnector",
		"DeleteVpcIngressConnection",
		"DescribeAutoScalingConfiguration",
		"DescribeCustomDomains",
		"DescribeObservabilityConfiguration",
		"DescribeService",
		"DescribeVpcConnector",
		"DescribeVpcIngressConnection",
		"DisassociateCustomDomain",
		"ListAutoScalingConfigurations",
		"ListConnections",
		"ListObservabilityConfigurations",
		"ListOperations",
		"ListServices",
		"ListServicesForAutoScalingConfiguration",
		"ListTagsForResource",
		"ListVpcConnectors",
		"ListVpcIngressConnections",
		"PauseService",
		"ResumeService",
		"StartDeployment",
		"TagResource",
		"UntagResource",
		"UpdateDefaultAutoScalingConfiguration",
		"UpdateService",
		"UpdateVpcIngressConnection",
	}
}

// TestExtractOperation_SDKRouteTable drives every real App Runner
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss branch (dispatch()'s
// h.ops lookup miss, returning errUnknownAction).
//
// The dispatch-miss branch's wire type, InvalidRequestException
// (invalidRequestType in errors.go), is NOT safe to assert alone:
// handleError's same case also catches errInvalidRequest,
// awserr.ErrInvalidParameter, and JSON syntax/type-decode errors -- all
// mapped to the identical wire type, per errors.go's own doc comment
// naming these as the real App Runner's actual exception names. A mistyped
// dispatch key would therefore 400 with the same __type as a legitimate
// validation error. The dispatch-miss message text ("unknown action: ") is
// unique to that one call site (grepped handler.go: errUnknownAction's
// only production use is dispatch()'s fmt.Errorf) and is what this test
// asserts against instead.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h := apprunner.NewHandler(apprunner.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			req.Header.Set("X-Amz-Target", "AppRunner."+op)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action:",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
