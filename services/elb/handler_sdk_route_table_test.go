package elb_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// sdkRouteCases is the authoritative Action value for every real Classic
// ELB operation, extracted from elasticloadbalancing@v1.36.4 serializers.go:
// each op's awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- ELB is AWS
// Query/XML (services/_PROTOCOLS.md), so unlike a REST-family service there
// is no path template to get wrong: dispatch is entirely by this one form
// field. ExtractOperation and Handler() (via the ops map built by
// buildOps(), dispatched through h.dispatch) both read the Action value the
// same way, so the class of bug this table catches is a dispatch-table key
// that doesn't exactly match the real op name (typo, wrong case), not a
// route-template mismatch.
//
// This table covers all 29 real ELB ops (elasticloadbalancing@v1.36.4) --
// confirmed by diffing both GetSupportedOperations() and the actual
// buildOps() dispatch map against this exact list: zero mismatches in
// either direction, no dead or excluded keys. The two diffs are genuinely
// independent -- GetSupportedOperations is a separately maintained literal,
// not built by ranging over the ops map.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"AddTags",
		"ApplySecurityGroupsToLoadBalancer",
		"AttachLoadBalancerToSubnets",
		"ConfigureHealthCheck",
		"CreateAppCookieStickinessPolicy",
		"CreateLBCookieStickinessPolicy",
		"CreateLoadBalancer",
		"CreateLoadBalancerListeners",
		"CreateLoadBalancerPolicy",
		"DeleteLoadBalancer",
		"DeleteLoadBalancerListeners",
		"DeleteLoadBalancerPolicy",
		"DeregisterInstancesFromLoadBalancer",
		"DescribeAccountLimits",
		"DescribeInstanceHealth",
		"DescribeLoadBalancerAttributes",
		"DescribeLoadBalancerPolicies",
		"DescribeLoadBalancerPolicyTypes",
		"DescribeLoadBalancers",
		"DescribeTags",
		"DetachLoadBalancerFromSubnets",
		"DisableAvailabilityZonesForLoadBalancer",
		"EnableAvailabilityZonesForLoadBalancer",
		"ModifyLoadBalancerAttributes",
		"RegisterInstancesWithLoadBalancer",
		"RemoveTags",
		"SetLoadBalancerListenerSSLCertificate",
		"SetLoadBalancerPoliciesForBackendServer",
		"SetLoadBalancerPoliciesOfListener",
	}
}

// TestExtractOperation_SDKRouteTable drives every real ELB operation's
// authoritative Action value through ExtractOperation and Handler(),
// asserting the form field resolves to the right op name and that Handler()
// does not fall through to the "InvalidAction" sentinel (ErrUnknownAction,
// handler.go's dispatch() single production call site) that a
// dispatch-table key mismatch would produce. ErrUnknownAction wraps
// awserr.ErrInvalidParameter, and elbErrorCode's mapping table does include
// a generic fallback on that same category (ErrInvalidParameter ->
// "ValidationError") -- but ErrUnknownAction's own entry is checked earlier
// in the ordered list, and errors.Is matches sentinels by pointer identity
// (pkgs/awserr.wrappedError has no custom Is()), so the generic fallback
// cannot shadow it. "InvalidAction" is not reused by any other entry in the
// table (grepped) -- so asserting on the wire code is safe here, unlike
// workmail/transfer, where the dispatch-miss sentinel shares its wire type
// with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := elb.NewHandler(elb.NewInMemoryBackend("123456789012", "us-east-1"))

			e := echo.New()
			body := "Action=" + op + "&Version=2012-06-01"
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidAction",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
