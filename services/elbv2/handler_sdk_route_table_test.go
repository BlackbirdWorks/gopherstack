package elbv2_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// sdkRouteCases is the authoritative Action value for every real Elastic Load
// Balancing v2 operation, extracted from elasticloadbalancingv2@v1.58.5
// serializers.go: each op's awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- ELBv2 is AWS
// Query/XML (services/_PROTOCOLS.md), so unlike a REST-family service there
// is no path template to get wrong: dispatch is entirely by this one form
// field. ExtractOperation and Handler() both read r.Form.Get("Action")
// directly, so the class of bug this table catches is a dispatch-table key
// that doesn't exactly match the real op name (typo, wrong case) -- not a
// route-template mismatch. Query protocol is case-insensitive for XML field
// names on the wire, but gopherstack's own dispatch is a Go map lookup in
// buildDispatchTable(), which is always exact-match regardless of protocol.
//
// This table covers all 51 real ELBv2 ops (elasticloadbalancingv2@v1.58.5)
// -- confirmed by diffing both GetSupportedOperations() and the actual
// buildDispatchTable() map's 51 keys against this exact list: zero
// mismatches in either direction, no dead or excluded keys.
//
// elbv2 did NOT already have an SDK route table: audit_elbv2_test.go in this
// package predates this table and exercises functional behaviour (draining
// state, DNS name format, rule conditions/actions, pagination, tag
// lifecycle) via real Action= requests -- it does not assert dispatch
// coverage against the SDK's op list, so this table is additive, not a
// duplicate.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"AddListenerCertificates",
		"AddTags",
		"AddTrustStoreRevocations",
		"CreateListener",
		"CreateLoadBalancer",
		"CreateRule",
		"CreateTargetGroup",
		"CreateTrustStore",
		"DeleteListener",
		"DeleteLoadBalancer",
		"DeleteRule",
		"DeleteSharedTrustStoreAssociation",
		"DeleteTargetGroup",
		"DeleteTrustStore",
		"DeregisterTargets",
		"DescribeAccountLimits",
		"DescribeCapacityReservation",
		"DescribeListenerAttributes",
		"DescribeListenerCertificates",
		"DescribeListeners",
		"DescribeLoadBalancerAttributes",
		"DescribeLoadBalancers",
		"DescribeRules",
		"DescribeSSLPolicies",
		"DescribeTags",
		"DescribeTargetGroupAttributes",
		"DescribeTargetGroups",
		"DescribeTargetHealth",
		"DescribeTrustStoreAssociations",
		"DescribeTrustStoreRevocations",
		"DescribeTrustStores",
		"GetResourcePolicy",
		"GetTrustStoreCaCertificatesBundle",
		"GetTrustStoreRevocationContent",
		"ModifyCapacityReservation",
		"ModifyIpPools",
		"ModifyListener",
		"ModifyListenerAttributes",
		"ModifyLoadBalancerAttributes",
		"ModifyRule",
		"ModifyTargetGroup",
		"ModifyTargetGroupAttributes",
		"ModifyTrustStore",
		"RegisterTargets",
		"RemoveListenerCertificates",
		"RemoveTags",
		"RemoveTrustStoreRevocations",
		"SetIpAddressType",
		"SetRulePriorities",
		"SetSecurityGroups",
		"SetSubnets",
	}
}

// TestExtractOperation_SDKRouteTable drives every real ELBv2 operation's
// authoritative Action value through ExtractOperation and Handler(),
// asserting the form field resolves to the right op name and that Handler()
// does not fall through to the "InvalidAction" sentinel (ErrUnknownAction,
// errors.go:27) that a dispatch-table key mismatch would produce.
// ErrUnknownAction has exactly one production call site (the dispatch() miss
// in handler.go) and "InvalidAction" is not reused by any other error path
// in this service (grepped), so asserting on the wire code is safe here --
// unlike workmail/transfer, where the dispatch-miss sentinel shares its wire
// type with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			b := elbv2.NewInMemoryBackend("111122223333", config.DefaultRegion)
			t.Cleanup(func() { b.Close() })
			h := elbv2.NewHandler(b)

			e := echo.New()
			body := "Action=" + op + "&Version=2015-12-01"
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
