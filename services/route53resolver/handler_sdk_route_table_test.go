package route53resolver_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Route 53
// Resolver operation, extracted from route53resolver@v1.48.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("Route53Resolver.<Op>")
// and always POSTs to "/" -- Route53Resolver is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header. The
// target prefix has NO version suffix ("Route53Resolver.", not
// "Route53Resolver_20180401." or similar) -- confirmed directly from
// serializers.go, not guessed; this is the same no-suffix shape the task
// names for swf's "SimpleWorkflowService". ExtractOperation and Handler()
// (via h.dispatch's h.ops map lookup) both derive the action the same way
// (TrimPrefix on "Route53Resolver."), so the class of bug this table
// catches is a dispatch-table key that doesn't exactly match the real op
// name (typo, wrong case -- Route53Resolver is case-sensitive JSON-RPC),
// not a route-template mismatch.
//
// This table covers all 72 real Route53Resolver ops (route53resolver@v1.48.4).
// GetSupportedOperations() is h.supportedOpsCache, precomputed in NewHandler
// as collections.SortedKeys(h.ops) -- i.e. it is built BY RANGING OVER the
// same dispatch map that Handler() dispatches through (h.ops, the merge of
// 13 per-family opsXxx() builders: opsResolverEndpoints, opsResolverRules,
// opsTags, opsFirewallRuleGroups, opsFirewallDomainLists, opsFirewallRules,
// opsOutpostResolvers, opsQueryLogConfigs, opsQueryLogAssociations,
// opsRuleAssociations, opsFirewallConfigs, opsResolverConfigs,
// opsDnssecConfigs). So the "diff against GetSupportedOperations" and "diff
// against the actual dispatch map" checks collapse into ONE independent
// check here, not two, per the task's explicit guidance for this case --
// confirmed by re-extracting every key from all 13 builder functions
// directly (72 total, no duplicates across groups) rather than trusting
// GetSupportedOperations' output alone. Result: zero mismatches against the
// SDK's target list in either direction, no dead or excluded keys.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Route53Resolver.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AssociateFirewallRuleGroup", "Route53Resolver.AssociateFirewallRuleGroup"},
		{"AssociateResolverEndpointIpAddress", "Route53Resolver.AssociateResolverEndpointIpAddress"},
		{"AssociateResolverQueryLogConfig", "Route53Resolver.AssociateResolverQueryLogConfig"},
		{"AssociateResolverRule", "Route53Resolver.AssociateResolverRule"},
		{"BatchCreateFirewallRule", "Route53Resolver.BatchCreateFirewallRule"},
		{"BatchDeleteFirewallRule", "Route53Resolver.BatchDeleteFirewallRule"},
		{"BatchUpdateFirewallRule", "Route53Resolver.BatchUpdateFirewallRule"},
		{"CreateFirewallDomainList", "Route53Resolver.CreateFirewallDomainList"},
		{"CreateFirewallRule", "Route53Resolver.CreateFirewallRule"},
		{"CreateFirewallRuleGroup", "Route53Resolver.CreateFirewallRuleGroup"},
		{"CreateOutpostResolver", "Route53Resolver.CreateOutpostResolver"},
		{"CreateResolverEndpoint", "Route53Resolver.CreateResolverEndpoint"},
		{"CreateResolverQueryLogConfig", "Route53Resolver.CreateResolverQueryLogConfig"},
		{"CreateResolverRule", "Route53Resolver.CreateResolverRule"},
		{"DeleteFirewallDomainList", "Route53Resolver.DeleteFirewallDomainList"},
		{"DeleteFirewallRule", "Route53Resolver.DeleteFirewallRule"},
		{"DeleteFirewallRuleGroup", "Route53Resolver.DeleteFirewallRuleGroup"},
		{"DeleteOutpostResolver", "Route53Resolver.DeleteOutpostResolver"},
		{"DeleteResolverEndpoint", "Route53Resolver.DeleteResolverEndpoint"},
		{"DeleteResolverQueryLogConfig", "Route53Resolver.DeleteResolverQueryLogConfig"},
		{"DeleteResolverRule", "Route53Resolver.DeleteResolverRule"},
		{"DisassociateFirewallRuleGroup", "Route53Resolver.DisassociateFirewallRuleGroup"},
		{"DisassociateResolverEndpointIpAddress", "Route53Resolver.DisassociateResolverEndpointIpAddress"},
		{"DisassociateResolverQueryLogConfig", "Route53Resolver.DisassociateResolverQueryLogConfig"},
		{"DisassociateResolverRule", "Route53Resolver.DisassociateResolverRule"},
		{"GetFirewallConfig", "Route53Resolver.GetFirewallConfig"},
		{"GetFirewallDomainList", "Route53Resolver.GetFirewallDomainList"},
		{"GetFirewallRuleGroup", "Route53Resolver.GetFirewallRuleGroup"},
		{"GetFirewallRuleGroupAssociation", "Route53Resolver.GetFirewallRuleGroupAssociation"},
		{"GetFirewallRuleGroupPolicy", "Route53Resolver.GetFirewallRuleGroupPolicy"},
		{"GetOutpostResolver", "Route53Resolver.GetOutpostResolver"},
		{"GetResolverConfig", "Route53Resolver.GetResolverConfig"},
		{"GetResolverDnssecConfig", "Route53Resolver.GetResolverDnssecConfig"},
		{"GetResolverEndpoint", "Route53Resolver.GetResolverEndpoint"},
		{"GetResolverQueryLogConfig", "Route53Resolver.GetResolverQueryLogConfig"},
		{"GetResolverQueryLogConfigAssociation", "Route53Resolver.GetResolverQueryLogConfigAssociation"},
		{"GetResolverQueryLogConfigPolicy", "Route53Resolver.GetResolverQueryLogConfigPolicy"},
		{"GetResolverRule", "Route53Resolver.GetResolverRule"},
		{"GetResolverRuleAssociation", "Route53Resolver.GetResolverRuleAssociation"},
		{"GetResolverRulePolicy", "Route53Resolver.GetResolverRulePolicy"},
		{"ImportFirewallDomains", "Route53Resolver.ImportFirewallDomains"},
		{"ListFirewallConfigs", "Route53Resolver.ListFirewallConfigs"},
		{"ListFirewallDomainLists", "Route53Resolver.ListFirewallDomainLists"},
		{"ListFirewallDomains", "Route53Resolver.ListFirewallDomains"},
		{"ListFirewallRuleGroupAssociations", "Route53Resolver.ListFirewallRuleGroupAssociations"},
		{"ListFirewallRuleGroups", "Route53Resolver.ListFirewallRuleGroups"},
		{"ListFirewallRules", "Route53Resolver.ListFirewallRules"},
		{"ListFirewallRuleTypes", "Route53Resolver.ListFirewallRuleTypes"},
		{"ListOutpostResolvers", "Route53Resolver.ListOutpostResolvers"},
		{"ListResolverConfigs", "Route53Resolver.ListResolverConfigs"},
		{"ListResolverDnssecConfigs", "Route53Resolver.ListResolverDnssecConfigs"},
		{"ListResolverEndpointIpAddresses", "Route53Resolver.ListResolverEndpointIpAddresses"},
		{"ListResolverEndpoints", "Route53Resolver.ListResolverEndpoints"},
		{"ListResolverQueryLogConfigAssociations", "Route53Resolver.ListResolverQueryLogConfigAssociations"},
		{"ListResolverQueryLogConfigs", "Route53Resolver.ListResolverQueryLogConfigs"},
		{"ListResolverRuleAssociations", "Route53Resolver.ListResolverRuleAssociations"},
		{"ListResolverRules", "Route53Resolver.ListResolverRules"},
		{"ListTagsForResource", "Route53Resolver.ListTagsForResource"},
		{"PutFirewallRuleGroupPolicy", "Route53Resolver.PutFirewallRuleGroupPolicy"},
		{"PutResolverQueryLogConfigPolicy", "Route53Resolver.PutResolverQueryLogConfigPolicy"},
		{"PutResolverRulePolicy", "Route53Resolver.PutResolverRulePolicy"},
		{"TagResource", "Route53Resolver.TagResource"},
		{"UntagResource", "Route53Resolver.UntagResource"},
		{"UpdateFirewallConfig", "Route53Resolver.UpdateFirewallConfig"},
		{"UpdateFirewallDomains", "Route53Resolver.UpdateFirewallDomains"},
		{"UpdateFirewallRule", "Route53Resolver.UpdateFirewallRule"},
		{"UpdateFirewallRuleGroupAssociation", "Route53Resolver.UpdateFirewallRuleGroupAssociation"},
		{"UpdateOutpostResolver", "Route53Resolver.UpdateOutpostResolver"},
		{"UpdateResolverConfig", "Route53Resolver.UpdateResolverConfig"},
		{"UpdateResolverDnssecConfig", "Route53Resolver.UpdateResolverDnssecConfig"},
		{"UpdateResolverEndpoint", "Route53Resolver.UpdateResolverEndpoint"},
		{"UpdateResolverRule", "Route53Resolver.UpdateResolverRule"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Route53Resolver
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to h.dispatch's single unmatched-route
// return (fmt.Errorf("%w: %s", errUnknownAction, action), handler.go's
// dispatch() single production call site).
//
// This asserts on MESSAGE TEXT ("unknown action"), not wire type -- unlike
// most of this campaign's tables, handleError's branch for errUnknownAction
// (handler.go:197-199) writes NO "__type"/Type field at all, just
// {"message": err.Error()}, the same shape the task calls out for swf. That
// branch is also shared with errInvalidRequest and raw JSON syntax/type
// errors, so even the (nonexistent) type field wouldn't help distinguish
// them -- message text is the only signal. errUnknownAction's message
// ("unknown action: <action>") has exactly one production call site
// (grepped) and is not produced by any other error path (errInvalidRequest's
// own message is "invalid request", a different string), so asserting on
// message text is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := route53resolver.NewHandler(route53resolver.NewInMemoryBackend("000000000000", "us-east-1"))

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
