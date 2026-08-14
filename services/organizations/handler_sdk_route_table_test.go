package organizations_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// Organizations operation, extracted from organizations@v1.53.5
// serializers.go: each op's awsAwsjson11_serializeOp<Op>.HandleSerialize
// sets httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "AWSOrganizationsV20161128.<Op>") and always request.Request.Method =
// "POST" against path "/" -- Organizations is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header.
// ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "AWSOrganizationsV20161128."), so the class of bug this
// table can catch is a dispatch-table key that doesn't exactly match the
// real op name (typo, wrong case -- Organizations is case-sensitive
// JSON-RPC), not a route-template mismatch.
//
// This table covers all 63 real Organizations ops -- confirmed by diffing
// both GetSupportedOperations() and the actual dispatch chain (13
// dispatchXxx(op string) helpers chained from dispatch(), handler.go:185)
// against this exact list: zero mismatches in either direction, no dead or
// excluded keys.
//
// dispatchRoot (handler_roots.go) dispatches ListRoots via a bare
// `if op == "ListRoots"` rather than the `switch op { case "X": }` style
// every other dispatchXxx helper uses -- an identifier-only grep for
// `case "..."` misses it and reports a false gap of 1, the same shape of
// risk dms's four literal-string-keyed ops posed. Re-extracting for both
// key styles resolved it to 63 of 63; ListRoots is genuinely wired, just
// through an if-statement.
//
// organizations is also the service flagged this campaign for a prior
// read-side bug where an op was wrong in both directions (wrong response
// wrapper and a request field read under the wrong name) -- extra care was
// taken confirming every dispatch key against its serializer target, not
// just diffing name lists.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSOrganizationsV20161128.` and
// pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AcceptHandshake", "AWSOrganizationsV20161128.AcceptHandshake"},
		{"AttachPolicy", "AWSOrganizationsV20161128.AttachPolicy"},
		{"CancelHandshake", "AWSOrganizationsV20161128.CancelHandshake"},
		{"CloseAccount", "AWSOrganizationsV20161128.CloseAccount"},
		{"CreateAccount", "AWSOrganizationsV20161128.CreateAccount"},
		{"CreateGovCloudAccount", "AWSOrganizationsV20161128.CreateGovCloudAccount"},
		{"CreateOrganization", "AWSOrganizationsV20161128.CreateOrganization"},
		{"CreateOrganizationalUnit", "AWSOrganizationsV20161128.CreateOrganizationalUnit"},
		{"CreatePolicy", "AWSOrganizationsV20161128.CreatePolicy"},
		{"DeclineHandshake", "AWSOrganizationsV20161128.DeclineHandshake"},
		{"DeleteOrganization", "AWSOrganizationsV20161128.DeleteOrganization"},
		{"DeleteOrganizationalUnit", "AWSOrganizationsV20161128.DeleteOrganizationalUnit"},
		{"DeletePolicy", "AWSOrganizationsV20161128.DeletePolicy"},
		{"DeleteResourcePolicy", "AWSOrganizationsV20161128.DeleteResourcePolicy"},
		{"DeregisterDelegatedAdministrator", "AWSOrganizationsV20161128.DeregisterDelegatedAdministrator"},
		{"DescribeAccount", "AWSOrganizationsV20161128.DescribeAccount"},
		{"DescribeCreateAccountStatus", "AWSOrganizationsV20161128.DescribeCreateAccountStatus"},
		{"DescribeEffectivePolicy", "AWSOrganizationsV20161128.DescribeEffectivePolicy"},
		{"DescribeHandshake", "AWSOrganizationsV20161128.DescribeHandshake"},
		{"DescribeOrganization", "AWSOrganizationsV20161128.DescribeOrganization"},
		{"DescribeOrganizationalUnit", "AWSOrganizationsV20161128.DescribeOrganizationalUnit"},
		{"DescribePolicy", "AWSOrganizationsV20161128.DescribePolicy"},
		{"DescribeResourcePolicy", "AWSOrganizationsV20161128.DescribeResourcePolicy"},
		{"DescribeResponsibilityTransfer", "AWSOrganizationsV20161128.DescribeResponsibilityTransfer"},
		{"DetachPolicy", "AWSOrganizationsV20161128.DetachPolicy"},
		{"DisableAWSServiceAccess", "AWSOrganizationsV20161128.DisableAWSServiceAccess"},
		{"DisablePolicyType", "AWSOrganizationsV20161128.DisablePolicyType"},
		{"EnableAllFeatures", "AWSOrganizationsV20161128.EnableAllFeatures"},
		{"EnableAWSServiceAccess", "AWSOrganizationsV20161128.EnableAWSServiceAccess"},
		{"EnablePolicyType", "AWSOrganizationsV20161128.EnablePolicyType"},
		{"InviteAccountToOrganization", "AWSOrganizationsV20161128.InviteAccountToOrganization"},
		{
			"InviteOrganizationToTransferResponsibility",
			"AWSOrganizationsV20161128.InviteOrganizationToTransferResponsibility",
		},
		{"LeaveOrganization", "AWSOrganizationsV20161128.LeaveOrganization"},
		{"ListAccounts", "AWSOrganizationsV20161128.ListAccounts"},
		{"ListAccountsForParent", "AWSOrganizationsV20161128.ListAccountsForParent"},
		{
			"ListAccountsWithInvalidEffectivePolicy",
			"AWSOrganizationsV20161128.ListAccountsWithInvalidEffectivePolicy",
		},
		{"ListAWSServiceAccessForOrganization", "AWSOrganizationsV20161128.ListAWSServiceAccessForOrganization"},
		{"ListChildren", "AWSOrganizationsV20161128.ListChildren"},
		{"ListCreateAccountStatus", "AWSOrganizationsV20161128.ListCreateAccountStatus"},
		{"ListDelegatedAdministrators", "AWSOrganizationsV20161128.ListDelegatedAdministrators"},
		{"ListDelegatedServicesForAccount", "AWSOrganizationsV20161128.ListDelegatedServicesForAccount"},
		{"ListEffectivePolicyValidationErrors", "AWSOrganizationsV20161128.ListEffectivePolicyValidationErrors"},
		{"ListHandshakesForAccount", "AWSOrganizationsV20161128.ListHandshakesForAccount"},
		{"ListHandshakesForOrganization", "AWSOrganizationsV20161128.ListHandshakesForOrganization"},
		{"ListInboundResponsibilityTransfers", "AWSOrganizationsV20161128.ListInboundResponsibilityTransfers"},
		{"ListOrganizationalUnitsForParent", "AWSOrganizationsV20161128.ListOrganizationalUnitsForParent"},
		{"ListOutboundResponsibilityTransfers", "AWSOrganizationsV20161128.ListOutboundResponsibilityTransfers"},
		{"ListParents", "AWSOrganizationsV20161128.ListParents"},
		{"ListPolicies", "AWSOrganizationsV20161128.ListPolicies"},
		{"ListPoliciesForTarget", "AWSOrganizationsV20161128.ListPoliciesForTarget"},
		{"ListRoots", "AWSOrganizationsV20161128.ListRoots"},
		{"ListTagsForResource", "AWSOrganizationsV20161128.ListTagsForResource"},
		{"ListTargetsForPolicy", "AWSOrganizationsV20161128.ListTargetsForPolicy"},
		{"MoveAccount", "AWSOrganizationsV20161128.MoveAccount"},
		{"PutResourcePolicy", "AWSOrganizationsV20161128.PutResourcePolicy"},
		{"RegisterDelegatedAdministrator", "AWSOrganizationsV20161128.RegisterDelegatedAdministrator"},
		{"RemoveAccountFromOrganization", "AWSOrganizationsV20161128.RemoveAccountFromOrganization"},
		{"TagResource", "AWSOrganizationsV20161128.TagResource"},
		{"TerminateResponsibilityTransfer", "AWSOrganizationsV20161128.TerminateResponsibilityTransfer"},
		{"UntagResource", "AWSOrganizationsV20161128.UntagResource"},
		{"UpdateOrganizationalUnit", "AWSOrganizationsV20161128.UpdateOrganizationalUnit"},
		{"UpdatePolicy", "AWSOrganizationsV20161128.UpdatePolicy"},
		{"UpdateResponsibilityTransfer", "AWSOrganizationsV20161128.UpdateResponsibilityTransfer"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Organizations
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss sentinel a
// dispatch-table key mismatch would produce.
//
// Organizations's sentinel (the "UnknownOperationException" wire type
// built at dispatch()'s single production call site, handler.go:238) is
// not reused by any other error path in this service (grepped) -- unlike
// workmail/transfer, whose dispatch-miss sentinel shares its wire type
// with ordinary validation errors, so asserting on the wire type here is
// safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := organizations.NewInMemoryBackend("111122223333", "us-east-1")
			h := organizations.NewHandler(backend)
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
