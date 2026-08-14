package ssoadmin_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS SSO
// Admin operation, extracted from ssoadmin@v1.43.1 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("SWBExternalService.<Op>")
// and always request.Request.Method = "POST" against path "/" -- SSO Admin
// is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "SWBExternalService."), so the class of bug this table can
// catch is a dispatch-table key that doesn't exactly match the real op name
// (typo, wrong case -- SSO Admin is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers all 79 real SSO Admin ops -- confirmed by diffing both
// GetSupportedOperations() and the actual ssoAdminOps dispatch map (the
// package-level var driven by dispatch()) against this exact list: zero
// mismatches in either direction, no dead or excluded keys.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("SWBExternalService.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddRegion", "SWBExternalService.AddRegion"},
		{
			"AttachCustomerManagedPolicyReferenceToPermissionSet",
			"SWBExternalService.AttachCustomerManagedPolicyReferenceToPermissionSet",
		},
		{"AttachManagedPolicyToPermissionSet", "SWBExternalService.AttachManagedPolicyToPermissionSet"},
		{"CreateAccountAssignment", "SWBExternalService.CreateAccountAssignment"},
		{"CreateApplication", "SWBExternalService.CreateApplication"},
		{"CreateApplicationAssignment", "SWBExternalService.CreateApplicationAssignment"},
		{"CreateInstance", "SWBExternalService.CreateInstance"},
		{
			"CreateInstanceAccessControlAttributeConfiguration",
			"SWBExternalService.CreateInstanceAccessControlAttributeConfiguration",
		},
		{"CreatePermissionSet", "SWBExternalService.CreatePermissionSet"},
		{"CreateTrustedTokenIssuer", "SWBExternalService.CreateTrustedTokenIssuer"},
		{"DeleteAccountAssignment", "SWBExternalService.DeleteAccountAssignment"},
		{"DeleteApplication", "SWBExternalService.DeleteApplication"},
		{"DeleteApplicationAccessScope", "SWBExternalService.DeleteApplicationAccessScope"},
		{"DeleteApplicationAssignment", "SWBExternalService.DeleteApplicationAssignment"},
		{"DeleteApplicationAuthenticationMethod", "SWBExternalService.DeleteApplicationAuthenticationMethod"},
		{"DeleteApplicationGrant", "SWBExternalService.DeleteApplicationGrant"},
		{"DeleteInlinePolicyFromPermissionSet", "SWBExternalService.DeleteInlinePolicyFromPermissionSet"},
		{"DeleteInstance", "SWBExternalService.DeleteInstance"},
		{
			"DeleteInstanceAccessControlAttributeConfiguration",
			"SWBExternalService.DeleteInstanceAccessControlAttributeConfiguration",
		},
		{"DeletePermissionsBoundaryFromPermissionSet", "SWBExternalService.DeletePermissionsBoundaryFromPermissionSet"},
		{"DeletePermissionSet", "SWBExternalService.DeletePermissionSet"},
		{"DeleteTrustedTokenIssuer", "SWBExternalService.DeleteTrustedTokenIssuer"},
		{"DescribeAccountAssignmentCreationStatus", "SWBExternalService.DescribeAccountAssignmentCreationStatus"},
		{"DescribeAccountAssignmentDeletionStatus", "SWBExternalService.DescribeAccountAssignmentDeletionStatus"},
		{"DescribeApplication", "SWBExternalService.DescribeApplication"},
		{"DescribeApplicationAssignment", "SWBExternalService.DescribeApplicationAssignment"},
		{"DescribeApplicationProvider", "SWBExternalService.DescribeApplicationProvider"},
		{"DescribeInstance", "SWBExternalService.DescribeInstance"},
		{
			"DescribeInstanceAccessControlAttributeConfiguration",
			"SWBExternalService.DescribeInstanceAccessControlAttributeConfiguration",
		},
		{"DescribePermissionSet", "SWBExternalService.DescribePermissionSet"},
		{"DescribePermissionSetProvisioningStatus", "SWBExternalService.DescribePermissionSetProvisioningStatus"},
		{"DescribeRegion", "SWBExternalService.DescribeRegion"},
		{"DescribeTrustedTokenIssuer", "SWBExternalService.DescribeTrustedTokenIssuer"},
		{
			"DetachCustomerManagedPolicyReferenceFromPermissionSet",
			"SWBExternalService.DetachCustomerManagedPolicyReferenceFromPermissionSet",
		},
		{"DetachManagedPolicyFromPermissionSet", "SWBExternalService.DetachManagedPolicyFromPermissionSet"},
		{"GetApplicationAccessScope", "SWBExternalService.GetApplicationAccessScope"},
		{"GetApplicationAssignmentConfiguration", "SWBExternalService.GetApplicationAssignmentConfiguration"},
		{"GetApplicationAuthenticationMethod", "SWBExternalService.GetApplicationAuthenticationMethod"},
		{"GetApplicationGrant", "SWBExternalService.GetApplicationGrant"},
		{"GetApplicationSessionConfiguration", "SWBExternalService.GetApplicationSessionConfiguration"},
		{"GetInlinePolicyForPermissionSet", "SWBExternalService.GetInlinePolicyForPermissionSet"},
		{"GetPermissionsBoundaryForPermissionSet", "SWBExternalService.GetPermissionsBoundaryForPermissionSet"},
		{"ListAccountAssignmentCreationStatus", "SWBExternalService.ListAccountAssignmentCreationStatus"},
		{"ListAccountAssignmentDeletionStatus", "SWBExternalService.ListAccountAssignmentDeletionStatus"},
		{"ListAccountAssignments", "SWBExternalService.ListAccountAssignments"},
		{"ListAccountAssignmentsForPrincipal", "SWBExternalService.ListAccountAssignmentsForPrincipal"},
		{"ListAccountsForProvisionedPermissionSet", "SWBExternalService.ListAccountsForProvisionedPermissionSet"},
		{"ListApplicationAccessScopes", "SWBExternalService.ListApplicationAccessScopes"},
		{"ListApplicationAssignments", "SWBExternalService.ListApplicationAssignments"},
		{"ListApplicationAssignmentsForPrincipal", "SWBExternalService.ListApplicationAssignmentsForPrincipal"},
		{"ListApplicationAuthenticationMethods", "SWBExternalService.ListApplicationAuthenticationMethods"},
		{"ListApplicationGrants", "SWBExternalService.ListApplicationGrants"},
		{"ListApplicationProviders", "SWBExternalService.ListApplicationProviders"},
		{"ListApplications", "SWBExternalService.ListApplications"},
		{
			"ListCustomerManagedPolicyReferencesInPermissionSet",
			"SWBExternalService.ListCustomerManagedPolicyReferencesInPermissionSet",
		},
		{"ListInstances", "SWBExternalService.ListInstances"},
		{"ListManagedPoliciesInPermissionSet", "SWBExternalService.ListManagedPoliciesInPermissionSet"},
		{"ListPermissionSetProvisioningStatus", "SWBExternalService.ListPermissionSetProvisioningStatus"},
		{"ListPermissionSets", "SWBExternalService.ListPermissionSets"},
		{"ListPermissionSetsProvisionedToAccount", "SWBExternalService.ListPermissionSetsProvisionedToAccount"},
		{"ListRegions", "SWBExternalService.ListRegions"},
		{"ListTagsForResource", "SWBExternalService.ListTagsForResource"},
		{"ListTrustedTokenIssuers", "SWBExternalService.ListTrustedTokenIssuers"},
		{"ProvisionPermissionSet", "SWBExternalService.ProvisionPermissionSet"},
		{"PutApplicationAccessScope", "SWBExternalService.PutApplicationAccessScope"},
		{"PutApplicationAssignmentConfiguration", "SWBExternalService.PutApplicationAssignmentConfiguration"},
		{"PutApplicationAuthenticationMethod", "SWBExternalService.PutApplicationAuthenticationMethod"},
		{"PutApplicationGrant", "SWBExternalService.PutApplicationGrant"},
		{"PutApplicationSessionConfiguration", "SWBExternalService.PutApplicationSessionConfiguration"},
		{"PutInlinePolicyToPermissionSet", "SWBExternalService.PutInlinePolicyToPermissionSet"},
		{"PutPermissionsBoundaryToPermissionSet", "SWBExternalService.PutPermissionsBoundaryToPermissionSet"},
		{"RemoveRegion", "SWBExternalService.RemoveRegion"},
		{"TagResource", "SWBExternalService.TagResource"},
		{"UntagResource", "SWBExternalService.UntagResource"},
		{"UpdateApplication", "SWBExternalService.UpdateApplication"},
		{"UpdateInstance", "SWBExternalService.UpdateInstance"},
		{
			"UpdateInstanceAccessControlAttributeConfiguration",
			"SWBExternalService.UpdateInstanceAccessControlAttributeConfiguration",
		},
		{"UpdatePermissionSet", "SWBExternalService.UpdatePermissionSet"},
		{"UpdateTrustedTokenIssuer", "SWBExternalService.UpdateTrustedTokenIssuer"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real SSO Admin operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the dispatch-miss sentinel a dispatch-table key
// mismatch would produce.
//
// SSO Admin's sentinel (the "UnknownOperationException" wire type built at
// dispatch()'s single production call site, handler.go:537) is not reused by
// any other error path in this service (grepped) -- unlike workmail/transfer,
// whose dispatch-miss sentinel shares its wire type with ordinary validation
// errors, so asserting on the wire type here is safe.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := ssoadmin.NewInMemoryBackend("111122223333", "us-east-1")
			h := ssoadmin.NewHandler(backend)
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
