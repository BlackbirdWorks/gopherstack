package workmail_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Amazon
// WorkMail operation, extracted from workmail@v1.39.4 serializers.go: each
// op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("WorkMailService.<Op>")
// and always request.Request.Method = "POST" against path "/" -- WorkMail is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "WorkMailService."), so the class of bug this table can
// catch is a dispatch-table key that doesn't exactly match the real op name
// (typo, wrong case -- WorkMail is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers all 92 real WorkMail ops, which is also gopherstack's
// full implemented set (h.GetSupportedOperations(), 92/92) as of
// workmail@v1.39.4 -- confirmed by diffing the actual buildOps() dispatch
// table (all four family builders combined) against this exact list, zero
// mismatches either direction: no dead key, no gap.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("WorkMailService.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AssociateDelegateToResource", "WorkMailService.AssociateDelegateToResource"},
		{"AssociateMemberToGroup", "WorkMailService.AssociateMemberToGroup"},
		{"AssumeImpersonationRole", "WorkMailService.AssumeImpersonationRole"},
		{"CancelMailboxExportJob", "WorkMailService.CancelMailboxExportJob"},
		{"CreateAlias", "WorkMailService.CreateAlias"},
		{"CreateAvailabilityConfiguration", "WorkMailService.CreateAvailabilityConfiguration"},
		{"CreateGroup", "WorkMailService.CreateGroup"},
		{"CreateIdentityCenterApplication", "WorkMailService.CreateIdentityCenterApplication"},
		{"CreateImpersonationRole", "WorkMailService.CreateImpersonationRole"},
		{"CreateMobileDeviceAccessRule", "WorkMailService.CreateMobileDeviceAccessRule"},
		{"CreateOrganization", "WorkMailService.CreateOrganization"},
		{"CreateResource", "WorkMailService.CreateResource"},
		{"CreateUser", "WorkMailService.CreateUser"},
		{"DeleteAccessControlRule", "WorkMailService.DeleteAccessControlRule"},
		{"DeleteAlias", "WorkMailService.DeleteAlias"},
		{"DeleteAvailabilityConfiguration", "WorkMailService.DeleteAvailabilityConfiguration"},
		{"DeleteEmailMonitoringConfiguration", "WorkMailService.DeleteEmailMonitoringConfiguration"},
		{"DeleteGroup", "WorkMailService.DeleteGroup"},
		{"DeleteIdentityCenterApplication", "WorkMailService.DeleteIdentityCenterApplication"},
		{"DeleteIdentityProviderConfiguration", "WorkMailService.DeleteIdentityProviderConfiguration"},
		{"DeleteImpersonationRole", "WorkMailService.DeleteImpersonationRole"},
		{"DeleteMailboxPermissions", "WorkMailService.DeleteMailboxPermissions"},
		{"DeleteMobileDeviceAccessOverride", "WorkMailService.DeleteMobileDeviceAccessOverride"},
		{"DeleteMobileDeviceAccessRule", "WorkMailService.DeleteMobileDeviceAccessRule"},
		{"DeleteOrganization", "WorkMailService.DeleteOrganization"},
		{"DeletePersonalAccessToken", "WorkMailService.DeletePersonalAccessToken"},
		{"DeleteResource", "WorkMailService.DeleteResource"},
		{"DeleteRetentionPolicy", "WorkMailService.DeleteRetentionPolicy"},
		{"DeleteUser", "WorkMailService.DeleteUser"},
		{"DeregisterFromWorkMail", "WorkMailService.DeregisterFromWorkMail"},
		{"DeregisterMailDomain", "WorkMailService.DeregisterMailDomain"},
		{"DescribeEmailMonitoringConfiguration", "WorkMailService.DescribeEmailMonitoringConfiguration"},
		{"DescribeEntity", "WorkMailService.DescribeEntity"},
		{"DescribeGroup", "WorkMailService.DescribeGroup"},
		{"DescribeIdentityProviderConfiguration", "WorkMailService.DescribeIdentityProviderConfiguration"},
		{"DescribeInboundDmarcSettings", "WorkMailService.DescribeInboundDmarcSettings"},
		{"DescribeMailboxExportJob", "WorkMailService.DescribeMailboxExportJob"},
		{"DescribeOrganization", "WorkMailService.DescribeOrganization"},
		{"DescribeResource", "WorkMailService.DescribeResource"},
		{"DescribeUser", "WorkMailService.DescribeUser"},
		{"DisassociateDelegateFromResource", "WorkMailService.DisassociateDelegateFromResource"},
		{"DisassociateMemberFromGroup", "WorkMailService.DisassociateMemberFromGroup"},
		{"GetAccessControlEffect", "WorkMailService.GetAccessControlEffect"},
		{"GetDefaultRetentionPolicy", "WorkMailService.GetDefaultRetentionPolicy"},
		{"GetImpersonationRole", "WorkMailService.GetImpersonationRole"},
		{"GetImpersonationRoleEffect", "WorkMailService.GetImpersonationRoleEffect"},
		{"GetMailboxDetails", "WorkMailService.GetMailboxDetails"},
		{"GetMailDomain", "WorkMailService.GetMailDomain"},
		{"GetMobileDeviceAccessEffect", "WorkMailService.GetMobileDeviceAccessEffect"},
		{"GetMobileDeviceAccessOverride", "WorkMailService.GetMobileDeviceAccessOverride"},
		{"GetPersonalAccessTokenMetadata", "WorkMailService.GetPersonalAccessTokenMetadata"},
		{"ListAccessControlRules", "WorkMailService.ListAccessControlRules"},
		{"ListAliases", "WorkMailService.ListAliases"},
		{"ListAvailabilityConfigurations", "WorkMailService.ListAvailabilityConfigurations"},
		{"ListGroupMembers", "WorkMailService.ListGroupMembers"},
		{"ListGroups", "WorkMailService.ListGroups"},
		{"ListGroupsForEntity", "WorkMailService.ListGroupsForEntity"},
		{"ListImpersonationRoles", "WorkMailService.ListImpersonationRoles"},
		{"ListMailboxExportJobs", "WorkMailService.ListMailboxExportJobs"},
		{"ListMailboxPermissions", "WorkMailService.ListMailboxPermissions"},
		{"ListMailDomains", "WorkMailService.ListMailDomains"},
		{"ListMobileDeviceAccessOverrides", "WorkMailService.ListMobileDeviceAccessOverrides"},
		{"ListMobileDeviceAccessRules", "WorkMailService.ListMobileDeviceAccessRules"},
		{"ListOrganizations", "WorkMailService.ListOrganizations"},
		{"ListPersonalAccessTokens", "WorkMailService.ListPersonalAccessTokens"},
		{"ListResourceDelegates", "WorkMailService.ListResourceDelegates"},
		{"ListResources", "WorkMailService.ListResources"},
		{"ListTagsForResource", "WorkMailService.ListTagsForResource"},
		{"ListUsers", "WorkMailService.ListUsers"},
		{"PutAccessControlRule", "WorkMailService.PutAccessControlRule"},
		{"PutEmailMonitoringConfiguration", "WorkMailService.PutEmailMonitoringConfiguration"},
		{"PutIdentityProviderConfiguration", "WorkMailService.PutIdentityProviderConfiguration"},
		{"PutInboundDmarcSettings", "WorkMailService.PutInboundDmarcSettings"},
		{"PutMailboxPermissions", "WorkMailService.PutMailboxPermissions"},
		{"PutMobileDeviceAccessOverride", "WorkMailService.PutMobileDeviceAccessOverride"},
		{"PutRetentionPolicy", "WorkMailService.PutRetentionPolicy"},
		{"RegisterMailDomain", "WorkMailService.RegisterMailDomain"},
		{"RegisterToWorkMail", "WorkMailService.RegisterToWorkMail"},
		{"ResetPassword", "WorkMailService.ResetPassword"},
		{"StartMailboxExportJob", "WorkMailService.StartMailboxExportJob"},
		{"TagResource", "WorkMailService.TagResource"},
		{"TestAvailabilityConfiguration", "WorkMailService.TestAvailabilityConfiguration"},
		{"UntagResource", "WorkMailService.UntagResource"},
		{"UpdateAvailabilityConfiguration", "WorkMailService.UpdateAvailabilityConfiguration"},
		{"UpdateDefaultMailDomain", "WorkMailService.UpdateDefaultMailDomain"},
		{"UpdateGroup", "WorkMailService.UpdateGroup"},
		{"UpdateImpersonationRole", "WorkMailService.UpdateImpersonationRole"},
		{"UpdateMailboxQuota", "WorkMailService.UpdateMailboxQuota"},
		{"UpdateMobileDeviceAccessRule", "WorkMailService.UpdateMobileDeviceAccessRule"},
		{"UpdatePrimaryEmailAddress", "WorkMailService.UpdatePrimaryEmailAddress"},
		{"UpdateResource", "WorkMailService.UpdateResource"},
		{"UpdateUser", "WorkMailService.UpdateUser"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real WorkMail operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the dispatch-miss sentinel a dispatch-table key
// mismatch would produce.
//
// WorkMail's sentinel (unknownOpError, constructed only by newUnknownOpError
// at its single production call site, the dispatch() miss in handler.go) is
// wire-mapped to "InvalidParameterException" -- the same wire type WorkMail's
// ordinary ErrValidation uses for ordinary bad input (see errors.go and the
// "X is required" call sites in organizations.go/users.go/resources.go/
// availability_config.go). Asserting on that shared wire type would produce
// false positives on this all-empty-body table, the same trap iam's
// InvalidAction reuse sets. So this test asserts on unknownOpError's own
// message text ("unknown operation: ") instead, which is unique in the
// package (grepped) and only ever produced by the dispatch miss.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
			h := workmail.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation:",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
