package cognitoidp_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Cognito
// IDP operation, extracted from cognitoidentityprovider@v1.67.4
// serializers.go: each op's awsAwsjson11_serializeOp<Op>.HandleSerialize
// sets httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "AWSCognitoIdentityProviderService.<Op>") and always
// request.Request.Method = "POST" against path "/" -- Cognito IDP is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same
// way (TrimPrefix on "AWSCognitoIdentityProviderService."), so the class
// of bug this table can catch is a dispatch-table key that doesn't exactly
// match the real op name (typo, wrong case -- Cognito IDP is case-sensitive
// JSON-RPC), not a route-template mismatch. The service's own local module
// name (cognitoidp) differs from the SDK package it imports
// (cognitoidentityprovider) -- confirmed in go.mod.
//
// This table covers all 129 real Cognito IDP ops, which is also
// gopherstack's full implemented set (h.GetSupportedOperations(),
// 129/129) as of cognitoidentityprovider@v1.67.4 -- confirmed by diffing
// GetSupportedOperations() against this exact list, zero mismatches either
// direction.
//
// One dispatch-table key was found and deliberately excluded from this
// table: "AdminSetUserMFASetting" is wired in h.dispatchTable() (via
// handler_mfa.go) and is dispatchable, but it is not a real Cognito IDP SDK
// operation name; the real op is "AdminSetUserMFAPreference", which *is*
// covered above and is wired separately. "AdminSetUserMFASetting" is
// unreachable by any real AWS client (the SDK never sends that target
// string) and GetSupportedOperations() correctly omits it, so routing is
// correct; this is dead/legacy dispatch cruft, not a bug, and is recorded
// here rather than "fixed".
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSCognitoIdentityProviderService.`
// and pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddCustomAttributes", "AWSCognitoIdentityProviderService.AddCustomAttributes"},
		{"AddUserPoolClientSecret", "AWSCognitoIdentityProviderService.AddUserPoolClientSecret"},
		{"AdminAddUserToGroup", "AWSCognitoIdentityProviderService.AdminAddUserToGroup"},
		{"AdminConfirmSignUp", "AWSCognitoIdentityProviderService.AdminConfirmSignUp"},
		{"AdminCreateUser", "AWSCognitoIdentityProviderService.AdminCreateUser"},
		{"AdminDeleteUser", "AWSCognitoIdentityProviderService.AdminDeleteUser"},
		{"AdminDeleteUserAttributes", "AWSCognitoIdentityProviderService.AdminDeleteUserAttributes"},
		{"AdminDisableProviderForUser", "AWSCognitoIdentityProviderService.AdminDisableProviderForUser"},
		{"AdminDisableUser", "AWSCognitoIdentityProviderService.AdminDisableUser"},
		{"AdminEnableUser", "AWSCognitoIdentityProviderService.AdminEnableUser"},
		{"AdminForgetDevice", "AWSCognitoIdentityProviderService.AdminForgetDevice"},
		{"AdminGetDevice", "AWSCognitoIdentityProviderService.AdminGetDevice"},
		{"AdminGetUser", "AWSCognitoIdentityProviderService.AdminGetUser"},
		{"AdminGetUserAuthFactors", "AWSCognitoIdentityProviderService.AdminGetUserAuthFactors"},
		{"AdminInitiateAuth", "AWSCognitoIdentityProviderService.AdminInitiateAuth"},
		{"AdminLinkProviderForUser", "AWSCognitoIdentityProviderService.AdminLinkProviderForUser"},
		{"AdminListDevices", "AWSCognitoIdentityProviderService.AdminListDevices"},
		{"AdminListGroupsForUser", "AWSCognitoIdentityProviderService.AdminListGroupsForUser"},
		{"AdminListUserAuthEvents", "AWSCognitoIdentityProviderService.AdminListUserAuthEvents"},
		{"AdminRemoveUserFromGroup", "AWSCognitoIdentityProviderService.AdminRemoveUserFromGroup"},
		{"AdminResetUserPassword", "AWSCognitoIdentityProviderService.AdminResetUserPassword"},
		{"AdminRespondToAuthChallenge", "AWSCognitoIdentityProviderService.AdminRespondToAuthChallenge"},
		{"AdminSetUserMFAPreference", "AWSCognitoIdentityProviderService.AdminSetUserMFAPreference"},
		{"AdminSetUserPassword", "AWSCognitoIdentityProviderService.AdminSetUserPassword"},
		{"AdminSetUserSettings", "AWSCognitoIdentityProviderService.AdminSetUserSettings"},
		{"AdminUpdateAuthEventFeedback", "AWSCognitoIdentityProviderService.AdminUpdateAuthEventFeedback"},
		{"AdminUpdateDeviceStatus", "AWSCognitoIdentityProviderService.AdminUpdateDeviceStatus"},
		{"AdminUpdateUserAttributes", "AWSCognitoIdentityProviderService.AdminUpdateUserAttributes"},
		{"AdminUserGlobalSignOut", "AWSCognitoIdentityProviderService.AdminUserGlobalSignOut"},
		{"AssociateSoftwareToken", "AWSCognitoIdentityProviderService.AssociateSoftwareToken"},
		{"ChangePassword", "AWSCognitoIdentityProviderService.ChangePassword"},
		{"CompleteWebAuthnRegistration", "AWSCognitoIdentityProviderService.CompleteWebAuthnRegistration"},
		{"ConfirmDevice", "AWSCognitoIdentityProviderService.ConfirmDevice"},
		{"ConfirmForgotPassword", "AWSCognitoIdentityProviderService.ConfirmForgotPassword"},
		{"ConfirmSignUp", "AWSCognitoIdentityProviderService.ConfirmSignUp"},
		{"CreateGroup", "AWSCognitoIdentityProviderService.CreateGroup"},
		{"CreateIdentityProvider", "AWSCognitoIdentityProviderService.CreateIdentityProvider"},
		{"CreateManagedLoginBranding", "AWSCognitoIdentityProviderService.CreateManagedLoginBranding"},
		{"CreateResourceServer", "AWSCognitoIdentityProviderService.CreateResourceServer"},
		{"CreateTerms", "AWSCognitoIdentityProviderService.CreateTerms"},
		{"CreateUserImportJob", "AWSCognitoIdentityProviderService.CreateUserImportJob"},
		{"CreateUserPool", "AWSCognitoIdentityProviderService.CreateUserPool"},
		{"CreateUserPoolClient", "AWSCognitoIdentityProviderService.CreateUserPoolClient"},
		{"CreateUserPoolDomain", "AWSCognitoIdentityProviderService.CreateUserPoolDomain"},
		{"CreateUserPoolReplica", "AWSCognitoIdentityProviderService.CreateUserPoolReplica"},
		{"DeleteGroup", "AWSCognitoIdentityProviderService.DeleteGroup"},
		{"DeleteIdentityProvider", "AWSCognitoIdentityProviderService.DeleteIdentityProvider"},
		{"DeleteManagedLoginBranding", "AWSCognitoIdentityProviderService.DeleteManagedLoginBranding"},
		{"DeleteResourceServer", "AWSCognitoIdentityProviderService.DeleteResourceServer"},
		{"DeleteTerms", "AWSCognitoIdentityProviderService.DeleteTerms"},
		{"DeleteUser", "AWSCognitoIdentityProviderService.DeleteUser"},
		{"DeleteUserAttributes", "AWSCognitoIdentityProviderService.DeleteUserAttributes"},
		{"DeleteUserPool", "AWSCognitoIdentityProviderService.DeleteUserPool"},
		{"DeleteUserPoolClient", "AWSCognitoIdentityProviderService.DeleteUserPoolClient"},
		{"DeleteUserPoolClientSecret", "AWSCognitoIdentityProviderService.DeleteUserPoolClientSecret"},
		{"DeleteUserPoolDomain", "AWSCognitoIdentityProviderService.DeleteUserPoolDomain"},
		{"DeleteUserPoolReplica", "AWSCognitoIdentityProviderService.DeleteUserPoolReplica"},
		{"DeleteWebAuthnCredential", "AWSCognitoIdentityProviderService.DeleteWebAuthnCredential"},
		{"DescribeIdentityProvider", "AWSCognitoIdentityProviderService.DescribeIdentityProvider"},
		{"DescribeManagedLoginBranding", "AWSCognitoIdentityProviderService.DescribeManagedLoginBranding"},
		{
			"DescribeManagedLoginBrandingByClient",
			"AWSCognitoIdentityProviderService.DescribeManagedLoginBrandingByClient",
		},
		{"DescribeResourceServer", "AWSCognitoIdentityProviderService.DescribeResourceServer"},
		{"DescribeRiskConfiguration", "AWSCognitoIdentityProviderService.DescribeRiskConfiguration"},
		{"DescribeTerms", "AWSCognitoIdentityProviderService.DescribeTerms"},
		{"DescribeUserImportJob", "AWSCognitoIdentityProviderService.DescribeUserImportJob"},
		{"DescribeUserPool", "AWSCognitoIdentityProviderService.DescribeUserPool"},
		{"DescribeUserPoolClient", "AWSCognitoIdentityProviderService.DescribeUserPoolClient"},
		{"DescribeUserPoolDomain", "AWSCognitoIdentityProviderService.DescribeUserPoolDomain"},
		{"ForgetDevice", "AWSCognitoIdentityProviderService.ForgetDevice"},
		{"ForgotPassword", "AWSCognitoIdentityProviderService.ForgotPassword"},
		{"GetCSVHeader", "AWSCognitoIdentityProviderService.GetCSVHeader"},
		{"GetDevice", "AWSCognitoIdentityProviderService.GetDevice"},
		{"GetGroup", "AWSCognitoIdentityProviderService.GetGroup"},
		{"GetIdentityProviderByIdentifier", "AWSCognitoIdentityProviderService.GetIdentityProviderByIdentifier"},
		{"GetLogDeliveryConfiguration", "AWSCognitoIdentityProviderService.GetLogDeliveryConfiguration"},
		{"GetProvisionedLimit", "AWSCognitoIdentityProviderService.GetProvisionedLimit"},
		{"GetSigningCertificate", "AWSCognitoIdentityProviderService.GetSigningCertificate"},
		{"GetTokensFromRefreshToken", "AWSCognitoIdentityProviderService.GetTokensFromRefreshToken"},
		{"GetUICustomization", "AWSCognitoIdentityProviderService.GetUICustomization"},
		{"GetUser", "AWSCognitoIdentityProviderService.GetUser"},
		{"GetUserAttributeVerificationCode", "AWSCognitoIdentityProviderService.GetUserAttributeVerificationCode"},
		{"GetUserAuthFactors", "AWSCognitoIdentityProviderService.GetUserAuthFactors"},
		{"GetUserPoolMfaConfig", "AWSCognitoIdentityProviderService.GetUserPoolMfaConfig"},
		{"GlobalSignOut", "AWSCognitoIdentityProviderService.GlobalSignOut"},
		{"InitiateAuth", "AWSCognitoIdentityProviderService.InitiateAuth"},
		{"ListDevices", "AWSCognitoIdentityProviderService.ListDevices"},
		{"ListGroups", "AWSCognitoIdentityProviderService.ListGroups"},
		{"ListIdentityProviders", "AWSCognitoIdentityProviderService.ListIdentityProviders"},
		{"ListResourceServers", "AWSCognitoIdentityProviderService.ListResourceServers"},
		{"ListTagsForResource", "AWSCognitoIdentityProviderService.ListTagsForResource"},
		{"ListTerms", "AWSCognitoIdentityProviderService.ListTerms"},
		{"ListUserImportJobs", "AWSCognitoIdentityProviderService.ListUserImportJobs"},
		{"ListUserPoolClientSecrets", "AWSCognitoIdentityProviderService.ListUserPoolClientSecrets"},
		{"ListUserPoolClients", "AWSCognitoIdentityProviderService.ListUserPoolClients"},
		{"ListUserPoolReplicas", "AWSCognitoIdentityProviderService.ListUserPoolReplicas"},
		{"ListUserPools", "AWSCognitoIdentityProviderService.ListUserPools"},
		{"ListUsers", "AWSCognitoIdentityProviderService.ListUsers"},
		{"ListUsersInGroup", "AWSCognitoIdentityProviderService.ListUsersInGroup"},
		{"ListWebAuthnCredentials", "AWSCognitoIdentityProviderService.ListWebAuthnCredentials"},
		{"ResendConfirmationCode", "AWSCognitoIdentityProviderService.ResendConfirmationCode"},
		{"RespondToAuthChallenge", "AWSCognitoIdentityProviderService.RespondToAuthChallenge"},
		{"RevokeToken", "AWSCognitoIdentityProviderService.RevokeToken"},
		{"SetLogDeliveryConfiguration", "AWSCognitoIdentityProviderService.SetLogDeliveryConfiguration"},
		{"SetRiskConfiguration", "AWSCognitoIdentityProviderService.SetRiskConfiguration"},
		{"SetUICustomization", "AWSCognitoIdentityProviderService.SetUICustomization"},
		{"SetUserMFAPreference", "AWSCognitoIdentityProviderService.SetUserMFAPreference"},
		{"SetUserPoolMfaConfig", "AWSCognitoIdentityProviderService.SetUserPoolMfaConfig"},
		{"SetUserSettings", "AWSCognitoIdentityProviderService.SetUserSettings"},
		{"SignUp", "AWSCognitoIdentityProviderService.SignUp"},
		{"StartUserImportJob", "AWSCognitoIdentityProviderService.StartUserImportJob"},
		{"StartWebAuthnRegistration", "AWSCognitoIdentityProviderService.StartWebAuthnRegistration"},
		{"StopUserImportJob", "AWSCognitoIdentityProviderService.StopUserImportJob"},
		{"TagResource", "AWSCognitoIdentityProviderService.TagResource"},
		{"UntagResource", "AWSCognitoIdentityProviderService.UntagResource"},
		{"UpdateAuthEventFeedback", "AWSCognitoIdentityProviderService.UpdateAuthEventFeedback"},
		{"UpdateDeviceStatus", "AWSCognitoIdentityProviderService.UpdateDeviceStatus"},
		{"UpdateGroup", "AWSCognitoIdentityProviderService.UpdateGroup"},
		{"UpdateIdentityProvider", "AWSCognitoIdentityProviderService.UpdateIdentityProvider"},
		{"UpdateManagedLoginBranding", "AWSCognitoIdentityProviderService.UpdateManagedLoginBranding"},
		{"UpdateProvisionedLimit", "AWSCognitoIdentityProviderService.UpdateProvisionedLimit"},
		{"UpdateResourceServer", "AWSCognitoIdentityProviderService.UpdateResourceServer"},
		{"UpdateTerms", "AWSCognitoIdentityProviderService.UpdateTerms"},
		{"UpdateUserAttributes", "AWSCognitoIdentityProviderService.UpdateUserAttributes"},
		{"UpdateUserPool", "AWSCognitoIdentityProviderService.UpdateUserPool"},
		{"UpdateUserPoolClient", "AWSCognitoIdentityProviderService.UpdateUserPoolClient"},
		{"UpdateUserPoolDomain", "AWSCognitoIdentityProviderService.UpdateUserPoolDomain"},
		{"UpdateUserPoolReplica", "AWSCognitoIdentityProviderService.UpdateUserPoolReplica"},
		{"VerifySoftwareToken", "AWSCognitoIdentityProviderService.VerifySoftwareToken"},
		{"VerifyUserAttribute", "AWSCognitoIdentityProviderService.VerifyUserAttribute"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Cognito IDP
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the "UnknownOperationException"
// sentinel that a dispatch-table key mismatch would produce. That sentinel
// (errUnknownAction in handler.go) has exactly one production call site --
// the dispatch() miss in the h.ops map lookup -- so it cannot collide with
// a legitimate error on this all-empty-body table.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "")
			h := cognitoidp.NewHandler(backend, "us-east-1")
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
