package iam_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteOps is the authoritative operation list for IAM, taken from the
// api_op_*.go filenames in iam@v1.58.1 (one file per real op) and
// cross-checked against the Action form field each op's
// awsAwsquery_serializeOp<Op> writes via body.Key("Action").String("<Op>")
// in serializers.go. IAM is AWS Query protocol: the Action value IS the wire
// op name (no path/method to drift), so ExtractOperation cannot misroute on
// its own -- the risk this table guards is Handler()'s dispatch table
// (buildDispatchTable, handler.go) silently missing an entry for a real op
// and falling through to the InvalidAction sentinel (ErrInvalidAction).
//
// Regenerate by listing api_op_*.go in the pinned iam module.
func sdkRouteOps() []string {
	return []string{
		"AcceptDelegationRequest",
		"AddClientIDToOpenIDConnectProvider",
		"AddRoleToInstanceProfile",
		"AddUserToGroup",
		"AssociateDelegationRequest",
		"AttachGroupPolicy",
		"AttachRolePolicy",
		"AttachUserPolicy",
		"ChangePassword",
		"CreateAccessKey",
		"CreateAccountAlias",
		"CreateDelegationRequest",
		"CreateGroup",
		"CreateInstanceProfile",
		"CreateLoginProfile",
		"CreateOpenIDConnectProvider",
		"CreatePolicy",
		"CreatePolicyVersion",
		"CreateRole",
		"CreateSAMLProvider",
		"CreateServiceLinkedRole",
		"CreateServiceSpecificCredential",
		"CreateUser",
		"CreateVirtualMFADevice",
		"DeactivateMFADevice",
		"DeleteAccessKey",
		"DeleteAccountAlias",
		"DeleteAccountPasswordPolicy",
		"DeleteGroup",
		"DeleteGroupPolicy",
		"DeleteInstanceProfile",
		"DeleteLoginProfile",
		"DeleteOpenIDConnectProvider",
		"DeletePolicy",
		"DeletePolicyVersion",
		"DeleteRole",
		"DeleteRolePermissionsBoundary",
		"DeleteRolePolicy",
		"DeleteSAMLProvider",
		"DeleteServerCertificate",
		"DeleteServiceLinkedRole",
		"DeleteServiceSpecificCredential",
		"DeleteSigningCertificate",
		"DeleteSSHPublicKey",
		"DeleteUser",
		"DeleteUserPermissionsBoundary",
		"DeleteUserPolicy",
		"DeleteVirtualMFADevice",
		"DetachGroupPolicy",
		"DetachRolePolicy",
		"DetachUserPolicy",
		"DisableOrganizationsRootCredentialsManagement",
		"DisableOrganizationsRootSessions",
		"DisableOutboundWebIdentityFederation",
		"EnableMFADevice",
		"EnableOrganizationsRootCredentialsManagement",
		"EnableOrganizationsRootSessions",
		"EnableOutboundWebIdentityFederation",
		"GenerateCredentialReport",
		"GenerateOrganizationsAccessReport",
		"GenerateServiceLastAccessedDetails",
		"GetAccessKeyLastUsed",
		"GetAccountAuthorizationDetails",
		"GetAccountPasswordPolicy",
		"GetAccountSummary",
		"GetContextKeysForCustomPolicy",
		"GetContextKeysForPrincipalPolicy",
		"GetCredentialReport",
		"GetDelegationRequest",
		"GetGroup",
		"GetGroupPolicy",
		"GetHumanReadableSummary",
		"GetInstanceProfile",
		"GetLoginProfile",
		"GetMFADevice",
		"GetOpenIDConnectProvider",
		"GetOrganizationsAccessReport",
		"GetOutboundWebIdentityFederationInfo",
		"GetPolicy",
		"GetPolicyVersion",
		"GetRole",
		"GetRolePolicy",
		"GetSAMLProvider",
		"GetServerCertificate",
		"GetServiceLastAccessedDetails",
		"GetServiceLastAccessedDetailsWithEntities",
		"GetServiceLinkedRoleDeletionStatus",
		"GetSSHPublicKey",
		"GetUser",
		"GetUserPolicy",
		"ListAccessKeys",
		"ListAccountAliases",
		"ListAttachedGroupPolicies",
		"ListAttachedRolePolicies",
		"ListAttachedUserPolicies",
		"ListDelegationRequests",
		"ListEntitiesForPolicy",
		"ListGroupPolicies",
		"ListGroups",
		"ListGroupsForUser",
		"ListInstanceProfiles",
		"ListInstanceProfilesForRole",
		"ListInstanceProfileTags",
		"ListMFADevices",
		"ListMFADeviceTags",
		"ListOpenIDConnectProviders",
		"ListOpenIDConnectProviderTags",
		"ListOrganizationsFeatures",
		"ListPolicies",
		"ListPoliciesGrantingServiceAccess",
		"ListPolicyTags",
		"ListPolicyVersions",
		"ListRolePolicies",
		"ListRoles",
		"ListRoleTags",
		"ListSAMLProviders",
		"ListSAMLProviderTags",
		"ListServerCertificates",
		"ListServerCertificateTags",
		"ListServiceSpecificCredentials",
		"ListSigningCertificates",
		"ListSSHPublicKeys",
		"ListUserPolicies",
		"ListUsers",
		"ListUserTags",
		"ListVirtualMFADevices",
		"PutGroupPolicy",
		"PutRolePermissionsBoundary",
		"PutRolePolicy",
		"PutUserPermissionsBoundary",
		"PutUserPolicy",
		"RejectDelegationRequest",
		"RemoveClientIDFromOpenIDConnectProvider",
		"RemoveRoleFromInstanceProfile",
		"RemoveUserFromGroup",
		"ResetServiceSpecificCredential",
		"ResyncMFADevice",
		"SendDelegationToken",
		"SetDefaultPolicyVersion",
		"SetSecurityTokenServicePreferences",
		"SimulateCustomPolicy",
		"SimulatePrincipalPolicy",
		"TagInstanceProfile",
		"TagMFADevice",
		"TagOpenIDConnectProvider",
		"TagPolicy",
		"TagRole",
		"TagSAMLProvider",
		"TagServerCertificate",
		"TagUser",
		"UntagInstanceProfile",
		"UntagMFADevice",
		"UntagOpenIDConnectProvider",
		"UntagPolicy",
		"UntagRole",
		"UntagSAMLProvider",
		"UntagServerCertificate",
		"UntagUser",
		"UpdateAccessKey",
		"UpdateAccountPasswordPolicy",
		"UpdateAssumeRolePolicy",
		"UpdateDelegationRequest",
		"UpdateGroup",
		"UpdateLoginProfile",
		"UpdateOpenIDConnectProviderThumbprint",
		"UpdateRole",
		"UpdateRoleDescription",
		"UpdateSAMLProvider",
		"UpdateServerCertificate",
		"UpdateServiceSpecificCredential",
		"UpdateSigningCertificate",
		"UpdateSSHPublicKey",
		"UpdateUser",
		"UploadServerCertificate",
		"UploadSigningCertificate",
		"UploadSSHPublicKey",
	}
}

// TestExtractOperation_SDKRouteTable drives every real IAM operation's
// authoritative Action form field through ExtractOperation and the real
// Handler(), asserting the response never falls through to dispatch()'s
// not-found branch ("<action> is not a valid IAM action", handler.go). Note
// ErrInvalidAction is also reused by several real handler functions as a
// generic bad-input sentinel (e.g. "account alias must not be empty"), so
// the check must match that exact phrase rather than the shared
// "InvalidAction" error code, or it false-positives on ordinary validation
// errors. A bare "Action=<Op>" body is enough: missing required parameters
// are expected to surface as ordinary validation/not-found errors from the
// real handler function, not the unknown-action branch.
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
			assert.NotContains(t, rec.Body.String(), "is not a valid IAM action",
				"op=%s: dispatched to the invalid-action handler", op)
		})
	}
}
