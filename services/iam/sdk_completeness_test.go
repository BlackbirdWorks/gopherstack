package iam_test

import (
	"testing"

	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// iam client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &iamsdk.Client{}, h.GetSupportedOperations(), []string{
		"DeactivateMFADevice",
		"DeleteAccountAlias",
		"DeleteAccountPasswordPolicy",
		"DeletePolicyVersion",
		"DeleteSSHPublicKey",
		"DeleteServerCertificate",
		"DeleteServiceLinkedRole",
		"DeleteServiceSpecificCredential",
		"DeleteSigningCertificate",
		"DeleteVirtualMFADevice",
		"DisableOrganizationsRootCredentialsManagement",
		"DisableOrganizationsRootSessions",
		"DisableOutboundWebIdentityFederation",
		"EnableMFADevice",
		"EnableOrganizationsRootCredentialsManagement",
		"EnableOrganizationsRootSessions",
		"EnableOutboundWebIdentityFederation",
		"GenerateOrganizationsAccessReport",
		"GenerateServiceLastAccessedDetails",
		"GetAccessKeyLastUsed",
		"GetAccountPasswordPolicy",
		"GetContextKeysForCustomPolicy",
		"GetContextKeysForPrincipalPolicy",
		"GetDelegationRequest",
		"GetHumanReadableSummary",
		"GetInstanceProfile",
		"GetMFADevice",
		"GetOrganizationsAccessReport",
		"GetOutboundWebIdentityFederationInfo",
		"GetSSHPublicKey",
		"GetServerCertificate",
		"GetServiceLastAccessedDetailsWithEntities",
		"GetServiceLinkedRoleDeletionStatus",
		"ListAccountAliases",
		"ListDelegationRequests",
		"ListEntitiesForPolicy",
		"ListGroupsForUser",
		"ListInstanceProfileTags",
		"ListInstanceProfilesForRole",
		"ListMFADeviceTags",
		"ListMFADevices",
		"ListOpenIDConnectProviderTags",
		"ListOrganizationsFeatures",
		"ListPoliciesGrantingServiceAccess",
		"ListPolicyVersions",
		"ListSAMLProviderTags",
		"ListSSHPublicKeys",
		"ListServerCertificateTags",
		"ListServerCertificates",
		"ListServiceSpecificCredentials",
		"ListSigningCertificates",
		"ListVirtualMFADevices",
		"RejectDelegationRequest",
		"RemoveClientIDFromOpenIDConnectProvider",
		"ResetServiceSpecificCredential",
		"ResyncMFADevice",
		"SendDelegationToken",
		"SetDefaultPolicyVersion",
		"SimulateCustomPolicy",
		"TagInstanceProfile",
		"TagMFADevice",
		"TagOpenIDConnectProvider",
		"TagSAMLProvider",
		"TagServerCertificate",
		"UntagInstanceProfile",
		"UntagMFADevice",
		"UntagOpenIDConnectProvider",
		"UntagSAMLProvider",
		"UntagServerCertificate",
		"UpdateAccessKey",
		"UpdateAccountPasswordPolicy",
		"UpdateDelegationRequest",
		"UpdateGroup",
		"UpdateRole",
		"UpdateRoleDescription",
		"UpdateSSHPublicKey",
		"UpdateServerCertificate",
		"UpdateServiceSpecificCredential",
		"UpdateSigningCertificate",
		"UpdateUser",
		"UploadSSHPublicKey",
		"UploadServerCertificate",
		"UploadSigningCertificate",
	})
}
