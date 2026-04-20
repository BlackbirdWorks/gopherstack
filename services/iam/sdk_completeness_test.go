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
		"DeleteSSHPublicKey",
		"DeleteServerCertificate",
		"DeleteServiceLinkedRole",
		"DeleteSigningCertificate",
		"DisableOrganizationsRootCredentialsManagement",
		"DisableOrganizationsRootSessions",
		"DisableOutboundWebIdentityFederation",
		"EnableMFADevice",
		"EnableOrganizationsRootCredentialsManagement",
		"EnableOrganizationsRootSessions",
		"EnableOutboundWebIdentityFederation",
		"GenerateOrganizationsAccessReport",
		"GenerateServiceLastAccessedDetails",
		"GetDelegationRequest",
		"GetHumanReadableSummary",
		"GetOrganizationsAccessReport",
		"GetOutboundWebIdentityFederationInfo",
		"GetSSHPublicKey",
		"GetServerCertificate",
		"GetServiceLastAccessedDetailsWithEntities",
		"ListDelegationRequests",
		"ListInstanceProfileTags",
		"ListMFADeviceTags",
		"ListMFADevices",
		"ListOpenIDConnectProviderTags",
		"ListOrganizationsFeatures",
		"ListPoliciesGrantingServiceAccess",
		"ListSAMLProviderTags",
		"ListSSHPublicKeys",
		"ListServerCertificateTags",
		"ListServerCertificates",
		"ListSigningCertificates",
		"RejectDelegationRequest",
		"ResetServiceSpecificCredential",
		"ResyncMFADevice",
		"SendDelegationToken",
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
		"UpdateDelegationRequest",
		"UpdateSSHPublicKey",
		"UpdateServerCertificate",
		"UpdateSigningCertificate",
		"UploadSSHPublicKey",
		"UploadServerCertificate",
		"UploadSigningCertificate",
	})
}
