package ssoadmin_test

import (
	"testing"

	ssoadminsdk "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// ssoadmin client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &ssoadminsdk.Client{}, h.GetSupportedOperations(), []string{
		"DeleteApplicationGrant",
		"DeleteInstanceAccessControlAttributeConfiguration",
		"DeletePermissionsBoundaryFromPermissionSet",
		"DeleteTrustedTokenIssuer",
		"DescribeApplication",
		"DescribeApplicationAssignment",
		"DescribeApplicationProvider",
		"DescribeInstanceAccessControlAttributeConfiguration",
		"DescribeRegion",
		"DescribeTrustedTokenIssuer",
		"DetachCustomerManagedPolicyReferenceFromPermissionSet",
		"GetApplicationAccessScope",
		"GetApplicationAssignmentConfiguration",
		"GetApplicationAuthenticationMethod",
		"GetApplicationGrant",
		"GetApplicationSessionConfiguration",
		"GetPermissionsBoundaryForPermissionSet",
		"ListAccountAssignmentCreationStatus",
		"ListAccountAssignmentDeletionStatus",
		"ListAccountAssignmentsForPrincipal",
		"ListAccountsForProvisionedPermissionSet",
		"ListApplicationAccessScopes",
		"ListApplicationAssignments",
		"ListApplicationAssignmentsForPrincipal",
		"ListApplicationAuthenticationMethods",
		"ListApplicationGrants",
		"ListApplicationProviders",
		"ListApplications",
		"ListCustomerManagedPolicyReferencesInPermissionSet",
		"ListPermissionSetProvisioningStatus",
		"ListPermissionSetsProvisionedToAccount",
		"ListRegions",
		"ListTrustedTokenIssuers",
		"PutApplicationAccessScope",
		"PutApplicationAssignmentConfiguration",
		"PutApplicationAuthenticationMethod",
		"PutApplicationGrant",
		"PutApplicationSessionConfiguration",
		"PutPermissionsBoundaryToPermissionSet",
		"RemoveRegion",
		"UpdateApplication",
		"UpdateInstance",
		"UpdateInstanceAccessControlAttributeConfiguration",
		"UpdateTrustedTokenIssuer",
	})
}
