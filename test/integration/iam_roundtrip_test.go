package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_IAM_AttachUserPolicy_RoundTrip verifies that AttachUserPolicy is
// visible through the listing op ListAttachedUserPolicies, not just a 200 response,
// and that DetachUserPolicy actually removes it from that same listing.
func TestIntegration_IAM_AttachUserPolicy_RoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	userName := "attach-user-" + uuid.NewString()[:8]
	policyName := "attach-pol-" + uuid.NewString()[:8]

	_, err := client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteUser(cleanupCtx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})
	})

	polOut, err := client.CreatePolicy(ctx, &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(allowAllS3Doc),
	})
	require.NoError(t, err)
	policyArn := aws.ToString(polOut.Policy.Arn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DetachUserPolicy(cleanupCtx, &iamsdk.DetachUserPolicyInput{
			UserName: aws.String(userName), PolicyArn: aws.String(policyArn),
		})
		_, _ = client.DeletePolicy(cleanupCtx, &iamsdk.DeletePolicyInput{PolicyArn: aws.String(policyArn)})
	})

	_, err = client.AttachUserPolicy(ctx, &iamsdk.AttachUserPolicyInput{
		UserName:  aws.String(userName),
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)

	// The attachment must be visible through the listing op — a policy that
	// AttachUserPolicy stores and ListAttachedUserPolicies never reads back is
	// indistinguishable from a no-op grant.
	listOut, err := client.ListAttachedUserPolicies(ctx, &iamsdk.ListAttachedUserPoliciesInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.AttachedPolicies, "attached policy must appear in ListAttachedUserPolicies")

	var found bool

	for _, p := range listOut.AttachedPolicies {
		if aws.ToString(p.PolicyArn) == policyArn {
			found = true

			assert.Equal(t, policyName, aws.ToString(p.PolicyName))
		}
	}

	assert.True(t, found, "attached policy ARN not found in listing")

	_, err = client.DetachUserPolicy(ctx, &iamsdk.DetachUserPolicyInput{
		UserName:  aws.String(userName),
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)

	listOut2, err := client.ListAttachedUserPolicies(ctx, &iamsdk.ListAttachedUserPoliciesInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	for _, p := range listOut2.AttachedPolicies {
		assert.NotEqual(t, policyArn, aws.ToString(p.PolicyArn), "policy still attached after Detach")
	}
}

// TestIntegration_IAM_InstanceProfileRole_RoundTrip verifies that
// AddRoleToInstanceProfile is visible through both GetInstanceProfile and
// ListInstanceProfilesForRole — the read side used by EC2 instance launch and by
// callers checking which profiles a role belongs to — and that
// RemoveRoleFromInstanceProfile actually clears it from both.
func TestIntegration_IAM_InstanceProfileRole_RoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	roleName := "ip-role-" + uuid.NewString()[:8]
	profileName := "ip-profile-" + uuid.NewString()[:8]

	_, err := client.CreateRole(ctx, &iamsdk.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(allowRoleTrustDoc),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteRole(cleanupCtx, &iamsdk.DeleteRoleInput{RoleName: aws.String(roleName)})
	})

	_, err = client.CreateInstanceProfile(ctx, &iamsdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.RemoveRoleFromInstanceProfile(cleanupCtx, &iamsdk.RemoveRoleFromInstanceProfileInput{
			InstanceProfileName: aws.String(profileName), RoleName: aws.String(roleName),
		})
		_, _ = client.DeleteInstanceProfile(cleanupCtx, &iamsdk.DeleteInstanceProfileInput{
			InstanceProfileName: aws.String(profileName),
		})
	})

	_, err = client.AddRoleToInstanceProfile(ctx, &iamsdk.AddRoleToInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
		RoleName:            aws.String(roleName),
	})
	require.NoError(t, err)

	getOut, err := client.GetInstanceProfile(ctx, &iamsdk.GetInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
	})
	require.NoError(t, err)
	require.Len(t, getOut.InstanceProfile.Roles, 1, "role must appear on the instance profile after Add")
	assert.Equal(t, roleName, aws.ToString(getOut.InstanceProfile.Roles[0].RoleName))

	listOut, err := client.ListInstanceProfilesForRole(ctx, &iamsdk.ListInstanceProfilesForRoleInput{
		RoleName: aws.String(roleName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.InstanceProfiles, "profile must be listed for the role after Add")

	var found bool
	for _, p := range listOut.InstanceProfiles {
		if aws.ToString(p.InstanceProfileName) == profileName {
			found = true
		}
	}

	assert.True(t, found, "instance profile not found via ListInstanceProfilesForRole")

	_, err = client.RemoveRoleFromInstanceProfile(ctx, &iamsdk.RemoveRoleFromInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
		RoleName:            aws.String(roleName),
	})
	require.NoError(t, err)

	getOut2, err := client.GetInstanceProfile(ctx, &iamsdk.GetInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
	})
	require.NoError(t, err)
	assert.Empty(t, getOut2.InstanceProfile.Roles, "role should be gone after Remove")
}
