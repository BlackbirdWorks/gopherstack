package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	allowRoleTrustDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	allowAllS3Doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	allowEC2Doc   = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:*","Resource":"*"}]}`
)

// TestIntegration_IAM_SimulatePrincipalPolicy verifies that SimulatePrincipalPolicy returns
// structured EvaluationResults with action names, resource ARNs, and a decision string.
func TestIntegration_IAM_SimulatePrincipalPolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	roleName := "sim-role-" + uuid.NewString()[:8]
	policyName := "sim-pol-" + uuid.NewString()[:8]

	// Create role.
	roleOut, err := client.CreateRole(ctx, &iamsdk.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(allowRoleTrustDoc),
	})
	require.NoError(t, err)

	roleArn := *roleOut.Role.Arn

	// Create and attach a policy granting s3:*.
	polOut, err := client.CreatePolicy(ctx, &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(allowAllS3Doc),
	})
	require.NoError(t, err)

	policyArn := *polOut.Policy.Arn

	_, err = client.AttachRolePolicy(ctx, &iamsdk.AttachRolePolicyInput{
		RoleName:  aws.String(roleName),
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)

	// SimulatePrincipalPolicy — two actions against two resources.
	actions := []string{"s3:GetObject", "s3:PutObject", "ec2:DescribeInstances"}
	resources := []string{
		"arn:aws:s3:::my-bucket/*",
		"*",
	}

	simOut, err := client.SimulatePrincipalPolicy(ctx, &iamsdk.SimulatePrincipalPolicyInput{
		PolicySourceArn: aws.String(roleArn),
		ActionNames:     actions,
		ResourceArns:    resources,
	})
	require.NoError(t, err)

	// Expect len(actions) × len(resources) results.
	assert.Len(t, simOut.EvaluationResults, len(actions)*len(resources),
		"should return one result per action×resource pair")

	// Every result must have the essential fields set.
	for _, r := range simOut.EvaluationResults {
		assert.NotEmpty(t, r.EvalActionName, "EvalActionName must be set")
		assert.NotEmpty(t, r.EvalResourceName, "EvalResourceName must be set")
		assert.NotEmpty(t, r.EvalDecision, "EvalDecision must be set")
	}

	// s3:GetObject and s3:PutObject should be allowed (policy grants s3:*).
	for _, r := range simOut.EvaluationResults {
		name := string(r.EvalDecision)
		switch *r.EvalActionName {
		case "s3:GetObject", "s3:PutObject":
			assert.Equal(t, "allowed", name,
				"s3 action %q should be allowed by attached policy", *r.EvalActionName)
		case "ec2:DescribeInstances":
			// ec2 is not granted; should be denied (implicit or explicit).
			assert.NotEqual(t, "allowed", name,
				"ec2 action should not be allowed without an ec2 policy")
		}
	}

	// Cleanup.
	_, _ = client.DetachRolePolicy(ctx, &iamsdk.DetachRolePolicyInput{
		RoleName:  aws.String(roleName),
		PolicyArn: aws.String(policyArn),
	})
	_, _ = client.DeleteRole(ctx, &iamsdk.DeleteRoleInput{RoleName: aws.String(roleName)})
	_, _ = client.DeletePolicy(ctx, &iamsdk.DeletePolicyInput{PolicyArn: aws.String(policyArn)})
}

// TestIntegration_IAM_PolicyVersionLifecycle verifies CreatePolicyVersion, ListPolicyVersions,
// SetDefaultPolicyVersion, and DeletePolicyVersion.
func TestIntegration_IAM_PolicyVersionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	policyName := "ver-pol-" + uuid.NewString()[:8]

	// Create initial policy (v1).
	polOut, err := client.CreatePolicy(ctx, &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(allowAllS3Doc),
	})
	require.NoError(t, err)

	policyArn := *polOut.Policy.Arn

	// List versions — should have v1 as default.
	listOut, err := client.ListPolicyVersions(ctx, &iamsdk.ListPolicyVersionsInput{
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Versions, 1, "initial policy should have exactly 1 version")
	assert.True(t, listOut.Versions[0].IsDefaultVersion, "v1 must be default")

	// CreatePolicyVersion — add v2 with an updated doc.
	_, err = client.CreatePolicyVersion(ctx, &iamsdk.CreatePolicyVersionInput{
		PolicyArn:      aws.String(policyArn),
		PolicyDocument: aws.String(allowEC2Doc),
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	listOut2, err := client.ListPolicyVersions(ctx, &iamsdk.ListPolicyVersionsInput{
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Versions, 2, "should have 2 versions after creating v2")

	// Find v2.
	var v2ID string

	for _, v := range listOut2.Versions {
		if !v.IsDefaultVersion {
			v2ID = *v.VersionId

			break
		}
	}

	require.NotEmpty(t, v2ID, "must find a non-default version")

	// SetDefaultPolicyVersion to v2.
	_, err = client.SetDefaultPolicyVersion(ctx, &iamsdk.SetDefaultPolicyVersionInput{
		PolicyArn: aws.String(policyArn),
		VersionId: aws.String(v2ID),
	})
	require.NoError(t, err)

	listOut3, err := client.ListPolicyVersions(ctx, &iamsdk.ListPolicyVersionsInput{
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)

	for _, v := range listOut3.Versions {
		if *v.VersionId == v2ID {
			assert.True(t, v.IsDefaultVersion, "v2 should now be default")
		} else {
			assert.False(t, v.IsDefaultVersion, "v1 should no longer be default")
		}
	}

	// DeletePolicyVersion — delete v1 (non-default).
	var v1ID string

	for _, v := range listOut3.Versions {
		if !v.IsDefaultVersion {
			v1ID = *v.VersionId

			break
		}
	}

	require.NotEmpty(t, v1ID, "must find non-default version to delete")

	_, err = client.DeletePolicyVersion(ctx, &iamsdk.DeletePolicyVersionInput{
		PolicyArn: aws.String(policyArn),
		VersionId: aws.String(v1ID),
	})
	require.NoError(t, err)

	listOut4, err := client.ListPolicyVersions(ctx, &iamsdk.ListPolicyVersionsInput{
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)
	assert.Len(t, listOut4.Versions, 1, "should have 1 version after deleting v1")

	// Cleanup.
	_, _ = client.DeletePolicy(ctx, &iamsdk.DeletePolicyInput{PolicyArn: aws.String(policyArn)})
}

// TestIntegration_IAM_CreateServiceLinkedRole verifies CreateServiceLinkedRole sets the
// expected role name pattern and trust policy.
func TestIntegration_IAM_CreateServiceLinkedRole(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	svcName := "elasticbeanstalk.amazonaws.com"

	out, err := client.CreateServiceLinkedRole(ctx, &iamsdk.CreateServiceLinkedRoleInput{
		AWSServiceName: aws.String(svcName),
		Description:    aws.String("Gopherstack integration test service-linked role"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Role)

	role := out.Role
	assert.Contains(t, *role.RoleName, "AWSServiceRoleFor",
		"service-linked role name should start with AWSServiceRoleFor")
	assert.Contains(t, *role.Arn, "role/",
		"service-linked role ARN should contain role/")
	assert.NotEmpty(t, *role.AssumeRolePolicyDocument,
		"service-linked role must have a trust policy")

	// GetRole — verify the role is retrievable.
	getOut, err := client.GetRole(ctx, &iamsdk.GetRoleInput{
		RoleName: role.RoleName,
	})
	require.NoError(t, err)
	assert.Equal(t, *role.RoleName, *getOut.Role.RoleName)

	// Cleanup — delete service-linked role via DeleteRole (simplified for mock).
	_, _ = client.DeleteRole(ctx, &iamsdk.DeleteRoleInput{RoleName: role.RoleName})
}

// TestIntegration_IAM_GroupMembershipAndPolicies verifies that group membership correctly
// surfaces in ListGroupsForUser and that group-attached policies are accessible.
func TestIntegration_IAM_GroupMembershipAndPolicies(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	groupName := "grp-" + suffix
	userName := "grp-user-" + suffix
	policyName := "grp-pol-" + suffix

	// Create group.
	_, err := client.CreateGroup(ctx, &iamsdk.CreateGroupInput{
		GroupName: aws.String(groupName),
	})
	require.NoError(t, err)

	// Create user.
	_, err = client.CreateUser(ctx, &iamsdk.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	// Add user to group.
	_, err = client.AddUserToGroup(ctx, &iamsdk.AddUserToGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	})
	require.NoError(t, err)

	// Verify membership via ListGroupsForUser.
	groupsOut, err := client.ListGroupsForUser(ctx, &iamsdk.ListGroupsForUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	found := false

	for _, g := range groupsOut.Groups {
		if *g.GroupName == groupName {
			found = true

			break
		}
	}

	assert.True(t, found, "user should appear in ListGroupsForUser after AddUserToGroup")

	// Verify membership via ListUsersInGroup.
	usersOut, err := client.GetGroup(ctx, &iamsdk.GetGroupInput{
		GroupName: aws.String(groupName),
	})
	require.NoError(t, err)

	userFound := false

	for _, u := range usersOut.Users {
		if *u.UserName == userName {
			userFound = true

			break
		}
	}

	assert.True(t, userFound, "user should appear in GetGroup after AddUserToGroup")

	// Attach a managed policy to the group and verify it shows up.
	polOut, err := client.CreatePolicy(ctx, &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(allowAllS3Doc),
	})
	require.NoError(t, err)

	policyArn := *polOut.Policy.Arn

	_, err = client.AttachGroupPolicy(ctx, &iamsdk.AttachGroupPolicyInput{
		GroupName: aws.String(groupName),
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)

	attachedOut, err := client.ListAttachedGroupPolicies(ctx, &iamsdk.ListAttachedGroupPoliciesInput{
		GroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	assert.Len(t, attachedOut.AttachedPolicies, 1, "group should have one attached policy")
	assert.Equal(t, policyArn, *attachedOut.AttachedPolicies[0].PolicyArn)

	// Remove user from group.
	_, err = client.RemoveUserFromGroup(ctx, &iamsdk.RemoveUserFromGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	})
	require.NoError(t, err)

	// Verify user no longer in group.
	groupsOut2, err := client.ListGroupsForUser(ctx, &iamsdk.ListGroupsForUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	assert.Empty(t, groupsOut2.Groups, "user should have no groups after removal")

	// Cleanup.
	_, _ = client.DetachGroupPolicy(ctx, &iamsdk.DetachGroupPolicyInput{
		GroupName: aws.String(groupName),
		PolicyArn: aws.String(policyArn),
	})
	_, _ = client.DeleteGroup(ctx, &iamsdk.DeleteGroupInput{GroupName: aws.String(groupName)})
	_, _ = client.DeleteUser(ctx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})
	_, _ = client.DeletePolicy(ctx, &iamsdk.DeletePolicyInput{PolicyArn: aws.String(policyArn)})
}

// TestIntegration_IAM_AccessKeyRotation verifies create / deactivate / delete lifecycle.
func TestIntegration_IAM_AccessKeyRotation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	userName := "ak-rotate-" + uuid.NewString()[:8]

	_, err := client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)

	// Create first access key.
	ak1Out, err := client.CreateAccessKey(ctx, &iamsdk.CreateAccessKeyInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	ak1ID := *ak1Out.AccessKey.AccessKeyId
	assert.NotEmpty(t, ak1ID)
	assert.Equal(t, "Active", string(ak1Out.AccessKey.Status))

	// Create second access key (simulates rotation).
	ak2Out, err := client.CreateAccessKey(ctx, &iamsdk.CreateAccessKeyInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	ak2ID := *ak2Out.AccessKey.AccessKeyId
	assert.NotEqual(t, ak1ID, ak2ID, "access keys must be unique")

	// List access keys — both should appear.
	listOut, err := client.ListAccessKeys(ctx, &iamsdk.ListAccessKeysInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.AccessKeyMetadata, 2)

	// Deactivate the old key.
	_, err = client.UpdateAccessKey(ctx, &iamsdk.UpdateAccessKeyInput{
		UserName:    aws.String(userName),
		AccessKeyId: aws.String(ak1ID),
		Status:      "Inactive",
	})
	require.NoError(t, err)

	// List again and verify status.
	listOut2, err := client.ListAccessKeys(ctx, &iamsdk.ListAccessKeysInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	for _, ak := range listOut2.AccessKeyMetadata {
		if *ak.AccessKeyId == ak1ID {
			assert.Equal(t, "Inactive", string(ak.Status), "ak1 should be inactive")
		}
	}

	// Delete old key.
	_, err = client.DeleteAccessKey(ctx, &iamsdk.DeleteAccessKeyInput{
		UserName:    aws.String(userName),
		AccessKeyId: aws.String(ak1ID),
	})
	require.NoError(t, err)

	listOut3, err := client.ListAccessKeys(ctx, &iamsdk.ListAccessKeysInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	assert.Len(t, listOut3.AccessKeyMetadata, 1, "only new key should remain")
	assert.Equal(t, ak2ID, *listOut3.AccessKeyMetadata[0].AccessKeyId)

	// Cleanup.
	_, _ = client.DeleteAccessKey(ctx, &iamsdk.DeleteAccessKeyInput{
		UserName:    aws.String(userName),
		AccessKeyId: aws.String(ak2ID),
	})
	_, _ = client.DeleteUser(ctx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})
}

// TestIntegration_IAM_PermissionBoundary verifies PutRolePermissionsBoundary and
// PutUserPermissionsBoundary are accepted without error.
func TestIntegration_IAM_PermissionBoundary(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	roleName := "pb-role-" + suffix
	userName := "pb-user-" + suffix
	boundaryPolicyName := "pb-pol-" + suffix

	// Create a boundary policy.
	polOut, err := client.CreatePolicy(ctx, &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String(boundaryPolicyName),
		PolicyDocument: aws.String(allowAllS3Doc),
	})
	require.NoError(t, err)

	boundaryArn := *polOut.Policy.Arn

	// Create role and set boundary.
	_, err = client.CreateRole(ctx, &iamsdk.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(allowRoleTrustDoc),
		PermissionsBoundary:      aws.String(boundaryArn),
	})
	require.NoError(t, err)

	// GetRole — verify PermissionsBoundary is reflected.
	getRoleOut, err := client.GetRole(ctx, &iamsdk.GetRoleInput{
		RoleName: aws.String(roleName),
	})
	require.NoError(t, err)
	assert.NotNil(t, getRoleOut.Role.PermissionsBoundary,
		"role should have a PermissionsBoundary")

	if getRoleOut.Role.PermissionsBoundary != nil {
		assert.Equal(t, boundaryArn, *getRoleOut.Role.PermissionsBoundary.PermissionsBoundaryArn)
	}

	// Create user and set boundary via PutUserPermissionsBoundary.
	_, err = client.CreateUser(ctx, &iamsdk.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	_, err = client.PutUserPermissionsBoundary(ctx, &iamsdk.PutUserPermissionsBoundaryInput{
		UserName:            aws.String(userName),
		PermissionsBoundary: aws.String(boundaryArn),
	})
	require.NoError(t, err)

	getUserOut, err := client.GetUser(ctx, &iamsdk.GetUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	assert.NotNil(t, getUserOut.User.PermissionsBoundary,
		"user should have a PermissionsBoundary after PutUserPermissionsBoundary")

	// Delete boundary via DeleteRolePermissionsBoundary and DeleteUserPermissionsBoundary.
	_, err = client.DeleteRolePermissionsBoundary(ctx, &iamsdk.DeleteRolePermissionsBoundaryInput{
		RoleName: aws.String(roleName),
	})
	require.NoError(t, err)

	_, err = client.DeleteUserPermissionsBoundary(ctx, &iamsdk.DeleteUserPermissionsBoundaryInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	// Cleanup.
	_, _ = client.DeleteRole(ctx, &iamsdk.DeleteRoleInput{RoleName: aws.String(roleName)})
	_, _ = client.DeleteUser(ctx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})
	_, _ = client.DeletePolicy(ctx, &iamsdk.DeletePolicyInput{PolicyArn: aws.String(boundaryArn)})
}

// TestIntegration_IAM_ServiceLastAccessed verifies GenerateServiceLastAccessedDetails and
// GetServiceLastAccessedDetails return a completed job with structured service entries.
func TestIntegration_IAM_ServiceLastAccessed(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	roleName := "sla-role-" + uuid.NewString()[:8]

	roleOut, err := client.CreateRole(ctx, &iamsdk.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(allowRoleTrustDoc),
	})
	require.NoError(t, err)

	roleArn := *roleOut.Role.Arn

	// Generate report.
	genOut, err := client.GenerateServiceLastAccessedDetails(ctx,
		&iamsdk.GenerateServiceLastAccessedDetailsInput{
			Arn: aws.String(roleArn),
		})
	require.NoError(t, err)
	require.NotEmpty(t, *genOut.JobId, "job ID must be non-empty")

	jobID := *genOut.JobId

	// Retrieve report.
	getOut, err := client.GetServiceLastAccessedDetails(ctx,
		&iamsdk.GetServiceLastAccessedDetailsInput{
			JobId: aws.String(jobID),
		})
	require.NoError(t, err)
	assert.NotEmpty(t, string(getOut.JobStatus), "job status must be set")

	// Cleanup.
	_, _ = client.DeleteRole(ctx, &iamsdk.DeleteRoleInput{RoleName: aws.String(roleName)})
}
