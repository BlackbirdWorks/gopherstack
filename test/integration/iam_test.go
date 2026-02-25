package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_IAM_UserAndRole(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	userName := "test-user-" + uuid.NewString()[:8]

	// CreateUser
	_, err := client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)

	// GetUser
	getOut, err := client.GetUser(ctx, &iamsdk.GetUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)
	assert.Equal(t, userName, *getOut.User.UserName)

	// ListUsers
	listOut, err := client.ListUsers(ctx, &iamsdk.ListUsersInput{})
	require.NoError(t, err)
	found := false
	for _, u := range listOut.Users {
		if *u.UserName == userName {
			found = true
			break
		}
	}
	assert.True(t, found, "created user should appear in ListUsers")

	// DeleteUser
	_, err = client.DeleteUser(ctx, &iamsdk.DeleteUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)
}

func TestIntegration_IAM_RoleAndPolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createIAMClient(t)
	ctx := t.Context()

	roleName := "test-role-" + uuid.NewString()[:8]
	policyName := "test-policy-" + uuid.NewString()[:8]
	assumeRoleDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

	// CreateRole
	roleOut, err := client.CreateRole(ctx, &iamsdk.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(assumeRoleDoc),
	})
	require.NoError(t, err)
	roleArn := *roleOut.Role.Arn
	assert.Contains(t, roleArn, roleName)

	// CreatePolicy
	policyOut, err := client.CreatePolicy(ctx, &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err)
	policyArn := *policyOut.Policy.Arn

	// AttachRolePolicy
	_, err = client.AttachRolePolicy(ctx, &iamsdk.AttachRolePolicyInput{
		RoleName:  aws.String(roleName),
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)

	// ListAttachedRolePolicies
	attachedOut, err := client.ListAttachedRolePolicies(ctx, &iamsdk.ListAttachedRolePoliciesInput{RoleName: aws.String(roleName)})
	require.NoError(t, err)
	assert.Len(t, attachedOut.AttachedPolicies, 1)

	// DetachRolePolicy
	_, err = client.DetachRolePolicy(ctx, &iamsdk.DetachRolePolicyInput{
		RoleName:  aws.String(roleName),
		PolicyArn: aws.String(policyArn),
	})
	require.NoError(t, err)

	// DeleteRole
	_, err = client.DeleteRole(ctx, &iamsdk.DeleteRoleInput{RoleName: aws.String(roleName)})
	require.NoError(t, err)

	// DeletePolicy
	_, err = client.DeletePolicy(ctx, &iamsdk.DeletePolicyInput{PolicyArn: aws.String(policyArn)})
	require.NoError(t, err)
}
