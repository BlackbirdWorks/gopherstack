package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssoadminsdk "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	ssoadmintypes "github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SSOAdmin_InstanceAndPermissionSet exercises the SSO
// Admin control-plane workflow via the AWS SDK v2: create an instance,
// list it, create a permission set, list permission sets, then clean up.
func TestIntegration_SSOAdmin_InstanceAndPermissionSet(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSSOAdminClient(t)
	ctx := t.Context()

	// Create instance.
	createInst, err := client.CreateInstance(ctx, &ssoadminsdk.CreateInstanceInput{
		Name: aws.String("it-ssoadmin-instance"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createInst.InstanceArn))
	instArn := aws.ToString(createInst.InstanceArn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteInstance(cleanupCtx, &ssoadminsdk.DeleteInstanceInput{
			InstanceArn: aws.String(instArn),
		})
	})

	// List instances should include the new one.
	listInst, err := client.ListInstances(ctx, &ssoadminsdk.ListInstancesInput{})
	require.NoError(t, err)

	foundInst := false

	for _, i := range listInst.Instances {
		if aws.ToString(i.InstanceArn) == instArn {
			foundInst = true

			break
		}
	}

	assert.True(t, foundInst, "newly created SSO instance should be listed")

	// Create permission set.
	createPS, err := client.CreatePermissionSet(ctx, &ssoadminsdk.CreatePermissionSetInput{
		InstanceArn: aws.String(instArn),
		Name:        aws.String("it-ssoadmin-ps"),
	})
	require.NoError(t, err)
	require.NotNil(t, createPS.PermissionSet)
	psArn := aws.ToString(createPS.PermissionSet.PermissionSetArn)
	require.NotEmpty(t, psArn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeletePermissionSet(cleanupCtx, &ssoadminsdk.DeletePermissionSetInput{
			InstanceArn:      aws.String(instArn),
			PermissionSetArn: aws.String(psArn),
		})
	})

	// List permission sets.
	listPS, err := client.ListPermissionSets(ctx, &ssoadminsdk.ListPermissionSetsInput{
		InstanceArn: aws.String(instArn),
	})
	require.NoError(t, err)
	assert.Contains(t, listPS.PermissionSets, psArn)
}

// TestIntegration_SSOAdmin_ProvisioningStatusFilter exercises the
// ProvisioningStatus filter on ListPermissionSetsProvisionedToAccount via
// the real AWS SDK v2 client: CreateAccountAssignment provisions implicitly,
// editing the permission set makes the account's copy stale
// (LATEST_PERMISSION_SET_NOT_PROVISIONED), and ProvisionPermissionSet brings
// it current again (LATEST_PERMISSION_SET_PROVISIONED).
func TestIntegration_SSOAdmin_ProvisioningStatusFilter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSSOAdminClient(t)
	ctx := t.Context()

	createInst, err := client.CreateInstance(ctx, &ssoadminsdk.CreateInstanceInput{
		Name: aws.String("it-ssoadmin-provstatus-instance"),
	})
	require.NoError(t, err)
	instArn := aws.ToString(createInst.InstanceArn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteInstance(
			cleanupCtx,
			&ssoadminsdk.DeleteInstanceInput{InstanceArn: aws.String(instArn)},
		)
	})

	createPS, err := client.CreatePermissionSet(ctx, &ssoadminsdk.CreatePermissionSetInput{
		InstanceArn: aws.String(instArn),
		Name:        aws.String("it-ssoadmin-provstatus-ps"),
	})
	require.NoError(t, err)
	psArn := aws.ToString(createPS.PermissionSet.PermissionSetArn)

	const accountID = "555555555555"

	_, err = client.CreateAccountAssignment(ctx, &ssoadminsdk.CreateAccountAssignmentInput{
		InstanceArn:      aws.String(instArn),
		PermissionSetArn: aws.String(psArn),
		TargetId:         aws.String(accountID),
		TargetType:       ssoadmintypes.TargetTypeAwsAccount,
		PrincipalId:      aws.String("11111111-1111-1111-1111-111111111111"),
		PrincipalType:    ssoadmintypes.PrincipalTypeUser,
	})
	require.NoError(t, err)

	listProvisioned := func() []string {
		out, listErr := client.ListPermissionSetsProvisionedToAccount(
			ctx,
			&ssoadminsdk.ListPermissionSetsProvisionedToAccountInput{
				InstanceArn:        aws.String(instArn),
				AccountId:          aws.String(accountID),
				ProvisioningStatus: ssoadmintypes.ProvisioningStatusLatestPermissionSetProvisioned,
			},
		)
		require.NoError(t, listErr)

		return out.PermissionSets
	}
	listNotProvisioned := func() []string {
		out, listErr := client.ListPermissionSetsProvisionedToAccount(
			ctx,
			&ssoadminsdk.ListPermissionSetsProvisionedToAccountInput{
				InstanceArn:        aws.String(instArn),
				AccountId:          aws.String(accountID),
				ProvisioningStatus: ssoadmintypes.ProvisioningStatusLatestPermissionSetNotProvisioned,
			},
		)
		require.NoError(t, listErr)

		return out.PermissionSets
	}

	assert.Contains(t, listProvisioned(), psArn, "CreateAccountAssignment provisions implicitly")
	assert.NotContains(t, listNotProvisioned(), psArn)

	_, err = client.UpdatePermissionSet(ctx, &ssoadminsdk.UpdatePermissionSetInput{
		InstanceArn:      aws.String(instArn),
		PermissionSetArn: aws.String(psArn),
		Description:      aws.String("edited via integration test"),
	})
	require.NoError(t, err)

	assert.NotContains(
		t,
		listProvisioned(),
		psArn,
		"editing the permission set makes the account's copy stale",
	)
	assert.Contains(t, listNotProvisioned(), psArn)

	_, err = client.ProvisionPermissionSet(ctx, &ssoadminsdk.ProvisionPermissionSetInput{
		InstanceArn:      aws.String(instArn),
		PermissionSetArn: aws.String(psArn),
		TargetId:         aws.String(accountID),
		TargetType:       ssoadmintypes.ProvisionTargetTypeAwsAccount,
	})
	require.NoError(t, err)

	assert.Contains(t, listProvisioned(), psArn, "re-provisioning brings it current again")
	assert.NotContains(t, listNotProvisioned(), psArn)
}
