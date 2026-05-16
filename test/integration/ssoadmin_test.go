package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssoadminsdk "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
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
		_, _ = client.DeleteInstance(ctx, &ssoadminsdk.DeleteInstanceInput{
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
		_, _ = client.DeletePermissionSet(ctx, &ssoadminsdk.DeletePermissionSetInput{
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
