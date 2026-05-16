package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	codedeploytypes "github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CodeDeploy_ApplicationAndDeploymentGroupLifecycle exercises
// the core CodeDeploy workflow via the AWS SDK v2: create application,
// list/get application, create a deployment group on it, get the deployment
// group, then delete. Primary integration coverage for the CodeDeploy
// JSON-RPC handler.
func TestIntegration_CodeDeploy_ApplicationAndDeploymentGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeDeployClient(t)
	ctx := t.Context()

	const (
		appName        = "it-codedeploy-app"
		groupName      = "it-codedeploy-group"
		serviceRoleArn = "arn:aws:iam::000000000000:role/CodeDeployServiceRole"
		deployConfig   = "CodeDeployDefault.OneAtATime"
	)

	// CreateApplication.
	createAppOut, err := client.CreateApplication(ctx, &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String(appName),
		ComputePlatform: codedeploytypes.ComputePlatformServer,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(createAppOut.ApplicationId))

	t.Cleanup(func() {
		_, _ = client.DeleteApplication(ctx, &codedeploysdk.DeleteApplicationInput{
			ApplicationName: aws.String(appName),
		})
	})

	// GetApplication.
	getAppOut, err := client.GetApplication(ctx, &codedeploysdk.GetApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err)
	assert.Equal(t, appName, aws.ToString(getAppOut.Application.ApplicationName))
	assert.Equal(t, codedeploytypes.ComputePlatformServer, getAppOut.Application.ComputePlatform)

	// ListApplications.
	listAppOut, err := client.ListApplications(ctx, &codedeploysdk.ListApplicationsInput{})
	require.NoError(t, err)
	assert.Contains(t, listAppOut.Applications, appName)

	// CreateDeploymentGroup.
	createDGOut, err := client.CreateDeploymentGroup(ctx, &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:      aws.String(appName),
		DeploymentGroupName:  aws.String(groupName),
		ServiceRoleArn:       aws.String(serviceRoleArn),
		DeploymentConfigName: aws.String(deployConfig),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(createDGOut.DeploymentGroupId))

	// GetDeploymentGroup.
	getDGOut, err := client.GetDeploymentGroup(ctx, &codedeploysdk.GetDeploymentGroupInput{
		ApplicationName:     aws.String(appName),
		DeploymentGroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	require.NotNil(t, getDGOut.DeploymentGroupInfo)
	assert.Equal(t, appName, aws.ToString(getDGOut.DeploymentGroupInfo.ApplicationName))
	assert.Equal(t, groupName, aws.ToString(getDGOut.DeploymentGroupInfo.DeploymentGroupName))
	assert.Equal(t, serviceRoleArn, aws.ToString(getDGOut.DeploymentGroupInfo.ServiceRoleArn))
	assert.Equal(t, deployConfig, aws.ToString(getDGOut.DeploymentGroupInfo.DeploymentConfigName))
}
