package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	amplifytypes "github.com/aws/aws-sdk-go-v2/service/amplify/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Amplify_AppAndBranchLifecycle exercises the core AWS
// Amplify workflow via the AWS SDK v2: create an app, list/get it, create a
// branch, list/get branches, then delete branch and app. Primary integration
// coverage for the Amplify REST handler (path-based dispatch).
func TestIntegration_Amplify_AppAndBranchLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createAmplifyClient(t)
	ctx := t.Context()

	const (
		appName    = "it-amplify-app"
		repository = "https://github.com/example/it-amplify"
		branchName = "main"
	)

	// CreateApp.
	createOut, err := client.CreateApp(ctx, &amplifysdk.CreateAppInput{
		Name:        aws.String(appName),
		Description: aws.String("integration test app"),
		Repository:  aws.String(repository),
		Platform:    amplifytypes.PlatformWeb,
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.App)
	appID := aws.ToString(createOut.App.AppId)
	require.NotEmpty(t, appID)
	assert.Equal(t, appName, aws.ToString(createOut.App.Name))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteApp(cleanupCtx, &amplifysdk.DeleteAppInput{AppId: aws.String(appID)})
	})

	// GetApp.
	getOut, err := client.GetApp(ctx, &amplifysdk.GetAppInput{AppId: aws.String(appID)})
	require.NoError(t, err)
	require.NotNil(t, getOut.App)
	assert.Equal(t, appName, aws.ToString(getOut.App.Name))

	// ListApps should contain the new app.
	listOut, err := client.ListApps(ctx, &amplifysdk.ListAppsInput{})
	require.NoError(t, err)

	foundApp := false

	for _, a := range listOut.Apps {
		if aws.ToString(a.AppId) == appID {
			foundApp = true

			break
		}
	}

	assert.True(t, foundApp, "newly created app should be listed")

	// CreateBranch on the new app.
	createBranchOut, err := client.CreateBranch(ctx, &amplifysdk.CreateBranchInput{
		AppId:           aws.String(appID),
		BranchName:      aws.String(branchName),
		Description:     aws.String("integration test branch"),
		Stage:           amplifytypes.StageProduction,
		EnableAutoBuild: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, createBranchOut.Branch)
	assert.Equal(t, branchName, aws.ToString(createBranchOut.Branch.BranchName))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteBranch(cleanupCtx, &amplifysdk.DeleteBranchInput{
			AppId:      aws.String(appID),
			BranchName: aws.String(branchName),
		})
	})

	// GetBranch.
	getBranchOut, err := client.GetBranch(ctx, &amplifysdk.GetBranchInput{
		AppId:      aws.String(appID),
		BranchName: aws.String(branchName),
	})
	require.NoError(t, err)
	require.NotNil(t, getBranchOut.Branch)
	assert.Equal(t, branchName, aws.ToString(getBranchOut.Branch.BranchName))

	// ListBranches should contain the new branch.
	listBranchesOut, err := client.ListBranches(ctx, &amplifysdk.ListBranchesInput{
		AppId: aws.String(appID),
	})
	require.NoError(t, err)

	foundBranch := false

	for _, b := range listBranchesOut.Branches {
		if aws.ToString(b.BranchName) == branchName {
			foundBranch = true

			break
		}
	}

	assert.True(t, foundBranch, "newly created branch should be listed")
}
