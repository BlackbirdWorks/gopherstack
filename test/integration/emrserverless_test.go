package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_EMRServerless_ApplicationLifecycle exercises the core
// EMR Serverless control-plane workflow via the AWS SDK v2: create an
// application, get/list it, then delete. Primary integration coverage for
// the EMR Serverless REST handler.
func TestIntegration_EMRServerless_ApplicationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEMRServerlessClient(t)
	ctx := t.Context()

	const (
		appName      = "it-emrserverless-app"
		appType      = "SPARK"
		releaseLabel = "emr-6.9.0"
	)

	// CreateApplication.
	createOut, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
		Name:         aws.String(appName),
		Type:         aws.String(appType),
		ReleaseLabel: aws.String(releaseLabel),
	})
	require.NoError(t, err)
	appID := aws.ToString(createOut.ApplicationId)
	require.NotEmpty(t, appID)
	assert.Equal(t, appName, aws.ToString(createOut.Name))

	t.Cleanup(func() {
		_, _ = client.DeleteApplication(ctx, &emrserverlesssdk.DeleteApplicationInput{
			ApplicationId: aws.String(appID),
		})
	})

	// GetApplication.
	getOut, err := client.GetApplication(ctx, &emrserverlesssdk.GetApplicationInput{
		ApplicationId: aws.String(appID),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Application)
	assert.Equal(t, appName, aws.ToString(getOut.Application.Name))
	assert.Equal(t, appType, aws.ToString(getOut.Application.Type))

	// ListApplications should contain the new application.
	listOut, err := client.ListApplications(ctx, &emrserverlesssdk.ListApplicationsInput{})
	require.NoError(t, err)

	foundApp := false

	for _, a := range listOut.Applications {
		if aws.ToString(a.Id) == appID {
			foundApp = true

			break
		}
	}

	assert.True(t, foundApp, "newly created application should be listed")
}
