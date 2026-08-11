//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createAppConfigClient returns an AppConfig client pointed at the shared test container.
func createAppConfigClient(t *testing.T) *appconfigsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return appconfigsdk.NewFromConfig(cfg, func(o *appconfigsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_AppConfig_ApplicationLifecycle tests application CRUD.
func TestIntegration_AppConfig_ApplicationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name    string
		appName string
	}{
		{
			name:    "full_lifecycle",
			appName: "integ-app-" + uuid.NewString()[:8],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAppConfigClient(t)

			// Create application.
			createOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
				Name: aws.String(tt.appName),
			})
			require.NoError(t, err, "CreateApplication should succeed")
			require.NotEmpty(t, aws.ToString(createOut.Id))
			appID := aws.ToString(createOut.Id)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteApplication(cleanupCtx, &appconfigsdk.DeleteApplicationInput{
					ApplicationId: aws.String(appID),
				})
			})

			// Get application.
			getOut, err := client.GetApplication(ctx, &appconfigsdk.GetApplicationInput{
				ApplicationId: aws.String(appID),
			})
			require.NoError(t, err, "GetApplication should succeed")
			assert.Equal(t, tt.appName, aws.ToString(getOut.Name))

			// List applications.
			listOut, err := client.ListApplications(ctx, &appconfigsdk.ListApplicationsInput{})
			require.NoError(t, err, "ListApplications should succeed")

			found := false
			for _, a := range listOut.Items {
				if aws.ToString(a.Id) == appID {
					found = true

					break
				}
			}

			assert.True(t, found, "created application should appear in list")

			// Delete application.
			_, err = client.DeleteApplication(ctx, &appconfigsdk.DeleteApplicationInput{
				ApplicationId: aws.String(appID),
			})
			require.NoError(t, err, "DeleteApplication should succeed")
		})
	}
}

// TestIntegration_AppConfig_EnvironmentLifecycle tests environment CRUD under an application.
func TestIntegration_AppConfig_EnvironmentLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	t.Run("get_nonexistent_app", func(t *testing.T) {
		t.Parallel()
		client := createAppConfigClient(t)
		_, err := client.GetApplication(t.Context(), &appconfigsdk.GetApplicationInput{
			ApplicationId: aws.String("nonexistent-app-id"),
		})
		assert.Error(t, err, "GetApplication for non-existent ID should return error")
	})

	tests := []struct {
		name    string
		envName string
	}{
		{
			name:    "full_lifecycle",
			envName: "integ-env-" + uuid.NewString()[:8],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createAppConfigClient(t)

			// Create parent application.
			appOut, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
				Name: aws.String("integ-parent-app-" + uuid.NewString()[:8]),
			})
			require.NoError(t, err, "CreateApplication should succeed")
			appID := aws.ToString(appOut.Id)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteApplication(cleanupCtx, &appconfigsdk.DeleteApplicationInput{
					ApplicationId: aws.String(appID),
				})
			})

			// Create environment.
			envOut, err := client.CreateEnvironment(ctx, &appconfigsdk.CreateEnvironmentInput{
				ApplicationId: aws.String(appID),
				Name:          aws.String(tt.envName),
			})
			require.NoError(t, err, "CreateEnvironment should succeed")
			require.NotEmpty(t, aws.ToString(envOut.Id))
			envID := aws.ToString(envOut.Id)

			// Get environment.
			getOut, err := client.GetEnvironment(ctx, &appconfigsdk.GetEnvironmentInput{
				ApplicationId: aws.String(appID),
				EnvironmentId: aws.String(envID),
			})
			require.NoError(t, err, "GetEnvironment should succeed")
			assert.Equal(t, tt.envName, aws.ToString(getOut.Name))

			// List environments.
			listOut, err := client.ListEnvironments(ctx, &appconfigsdk.ListEnvironmentsInput{
				ApplicationId: aws.String(appID),
			})
			require.NoError(t, err, "ListEnvironments should succeed")

			found := false
			for _, e := range listOut.Items {
				if aws.ToString(e.Id) == envID {
					found = true

					break
				}
			}

			assert.True(t, found, "created environment should appear in list")

			// Delete environment.
			_, err = client.DeleteEnvironment(ctx, &appconfigsdk.DeleteEnvironmentInput{
				ApplicationId: aws.String(appID),
				EnvironmentId: aws.String(envID),
			})
			require.NoError(t, err, "DeleteEnvironment should succeed")

			// List environments after delete.
			listOut2, err := client.ListEnvironments(ctx, &appconfigsdk.ListEnvironmentsInput{
				ApplicationId: aws.String(appID),
			})
			require.NoError(t, err, "ListEnvironments after delete should succeed")

			for _, e := range listOut2.Items {
				assert.NotEqual(t, envID, aws.ToString(e.Id), "deleted environment should not appear in list")
			}
		})
	}
}
