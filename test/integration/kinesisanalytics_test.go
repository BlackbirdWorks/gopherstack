//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kinesisanalyticssdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics" //nolint:staticcheck // Kinesis Analytics v1 SDK is deprecated but still in use
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createKinesisAnalyticsClient returns a Kinesis Analytics client pointed at the shared test container.
//
//nolint:staticcheck // Kinesis Analytics v1 SDK is deprecated but still in use
func createKinesisAnalyticsClient(t *testing.T) *kinesisanalyticssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return kinesisanalyticssdk.NewFromConfig(cfg, func(o *kinesisanalyticssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_KinesisAnalytics_ApplicationLifecycle tests the full application CRUD lifecycle.
//
//nolint:staticcheck // Kinesis Analytics v1 SDK is deprecated but still in use
func TestIntegration_KinesisAnalytics_ApplicationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
	}{
		{
			name:    "full_lifecycle",
			appName: "integration-test-app",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createKinesisAnalyticsClient(t)
			appName := tt.appName + "-" + t.Name()

			// Create application.
			createOut, err := client.CreateApplication(ctx, &kinesisanalyticssdk.CreateApplicationInput{
				ApplicationName: aws.String(appName),
			})
			require.NoError(t, err, "CreateApplication should succeed")
			require.NotNil(t, createOut.ApplicationSummary)
			assert.Equal(t, appName, aws.ToString(createOut.ApplicationSummary.ApplicationName))

			// Describe application.
			descOut, err := client.DescribeApplication(ctx, &kinesisanalyticssdk.DescribeApplicationInput{
				ApplicationName: aws.String(appName),
			})
			require.NoError(t, err, "DescribeApplication should succeed")
			require.NotNil(t, descOut.ApplicationDetail)
			assert.Equal(t, appName, aws.ToString(descOut.ApplicationDetail.ApplicationName))

			// List applications — should contain the created one.
			listOut, err := client.ListApplications(ctx, &kinesisanalyticssdk.ListApplicationsInput{})
			require.NoError(t, err, "ListApplications should succeed")

			found := false

			for _, a := range listOut.ApplicationSummaries {
				if aws.ToString(a.ApplicationName) == appName {
					found = true

					break
				}
			}

			assert.True(t, found, "created application should appear in list")

			// Delete application.
			_, err = client.DeleteApplication(ctx, &kinesisanalyticssdk.DeleteApplicationInput{
				ApplicationName: aws.String(appName),
				CreateTimestamp: descOut.ApplicationDetail.CreateTimestamp,
			})
			require.NoError(t, err, "DeleteApplication should succeed")

			// Verify deletion.
			listAfter, err := client.ListApplications(ctx, &kinesisanalyticssdk.ListApplicationsInput{})
			require.NoError(t, err, "ListApplications should succeed after deletion")

			foundAfter := false

			for _, a := range listAfter.ApplicationSummaries {
				if aws.ToString(a.ApplicationName) == appName {
					foundAfter = true

					break
				}
			}

			assert.False(t, foundAfter, "deleted application should not appear in list")
		})
	}
}
