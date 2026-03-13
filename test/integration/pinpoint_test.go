//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pinpointSDK "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	pinpointSDKtypes "github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createPinpointClient returns a Pinpoint client pointed at the shared test container.
func createPinpointClient(t *testing.T) *pinpointSDK.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return pinpointSDK.NewFromConfig(cfg, func(o *pinpointSDK.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Pinpoint_AppLifecycle tests app creation, retrieval, listing, tagging, and deletion.
func TestIntegration_Pinpoint_AppLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
	}{
		{
			name:    "full_lifecycle",
			appName: "int-test-pinpoint-app",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createPinpointClient(t)

			uniqueName := tt.appName + "-" + t.Name()

			// CreateApp.
			createOut, err := client.CreateApp(ctx, &pinpointSDK.CreateAppInput{
				CreateApplicationRequest: &pinpointSDKtypes.CreateApplicationRequest{
					Name: aws.String(uniqueName),
					Tags: map[string]string{"owner": "integration-test"},
				},
			})
			require.NoError(t, err, "CreateApp should succeed")
			require.NotNil(t, createOut.ApplicationResponse)
			assert.NotEmpty(t, aws.ToString(createOut.ApplicationResponse.Id))
			assert.Equal(t, uniqueName, aws.ToString(createOut.ApplicationResponse.Name))

			appID := aws.ToString(createOut.ApplicationResponse.Id)
			appARN := aws.ToString(createOut.ApplicationResponse.Arn)

			t.Cleanup(func() {
				_, _ = client.DeleteApp(ctx, &pinpointSDK.DeleteAppInput{
					ApplicationId: aws.String(appID),
				})
			})

			// GetApp.
			getOut, err := client.GetApp(ctx, &pinpointSDK.GetAppInput{
				ApplicationId: aws.String(appID),
			})
			require.NoError(t, err, "GetApp should succeed")
			require.NotNil(t, getOut.ApplicationResponse)
			assert.Equal(t, uniqueName, aws.ToString(getOut.ApplicationResponse.Name))
			assert.Equal(t, appID, aws.ToString(getOut.ApplicationResponse.Id))

			// GetApps (listing).
			listOut, err := client.GetApps(ctx, &pinpointSDK.GetAppsInput{})
			require.NoError(t, err, "GetApps should succeed")

			found := false

			for _, item := range listOut.ApplicationsResponse.Item {
				if aws.ToString(item.Id) == appID {
					found = true

					break
				}
			}

			assert.True(t, found, "app should appear in GetApps")

			// TagResource.
			_, err = client.TagResource(ctx, &pinpointSDK.TagResourceInput{
				ResourceArn: aws.String(appARN),
				TagsModel: &pinpointSDKtypes.TagsModel{
					Tags: map[string]string{"team": "platform"},
				},
			})
			require.NoError(t, err, "TagResource should succeed")

			// ListTagsForResource.
			tagsOut, err := client.ListTagsForResource(ctx, &pinpointSDK.ListTagsForResourceInput{
				ResourceArn: aws.String(appARN),
			})
			require.NoError(t, err, "ListTagsForResource should succeed")
			require.NotNil(t, tagsOut.TagsModel)
			assert.Equal(t, "platform", tagsOut.TagsModel.Tags["team"])

			// UntagResource.
			_, err = client.UntagResource(ctx, &pinpointSDK.UntagResourceInput{
				ResourceArn: aws.String(appARN),
				TagKeys:     []string{"team"},
			})
			require.NoError(t, err, "UntagResource should succeed")

			// DeleteApp.
			deleteOut, err := client.DeleteApp(ctx, &pinpointSDK.DeleteAppInput{
				ApplicationId: aws.String(appID),
			})
			require.NoError(t, err, "DeleteApp should succeed")
			require.NotNil(t, deleteOut.ApplicationResponse)
			assert.Equal(t, appID, aws.ToString(deleteOut.ApplicationResponse.Id))

			// Verify deleted.
			_, err = client.GetApp(ctx, &pinpointSDK.GetAppInput{
				ApplicationId: aws.String(appID),
			})
			require.Error(t, err, "GetApp should fail after deletion")
		})
	}
}
