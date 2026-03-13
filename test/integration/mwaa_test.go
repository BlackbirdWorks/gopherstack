//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mwaaSDK "github.com/aws/aws-sdk-go-v2/service/mwaa"
	mwaaSDKtypes "github.com/aws/aws-sdk-go-v2/service/mwaa/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMWAAClient returns an MWAA client pointed at the shared test container.
func createMWAAClient(t *testing.T) *mwaaSDK.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mwaaSDK.NewFromConfig(cfg, func(o *mwaaSDK.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MWAA_EnvironmentLifecycle tests environment creation, retrieval, and deletion.
func TestIntegration_MWAA_EnvironmentLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
	}{
		{
			name:    "full_lifecycle",
			envName: "int-test-mwaa-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMWAAClient(t)

			uniqueName := tt.envName + "-" + t.Name()

			// CreateEnvironment.
			createOut, err := client.CreateEnvironment(ctx, &mwaaSDK.CreateEnvironmentInput{
				Name:             aws.String(uniqueName),
				DagS3Path:        aws.String("dags/"),
				ExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/mwaa-role"),
				SourceBucketArn:  aws.String("arn:aws:s3:::my-mwaa-bucket"),
				NetworkConfiguration: &mwaaSDKtypes.NetworkConfiguration{
					SecurityGroupIds: []string{"sg-12345678"},
					SubnetIds:        []string{"subnet-12345678", "subnet-87654321"},
				},
			})
			require.NoError(t, err, "CreateEnvironment should succeed")
			require.NotNil(t, createOut.Arn)
			assert.NotEmpty(t, aws.ToString(createOut.Arn))

			envARN := aws.ToString(createOut.Arn)

			t.Cleanup(func() {
				_, _ = client.DeleteEnvironment(ctx, &mwaaSDK.DeleteEnvironmentInput{
					Name: aws.String(uniqueName),
				})
			})

			// GetEnvironment.
			getOut, err := client.GetEnvironment(ctx, &mwaaSDK.GetEnvironmentInput{
				Name: aws.String(uniqueName),
			})
			require.NoError(t, err, "GetEnvironment should succeed")
			require.NotNil(t, getOut.Environment)
			assert.Equal(t, uniqueName, aws.ToString(getOut.Environment.Name))
			assert.Equal(t, "AVAILABLE", string(getOut.Environment.Status))

			// ListEnvironments.
			listOut, err := client.ListEnvironments(ctx, &mwaaSDK.ListEnvironmentsInput{})
			require.NoError(t, err, "ListEnvironments should succeed")

			found := false
			for _, name := range listOut.Environments {
				if name == uniqueName {
					found = true

					break
				}
			}
			assert.True(t, found, "environment should appear in ListEnvironments")

			// TagResource.
			_, err = client.TagResource(ctx, &mwaaSDK.TagResourceInput{
				ResourceArn: aws.String(envARN),
				Tags:        map[string]string{"owner": "integration-test"},
			})
			require.NoError(t, err, "TagResource should succeed")

			// ListTagsForResource.
			tagsOut, err := client.ListTagsForResource(ctx, &mwaaSDK.ListTagsForResourceInput{
				ResourceArn: aws.String(envARN),
			})
			require.NoError(t, err, "ListTagsForResource should succeed")
			assert.Equal(t, "integration-test", tagsOut.Tags["owner"])

			// UntagResource.
			_, err = client.UntagResource(ctx, &mwaaSDK.UntagResourceInput{
				ResourceArn: aws.String(envARN),
				TagKeys:     []string{"owner"},
			})
			require.NoError(t, err, "UntagResource should succeed")

			// DeleteEnvironment.
			_, err = client.DeleteEnvironment(ctx, &mwaaSDK.DeleteEnvironmentInput{
				Name: aws.String(uniqueName),
			})
			require.NoError(t, err, "DeleteEnvironment should succeed")
		})
	}
}
