//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediastoreSDK "github.com/aws/aws-sdk-go-v2/service/mediastore"
	mediastoretypes "github.com/aws/aws-sdk-go-v2/service/mediastore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMediaStoreClient returns a MediaStore client pointed at the shared test container.
func createMediaStoreClient(t *testing.T) *mediastoreSDK.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mediastoreSDK.NewFromConfig(cfg, func(o *mediastoreSDK.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MediaStore_ContainerLifecycle tests container creation and management.
func TestIntegration_MediaStore_ContainerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		containerName string
	}{
		{
			name:          "full_lifecycle",
			containerName: "integration-test-container",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaStoreClient(t)

			uniqueSuffix := t.Name()
			containerName := tt.containerName + "-" + uniqueSuffix

			// CreateContainer.
			createOut, err := client.CreateContainer(ctx, &mediastoreSDK.CreateContainerInput{
				ContainerName: aws.String(containerName),
			})
			require.NoError(t, err, "CreateContainer should succeed")
			require.NotNil(t, createOut.Container)
			assert.Equal(t, containerName, aws.ToString(createOut.Container.Name))
			assert.NotEmpty(t, aws.ToString(createOut.Container.ARN))

			containerARN := aws.ToString(createOut.Container.ARN)

			// DescribeContainer.
			descOut, err := client.DescribeContainer(ctx, &mediastoreSDK.DescribeContainerInput{
				ContainerName: aws.String(containerName),
			})
			require.NoError(t, err, "DescribeContainer should succeed")
			require.NotNil(t, descOut.Container)
			assert.Equal(t, containerName, aws.ToString(descOut.Container.Name))

			// ListContainers — should contain the created one.
			listOut, err := client.ListContainers(ctx, &mediastoreSDK.ListContainersInput{})
			require.NoError(t, err, "ListContainers should succeed")

			found := false

			for _, c := range listOut.Containers {
				if aws.ToString(c.ARN) == containerARN {
					found = true

					break
				}
			}

			assert.True(t, found, "created container should appear in list")

			// TagResource.
			_, err = client.TagResource(ctx, &mediastoreSDK.TagResourceInput{
				Resource: aws.String(containerARN),
				Tags: []mediastoretypes.Tag{
					{Key: aws.String("env"), Value: aws.String("integration")},
				},
			})
			require.NoError(t, err, "TagResource should succeed")

			// ListTagsForResource.
			tagsOut, err := client.ListTagsForResource(ctx, &mediastoreSDK.ListTagsForResourceInput{
				Resource: aws.String(containerARN),
			})
			require.NoError(t, err, "ListTagsForResource should succeed")
			assert.Len(t, tagsOut.Tags, 1)
			assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))
			assert.Equal(t, "integration", aws.ToString(tagsOut.Tags[0].Value))

			// UntagResource.
			_, err = client.UntagResource(ctx, &mediastoreSDK.UntagResourceInput{
				Resource: aws.String(containerARN),
				TagKeys:  []string{"env"},
			})
			require.NoError(t, err, "UntagResource should succeed")

			// DeleteContainer.
			_, err = client.DeleteContainer(ctx, &mediastoreSDK.DeleteContainerInput{
				ContainerName: aws.String(containerName),
			})
			require.NoError(t, err, "DeleteContainer should succeed")

			// Verify deletion via DescribeContainer → should return 404.
			_, err = client.DescribeContainer(ctx, &mediastoreSDK.DescribeContainerInput{
				ContainerName: aws.String(containerName),
			})
			require.Error(t, err, "DescribeContainer after deletion should fail")
		})
	}
}
