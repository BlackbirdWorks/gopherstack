package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dssdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	dstypes "github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createDirectoryServiceClient returns a Directory Service client pointed at the shared test container.
func createDirectoryServiceClient(t *testing.T) *dssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return dssdk.NewFromConfig(cfg, func(o *dssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_DirectoryService_DirectoryLifecycle drives create→describe→delete of a
// SimpleAD directory.
func TestIntegration_DirectoryService_DirectoryLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name    string
		dirName string
		size    dstypes.DirectorySize
	}{
		{name: "small_simplead", dirName: "corp.integ.example.com", size: dstypes.DirectorySizeSmall},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDirectoryServiceClient(t)

			createOut, err := client.CreateDirectory(ctx, &dssdk.CreateDirectoryInput{
				Name:     aws.String(tt.dirName),
				Password: aws.String("P@ssw0rd123!"),
				Size:     tt.size,
			})
			require.NoError(t, err, "CreateDirectory should succeed")
			dirID := aws.ToString(createOut.DirectoryId)
			require.NotEmpty(t, dirID, "directory id must be returned")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDirectory(cleanupCtx, &dssdk.DeleteDirectoryInput{DirectoryId: aws.String(dirID)})
			})

			descOut, err := client.DescribeDirectories(ctx, &dssdk.DescribeDirectoriesInput{
				DirectoryIds: []string{dirID},
			})
			require.NoError(t, err, "DescribeDirectories should succeed")
			require.Len(t, descOut.DirectoryDescriptions, 1)
			assert.Equal(t, tt.dirName, aws.ToString(descOut.DirectoryDescriptions[0].Name))

			_, err = client.DeleteDirectory(ctx, &dssdk.DeleteDirectoryInput{DirectoryId: aws.String(dirID)})
			require.NoError(t, err, "DeleteDirectory should succeed")
		})
	}
}
