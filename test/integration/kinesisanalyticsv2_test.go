package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kinesisanalyticsv2svc "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	kinesisanalyticsv2types "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createKinesisAnalyticsV2SDKClient returns a Kinesis Data Analytics v2 client pointed at the shared test container.
func createKinesisAnalyticsV2SDKClient(t *testing.T) *kinesisanalyticsv2svc.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return kinesisanalyticsv2svc.NewFromConfig(cfg, func(o *kinesisanalyticsv2svc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_KinesisAnalyticsV2_ApplicationLifecycle tests the full application CRUD lifecycle via the SDK.
func TestIntegration_KinesisAnalyticsV2_ApplicationLifecycle(t *testing.T) {
	t.Parallel()

	client := createKinesisAnalyticsV2SDKClient(t)
	appName := "integration-kav2-lifecycle-" + t.Name()

	// Create application.
	createOut, err := client.CreateApplication(t.Context(), &kinesisanalyticsv2svc.CreateApplicationInput{
		ApplicationName:      aws.String(appName),
		RuntimeEnvironment:   "FLINK-1_18",
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/service-role"),
		Tags:                 []kinesisanalyticsv2types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err, "CreateApplication should succeed")
	assert.NotEmpty(t, aws.ToString(createOut.ApplicationDetail.ApplicationARN))
	assert.Equal(t, appName, aws.ToString(createOut.ApplicationDetail.ApplicationName))
	assert.Equal(t, "READY", string(createOut.ApplicationDetail.ApplicationStatus))

	appARN := aws.ToString(createOut.ApplicationDetail.ApplicationARN)

	// Describe application.
	descOut, err := client.DescribeApplication(t.Context(), &kinesisanalyticsv2svc.DescribeApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err, "DescribeApplication should succeed")
	assert.Equal(t, appName, aws.ToString(descOut.ApplicationDetail.ApplicationName))
	assert.Equal(t, "READY", string(descOut.ApplicationDetail.ApplicationStatus))

	// List applications.
	listOut, err := client.ListApplications(t.Context(), &kinesisanalyticsv2svc.ListApplicationsInput{})
	require.NoError(t, err, "ListApplications should succeed")

	found := false

	for _, app := range listOut.ApplicationSummaries {
		if aws.ToString(app.ApplicationName) == appName {
			found = true

			break
		}
	}

	assert.True(t, found, "application should appear in list")

	// ListTagsForResource.
	tagsOut, err := client.ListTagsForResource(t.Context(), &kinesisanalyticsv2svc.ListTagsForResourceInput{
		ResourceARN: aws.String(appARN),
	})
	require.NoError(t, err, "ListTagsForResource should succeed")
	assert.NotEmpty(t, tagsOut.Tags)

	// Delete application.
	_, err = client.DeleteApplication(t.Context(), &kinesisanalyticsv2svc.DeleteApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err, "DeleteApplication should succeed")

	// Verify deletion.
	_, err = client.DescribeApplication(t.Context(), &kinesisanalyticsv2svc.DescribeApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.Error(t, err, "DescribeApplication should fail after deletion")
}

// TestIntegration_KinesisAnalyticsV2_StartStop tests start/stop lifecycle.
func TestIntegration_KinesisAnalyticsV2_StartStop(t *testing.T) {
	t.Parallel()

	client := createKinesisAnalyticsV2SDKClient(t)
	appName := "integration-kav2-startstop-" + t.Name()

	_, err := client.CreateApplication(t.Context(), &kinesisanalyticsv2svc.CreateApplicationInput{
		ApplicationName:    aws.String(appName),
		RuntimeEnvironment: "FLINK-1_18",
	})
	require.NoError(t, err)

	// Start application.
	_, err = client.StartApplication(t.Context(), &kinesisanalyticsv2svc.StartApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err, "StartApplication should succeed")

	// Verify RUNNING status.
	descOut, err := client.DescribeApplication(t.Context(), &kinesisanalyticsv2svc.DescribeApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", string(descOut.ApplicationDetail.ApplicationStatus))

	// Stop application.
	_, err = client.StopApplication(t.Context(), &kinesisanalyticsv2svc.StopApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err, "StopApplication should succeed")

	// Verify READY status.
	descOut2, err := client.DescribeApplication(t.Context(), &kinesisanalyticsv2svc.DescribeApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err)
	assert.Equal(t, "READY", string(descOut2.ApplicationDetail.ApplicationStatus))

	// Clean up.
	_, err = client.DeleteApplication(t.Context(), &kinesisanalyticsv2svc.DeleteApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err)
}

// TestIntegration_KinesisAnalyticsV2_Snapshots tests snapshot lifecycle.
func TestIntegration_KinesisAnalyticsV2_Snapshots(t *testing.T) {
	t.Parallel()

	client := createKinesisAnalyticsV2SDKClient(t)
	appName := "integration-kav2-snapshots-" + t.Name()

	_, err := client.CreateApplication(t.Context(), &kinesisanalyticsv2svc.CreateApplicationInput{
		ApplicationName:    aws.String(appName),
		RuntimeEnvironment: "FLINK-1_18",
	})
	require.NoError(t, err)

	// Create snapshot.
	_, err = client.CreateApplicationSnapshot(t.Context(), &kinesisanalyticsv2svc.CreateApplicationSnapshotInput{
		ApplicationName: aws.String(appName),
		SnapshotName:    aws.String("my-snapshot"),
	})
	require.NoError(t, err, "CreateApplicationSnapshot should succeed")

	// List snapshots.
	listOut, err := client.ListApplicationSnapshots(t.Context(), &kinesisanalyticsv2svc.ListApplicationSnapshotsInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err, "ListApplicationSnapshots should succeed")
	assert.NotEmpty(t, listOut.SnapshotSummaries)

	// Delete snapshot.
	_, err = client.DeleteApplicationSnapshot(t.Context(), &kinesisanalyticsv2svc.DeleteApplicationSnapshotInput{
		ApplicationName:           aws.String(appName),
		SnapshotName:              aws.String("my-snapshot"),
		SnapshotCreationTimestamp: listOut.SnapshotSummaries[0].SnapshotCreationTimestamp,
	})
	require.NoError(t, err, "DeleteApplicationSnapshot should succeed")

	// Clean up.
	_, err = client.DeleteApplication(t.Context(), &kinesisanalyticsv2svc.DeleteApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err)
}

// TestIntegration_KinesisAnalyticsV2_Tags tests tagging operations.
func TestIntegration_KinesisAnalyticsV2_Tags(t *testing.T) {
	t.Parallel()

	client := createKinesisAnalyticsV2SDKClient(t)
	appName := "integration-kav2-tags-" + t.Name()

	createOut, err := client.CreateApplication(t.Context(), &kinesisanalyticsv2svc.CreateApplicationInput{
		ApplicationName:    aws.String(appName),
		RuntimeEnvironment: "FLINK-1_18",
		Tags:               []kinesisanalyticsv2types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)

	appARN := aws.ToString(createOut.ApplicationDetail.ApplicationARN)

	// ListTagsForResource.
	tagsOut, err := client.ListTagsForResource(t.Context(), &kinesisanalyticsv2svc.ListTagsForResourceInput{
		ResourceARN: aws.String(appARN),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, tagsOut.Tags)

	// TagResource.
	_, err = client.TagResource(t.Context(), &kinesisanalyticsv2svc.TagResourceInput{
		ResourceARN: aws.String(appARN),
		Tags:        []kinesisanalyticsv2types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err, "TagResource should succeed")

	// UntagResource.
	_, err = client.UntagResource(t.Context(), &kinesisanalyticsv2svc.UntagResourceInput{
		ResourceARN: aws.String(appARN),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err, "UntagResource should succeed")

	// Clean up.
	_, err = client.DeleteApplication(t.Context(), &kinesisanalyticsv2svc.DeleteApplicationInput{
		ApplicationName: aws.String(appName),
	})
	require.NoError(t, err)
}
