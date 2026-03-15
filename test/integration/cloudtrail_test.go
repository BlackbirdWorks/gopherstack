package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_CloudTrail_TrailLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudTrailClient(t)
	ctx := t.Context()

	trailName := fmt.Sprintf("test-trail-%s", uuid.NewString()[:8])
	s3Bucket := fmt.Sprintf("test-bucket-%s", uuid.NewString()[:8])

	// CreateTrail
	createOut, err := client.CreateTrail(ctx, &cloudtrail.CreateTrailInput{
		Name:         aws.String(trailName),
		S3BucketName: aws.String(s3Bucket),
	})
	require.NoError(t, err)
	assert.Equal(t, trailName, aws.ToString(createOut.Name))
	assert.NotEmpty(t, aws.ToString(createOut.TrailARN))

	t.Cleanup(func() {
		_, _ = client.DeleteTrail(ctx, &cloudtrail.DeleteTrailInput{
			Name: aws.String(trailName),
		})
	})

	// DescribeTrails
	descOut, err := client.DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{
		TrailNameList: []string{trailName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.TrailList, 1)
	assert.Equal(t, trailName, aws.ToString(descOut.TrailList[0].Name))
	assert.Equal(t, s3Bucket, aws.ToString(descOut.TrailList[0].S3BucketName))

	// GetTrailStatus
	statusOut, err := client.GetTrailStatus(ctx, &cloudtrail.GetTrailStatusInput{
		Name: aws.String(trailName),
	})
	require.NoError(t, err)
	assert.NotNil(t, statusOut)

	// DeleteTrail
	_, err = client.DeleteTrail(ctx, &cloudtrail.DeleteTrailInput{
		Name: aws.String(trailName),
	})
	require.NoError(t, err)

	// Verify deleted
	descOut2, err := client.DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{
		TrailNameList: []string{trailName},
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.TrailList)
}

func TestIntegration_CloudTrail_DescribeTrailNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudTrailClient(t)
	ctx := t.Context()

	out, err := client.DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{
		TrailNameList: []string{"does-not-exist"},
	})
	require.NoError(t, err)
	assert.Empty(t, out.TrailList)
}

func TestIntegration_CloudTrail_ListTrails(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudTrailClient(t)
	ctx := t.Context()

	trailName := fmt.Sprintf("list-trail-%s", uuid.NewString()[:8])
	s3Bucket := fmt.Sprintf("list-bucket-%s", uuid.NewString()[:8])

	_, err := client.CreateTrail(ctx, &cloudtrail.CreateTrailInput{
		Name:         aws.String(trailName),
		S3BucketName: aws.String(s3Bucket),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteTrail(ctx, &cloudtrail.DeleteTrailInput{
			Name: aws.String(trailName),
		})
	})

	listOut, err := client.ListTrails(ctx, &cloudtrail.ListTrailsInput{})
	require.NoError(t, err)

	found := false

	for _, ti := range listOut.Trails {
		if aws.ToString(ti.Name) == trailName {
			found = true

			break
		}
	}

	assert.True(t, found, "created trail should appear in ListTrails")
}

func TestIntegration_CloudTrail_LookupEvents(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudTrailClient(t)
	ctx := t.Context()

	// LookupEvents returns an empty list when no events are recorded.
	out, err := client.LookupEvents(ctx, &cloudtrail.LookupEventsInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
	assert.Empty(t, out.Events)
}
