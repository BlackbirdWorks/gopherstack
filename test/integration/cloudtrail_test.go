package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_CloudTrail_TrailLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudTrailClient(t)
	ctx := t.Context()

	trailName := "test-trail-" + uuid.NewString()[:8]
	s3Bucket := "test-bucket-" + uuid.NewString()[:8]

	// CreateTrail
	createOut, err := client.CreateTrail(ctx, &cloudtrail.CreateTrailInput{
		Name:         aws.String(trailName),
		S3BucketName: aws.String(s3Bucket),
	})
	require.NoError(t, err)
	assert.Equal(t, trailName, aws.ToString(createOut.Name))
	assert.NotEmpty(t, aws.ToString(createOut.TrailARN))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteTrail(cleanupCtx, &cloudtrail.DeleteTrailInput{
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

	trailName := "list-trail-" + uuid.NewString()[:8]
	s3Bucket := "list-bucket-" + uuid.NewString()[:8]

	_, err := client.CreateTrail(ctx, &cloudtrail.CreateTrailInput{
		Name:         aws.String(trailName),
		S3BucketName: aws.String(s3Bucket),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteTrail(cleanupCtx, &cloudtrail.DeleteTrailInput{
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

// TestIntegration_CloudTrail_LookupEvents verifies that LookupEvents returns
// well-formed events for real API activity. It does not assert that the
// account-wide event history is empty: real AWS CloudTrail retains a rolling
// 90-day management-event history for every API call made in the account
// (LookupEvents works even with no trail configured), and this suite runs
// many tests concurrently against a single shared backend, so other tests'
// mutating calls are legitimately present in the log. Instead, this test
// performs a known action and confirms a matching, correctly shaped event
// shows up when filtered by that action's EventName.
func TestIntegration_CloudTrail_LookupEvents(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudTrailClient(t)
	ctx := t.Context()

	trailName := "lookup-events-trail-" + uuid.NewString()[:8]
	s3Bucket := "lookup-events-bucket-" + uuid.NewString()[:8]

	_, err := client.CreateTrail(ctx, &cloudtrail.CreateTrailInput{
		Name:         aws.String(trailName),
		S3BucketName: aws.String(s3Bucket),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteTrail(cleanupCtx, &cloudtrail.DeleteTrailInput{Name: aws.String(trailName)})
	})

	out, err := client.LookupEvents(ctx, &cloudtrail.LookupEventsInput{
		LookupAttributes: []cloudtrailtypes.LookupAttribute{
			{AttributeKey: cloudtrailtypes.LookupAttributeKeyEventName, AttributeValue: aws.String("CreateTrail")},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Events, "LookupEvents should reflect the CreateTrail call just made")

	for _, ev := range out.Events {
		assert.Equal(t, "CreateTrail", aws.ToString(ev.EventName))
		assert.Equal(t, "cloudtrail.amazonaws.com", aws.ToString(ev.EventSource))
		assert.NotEmpty(t, aws.ToString(ev.EventId))
		require.NotNil(t, ev.EventTime, "EventTime must round-trip through the JSON-protocol unixTimestamp wire format")
		assert.WithinDuration(t, time.Now(), *ev.EventTime, time.Hour)
	}
}
