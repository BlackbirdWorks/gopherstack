package mediapackage_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediapackagesdk "github.com/aws/aws-sdk-go-v2/service/mediapackage"
	mediapackagetypes "github.com/aws/aws-sdk-go-v2/service/mediapackage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// TestTypedSlice21RealClient drives mediapackage's remaining
// typed-coverage-blind ops (gopherstack-n3zi slice 21) through the real
// aws-sdk-go-v2 client: ConfigureLogs, CreateHarvestJob,
// DeleteOriginEndpoint, DescribeHarvestJob, ListHarvestJobs,
// ListOriginEndpoints, RotateChannelCredentials,
// RotateIngestEndpointCredentials, TagResource, UntagResource,
// UpdateChannel, UpdateOriginEndpoint.
func TestTypedSlice21RealClient(t *testing.T) {
	t.Parallel()

	t.Run("channel lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestMediaPackageClient(t, mediapackage.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateChannel(ctx, &mediapackagesdk.CreateChannelInput{
			Id: aws.String("s21-channel"), Description: aws.String("s21 channel"),
		})
		require.NoError(t, err)
		require.NotNil(t, createOut.HlsIngest)
		require.NotEmpty(t, createOut.HlsIngest.IngestEndpoints)
		ingestEndpointID := createOut.HlsIngest.IngestEndpoints[0].Id
		originalPassword := aws.ToString(createOut.HlsIngest.IngestEndpoints[0].Password)

		updOut, err := client.UpdateChannel(ctx, &mediapackagesdk.UpdateChannelInput{
			Id: aws.String("s21-channel"), Description: aws.String("s21 channel updated"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s21 channel updated", aws.ToString(updOut.Description))

		logsOut, err := client.ConfigureLogs(ctx, &mediapackagesdk.ConfigureLogsInput{
			Id: aws.String("s21-channel"),
			EgressAccessLogs: &mediapackagetypes.EgressAccessLogs{
				LogGroupName: aws.String("/s21/egress"),
			},
			IngressAccessLogs: &mediapackagetypes.IngressAccessLogs{
				LogGroupName: aws.String("/s21/ingress"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, logsOut.EgressAccessLogs)
		assert.Equal(t, "/s21/egress", aws.ToString(logsOut.EgressAccessLogs.LogGroupName))
		require.NotNil(t, logsOut.IngressAccessLogs)
		assert.Equal(t, "/s21/ingress", aws.ToString(logsOut.IngressAccessLogs.LogGroupName))

		rotChOut, err := client.RotateChannelCredentials(
			ctx,
			&mediapackagesdk.RotateChannelCredentialsInput{Id: aws.String("s21-channel")},
		)
		require.NoError(t, err)
		require.NotEmpty(t, rotChOut.HlsIngest.IngestEndpoints)

		rotEpOut, err := client.RotateIngestEndpointCredentials(
			ctx, &mediapackagesdk.RotateIngestEndpointCredentialsInput{
				Id: aws.String("s21-channel"), IngestEndpointId: ingestEndpointID,
			},
		)
		require.NoError(t, err)
		require.NotEmpty(t, rotEpOut.HlsIngest.IngestEndpoints)

		var rotatedPassword string

		for _, ep := range rotEpOut.HlsIngest.IngestEndpoints {
			if aws.ToString(ep.Id) == aws.ToString(ingestEndpointID) {
				rotatedPassword = aws.ToString(ep.Password)
			}
		}

		assert.NotEmpty(t, rotatedPassword)
		assert.NotEqual(t, originalPassword, rotatedPassword)
	})

	t.Run("origin endpoint lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestMediaPackageClient(t, mediapackage.NewHandler(backend))
		ctx := t.Context()

		_, err := client.CreateChannel(
			ctx,
			&mediapackagesdk.CreateChannelInput{Id: aws.String("s21-ep-channel")},
		)
		require.NoError(t, err)

		_, err = client.CreateOriginEndpoint(ctx, &mediapackagesdk.CreateOriginEndpointInput{
			ChannelId: aws.String("s21-ep-channel"), Id: aws.String("s21-endpoint"),
		})
		require.NoError(t, err)

		listOut, err := client.ListOriginEndpoints(ctx, &mediapackagesdk.ListOriginEndpointsInput{
			ChannelId: aws.String("s21-ep-channel"),
		})
		require.NoError(t, err)
		require.Len(t, listOut.OriginEndpoints, 1)
		assert.Equal(t, "s21-endpoint", aws.ToString(listOut.OriginEndpoints[0].Id))

		updOut, err := client.UpdateOriginEndpoint(ctx, &mediapackagesdk.UpdateOriginEndpointInput{
			Id: aws.String("s21-endpoint"), Description: aws.String("s21 endpoint updated"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s21 endpoint updated", aws.ToString(updOut.Description))

		_, err = client.DeleteOriginEndpoint(
			ctx,
			&mediapackagesdk.DeleteOriginEndpointInput{Id: aws.String("s21-endpoint")},
		)
		require.NoError(t, err)

		listOut2, err := client.ListOriginEndpoints(ctx, &mediapackagesdk.ListOriginEndpointsInput{
			ChannelId: aws.String("s21-ep-channel"),
		})
		require.NoError(t, err)
		assert.Empty(t, listOut2.OriginEndpoints)
	})

	t.Run("harvest jobs", func(t *testing.T) {
		t.Parallel()

		backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestMediaPackageClient(t, mediapackage.NewHandler(backend))
		ctx := t.Context()

		_, err := client.CreateChannel(
			ctx,
			&mediapackagesdk.CreateChannelInput{Id: aws.String("s21-hj-channel")},
		)
		require.NoError(t, err)

		_, err = client.CreateOriginEndpoint(ctx, &mediapackagesdk.CreateOriginEndpointInput{
			ChannelId: aws.String("s21-hj-channel"), Id: aws.String("s21-hj-endpoint"),
		})
		require.NoError(t, err)

		createOut, err := client.CreateHarvestJob(ctx, &mediapackagesdk.CreateHarvestJobInput{
			Id:               aws.String("s21-harvest-job"),
			OriginEndpointId: aws.String("s21-hj-endpoint"),
			StartTime:        aws.String("2026-01-01T00:00:00Z"),
			EndTime:          aws.String("2026-01-01T01:00:00Z"),
			S3Destination: &mediapackagetypes.S3Destination{
				BucketName:  aws.String("s21-bucket"),
				ManifestKey: aws.String("manifest/s21.m3u8"),
				RoleArn:     aws.String("arn:aws:iam::000000000000:role/S3Role"),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "s21-harvest-job", aws.ToString(createOut.Id))

		descOut, err := client.DescribeHarvestJob(
			ctx,
			&mediapackagesdk.DescribeHarvestJobInput{Id: aws.String("s21-harvest-job")},
		)
		require.NoError(t, err)
		assert.Equal(t, "s21-hj-endpoint", aws.ToString(descOut.OriginEndpointId))
		assert.NotEmpty(t, string(descOut.Status))

		listOut, err := client.ListHarvestJobs(ctx, &mediapackagesdk.ListHarvestJobsInput{
			IncludeChannelId: aws.String("s21-hj-channel"),
		})
		require.NoError(t, err)
		require.Len(t, listOut.HarvestJobs, 1)
		assert.Equal(t, "s21-harvest-job", aws.ToString(listOut.HarvestJobs[0].Id))
	})

	t.Run("tag and untag resource", func(t *testing.T) {
		t.Parallel()

		backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestMediaPackageClient(t, mediapackage.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateChannel(
			ctx,
			&mediapackagesdk.CreateChannelInput{Id: aws.String("s21-tag-channel")},
		)
		require.NoError(t, err)
		arn := createOut.Arn

		_, err = client.TagResource(ctx, &mediapackagesdk.TagResourceInput{
			ResourceArn: arn, Tags: map[string]string{"env": "s21"},
		})
		require.NoError(t, err)

		descOut, err := client.DescribeChannel(
			ctx,
			&mediapackagesdk.DescribeChannelInput{Id: aws.String("s21-tag-channel")},
		)
		require.NoError(t, err)
		assert.Equal(t, "s21", descOut.Tags["env"])

		_, err = client.UntagResource(ctx, &mediapackagesdk.UntagResourceInput{
			ResourceArn: arn, TagKeys: []string{"env"},
		})
		require.NoError(t, err)

		descOut2, err := client.DescribeChannel(
			ctx,
			&mediapackagesdk.DescribeChannelInput{Id: aws.String("s21-tag-channel")},
		)
		require.NoError(t, err)
		assert.NotContains(t, descOut2.Tags, "env")
	})
}
