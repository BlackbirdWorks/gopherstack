package kinesisvideo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamTagLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
		StreamName: aws.String("tagged-stream"),
		Tags:       map[string]string{"team": "video"},
	})
	require.NoError(t, err)

	_, err = client.TagStream(ctx, &kinesisvideosdk.TagStreamInput{
		StreamARN: created.StreamARN,
		Tags:      map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForStream(ctx, &kinesisvideosdk.ListTagsForStreamInput{StreamARN: created.StreamARN})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "video", "env": "prod"}, listed.Tags)

	_, err = client.UntagStream(ctx, &kinesisvideosdk.UntagStreamInput{
		StreamARN:  created.StreamARN,
		TagKeyList: []string{"team"},
	})
	require.NoError(t, err)

	after, err := client.ListTagsForStream(ctx, &kinesisvideosdk.ListTagsForStreamInput{StreamARN: created.StreamARN})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, after.Tags)
}

func TestListTagsForStream_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.ListTagsForStream(t.Context(), &kinesisvideosdk.ListTagsForStreamInput{
		StreamName: aws.String("missing-stream"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

func TestChannelTagLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("tagged-channel"),
		Tags:        []types.Tag{{Key: aws.String("team"), Value: aws.String("video")}},
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &kinesisvideosdk.TagResourceInput{
		ResourceARN: created.ChannelARN,
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(ctx, &kinesisvideosdk.ListTagsForResourceInput{
		ResourceARN: created.ChannelARN,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "video", "env": "prod"}, listed.Tags)

	_, err = client.UntagResource(ctx, &kinesisvideosdk.UntagResourceInput{
		ResourceARN: created.ChannelARN,
		TagKeyList:  []string{"team"},
	})
	require.NoError(t, err)

	after, err := client.ListTagsForResource(ctx, &kinesisvideosdk.ListTagsForResourceInput{
		ResourceARN: created.ChannelARN,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, after.Tags)
}

func TestListTagsForResource_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.ListTagsForResource(t.Context(), &kinesisvideosdk.ListTagsForResourceInput{
		ResourceARN: aws.String("arn:aws:kinesisvideo:us-east-1:123456789012:channel/missing/1"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
