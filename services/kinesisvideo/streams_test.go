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

func TestCreateStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input kinesisvideosdk.CreateStreamInput
	}{
		{
			name:  "minimal",
			input: kinesisvideosdk.CreateStreamInput{StreamName: aws.String("stream-one")},
		},
		{
			name: "with retention and media type",
			input: kinesisvideosdk.CreateStreamInput{
				StreamName:           aws.String("stream-two"),
				DataRetentionInHours: aws.Int32(24),
				MediaType:            aws.String("video/h264"),
				DeviceName:           aws.String("my-camera"),
				Tags:                 map[string]string{"env": "test"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())

			out, err := client.CreateStream(t.Context(), &tt.input)
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(out.StreamARN), "stream/"+aws.ToString(tt.input.StreamName)+"/")
		})
	}
}

func TestCreateStream_DuplicateNameReturnsResourceInUse(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String("dup-stream")})
	require.NoError(t, err)

	_, err = client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String("dup-stream")})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceInUseException", apiErr.ErrorCode())
}

func TestDescribeStream(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, createErr := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
		StreamName:           aws.String("describe-me"),
		DataRetentionInHours: aws.Int32(5),
	})
	require.NoError(t, createErr)

	tests := []struct {
		name  string
		input kinesisvideosdk.DescribeStreamInput
	}{
		{name: "by name", input: kinesisvideosdk.DescribeStreamInput{StreamName: aws.String("describe-me")}},
		{name: "by arn", input: kinesisvideosdk.DescribeStreamInput{StreamARN: created.StreamARN}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.DescribeStream(ctx, &tt.input)
			require.NoError(t, err)
			require.NotNil(t, out.StreamInfo)
			assert.Equal(t, "describe-me", aws.ToString(out.StreamInfo.StreamName))
			assert.Equal(t, types.StatusActive, out.StreamInfo.Status)
			assert.EqualValues(t, 5, aws.ToInt32(out.StreamInfo.DataRetentionInHours))
			assert.NotZero(t, aws.ToTime(out.StreamInfo.CreationTime))
			assert.NotEmpty(t, aws.ToString(out.StreamInfo.Version))
		})
	}
}

func TestDescribeStream_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.DescribeStream(t.Context(), &kinesisvideosdk.DescribeStreamInput{
		StreamName: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

func TestListStreams(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	for _, name := range []string{"alpha-1", "alpha-2", "beta-1"} {
		_, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String(name)})
		require.NoError(t, err)
	}

	out, err := client.ListStreams(ctx, &kinesisvideosdk.ListStreamsInput{
		StreamNameCondition: &types.StreamNameCondition{
			ComparisonOperator: types.ComparisonOperatorBeginsWith,
			ComparisonValue:    aws.String("alpha-"),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.StreamInfoList, 2)

	names := []string{aws.ToString(out.StreamInfoList[0].StreamName), aws.ToString(out.StreamInfoList[1].StreamName)}
	assert.ElementsMatch(t, []string{"alpha-1", "alpha-2"}, names)
}

func TestUpdateStream(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String("update-me")})
	require.NoError(t, err)

	described, err := client.DescribeStream(ctx, &kinesisvideosdk.DescribeStreamInput{StreamARN: created.StreamARN})
	require.NoError(t, err)

	_, err = client.UpdateStream(ctx, &kinesisvideosdk.UpdateStreamInput{
		StreamARN:      created.StreamARN,
		CurrentVersion: described.StreamInfo.Version,
		MediaType:      aws.String("video/h264"),
	})
	require.NoError(t, err)

	after, err := client.DescribeStream(ctx, &kinesisvideosdk.DescribeStreamInput{StreamARN: created.StreamARN})
	require.NoError(t, err)
	assert.Equal(t, "video/h264", aws.ToString(after.StreamInfo.MediaType))
	assert.NotEqual(t, aws.ToString(described.StreamInfo.Version), aws.ToString(after.StreamInfo.Version))
}

func TestUpdateStream_VersionMismatch(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(
		ctx,
		&kinesisvideosdk.CreateStreamInput{StreamName: aws.String("stale-version")},
	)
	require.NoError(t, err)

	_, err = client.UpdateStream(ctx, &kinesisvideosdk.UpdateStreamInput{
		StreamARN:      created.StreamARN,
		CurrentVersion: aws.String("not-the-real-version"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "VersionMismatchException", apiErr.ErrorCode())
}

func TestDeleteStream(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String("delete-me")})
	require.NoError(t, err)

	_, err = client.DeleteStream(ctx, &kinesisvideosdk.DeleteStreamInput{StreamARN: created.StreamARN})
	require.NoError(t, err)

	_, err = client.DescribeStream(ctx, &kinesisvideosdk.DescribeStreamInput{StreamARN: created.StreamARN})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

func TestDeleteStream_VersionMismatch(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(
		ctx,
		&kinesisvideosdk.CreateStreamInput{StreamName: aws.String("delete-mismatch")},
	)
	require.NoError(t, err)

	_, err = client.DeleteStream(ctx, &kinesisvideosdk.DeleteStreamInput{
		StreamARN:      created.StreamARN,
		CurrentVersion: aws.String("wrong-version"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "VersionMismatchException", apiErr.ErrorCode())
}

func TestUpdateDataRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		operation   types.UpdateDataRetentionOperation
		startHours  int32
		changeHours int32
		wantHours   int32
	}{
		{
			name:        "increase",
			operation:   types.UpdateDataRetentionOperationIncreaseDataRetention,
			startHours:  10,
			changeHours: 5,
			wantHours:   15,
		},
		{
			name:        "decrease",
			operation:   types.UpdateDataRetentionOperationDecreaseDataRetention,
			startHours:  10,
			changeHours: 5,
			wantHours:   5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			created, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
				StreamName:           aws.String("retention-" + tt.name),
				DataRetentionInHours: aws.Int32(tt.startHours),
			})
			require.NoError(t, err)

			described, err := client.DescribeStream(
				ctx,
				&kinesisvideosdk.DescribeStreamInput{StreamARN: created.StreamARN},
			)
			require.NoError(t, err)

			_, err = client.UpdateDataRetention(ctx, &kinesisvideosdk.UpdateDataRetentionInput{
				StreamARN:                  created.StreamARN,
				CurrentVersion:             described.StreamInfo.Version,
				Operation:                  tt.operation,
				DataRetentionChangeInHours: aws.Int32(tt.changeHours),
			})
			require.NoError(t, err)

			after, err := client.DescribeStream(ctx, &kinesisvideosdk.DescribeStreamInput{StreamARN: created.StreamARN})
			require.NoError(t, err)
			assert.Equal(t, tt.wantHours, aws.ToInt32(after.StreamInfo.DataRetentionInHours))
		})
	}
}

func TestGetDataEndpoint(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(
		ctx,
		&kinesisvideosdk.CreateStreamInput{StreamName: aws.String("endpoint-stream")},
	)
	require.NoError(t, err)

	out, err := client.GetDataEndpoint(ctx, &kinesisvideosdk.GetDataEndpointInput{
		StreamARN: created.StreamARN,
		APIName:   types.APINamePutMedia,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.DataEndpoint), ".kinesisvideo."+testRegion+".amazonaws.com")
}
