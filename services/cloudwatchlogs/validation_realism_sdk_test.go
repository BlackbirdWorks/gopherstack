package cloudwatchlogs_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateLogStream_NameRules_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stream  string
		wantErr bool
	}{
		{name: "plain", stream: "app/stream-1_x.y"},
		{name: "colon", stream: "a:b", wantErr: true},
		{name: "asterisk", stream: "a*b", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/g")})
			require.NoError(t, err)

			_, err = client.CreateLogStream(t.Context(), &cwlsdk.CreateLogStreamInput{
				LogGroupName: aws.String("/g"), LogStreamName: aws.String(tc.stream),
			})
			if tc.wantErr {
				var target *cwltypes.InvalidParameterException
				assert.ErrorAs(t, err, &target, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDescribeLimits_Max50_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(c *cwlsdk.Client, limit int32) error
		name    string
		limit   int32
		wantErr bool
	}{
		{name: "groups at max", limit: 50, run: describeGroupsWithLimit},
		{name: "groups over max", limit: 51, run: describeGroupsWithLimit, wantErr: true},
		{name: "streams at max", limit: 50, run: describeStreamsWithLimit},
		{name: "streams over max", limit: 51, run: describeStreamsWithLimit, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/g")})
			require.NoError(t, err)

			err = tc.run(client, tc.limit)
			if tc.wantErr {
				var target *cwltypes.InvalidParameterException
				assert.ErrorAs(t, err, &target, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func describeGroupsWithLimit(c *cwlsdk.Client, limit int32) error {
	_, err := c.DescribeLogGroups(context.Background(), &cwlsdk.DescribeLogGroupsInput{Limit: aws.Int32(limit)})

	return err
}

func describeStreamsWithLimit(c *cwlsdk.Client, limit int32) error {
	_, err := c.DescribeLogStreams(context.Background(), &cwlsdk.DescribeLogStreamsInput{
		LogGroupName: aws.String("/g"), Limit: aws.Int32(limit),
	})

	return err
}
