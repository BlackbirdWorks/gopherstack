package kinesis_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListShards_SDKStreamCreationTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		offset  time.Duration
		wantErr bool
	}{
		{name: "matching_timestamp", offset: 0},
		{name: "different_incarnation", offset: time.Hour, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKinesisClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := client.CreateStream(ctx, &kinesissdk.CreateStreamInput{
				StreamName: aws.String("s"), ShardCount: aws.Int32(2),
			})
			require.NoError(t, err)

			sum, err := client.DescribeStreamSummary(
				ctx,
				&kinesissdk.DescribeStreamSummaryInput{StreamName: aws.String("s")},
			)
			require.NoError(t, err)

			out, err := client.ListShards(ctx, &kinesissdk.ListShardsInput{
				StreamName:              aws.String("s"),
				StreamCreationTimestamp: aws.Time(sum.StreamDescriptionSummary.StreamCreationTimestamp.Add(tt.offset)),
			})
			if tt.wantErr {
				var nf *types.ResourceNotFoundException

				require.ErrorAs(t, err, &nf)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Shards, 2)
		})
	}
}
