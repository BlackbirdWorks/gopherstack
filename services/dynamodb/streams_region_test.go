package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestStreams_IteratorCarriesTableRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: "us-east-1"},
		{name: "other", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := ddb.NewInMemoryDB()
			ctx := ddb.WithRegion(t.Context(), tc.region)

			_, err := db.CreateTable(ctx, makeCreateTableInput("T", "pk"))
			require.NoError(t, err)
			require.NoError(t, db.EnableStream(ctx, "T", "NEW_IMAGE"))
			_, err = db.PutItem(ctx, makePutItem("T", "pk", "a"))
			require.NoError(t, err)

			streams, err := db.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{})
			require.NoError(t, err)
			require.Len(t, streams.Streams, 1)

			streamARN := aws.ToString(streams.Streams[0].StreamArn)

			shard, err := db.DescribeStream(t.Context(), &dynamodbstreams.DescribeStreamInput{
				StreamArn: aws.String(streamARN),
			})
			require.NoError(t, err)
			require.NotEmpty(t, shard.StreamDescription.Shards)

			it, err := db.GetShardIterator(t.Context(), &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(streamARN),
				ShardId:           shard.StreamDescription.Shards[0].ShardId,
				ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
			})
			require.NoError(t, err)

			out, err := db.GetRecords(t.Context(), &dynamodbstreams.GetRecordsInput{ShardIterator: it.ShardIterator})
			require.NoError(t, err)
			require.Len(t, out.Records, 1)
			assert.Equal(t, tc.region, aws.ToString(out.Records[0].AwsRegion))

			next, err := db.GetRecords(t.Context(), &dynamodbstreams.GetRecordsInput{
				ShardIterator: out.NextShardIterator,
			})
			require.NoError(t, err)
			assert.Empty(t, next.Records)
		})
	}
}
