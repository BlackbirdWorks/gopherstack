package main

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awsddbstreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const regionTargetsAccount = "000000000000"

func TestIoTRuleTargets_RouteToRuleRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home-region", region: regionA},
		{name: "other-region", region: regionB},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			sqsBk := sqsbackend.NewInMemoryBackend()
			q, err := sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "iot-q", Region: tc.region})
			require.NoError(t, err)

			require.NoError(t, (&iotRuleDispatcher{sqs: sqsBk}).SendToSQS(tc.region, q.QueueURL, "hello"))

			got, err := sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
				QueueURL: q.QueueURL, Region: tc.region, MaxNumberOfMessages: 1,
			})
			require.NoError(t, err)
			require.Len(t, got.Messages, 1)
			assert.Equal(t, "hello", got.Messages[0].Body)

			clock := newKinesisFakeClock(time.Now())
			kBk := kinesisbackend.NewInMemoryBackendWithConfig(regionTargetsAccount, regionA).WithClock(clock.Now)
			kctx, _ := kinesisbackend.ContextAndNameFromStreamARN(
				ctx, "arn:aws:kinesis:"+tc.region+":"+regionTargetsAccount+":stream/iot-s")
			require.NoError(t, kBk.CreateStream(kctx, &kinesisbackend.CreateStreamInput{
				StreamName: "iot-s", ShardCount: 1,
			}))
			clock.Advance(kinesisStreamSettleWait)
			require.NoError(t, (&iotKinesisTarget{backend: kBk}).PutRecord(ctx, tc.region, "iot-s", "pk", []byte("d")))

			snsBk := snsbackend.NewInMemoryBackend()
			topic, err := snsBk.CreateTopicInRegion("iot-t", tc.region, nil)
			require.NoError(t, err)
			snsTarget := &iotSNSTarget{backend: snsBk}
			require.NoError(t, snsTarget.PublishToTopic(ctx, tc.region, topic.TopicArn, "m", false))
		})
	}
}

func TestDynamoDBStreamsReaders_ReadNonHomeRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		read func(db *ddbbackend.InMemoryDB, streamARN string) (int, error)
		name string
	}{
		{
			name: "lambda-esm-reader",
			read: func(db *ddbbackend.InMemoryDB, streamARN string) (int, error) {
				a := &ddbStreamsReaderAdapter{backend: db}

				return readAllShards(streamARN, a.DescribeStreamShards, a.GetStreamShardIterator,
					func(it string) (int, string, error) {
						recs, next, err := a.GetStreamRecords(it, 10)

						return len(recs), next, err
					})
			},
		},
		{
			name: "pipes-reader",
			read: func(db *ddbbackend.InMemoryDB, streamARN string) (int, error) {
				a := &pipesDDBStreamsReaderAdapter{backend: db}

				return readAllShards(streamARN, a.DescribeStreamShards, a.GetStreamShardIterator,
					func(it string) (int, string, error) {
						recs, next, err := a.GetStreamRecords(it, 10)

						return len(recs), next, err
					})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := ddbbackend.NewInMemoryDB()
			ctx := ddbbackend.WithRegion(context.Background(), regionB)

			_, err := db.CreateTable(ctx, &ddbsdk.CreateTableInput{
				TableName: aws.String("src"),
				KeySchema: []ddbsdktypes.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: ddbsdktypes.KeyTypeHash},
				},
				AttributeDefinitions: []ddbsdktypes.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: ddbsdktypes.ScalarAttributeTypeS},
				},
				ProvisionedThroughput: &ddbsdktypes.ProvisionedThroughput{
					ReadCapacityUnits: aws.Int64(1), WriteCapacityUnits: aws.Int64(1),
				},
			})
			require.NoError(t, err)
			require.NoError(t, db.EnableStream(ctx, "src", "NEW_IMAGE"))

			_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
				TableName: aws.String("src"),
				Item:      map[string]ddbsdktypes.AttributeValue{"pk": &ddbsdktypes.AttributeValueMemberS{Value: "a"}},
			})
			require.NoError(t, err)

			streams, err := db.ListStreams(ctx, &awsddbstreams.ListStreamsInput{})
			require.NoError(t, err)
			require.Len(t, streams.Streams, 1)

			n, err := tc.read(db, aws.ToString(streams.Streams[0].StreamArn))
			require.NoError(t, err)
			assert.Equal(t, 1, n)
		})
	}
}

func readAllShards(
	streamARN string,
	describe func(string) ([]string, error),
	iterator func(arn, shard, typ string) (string, error),
	records func(string) (int, string, error),
) (int, error) {
	shards, err := describe(streamARN)
	if err != nil {
		return 0, err
	}

	total := 0

	for _, shard := range shards {
		it, itErr := iterator(streamARN, shard, "TRIM_HORIZON")
		if itErr != nil {
			return 0, itErr
		}

		n, _, recErr := records(it)
		if recErr != nil {
			return 0, recErr
		}

		total += n
	}

	return total, nil
}
