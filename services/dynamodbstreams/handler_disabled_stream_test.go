package dynamodbstreams_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamodbstreamssdk "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

func newDDBAndStreamsClients(
	t *testing.T,
	db *ddbbackend.InMemoryDB,
) (*ddbsdk.Client, *dynamodbstreamssdk.Client) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(ddbbackend.NewHandler(db)))
	require.NoError(t, registry.Register(dynamodbstreams.NewHandler(db)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return ddbsdk.NewFromConfig(cfg, func(o *ddbsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
		dynamodbstreamssdk.NewFromConfig(
			cfg,
			func(o *dynamodbstreamssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) },
		)
}

func TestDisabledStream_RemainsReadable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  streamstypes.StreamStatus
		delay bool
	}{
		{name: "disabled", want: streamstypes.StreamStatusDisabled},
		{name: "disabling", delay: true, want: streamstypes.StreamStatusDisabling},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddbbackend.NewInMemoryDB()
			if tt.delay {
				db.SetStreamDisablingDelay(time.Hour)
			}

			ddb, streams := newDDBAndStreamsClients(t, db)
			ctx := t.Context()

			_, err := ddb.CreateTable(ctx, &ddbsdk.CreateTableInput{
				TableName:   aws.String("disabled-stream"),
				BillingMode: ddbtypes.BillingModePayPerRequest,
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
				},
				AttributeDefinitions: []ddbtypes.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
				},
				StreamSpecification: &ddbtypes.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: ddbtypes.StreamViewTypeKeysOnly,
				},
			})
			require.NoError(t, err)

			_, err = ddb.PutItem(ctx, &ddbsdk.PutItemInput{
				TableName: aws.String("disabled-stream"),
				Item:      map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "k"}},
			})
			require.NoError(t, err)

			list, err := streams.ListStreams(ctx, &dynamodbstreamssdk.ListStreamsInput{})
			require.NoError(t, err)
			require.Len(t, list.Streams, 1)
			oldARN := aws.ToString(list.Streams[0].StreamArn)

			_, err = ddb.UpdateTable(ctx, &ddbsdk.UpdateTableInput{
				TableName:           aws.String("disabled-stream"),
				StreamSpecification: &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(false)},
			})
			require.NoError(t, err)

			list, err = streams.ListStreams(ctx, &dynamodbstreamssdk.ListStreamsInput{})
			require.NoError(t, err)
			require.Len(t, list.Streams, 1)
			assert.Equal(t, oldARN, aws.ToString(list.Streams[0].StreamArn))

			desc, err := streams.DescribeStream(
				ctx,
				&dynamodbstreamssdk.DescribeStreamInput{StreamArn: aws.String(oldARN)},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.want, desc.StreamDescription.StreamStatus)
			require.Len(t, desc.StreamDescription.Shards, 1)
			assert.NotEmpty(t, aws.ToString(desc.StreamDescription.Shards[0].SequenceNumberRange.EndingSequenceNumber))

			it, err := streams.GetShardIterator(ctx, &dynamodbstreamssdk.GetShardIteratorInput{
				StreamArn:         aws.String(oldARN),
				ShardId:           desc.StreamDescription.Shards[0].ShardId,
				ShardIteratorType: streamstypes.ShardIteratorTypeTrimHorizon,
			})
			require.NoError(t, err)

			recs, err := streams.GetRecords(ctx, &dynamodbstreamssdk.GetRecordsInput{ShardIterator: it.ShardIterator})
			require.NoError(t, err)
			require.Len(t, recs.Records, 1)
			assert.Nil(t, recs.NextShardIterator)

			_, err = ddb.UpdateTable(ctx, &ddbsdk.UpdateTableInput{
				TableName: aws.String("disabled-stream"),
				StreamSpecification: &ddbtypes.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: ddbtypes.StreamViewTypeNewImage,
				},
			})
			require.NoError(t, err)

			list, err = streams.ListStreams(
				ctx,
				&dynamodbstreamssdk.ListStreamsInput{TableName: aws.String("disabled-stream")},
			)
			require.NoError(t, err)
			require.Len(t, list.Streams, 2)
			assert.NotEqual(t, aws.ToString(list.Streams[0].StreamArn), aws.ToString(list.Streams[1].StreamArn))
		})
	}
}
