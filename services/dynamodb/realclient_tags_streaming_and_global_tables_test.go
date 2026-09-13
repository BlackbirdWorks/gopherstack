package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestRealClient_TagsStreamingAndGlobalTables drives dynamodb's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_TagsStreamingAndGlobalTables(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "describe limits and endpoints", run: func(t *testing.T) {
			t.Helper()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()

			limitsOut, err := client.DescribeLimits(ctx, &dynamodbsdk.DescribeLimitsInput{})
			require.NoError(t, err)
			assert.Positive(t, aws.ToInt64(limitsOut.AccountMaxReadCapacityUnits))
			assert.Positive(t, aws.ToInt64(limitsOut.AccountMaxWriteCapacityUnits))
			assert.Positive(t, aws.ToInt64(limitsOut.TableMaxReadCapacityUnits))
			assert.Positive(t, aws.ToInt64(limitsOut.TableMaxWriteCapacityUnits))

			endpointsOut, err := client.DescribeEndpoints(ctx, &dynamodbsdk.DescribeEndpointsInput{})
			require.NoError(t, err)
			require.Len(t, endpointsOut.Endpoints, 1)
			assert.Contains(t, aws.ToString(endpointsOut.Endpoints[0].Address), "dynamodb.")
			assert.Positive(t, endpointsOut.Endpoints[0].CachePeriodInMinutes)
		}},
		{name: "tag resource", run: func(t *testing.T) {
			t.Helper()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()

			createOut, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
				TableName: aws.String("s11-tag-table"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)
			tableArn := aws.ToString(createOut.TableDescription.TableArn)

			_, err = client.TagResource(ctx, &dynamodbsdk.TagResourceInput{
				ResourceArn: aws.String(tableArn),
				Tags: []types.Tag{
					{Key: aws.String("env"), Value: aws.String("test")},
				},
			})
			require.NoError(t, err)

			listOut, err := client.ListTagsOfResource(ctx, &dynamodbsdk.ListTagsOfResourceInput{
				ResourceArn: aws.String(tableArn),
			})
			require.NoError(t, err)
			require.Len(t, listOut.Tags, 1)
			assert.Equal(t, "env", aws.ToString(listOut.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(listOut.Tags[0].Value))

			_, err = client.UntagResource(ctx, &dynamodbsdk.UntagResourceInput{
				ResourceArn: aws.String(tableArn),
				TagKeys:     []string{"env"},
			})
			require.NoError(t, err)

			afterOut, err := client.ListTagsOfResource(ctx, &dynamodbsdk.ListTagsOfResourceInput{
				ResourceArn: aws.String(tableArn),
			})
			require.NoError(t, err)
			assert.Empty(t, afterOut.Tags)
		}},
		{name: "kinesis streaming destination", run: func(t *testing.T) {
			t.Helper()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()

			_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
				TableName: aws.String("s11-kinesis-table"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			streamArn := "arn:aws:kinesis:us-east-1:000000000000:stream/s11-stream"
			_, err = client.EnableKinesisStreamingDestination(ctx, &dynamodbsdk.EnableKinesisStreamingDestinationInput{
				TableName: aws.String("s11-kinesis-table"),
				StreamArn: aws.String(streamArn),
			})
			require.NoError(t, err)

			descOut, err := client.DescribeKinesisStreamingDestination(
				ctx, &dynamodbsdk.DescribeKinesisStreamingDestinationInput{
					TableName: aws.String("s11-kinesis-table"),
				},
			)
			require.NoError(t, err)
			require.Len(t, descOut.KinesisDataStreamDestinations, 1)
			assert.Equal(t, streamArn, aws.ToString(descOut.KinesisDataStreamDestinations[0].StreamArn))
			assert.Equal(t, types.DestinationStatusActive, descOut.KinesisDataStreamDestinations[0].DestinationStatus)

			disableOut, err := client.DisableKinesisStreamingDestination(
				ctx, &dynamodbsdk.DisableKinesisStreamingDestinationInput{
					TableName: aws.String("s11-kinesis-table"),
					StreamArn: aws.String(streamArn),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, types.DestinationStatusDisabling, disableOut.DestinationStatus)
			assert.Equal(t, streamArn, aws.ToString(disableOut.StreamArn))
		}},
		{name: "global table describe and update", run: func(t *testing.T) {
			t.Helper()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()

			_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
				TableName: aws.String("s11-global-table"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			_, err = client.CreateGlobalTable(ctx, &dynamodbsdk.CreateGlobalTableInput{
				GlobalTableName: aws.String("s11-global-table"),
				ReplicationGroup: []types.Replica{
					{RegionName: aws.String("us-east-1")},
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeGlobalTable(ctx, &dynamodbsdk.DescribeGlobalTableInput{
				GlobalTableName: aws.String("s11-global-table"),
			})
			require.NoError(t, err)
			require.NotNil(t, descOut.GlobalTableDescription)
			assert.Equal(t, "s11-global-table", aws.ToString(descOut.GlobalTableDescription.GlobalTableName))
			require.Len(t, descOut.GlobalTableDescription.ReplicationGroup, 1)
			assert.Equal(t, "us-east-1", aws.ToString(descOut.GlobalTableDescription.ReplicationGroup[0].RegionName))

			updOut, err := client.UpdateGlobalTable(ctx, &dynamodbsdk.UpdateGlobalTableInput{
				GlobalTableName: aws.String("s11-global-table"),
				ReplicaUpdates: []types.ReplicaUpdate{
					{Create: &types.CreateReplicaAction{RegionName: aws.String("eu-west-1")}},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, updOut.GlobalTableDescription)

			regions := map[string]bool{}
			for _, r := range updOut.GlobalTableDescription.ReplicationGroup {
				regions[aws.ToString(r.RegionName)] = true
			}
			assert.True(t, regions["us-east-1"])
			assert.True(t, regions["eu-west-1"], "UpdateGlobalTable must add the new replica region")
		}},
		{name: "search vectors honest not-found", run: func(t *testing.T) {
			t.Helper()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()

			_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
				TableName: aws.String("s11-vector-table"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			_, err = client.SearchVectors(ctx, &dynamodbsdk.SearchVectorsInput{
				TableName: aws.String("s11-vector-table"),
				IndexName: aws.String("no-such-vector-index"),
				SearchVector: []types.AttributeValue{
					&types.AttributeValueMemberN{Value: "0.1"},
					&types.AttributeValueMemberN{Value: "0.2"},
					&types.AttributeValueMemberN{Value: "0.3"},
				},
				TopK: aws.Int32(5),
			})
			require.Error(t, err, "gopherstack doesn't model vector indexes, so SearchVectors must honestly fail")

			var rnf *types.ResourceNotFoundException
			require.ErrorAs(t, err, &rnf)
			assert.Contains(t, aws.ToString(rnf.Message), "no-such-vector-index")
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
