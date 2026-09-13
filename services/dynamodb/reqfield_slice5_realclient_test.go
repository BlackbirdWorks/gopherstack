package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestUpdateTable_MultiRegionConsistency_SurvivesWireConversion verifies
// UpdateTable.MultiRegionConsistency (reqfielddiff slice 5, gopherstack-xhu2t)
// is honoured when promoting a table to a Global Tables v2 global table via
// ReplicaUpdates: the requested consistency mode is echoed back on both the
// UpdateTable response and a subsequent DescribeTable.
func TestUpdateTable_MultiRegionConsistency_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	t.Run("explicit STRONG survives", func(t *testing.T) {
		t.Parallel()

		client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
		ctx := t.Context()
		keySchema, attrDefs := wireTestKeySchema()

		_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
			TableName:            aws.String("mrc-strong-table"),
			KeySchema:            keySchema,
			AttributeDefinitions: attrDefs,
			BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		})
		require.NoError(t, err)

		updOut, err := client.UpdateTable(ctx, &dynamodbsdk.UpdateTableInput{
			TableName:              aws.String("mrc-strong-table"),
			MultiRegionConsistency: dynamodbtypes.MultiRegionConsistencyStrong,
			ReplicaUpdates: []dynamodbtypes.ReplicationGroupUpdate{
				{Create: &dynamodbtypes.CreateReplicationGroupMemberAction{RegionName: aws.String("eu-west-1")}},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, dynamodbtypes.MultiRegionConsistencyStrong, updOut.TableDescription.MultiRegionConsistency)

		descOut, err := client.DescribeTable(ctx, &dynamodbsdk.DescribeTableInput{
			TableName: aws.String("mrc-strong-table"),
		})
		require.NoError(t, err)
		assert.Equal(t, dynamodbtypes.MultiRegionConsistencyStrong, descOut.Table.MultiRegionConsistency)
	})

	t.Run("omitted defaults to EVENTUAL when promoted to global table", func(t *testing.T) {
		t.Parallel()

		client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
		ctx := t.Context()
		keySchema, attrDefs := wireTestKeySchema()

		_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
			TableName:            aws.String("mrc-default-table"),
			KeySchema:            keySchema,
			AttributeDefinitions: attrDefs,
			BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		})
		require.NoError(t, err)

		updOut, err := client.UpdateTable(ctx, &dynamodbsdk.UpdateTableInput{
			TableName: aws.String("mrc-default-table"),
			ReplicaUpdates: []dynamodbtypes.ReplicationGroupUpdate{
				{Create: &dynamodbtypes.CreateReplicationGroupMemberAction{RegionName: aws.String("eu-west-1")}},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, dynamodbtypes.MultiRegionConsistencyEventual, updOut.TableDescription.MultiRegionConsistency)

		descOut, err := client.DescribeTable(ctx, &dynamodbsdk.DescribeTableInput{
			TableName: aws.String("mrc-default-table"),
		})
		require.NoError(t, err)
		assert.Equal(t, dynamodbtypes.MultiRegionConsistencyEventual, descOut.Table.MultiRegionConsistency)
	})

	t.Run("not a global table leaves consistency unset", func(t *testing.T) {
		t.Parallel()

		client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
		ctx := t.Context()
		keySchema, attrDefs := wireTestKeySchema()

		_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
			TableName:            aws.String("mrc-plain-table"),
			KeySchema:            keySchema,
			AttributeDefinitions: attrDefs,
			BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		})
		require.NoError(t, err)

		descOut, err := client.DescribeTable(ctx, &dynamodbsdk.DescribeTableInput{
			TableName: aws.String("mrc-plain-table"),
		})
		require.NoError(t, err)
		assert.Empty(t, descOut.Table.MultiRegionConsistency)
	})
}
