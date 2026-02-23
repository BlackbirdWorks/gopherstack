package dynamodb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamodbstreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/dynamodb"
)

// newTestDB creates a minimal DynamoDB backend with a single test table.
func newTestDB(t *testing.T, tableName string) *ddb.InMemoryDB {
	t.Helper()

	db := ddb.NewInMemoryDB()

	_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	return db
}

func TestUpdateTable_ProvisionedThroughput(t *testing.T) {
	t.Parallel()

	db := newTestDB(t, "my-table")

	out, err := db.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("my-table"),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(20),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.TableDescription)
	require.NotNil(t, out.TableDescription.ProvisionedThroughput)

	assert.EqualValues(t, 10, aws.ToInt64(out.TableDescription.ProvisionedThroughput.ReadCapacityUnits))
	assert.EqualValues(t, 20, aws.ToInt64(out.TableDescription.ProvisionedThroughput.WriteCapacityUnits))

	// DescribeTable should reflect the new values.
	desc, err := db.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String("my-table"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 10, aws.ToInt64(desc.Table.ProvisionedThroughput.ReadCapacityUnits))
	assert.EqualValues(t, 20, aws.ToInt64(desc.Table.ProvisionedThroughput.WriteCapacityUnits))
}

func TestUpdateTable_EnableStream(t *testing.T) {
	t.Parallel()

	db := newTestDB(t, "stream-table")

	out, err := db.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("stream-table"),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "stream-table", aws.ToString(out.TableDescription.TableName))

	// Verify stream is now enabled via ListStreams.
	outs, err := db.ListStreams(context.Background(), &dynamodbstreams.ListStreamsInput{
		TableName: aws.String("stream-table"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, outs.Streams, "stream should be available after enabling")
}

func TestUpdateTable_CreateGSI(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()

	_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("gsi-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	_, err = db.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("gsi-table"),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Create: &types.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String("sk-index"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("sk"), KeyType: types.KeyTypeHash},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
					ProvisionedThroughput: &types.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(5),
						WriteCapacityUnits: aws.Int64(5),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	desc, err := db.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String("gsi-table"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Table.GlobalSecondaryIndexes, 1)
	assert.Equal(t, "sk-index", aws.ToString(desc.Table.GlobalSecondaryIndexes[0].IndexName))
}

func TestUpdateTable_DeleteGSI(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()

	_, err := db.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("del-gsi-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gk"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gk-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gk"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	_, err = db.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("del-gsi-table"),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Delete: &types.DeleteGlobalSecondaryIndexAction{
					IndexName: aws.String("gk-index"),
				},
			},
		},
	})
	require.NoError(t, err)

	desc, err := db.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String("del-gsi-table"),
	})
	require.NoError(t, err)
	assert.Empty(t, desc.Table.GlobalSecondaryIndexes, "GSI should be removed after delete")
}

func TestUpdateTable_NotFound(t *testing.T) {
	t.Parallel()

	db := ddb.NewInMemoryDB()

	_, err := db.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("no-such-table"),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "ResourceNotFoundException")
}
