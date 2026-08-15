package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestCreateTable_IndexArn_Populated verifies GlobalSecondaryIndexDescription
// and LocalSecondaryIndexDescription carry IndexArn (dynamodb@v1.63.1
// types/types.go:1676 and :2292, both "This member is required" on the real
// service, i.e. every real index description response line has a non-empty
// value here) on CreateTable and DescribeTable responses.
func TestCreateTable_IndexArn_Populated(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	ctx := t.Context()

	tableName := "index-arn-table"
	createOut, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gsi1"),
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: dynamodbtypes.KeyTypeHash},
				},
				Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
			},
		},
		LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{
			{
				IndexName: aws.String("lsi1"),
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
				},
				Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
			},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	tableArn := aws.ToString(createOut.TableDescription.TableArn)
	require.NotEmpty(t, tableArn)

	require.Len(t, createOut.TableDescription.GlobalSecondaryIndexes, 1)
	require.Equal(
		t,
		tableArn+"/index/gsi1",
		aws.ToString(createOut.TableDescription.GlobalSecondaryIndexes[0].IndexArn),
	)

	require.Len(t, createOut.TableDescription.LocalSecondaryIndexes, 1)
	require.Equal(
		t,
		tableArn+"/index/lsi1",
		aws.ToString(createOut.TableDescription.LocalSecondaryIndexes[0].IndexArn),
	)

	descOut, err := client.DescribeTable(ctx, &dynamodbsdk.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)

	require.Len(t, descOut.Table.GlobalSecondaryIndexes, 1)
	require.Equal(
		t,
		tableArn+"/index/gsi1",
		aws.ToString(descOut.Table.GlobalSecondaryIndexes[0].IndexArn),
	)

	require.Len(t, descOut.Table.LocalSecondaryIndexes, 1)
	require.Equal(
		t,
		tableArn+"/index/lsi1",
		aws.ToString(descOut.Table.LocalSecondaryIndexes[0].IndexArn),
	)
}
