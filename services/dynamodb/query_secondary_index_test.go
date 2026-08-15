package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// createQSITable creates a table with a composite base key (pk, sk), a GSI
// (gpk, gsk) with ALL projection, and an LSI (pk, lsk) with ALL projection,
// then drives every write through the real aws-sdk-go-v2 client so this test
// proves wire compatibility of gopherstack-anlc's indexed Query path, not
// just its internal Go behaviour.
func createQSITable(t *testing.T, client *dynamodbsdk.Client, tableName string) {
	t.Helper()

	rc, wc := int64(50), int64(50)
	_, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gpk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsk"), AttributeType: dynamodbtypes.ScalarAttributeTypeN},
			{AttributeName: aws.String("lsk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("byGpk"),
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("gpk"), KeyType: dynamodbtypes.KeyTypeHash},
					{AttributeName: aws.String("gsk"), KeyType: dynamodbtypes.KeyTypeRange},
				},
				Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
				ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
					ReadCapacityUnits: aws.Int64(rc), WriteCapacityUnits: aws.Int64(wc),
				},
			},
		},
		LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{
			{
				IndexName: aws.String("byLsk"),
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
					{AttributeName: aws.String("lsk"), KeyType: dynamodbtypes.KeyTypeRange},
				},
				Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
			},
		},
		ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
			ReadCapacityUnits: aws.Int64(rc), WriteCapacityUnits: aws.Int64(wc),
		},
	})
	require.NoError(t, err)
}

// TestQuery_GSI_SharedKeyAcrossItems proves that a Query against a GSI
// returns every item sharing that key, wire round-tripped through the real
// SDK client -- the base-table analogue would return at most one match, so
// this is the case a naive "copy pkIndex" fix would get wrong.
func TestQuery_GSI_SharedKeyAcrossItems(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))
	createQSITable(t, client, "shared-gsi-key")

	for _, sk := range []string{"a", "b", "c"} {
		_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
			TableName: aws.String("shared-gsi-key"),
			Item: map[string]dynamodbtypes.AttributeValue{
				"pk":  &dynamodbtypes.AttributeValueMemberS{Value: "item-" + sk},
				"sk":  &dynamodbtypes.AttributeValueMemberS{Value: sk},
				"gpk": &dynamodbtypes.AttributeValueMemberS{Value: "shared"},
				"gsk": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)
	}

	out, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
		TableName:              aws.String("shared-gsi-key"),
		IndexName:              aws.String("byGpk"),
		KeyConditionExpression: aws.String("gpk = :g"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":g": &dynamodbtypes.AttributeValueMemberS{Value: "shared"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 3, "all three items sharing the GSI key must be returned, not just one")
}

// TestQuery_GSI_SparseItemExcluded proves an item missing the GSI's key
// attribute is invisible to a GSI Query while remaining a normal row.
func TestQuery_GSI_SparseItemExcluded(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))
	createQSITable(t, client, "sparse-gsi")

	_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
		TableName: aws.String("sparse-gsi"),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk":  &dynamodbtypes.AttributeValueMemberS{Value: "has-gsi"},
			"sk":  &dynamodbtypes.AttributeValueMemberS{Value: "1"},
			"gpk": &dynamodbtypes.AttributeValueMemberS{Value: "g1"},
			"gsk": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	// No gpk/gsk at all -- must never surface from the GSI.
	_, err = client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
		TableName: aws.String("sparse-gsi"),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk": &dynamodbtypes.AttributeValueMemberS{Value: "no-gsi"},
			"sk": &dynamodbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)

	out, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
		TableName:              aws.String("sparse-gsi"),
		IndexName:              aws.String("byGpk"),
		KeyConditionExpression: aws.String("gpk = :g"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":g": &dynamodbtypes.AttributeValueMemberS{Value: "g1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	pkAttr, ok := out.Items[0]["pk"].(*dynamodbtypes.AttributeValueMemberS)
	require.True(t, ok)
	require.Equal(t, "has-gsi", pkAttr.Value)
}

// TestQuery_LSI_RangeCondition proves an LSI Query applies a sort-key range
// condition correctly and still shares the base table's partition key.
func TestQuery_LSI_RangeCondition(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))
	createQSITable(t, client, "lsi-range")

	for _, lsk := range []string{"m1", "m2", "m3", "m4"} {
		_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
			TableName: aws.String("lsi-range"),
			Item: map[string]dynamodbtypes.AttributeValue{
				"pk":  &dynamodbtypes.AttributeValueMemberS{Value: "owner"},
				"sk":  &dynamodbtypes.AttributeValueMemberS{Value: lsk},
				"lsk": &dynamodbtypes.AttributeValueMemberS{Value: lsk},
			},
		})
		require.NoError(t, err)
	}

	out, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
		TableName:              aws.String("lsi-range"),
		IndexName:              aws.String("byLsk"),
		KeyConditionExpression: aws.String("pk = :p AND lsk > :m"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":p": &dynamodbtypes.AttributeValueMemberS{Value: "owner"},
			":m": &dynamodbtypes.AttributeValueMemberS{Value: "m2"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 2, "lsk > m2 should match m3 and m4")
}

// TestQuery_GSI_UpdateMovesItemBetweenKeys proves an UpdateItem that changes
// a GSI key attribute's value is treated as a delete-then-insert in that
// index, wire round-tripped through the real client.
func TestQuery_GSI_UpdateMovesItemBetweenKeys(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))
	createQSITable(t, client, "gsi-move")

	_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
		TableName: aws.String("gsi-move"),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk":  &dynamodbtypes.AttributeValueMemberS{Value: "p1"},
			"sk":  &dynamodbtypes.AttributeValueMemberS{Value: "s1"},
			"gpk": &dynamodbtypes.AttributeValueMemberS{Value: "old"},
			"gsk": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	queryGpk := func(v string) int {
		out, qerr := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName:              aws.String("gsi-move"),
			IndexName:              aws.String("byGpk"),
			KeyConditionExpression: aws.String("gpk = :g"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":g": &dynamodbtypes.AttributeValueMemberS{Value: v},
			},
		})
		require.NoError(t, qerr)

		return len(out.Items)
	}

	require.Equal(t, 1, queryGpk("old"))

	_, err = client.UpdateItem(t.Context(), &dynamodbsdk.UpdateItemInput{
		TableName: aws.String("gsi-move"),
		Key: map[string]dynamodbtypes.AttributeValue{
			"pk": &dynamodbtypes.AttributeValueMemberS{Value: "p1"},
			"sk": &dynamodbtypes.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression: aws.String("SET gpk = :g"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":g": &dynamodbtypes.AttributeValueMemberS{Value: "new"},
		},
	})
	require.NoError(t, err)

	require.Equal(t, 0, queryGpk("old"), "item must leave its old GSI key")
	require.Equal(t, 1, queryGpk("new"), "item must be findable under its new GSI key")
}
