package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestQueryScan_AttributesToGet_SurvivesWireConversion proves that the legacy
// AttributesToGet projection parameter (still a real, wire-serialized field on
// both QueryInput and ScanInput per dynamodb@v1.63.1's api_op_Query.go:92 and
// api_op_Scan.go) reaches the backend. models.QueryInput/models.ScanInput
// previously had no AttributesToGet field at all, so the JSON key was dropped
// silently by json.Unmarshal and the backend's own AttributesToGet-aware
// projection logic (item_ops_query.go, item_ops_scan.go) could never see it.
func TestQueryScan_AttributesToGet_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	ctx := t.Context()

	tableName := "attrs-to-get-table"
	_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodbsdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk":     &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			"wanted": &dynamodbtypes.AttributeValueMemberS{Value: "keep-me"},
			"other":  &dynamodbtypes.AttributeValueMemberS{Value: "drop-me"},
		},
	})
	require.NoError(t, err)

	t.Run("query", func(t *testing.T) {
		t.Parallel()

		out, queryErr := client.Query(ctx, &dynamodbsdk.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			AttributesToGet: []string{"pk", "wanted"},
		})
		require.NoError(t, queryErr)
		require.Len(t, out.Items, 1)
		_, hasOther := out.Items[0]["other"]
		require.False(t, hasOther, "AttributesToGet should have excluded 'other'")
		_, hasWanted := out.Items[0]["wanted"]
		require.True(t, hasWanted, "AttributesToGet should have kept 'wanted'")
	})

	t.Run("scan", func(t *testing.T) {
		t.Parallel()

		out, scanErr := client.Scan(ctx, &dynamodbsdk.ScanInput{
			TableName:       aws.String(tableName),
			AttributesToGet: []string{"pk", "wanted"},
		})
		require.NoError(t, scanErr)
		require.Len(t, out.Items, 1)
		_, hasOther := out.Items[0]["other"]
		require.False(t, hasOther, "AttributesToGet should have excluded 'other'")
		_, hasWanted := out.Items[0]["wanted"]
		require.True(t, hasWanted, "AttributesToGet should have kept 'wanted'")
	})
}
