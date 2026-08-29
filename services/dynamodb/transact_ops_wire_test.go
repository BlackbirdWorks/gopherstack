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

// TestTransactGetItems_ConsumedCapacity_SurvivesWireConversion verifies that
// TransactGetItems' ConsumedCapacity, computed by the backend in
// transact_ops.go's TransactGetItems, actually reaches the caller.
// models.TransactGetItemsOutput declares a ConsumedCapacity field, but
// FromSDKTransactGetItemsOutput never copied output.ConsumedCapacity onto it --
// declared on the wire struct, dropped by the converter.
func TestTransactGetItems_ConsumedCapacity_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	keySchema, attrDefs := wireTestKeySchema()

	_, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName:            aws.String("tgi-table"),
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		BillingMode:          dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
		TableName: aws.String("tgi-table"),
		Item: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)

	out, err := client.TransactGetItems(t.Context(), &dynamodbsdk.TransactGetItemsInput{
		TransactItems: []dynamodbtypes.TransactGetItem{
			{
				Get: &dynamodbtypes.Get{
					TableName: aws.String("tgi-table"),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "1"},
					},
				},
			},
		},
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	require.NoError(t, err)

	require.Len(t, out.ConsumedCapacity, 1, "ConsumedCapacity must survive the wire round-trip")
	assert.Positive(t, aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits))
}

// TestTransactWriteItems_ReusedTokenDifferentPayload_IdempotentParameterMismatch
// verifies that reusing a ClientRequestToken with a different set of
// TransactItems is rejected. dynamodb@v1.63.1 deserializers.go's
// awsAwsjson10_deserializeOpErrorTransactWriteItems switch models
// IdempotentParameterMismatchException specifically for this case; gopherstack's
// token-commit tracking (transact_ops.go's txnTokens) stored only an expiry, with
// no record of what the committed request actually contained, so any second call
// reusing a committed token was treated as a matching replay and returned a bare
// empty success -- even when the item set was entirely different.
func TestTransactWriteItems_ReusedTokenDifferentPayload_IdempotentParameterMismatch(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	keySchema, attrDefs := wireTestKeySchema()

	_, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName:            aws.String("twi-mismatch-table"),
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		BillingMode:          dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	const token = "reused-token-1"

	_, err = client.TransactWriteItems(t.Context(), &dynamodbsdk.TransactWriteItemsInput{
		ClientRequestToken: aws.String(token),
		TransactItems: []dynamodbtypes.TransactWriteItem{
			{
				Put: &dynamodbtypes.Put{
					TableName: aws.String("twi-mismatch-table"),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "a"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(t.Context(), &dynamodbsdk.TransactWriteItemsInput{
		ClientRequestToken: aws.String(token),
		TransactItems: []dynamodbtypes.TransactWriteItem{
			{
				Put: &dynamodbtypes.Put{
					TableName: aws.String("twi-mismatch-table"),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "b"},
					},
				},
			},
		},
	})
	require.Error(t, err)

	var mismatchErr *dynamodbtypes.IdempotentParameterMismatchException
	require.ErrorAs(
		t, err, &mismatchErr,
		"expected a real IdempotentParameterMismatchException from the SDK deserializer",
	)

	out, err := client.GetItem(t.Context(), &dynamodbsdk.GetItemInput{
		TableName: aws.String("twi-mismatch-table"),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{Value: "b"},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, out.Item, "the mismatched retry must not have been applied")
}
