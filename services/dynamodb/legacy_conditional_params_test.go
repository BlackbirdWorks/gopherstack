package dynamodb_test

import (
	"testing"

	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newLegacyParamsTestTable creates a fresh table with a single item
// {pk:"k1", n:5, tags:SS{"a","b"}} and returns the client and table name.
func newLegacyParamsTestTable(t *testing.T) (*dynamodbsdk.Client, string) {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	ctx := t.Context()

	tableName := "legacy-params-table"
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
			"pk":   &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			"n":    &dynamodbtypes.AttributeValueMemberN{Value: "5"},
			"tags": &dynamodbtypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
		},
	})
	require.NoError(t, err)

	return client, tableName
}

func requireConditionalCheckFailed(t *testing.T, err error) {
	t.Helper()

	var ccf *dynamodbtypes.ConditionalCheckFailedException
	require.ErrorAs(t, err, &ccf, "expected a real ConditionalCheckFailedException from the SDK deserializer")
}

func requireValidationException(t *testing.T, err error) {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "expected a smithy.APIError")
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
}

// TestPutItem_LegacyExpected proves the legacy Expected/ConditionalOperator
// parameters actually gate the write -- a caller relying on Expected for a
// conditional check must get ConditionalCheckFailedException (and the item
// must stay unchanged) when the condition doesn't hold, not a silently
// unconditional PutItem.
func TestPutItem_LegacyExpected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expected map[string]dynamodbtypes.ExpectedAttributeValue
		condOp   dynamodbtypes.ConditionalOperator
		name     string
		wantFail bool
	}{
		{
			name: "old style EQ pass",
			expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"n": {Value: &dynamodbtypes.AttributeValueMemberN{Value: "5"}},
			},
		},
		{
			name: "old style EQ fail",
			expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"n": {Value: &dynamodbtypes.AttributeValueMemberN{Value: "999"}},
			},
			wantFail: true,
		},
		{
			name: "Exists false fails when attribute present",
			expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"pk": {Exists: aws.Bool(false)},
			},
			wantFail: true,
		},
		{
			name: "ComparisonOperator GT pass",
			expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"n": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorGt,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
				},
			},
		},
		{
			name:   "ConditionalOperator OR one true",
			condOp: dynamodbtypes.ConditionalOperatorOr,
			expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"n":  {Value: &dynamodbtypes.AttributeValueMemberN{Value: "999"}},
				"pk": {Value: &dynamodbtypes.AttributeValueMemberS{Value: "k1"}},
			},
		},
		{
			name:   "ConditionalOperator AND one false",
			condOp: dynamodbtypes.ConditionalOperatorAnd,
			expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"n":  {Value: &dynamodbtypes.AttributeValueMemberN{Value: "5"}},
				"pk": {Value: &dynamodbtypes.AttributeValueMemberS{Value: "wrong"}},
			},
			wantFail: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, tableName := newLegacyParamsTestTable(t)
			ctx := t.Context()

			_, err := client.PutItem(ctx, &dynamodbsdk.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
					"n":  &dynamodbtypes.AttributeValueMemberN{Value: "42"},
				},
				Expected:            tc.expected,
				ConditionalOperator: tc.condOp,
			})

			out, getErr := client.GetItem(ctx, &dynamodbsdk.GetItemInput{
				TableName: aws.String(tableName),
				Key: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
				},
			})
			require.NoError(t, getErr)

			if tc.wantFail {
				requireConditionalCheckFailed(t, err)
				// The write must not have happened: n stays 5, not 42.
				nVal, ok := out.Item["n"].(*dynamodbtypes.AttributeValueMemberN)
				require.True(t, ok)
				require.Equal(t, "5", nVal.Value)

				return
			}

			require.NoError(t, err)
			nVal, ok := out.Item["n"].(*dynamodbtypes.AttributeValueMemberN)
			require.True(t, ok)
			require.Equal(t, "42", nVal.Value)
		})
	}
}

// TestDeleteItem_LegacyExpected proves a failed legacy-Expected condition
// blocks DeleteItem -- the item must still be readable afterwards.
func TestDeleteItem_LegacyExpected(t *testing.T) {
	t.Parallel()

	t.Run("condition fails, item survives", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyParamsTestTable(t)
		ctx := t.Context()

		_, err := client.DeleteItem(ctx, &dynamodbsdk.DeleteItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			Expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"n": {Value: &dynamodbtypes.AttributeValueMemberN{Value: "999"}},
			},
		})
		requireConditionalCheckFailed(t, err)

		out, getErr := client.GetItem(ctx, &dynamodbsdk.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
		})
		require.NoError(t, getErr)
		require.NotEmpty(t, out.Item, "item must not have been deleted")
	})

	t.Run("condition passes, item deleted", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyParamsTestTable(t)
		ctx := t.Context()

		_, err := client.DeleteItem(ctx, &dynamodbsdk.DeleteItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			Expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"n": {Value: &dynamodbtypes.AttributeValueMemberN{Value: "5"}},
			},
		})
		require.NoError(t, err)

		out, getErr := client.GetItem(ctx, &dynamodbsdk.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
		})
		require.NoError(t, getErr)
		require.Empty(t, out.Item, "item must have been deleted")
	})
}

// TestUpdateItem_LegacyExpected proves the conditional-check-bypass failure
// mode named in gopherstack-lze5: a caller passing Expected on UpdateItem
// must have the write rejected (not silently applied) when the condition
// doesn't hold.
func TestUpdateItem_LegacyExpected(t *testing.T) {
	t.Parallel()

	t.Run("condition fails, update rejected", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyParamsTestTable(t)
		ctx := t.Context()

		_, err := client.UpdateItem(ctx, &dynamodbsdk.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			AttributeUpdates: map[string]dynamodbtypes.AttributeValueUpdate{
				"n": {
					Action: dynamodbtypes.AttributeActionPut,
					Value:  &dynamodbtypes.AttributeValueMemberN{Value: "999"},
				},
			},
			Expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"n": {Value: &dynamodbtypes.AttributeValueMemberN{Value: "not-five"}},
			},
		})
		requireConditionalCheckFailed(t, err)

		out, getErr := client.GetItem(ctx, &dynamodbsdk.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
		})
		require.NoError(t, getErr)
		nVal, ok := out.Item["n"].(*dynamodbtypes.AttributeValueMemberN)
		require.True(t, ok)
		require.Equal(t, "5", nVal.Value, "the conditional write must not have applied")
	})
}

// TestUpdateItem_LegacyAttributeUpdates proves each AttributeUpdates action
// actually mutates the item -- not just that the call succeeds (the failure
// mode named in gopherstack-lze5: a no-op UpdateItem that returns 200 with no
// effect).
func TestUpdateItem_LegacyAttributeUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updates map[string]dynamodbtypes.AttributeValueUpdate
		check   func(t *testing.T, item map[string]dynamodbtypes.AttributeValue)
		name    string
	}{
		{
			name: "PUT sets attribute",
			updates: map[string]dynamodbtypes.AttributeValueUpdate{
				"n": {
					Action: dynamodbtypes.AttributeActionPut,
					Value:  &dynamodbtypes.AttributeValueMemberN{Value: "100"},
				},
			},
			check: func(t *testing.T, item map[string]dynamodbtypes.AttributeValue) {
				t.Helper()
				v, ok := item["n"].(*dynamodbtypes.AttributeValueMemberN)
				require.True(t, ok)
				require.Equal(t, "100", v.Value)
			},
		},
		{
			name: "DELETE without value removes attribute",
			updates: map[string]dynamodbtypes.AttributeValueUpdate{
				"n": {Action: dynamodbtypes.AttributeActionDelete},
			},
			check: func(t *testing.T, item map[string]dynamodbtypes.AttributeValue) {
				t.Helper()
				_, has := item["n"]
				require.False(t, has, "n should have been removed")
			},
		},
		{
			name: "ADD on number increments",
			updates: map[string]dynamodbtypes.AttributeValueUpdate{
				"n": {
					Action: dynamodbtypes.AttributeActionAdd,
					Value:  &dynamodbtypes.AttributeValueMemberN{Value: "3"},
				},
			},
			check: func(t *testing.T, item map[string]dynamodbtypes.AttributeValue) {
				t.Helper()
				v, ok := item["n"].(*dynamodbtypes.AttributeValueMemberN)
				require.True(t, ok)
				require.Equal(t, "8", v.Value)
			},
		},
		{
			name: "ADD on set unions",
			updates: map[string]dynamodbtypes.AttributeValueUpdate{
				"tags": {
					Action: dynamodbtypes.AttributeActionAdd,
					Value:  &dynamodbtypes.AttributeValueMemberSS{Value: []string{"c"}},
				},
			},
			check: func(t *testing.T, item map[string]dynamodbtypes.AttributeValue) {
				t.Helper()
				v, ok := item["tags"].(*dynamodbtypes.AttributeValueMemberSS)
				require.True(t, ok)
				require.ElementsMatch(t, []string{"a", "b", "c"}, v.Value)
			},
		},
		{
			name: "DELETE with set value subtracts",
			updates: map[string]dynamodbtypes.AttributeValueUpdate{
				"tags": {
					Action: dynamodbtypes.AttributeActionDelete,
					Value:  &dynamodbtypes.AttributeValueMemberSS{Value: []string{"a"}},
				},
			},
			check: func(t *testing.T, item map[string]dynamodbtypes.AttributeValue) {
				t.Helper()
				v, ok := item["tags"].(*dynamodbtypes.AttributeValueMemberSS)
				require.True(t, ok)
				require.ElementsMatch(t, []string{"b"}, v.Value)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, tableName := newLegacyParamsTestTable(t)
			ctx := t.Context()

			_, err := client.UpdateItem(ctx, &dynamodbsdk.UpdateItemInput{
				TableName: aws.String(tableName),
				Key: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
				},
				AttributeUpdates: tc.updates,
			})
			require.NoError(t, err)

			out, getErr := client.GetItem(ctx, &dynamodbsdk.GetItemInput{
				TableName: aws.String(tableName),
				Key: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
				},
			})
			require.NoError(t, getErr)
			tc.check(t, out.Item)
		})
	}
}

// TestLegacyParams_MutualExclusion proves AWS's rule that a request may not
// mix legacy (Expected/ConditionalOperator/AttributeUpdates) and modern
// expression (ConditionExpression/UpdateExpression) parameters.
func TestLegacyParams_MutualExclusion(t *testing.T) {
	t.Parallel()

	t.Run("PutItem Expected plus ConditionExpression", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyParamsTestTable(t)

		_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			ConditionExpression: aws.String("attribute_exists(pk)"),
			Expected: map[string]dynamodbtypes.ExpectedAttributeValue{
				"pk": {Value: &dynamodbtypes.AttributeValueMemberS{Value: "k1"}},
			},
		})
		requireValidationException(t, err)
	})

	t.Run("UpdateItem AttributeUpdates plus UpdateExpression", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyParamsTestTable(t)

		_, err := client.UpdateItem(t.Context(), &dynamodbsdk.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			UpdateExpression: aws.String("SET n = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":v": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
			},
			AttributeUpdates: map[string]dynamodbtypes.AttributeValueUpdate{
				"n": {
					Action: dynamodbtypes.AttributeActionPut,
					Value:  &dynamodbtypes.AttributeValueMemberN{Value: "2"},
				},
			},
		})
		requireValidationException(t, err)
	})

	t.Run("ConditionalOperator without Expected", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyParamsTestTable(t)

		_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			ConditionalOperator: dynamodbtypes.ConditionalOperatorAnd,
		})
		requireValidationException(t, err)
	})
}
