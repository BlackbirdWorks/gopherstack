package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newFollowupClient stands up a real SDK client over a pk/sk table named
// pfTableName ("PFTable"), used by the gopherstack-tjgbr follow-up tests
// below (reserved words, batch/transaction read-write mixing, standalone
// EXISTS).
const pfTableName = "PFTable"

func newFollowupClient(t *testing.T) *sdk.Client {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String(pfTableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return client
}

// requireValidationExceptionMsg asserts err round-tripped through the real SDK
// deserializer as a ValidationException, and that its message contains
// wantMsgSubstr. Every gopherstack-tjgbr follow-up rejection is a
// ValidationException, so the error code is fixed rather than a parameter.
// (legacy_conditional_params_test.go's requireValidationException checks only
// the code, not the message -- kept separate rather than widening it.)
func requireValidationExceptionMsg(t *testing.T, err error, wantMsgSubstr string) {
	t.Helper()

	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())
	assert.Contains(t, apiErr.ErrorMessage(), wantMsgSubstr)
}

// TestReservedWord_RealClient item 1 (gopherstack-tjgbr): DynamoDB rejects a
// bare (unescaped) reserved-word attribute name in an expression with
// "Attribute name is a reserved keyword; reserved keyword: <WORD>" --
// confirmed via https://github.com/aws/aws-sdk-php/issues/1233 (the developer
// guide documents the rule -- ReservedWords.html,
// Expressions.ExpressionAttributeNames.html -- but not this verbatim string).
// Driven through the real aws-sdk-go-v2 client so the error round-trips via
// the SDK's own deserializer.
func TestReservedWord_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		run     func(t *testing.T, client *sdk.Client) error
		wantMsg string
	}{
		{
			name: "UpdateExpression bare reserved word",
			run: func(t *testing.T, client *sdk.Client) error {
				t.Helper()
				_, err := client.UpdateItem(t.Context(), &sdk.UpdateItemInput{
					TableName: aws.String(pfTableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "rw1"},
						"sk": &types.AttributeValueMemberS{Value: "s1"},
					},
					UpdateExpression: aws.String("SET status = :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":v": &types.AttributeValueMemberS{Value: "active"},
					},
				})

				return err
			},
			wantMsg: "Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: status",
		},
		{
			name: "ConditionExpression bare reserved word, nested path",
			run: func(t *testing.T, client *sdk.Client) error {
				t.Helper()
				_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String(pfTableName),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "rw2"},
						"sk": &types.AttributeValueMemberS{Value: "s1"},
					},
					ConditionExpression: aws.String("a.SIZE = :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":v": &types.AttributeValueMemberN{Value: "1"},
					},
				})

				return err
			},
			wantMsg: "Invalid ConditionExpression: Attribute name is a reserved keyword; reserved keyword: SIZE",
		},
		{
			name: "escaped reserved word via ExpressionAttributeNames succeeds",
			run: func(t *testing.T, client *sdk.Client) error {
				t.Helper()
				_, err := client.UpdateItem(t.Context(), &sdk.UpdateItemInput{
					TableName: aws.String(pfTableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "rw3"},
						"sk": &types.AttributeValueMemberS{Value: "s1"},
					},
					UpdateExpression:         aws.String("SET #s = :v"),
					ExpressionAttributeNames: map[string]string{"#s": "status"},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":v": &types.AttributeValueMemberS{Value: "active"},
					},
				})

				return err
			},
			wantMsg: "",
		},
	}

	client := newFollowupClient(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run(t, client)
			if tt.wantMsg == "" {
				require.NoError(t, err)

				return
			}

			requireValidationExceptionMsg(t, err, tt.wantMsg)
		})
	}
}

// TestBatchExecuteStatement_ReadWriteMix_RealClient item 2 (gopherstack-tjgbr):
// "The entire batch must consist of either read statements or write
// statements, you cannot mix both in one batch."
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html
func TestBatchExecuteStatement_ReadWriteMix_RealClient(t *testing.T) {
	t.Parallel()

	client := newFollowupClient(t)

	_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(pfTableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "mix1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	_, err = client.BatchExecuteStatement(t.Context(), &sdk.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{
			{
				Statement: aws.String(
					`SELECT * FROM "` + pfTableName + `" WHERE pk = 'mix1' AND sk = 's1'`,
				),
			},
			{
				Statement: aws.String(
					`INSERT INTO "` + pfTableName + `" VALUE {'pk': 'mix2', 'sk': 's1'}`,
				),
			},
		},
	})

	requireValidationExceptionMsg(t, err,
		"The entire batch must consist of either read statements or write statements, "+
			"you cannot mix both in one batch")
}

// TestBatchExecuteStatement_SelectMustBeFullyKeyed_RealClient item 2
// (gopherstack-tjgbr): "Each read statement in a BatchExecuteStatement must
// specify an equality condition on all key attributes. This enforces that
// each SELECT statement in a batch returns at most a single item."
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html
func TestBatchExecuteStatement_SelectMustBeFullyKeyed_RealClient(t *testing.T) {
	t.Parallel()

	client := newFollowupClient(t)

	_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(pfTableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "keyed1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	// Missing the sort-key equality -- under-specified for a composite key
	// table.
	_, err = client.BatchExecuteStatement(t.Context(), &sdk.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{
			{Statement: aws.String(`SELECT * FROM "` + pfTableName + `" WHERE pk = 'keyed1'`)},
		},
	})

	requireValidationExceptionMsg(t, err,
		"Each read statement in a BatchExecuteStatement must specify an equality "+
			"condition on all key attributes")
}

// TestBatchExecuteStatement_SelectFullyKeyed_Succeeds_RealClient is the
// positive counterpart to the above: a SELECT with an equality condition on
// every key attribute is a valid batch read.
func TestBatchExecuteStatement_SelectFullyKeyed_Succeeds_RealClient(t *testing.T) {
	t.Parallel()

	client := newFollowupClient(t)

	_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(pfTableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "keyed2"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	out, err := client.BatchExecuteStatement(t.Context(), &sdk.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{
			{
				Statement: aws.String(
					`SELECT * FROM "` + pfTableName + `" WHERE pk = 'keyed2' AND sk = 's1'`,
				),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Responses, 1)
	assert.NotNil(t, out.Responses[0].Item)
}

// TestExecuteTransaction_ReadWriteMix_RealClient item 2 (gopherstack-tjgbr):
// "The entire transaction must consist of either read statements or write
// statements. You can't mix both in one transaction. The EXISTS function is
// an exception."
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.multiplestatements.transactions.html
func TestExecuteTransaction_ReadWriteMix_RealClient(t *testing.T) {
	t.Parallel()

	client := newFollowupClient(t)

	_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(pfTableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "txmix1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	_, err = client.ExecuteTransaction(t.Context(), &sdk.ExecuteTransactionInput{
		TransactStatements: []types.ParameterizedStatement{
			{
				Statement: aws.String(
					`SELECT * FROM "` + pfTableName + `" WHERE pk = 'txmix1' AND sk = 's1'`,
				),
			},
			{
				Statement: aws.String(
					`UPDATE "` + pfTableName + `" SET phase = 'x' WHERE pk = 'txmix1' AND sk = 's1'`,
				),
			},
		},
	})

	requireValidationExceptionMsg(t, err,
		"The entire transaction must consist of either read statements or write "+
			"statements. You can't mix both in one transaction")
}

// TestExecuteTransaction_ExistsWithWrite_StillSucceeds_RealClient confirms
// the documented EXISTS exception to the transaction mixing rule still works
// after the item-2 mixing check was added: EXISTS alongside a write statement
// is not "mixing reads and writes".
func TestExecuteTransaction_ExistsWithWrite_StillSucceeds_RealClient(t *testing.T) {
	t.Parallel()

	client := newFollowupClient(t)

	_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(pfTableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "txexists1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	out, err := client.ExecuteTransaction(t.Context(), &sdk.ExecuteTransactionInput{
		TransactStatements: []types.ParameterizedStatement{
			{
				Statement: aws.String(
					`EXISTS(SELECT * FROM "` + pfTableName + `" WHERE pk = 'txexists1' AND sk = 's1')`,
				),
			},
			{
				Statement: aws.String(
					`UPDATE "` + pfTableName + `" SET phase = 'y' WHERE pk = 'txexists1' AND sk = 's1'`,
				),
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.Responses, 2)
}

// TestExecuteStatement_ExistsStandalone_Rejected_RealClient item 3
// (gopherstack-tjgbr): "The EXISTS function can only be used in
// transactions." / "This function can only be used in transactional
// operations."
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-functions.exists.html
func TestExecuteStatement_ExistsStandalone_Rejected_RealClient(t *testing.T) {
	t.Parallel()

	client := newFollowupClient(t)

	_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(pfTableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "exstd1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	_, err = client.ExecuteStatement(t.Context(), &sdk.ExecuteStatementInput{
		Statement: aws.String(
			`EXISTS(SELECT * FROM "` + pfTableName + `" WHERE pk = 'exstd1' AND sk = 's1')`,
		),
	})

	requireValidationExceptionMsg(t, err,
		"The EXISTS function can only be used in transactions")
}

// TestBatchExecuteStatement_ExistsStandalone_Rejected_RealClient is the
// BatchExecuteStatement counterpart to the standalone-ExecuteStatement case
// above: EXISTS is documented as usable only inside a transaction, so it must
// also be rejected when it reaches BatchExecuteStatement's per-statement
// dispatch (the two share executeStatement -- see partiql.go).
func TestBatchExecuteStatement_ExistsStandalone_Rejected_RealClient(t *testing.T) {
	t.Parallel()

	client := newFollowupClient(t)

	_, err := client.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(pfTableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "exstd2"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	out, err := client.BatchExecuteStatement(t.Context(), &sdk.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{
			{
				Statement: aws.String(
					`EXISTS(SELECT * FROM "` + pfTableName + `" WHERE pk = 'exstd2' AND sk = 's1')`,
				),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Responses, 1)
	require.NotNil(t, out.Responses[0].Error)
	assert.Contains(t, aws.ToString(out.Responses[0].Error.Message),
		"The EXISTS function can only be used in transactions")
}
