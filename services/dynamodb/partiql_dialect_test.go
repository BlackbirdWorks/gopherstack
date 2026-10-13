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

type partiqlStep struct {
	check     func(t *testing.T, out *sdk.ExecuteStatementOutput)
	stmt      string
	wantErr   string
	params    []types.AttributeValue
	wantItems int
}

func newPartiQLDialectClient(t *testing.T) *sdk.Client {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String("Orders"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return client
}

func TestPartiQLDialect(t *testing.T) {
	t.Parallel()

	seed := partiqlStep{
		stmt: `INSERT INTO Orders VALUE {'pk':'p','sk':1,'v':10,'tags':<<'a','b'>>,` +
			`'l':[1,'two',{'k':true}],'m':{'x':null}}`,
	}

	tests := []struct {
		name  string
		steps []partiqlStep
	}{
		{
			name: "unquoted table select",
			steps: []partiqlStep{seed, {
				stmt: `SELECT pk, v FROM Orders WHERE pk = ? AND sk >= 0`, wantItems: 1,
				params: []types.AttributeValue{&types.AttributeValueMemberS{Value: "p"}},
			}},
		},
		{
			name: "collection literals round trip",
			steps: []partiqlStep{seed, {
				stmt: `SELECT * FROM Orders WHERE pk = 'p'`, wantItems: 1,
				check: func(t *testing.T, out *sdk.ExecuteStatementOutput) {
					t.Helper()

					item := out.Items[0]
					assert.Equal(t, &types.AttributeValueMemberSS{Value: []string{"a", "b"}}, item["tags"])
					assert.IsType(t, &types.AttributeValueMemberL{}, item["l"])
					assert.IsType(t, &types.AttributeValueMemberM{}, item["m"])
				},
			}},
		},
		{
			name: "in list and numeric filter",
			steps: []partiqlStep{seed, {
				stmt: `SELECT pk FROM Orders WHERE pk IN ['p', 'q'] AND v > 5`, wantItems: 1,
			}, {
				stmt: `SELECT pk FROM Orders WHERE pk IN ['p', 'q'] AND v > 50`, wantItems: 0,
			}},
		},
		{
			name: "update literal key returning and set add",
			steps: []partiqlStep{seed, {
				stmt: `UPDATE Orders SET v = v + 1 SET tags = set_add(tags, <<'c'>>) ` +
					`WHERE pk = 'p' AND sk = 1 RETURNING ALL NEW *`,
				wantItems: 1,
				check: func(t *testing.T, out *sdk.ExecuteStatementOutput) {
					t.Helper()

					assert.Equal(t, &types.AttributeValueMemberN{Value: "11"}, out.Items[0]["v"])
					assert.Equal(t, &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}}, out.Items[0]["tags"])
				},
			}},
		},
		{
			name: "update missing item fails",
			steps: []partiqlStep{{
				stmt:    `UPDATE Orders SET v = 1 WHERE pk = 'nope' AND sk = 1`,
				wantErr: "ConditionalCheckFailedException",
			}},
		},
		{
			name: "update extra condition",
			steps: []partiqlStep{seed, {
				stmt:    `UPDATE Orders SET v = 99 WHERE pk = 'p' AND sk = 1 AND v > 100`,
				wantErr: "ConditionalCheckFailedException",
			}, {
				stmt: `UPDATE Orders SET v = 99 WHERE pk = 'p' AND sk = 1 AND v < 100 ` +
					`RETURNING MODIFIED OLD *`,
				wantItems: 1,
				check: func(t *testing.T, out *sdk.ExecuteStatementOutput) {
					t.Helper()

					assert.Equal(t, &types.AttributeValueMemberN{Value: "10"}, out.Items[0]["v"])
				},
			}},
		},
		{
			name: "delete returning old",
			steps: []partiqlStep{seed, {
				stmt: `DELETE FROM Orders WHERE pk = 'p' AND sk = 1 RETURNING ALL OLD *`, wantItems: 1,
			}, {
				stmt: `SELECT * FROM Orders WHERE pk = 'p'`, wantItems: 0,
			}},
		},
		{
			name: "reserved word attributes",
			steps: []partiqlStep{
				{stmt: `INSERT INTO Orders VALUE {'pk':'p','sk':1,'name':'n','status':'new'}`},
				{stmt: `SELECT name, status FROM Orders WHERE name = 'n'`, wantItems: 1},
				{stmt: `SELECT * FROM Orders WHERE pk = 'p' AND "status" = 'new'`, wantItems: 1},
				{stmt: `UPDATE Orders SET status = 'done' REMOVE name WHERE pk = 'p' AND sk = 1 AND status = 'new'`},
				{
					stmt:      `DELETE FROM Orders WHERE pk = 'p' AND sk = 1 AND status = 'done' RETURNING ALL OLD *`,
					wantItems: 1,
				},
			},
		},
		{
			name: "duplicate insert",
			steps: []partiqlStep{seed, {
				stmt: `INSERT INTO Orders VALUE {'pk':'p','sk':1}`, wantErr: "DuplicateItemException",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newPartiQLDialectClient(t)

			for _, step := range tt.steps {
				out, err := client.ExecuteStatement(t.Context(), &sdk.ExecuteStatementInput{
					Statement: aws.String(step.stmt), Parameters: step.params,
				})

				if step.wantErr != "" {
					var apiErr smithy.APIError

					require.ErrorAs(t, err, &apiErr, step.stmt)
					assert.Equal(t, step.wantErr, apiErr.ErrorCode())

					continue
				}

				require.NoError(t, err, step.stmt)
				assert.Len(t, out.Items, step.wantItems, step.stmt)

				if step.check != nil {
					step.check(t, out)
				}
			}
		})
	}
}

func TestBatchExecuteStatementErrorCodes(t *testing.T) {
	t.Parallel()

	client := newPartiQLDialectClient(t)

	out, err := client.BatchExecuteStatement(t.Context(), &sdk.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{
			{Statement: aws.String(`SELECT * FROM Missing WHERE pk = 'a' AND sk = 1`)},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Responses, 1)
	require.NotNil(t, out.Responses[0].Error)
	assert.Equal(t, types.BatchStatementErrorCodeEnumResourceNotFound, out.Responses[0].Error.Code)
}
