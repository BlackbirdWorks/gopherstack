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

const (
	partiqlIdxTable = "PIdx"
	partiqlIdxGSI   = "GSI1"
	partiqlIdxLSI   = "LSI1"
)

// newPartiQLIndexClient stands up a real SDK client over a table with one
// GSI (partition key gsipk) and one LSI (partition key pk, sort key lsisk),
// both KEYS_ONLY -- so a SELECT that lands on the index (Query or Scan) can
// only ever see pk/sk/gsipk/lsisk, never "payload". That makes projection
// enforcement an observable pass/fail signal, not just an item-count check.
func newPartiQLIndexClient(t *testing.T) *sdk.Client {
	t.Helper()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))

	keysOnly := &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly}

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String(partiqlIdxTable),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsipk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("lsisk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(partiqlIdxGSI),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsipk"), KeyType: types.KeyTypeHash},
				},
				Projection: keysOnly,
			},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(partiqlIdxLSI),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("lsisk"), KeyType: types.KeyTypeRange},
				},
				Projection: keysOnly,
			},
		},
	})
	require.NoError(t, err)

	items := []map[string]types.AttributeValue{
		{
			"pk": &types.AttributeValueMemberS{Value: "p1"}, "sk": &types.AttributeValueMemberS{Value: "s1"},
			"gsipk": &types.AttributeValueMemberS{Value: "g1"}, "lsisk": &types.AttributeValueMemberS{Value: "l1"},
			"payload": &types.AttributeValueMemberS{Value: "A"},
		},
		{
			"pk": &types.AttributeValueMemberS{Value: "p1"}, "sk": &types.AttributeValueMemberS{Value: "s2"},
			"gsipk": &types.AttributeValueMemberS{Value: "g1"}, "lsisk": &types.AttributeValueMemberS{Value: "l2"},
			"payload": &types.AttributeValueMemberS{Value: "B"},
		},
		{
			"pk": &types.AttributeValueMemberS{Value: "p2"}, "sk": &types.AttributeValueMemberS{Value: "s1"},
			"gsipk": &types.AttributeValueMemberS{Value: "g2"}, "lsisk": &types.AttributeValueMemberS{Value: "l1"},
			"payload": &types.AttributeValueMemberS{Value: "C"},
		},
	}

	for _, item := range items {
		_, putErr := client.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String(partiqlIdxTable),
			Item:      item,
		})
		require.NoError(t, putErr)
	}

	return client
}

func partiqlIdxPKs(t *testing.T, items []map[string]types.AttributeValue) []string {
	t.Helper()

	pks := make([]string, 0, len(items))
	for _, item := range items {
		s, ok := item["pk"].(*types.AttributeValueMemberS)
		require.True(t, ok, "item missing string pk: %#v", item)
		pks = append(pks, s.Value)
	}

	return pks
}

func TestExecuteStatement_IndexSelect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statement string
		wantPKs   []string
	}{
		{
			name:      "gsi partition key",
			statement: `SELECT * FROM "PIdx"."GSI1" WHERE gsipk = 'g1'`,
			wantPKs:   []string{"p1", "p1"},
		},
		{
			name:      "lsi partition key",
			statement: `SELECT * FROM "PIdx"."LSI1" WHERE pk = 'p1'`,
			wantPKs:   []string{"p1", "p1"},
		},
		{
			name:      "gsi non-key attribute",
			statement: `SELECT * FROM "PIdx"."GSI1" WHERE sk = 's1'`,
			wantPKs:   []string{"p1", "p2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newPartiQLIndexClient(t)

			out, err := client.ExecuteStatement(t.Context(), &sdk.ExecuteStatementInput{
				Statement: aws.String(tc.statement),
			})
			require.NoError(t, err)

			assert.ElementsMatch(t, tc.wantPKs, partiqlIdxPKs(t, out.Items))

			for _, item := range out.Items {
				assert.NotContains(t, item, "payload", "KEYS_ONLY projection must not leak non-key attributes")
			}
		})
	}
}

func TestExecuteStatement_IndexSelect_ConsumedCapacity(t *testing.T) {
	t.Parallel()

	client := newPartiQLIndexClient(t)

	out, err := client.ExecuteStatement(t.Context(), &sdk.ExecuteStatementInput{
		Statement:              aws.String(`SELECT * FROM "PIdx"."GSI1" WHERE gsipk = 'g1'`),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ConsumedCapacity)

	assert.Nil(t, out.ConsumedCapacity.Table, "RCU must not be attributed to the table")
	require.Contains(t, out.ConsumedCapacity.GlobalSecondaryIndexes, partiqlIdxGSI)
	assert.Positive(t, *out.ConsumedCapacity.GlobalSecondaryIndexes[partiqlIdxGSI].CapacityUnits)
}

func TestExecuteStatement_UnknownIndex(t *testing.T) {
	t.Parallel()

	client := newPartiQLIndexClient(t)

	_, err := client.ExecuteStatement(t.Context(), &sdk.ExecuteStatementInput{
		Statement: aws.String(`SELECT * FROM "PIdx"."NoSuchIndex" WHERE gsipk = 'g1'`),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

func TestExecuteStatement_InsertRejectsIndex(t *testing.T) {
	t.Parallel()

	client := newPartiQLIndexClient(t)

	_, err := client.ExecuteStatement(t.Context(), &sdk.ExecuteStatementInput{
		Statement: aws.String(`INSERT INTO "PIdx"."GSI1" VALUE {'pk': 'p9', 'sk': 's9'}`),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())

	// The rejected INSERT must not have silently landed on the base table.
	got, getErr := client.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String(partiqlIdxTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p9"},
			"sk": &types.AttributeValueMemberS{Value: "s9"},
		},
	})
	require.NoError(t, getErr)
	assert.Empty(t, got.Item)
}
