package dynamodb_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

const (
	ccGSIName = "gsi1"
	ccLSIName = "lsi1"
)

// ccTableInput returns a CreateTableInput for a pk/sk table with one GSI (on
// gsi_pk) and one LSI (on pk + lsi_sk) -- the same shape
// TestExecuteTransaction_ConsumedCapacity_TableDriven uses.
func ccTableInput(tableName string) *sdk.CreateTableInput {
	return &sdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("lsi_sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(ccGSIName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(ccLSIName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("lsi_sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	}
}

// TestExecuteStatement_ConsumedCapacity drives ExecuteStatement through the
// real typed client and checks that ReturnConsumedCapacity=INDEXES/TOTAL/NONE
// each produce distinct output (dynamodb SDK v1.67.0
// api_op_ExecuteStatement.go: ExecuteStatementOutput.ConsumedCapacity is a
// single *types.ConsumedCapacity, unlike BatchExecuteStatement's list).
//
// PartiQL SELECT can only ever Query by the table's own primary key here --
// partiql.go's tryQueryOptimization has no IndexName support, so a SELECT can
// never target a GSI/LSI. The insert case is what proves INDEXES' GSI/LSI
// attribution instead, the same way
// TestExecuteTransaction_ConsumedCapacity_TableDriven proves it for writes.
func TestExecuteStatement_ConsumedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  func(t *testing.T, cc *types.ConsumedCapacity)
		reqCC types.ReturnConsumedCapacity
		name  string
		table string
		read  bool
	}{
		{
			name:  "insert indexes",
			table: "ESCC_InsertIndexes",
			reqCC: types.ReturnConsumedCapacityIndexes,
			want: func(t *testing.T, cc *types.ConsumedCapacity) {
				t.Helper()
				require.NotNil(t, cc)
				assert.NotNil(t, cc.Table)
				assert.Contains(t, cc.GlobalSecondaryIndexes, ccGSIName)
				assert.Contains(t, cc.LocalSecondaryIndexes, ccLSIName)
			},
		},
		{
			name:  "select indexes",
			table: "ESCC_SelectIndexes",
			reqCC: types.ReturnConsumedCapacityIndexes,
			read:  true,
			want: func(t *testing.T, cc *types.ConsumedCapacity) {
				t.Helper()
				require.NotNil(t, cc)
				assert.NotNil(t, cc.Table)
				assert.Nil(t, cc.GlobalSecondaryIndexes)
				assert.Nil(t, cc.LocalSecondaryIndexes)
			},
		},
		{
			name:  "total",
			table: "ESCC_Total",
			reqCC: types.ReturnConsumedCapacityTotal,
			want: func(t *testing.T, cc *types.ConsumedCapacity) {
				t.Helper()
				require.NotNil(t, cc)
				assert.Positive(t, aws.ToFloat64(cc.CapacityUnits))
				assert.Nil(t, cc.Table)
				assert.Nil(t, cc.GlobalSecondaryIndexes)
				assert.Nil(t, cc.LocalSecondaryIndexes)
			},
		},
		{
			name:  "none",
			table: "ESCC_None",
			reqCC: types.ReturnConsumedCapacityNone,
			want: func(t *testing.T, cc *types.ConsumedCapacity) {
				t.Helper()
				assert.Nil(t, cc)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()

			_, err := client.CreateTable(ctx, ccTableInput(tt.table))
			require.NoError(t, err)

			var stmt string
			if tt.read {
				_, err = client.PutItem(ctx, &sdk.PutItemInput{
					TableName: aws.String(tt.table),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "k1"},
						"sk":  &types.AttributeValueMemberS{Value: "s1"},
						"val": &types.AttributeValueMemberS{Value: "seed"},
					},
				})
				require.NoError(t, err)

				stmt = fmt.Sprintf(`SELECT * FROM %q WHERE pk = 'k1' AND sk = 's1'`, tt.table)
			} else {
				stmt = fmt.Sprintf(
					`INSERT INTO %q VALUE {'pk':'k1','sk':'s1','gsi_pk':'g1','lsi_sk':'l1','val':'inserted'}`,
					tt.table,
				)
			}

			out, err := client.ExecuteStatement(ctx, &sdk.ExecuteStatementInput{
				Statement:              aws.String(stmt),
				ReturnConsumedCapacity: tt.reqCC,
			})
			require.NoError(t, err)

			tt.want(t, out.ConsumedCapacity)
		})
	}
}

// TestBatchExecuteStatement_ConsumedCapacity drives three statements across
// two tables through the real typed client, proving ConsumedCapacity is one
// entry per statement in request order -- not merged per table like
// BatchWriteItem -- matching dynamodb SDK v1.67.0
// api_op_BatchExecuteStatement.go:70-71 ("ordered according to the ordering
// of the statements", the same phrasing and ordering ExecuteTransaction uses).
func TestBatchExecuteStatement_ConsumedCapacity(t *testing.T) {
	t.Parallel()

	t.Run("indexes across two tables", func(t *testing.T) {
		t.Parallel()

		client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
		ctx := t.Context()

		const tableA, tableB = "BECC_A", "BECC_B"

		for _, tbl := range []string{tableA, tableB} {
			_, err := client.CreateTable(ctx, ccTableInput(tbl))
			require.NoError(t, err)
		}

		// UPDATE target on tableA: has lsi_sk, no gsi_pk.
		_, err := client.PutItem(ctx, &sdk.PutItemInput{
			TableName: aws.String(tableA),
			Item: map[string]types.AttributeValue{
				"pk":     &types.AttributeValueMemberS{Value: "k2"},
				"sk":     &types.AttributeValueMemberS{Value: "s2"},
				"lsi_sk": &types.AttributeValueMemberS{Value: "l2"},
				"val":    &types.AttributeValueMemberS{Value: "before"},
			},
		})
		require.NoError(t, err)

		insertA := fmt.Sprintf(
			`INSERT INTO %q VALUE {'pk':'k1','sk':'s1','gsi_pk':'g1','lsi_sk':'l1','val':'inserted'}`,
			tableA,
		)
		insertB := fmt.Sprintf(
			`INSERT INTO %q VALUE {'pk':'k1','sk':'s1','gsi_pk':'g1','val':'inserted'}`,
			tableB,
		)
		updateA := fmt.Sprintf(`UPDATE %q SET val = 'updated' WHERE pk = 'k2' AND sk = 's2'`, tableA)

		out, err := client.BatchExecuteStatement(ctx, &sdk.BatchExecuteStatementInput{
			Statements: []types.BatchStatementRequest{
				{Statement: aws.String(insertA)},
				{Statement: aws.String(insertB)},
				{Statement: aws.String(updateA)},
			},
			ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
		})
		require.NoError(t, err)
		require.Len(t, out.Responses, 3)
		require.Len(t, out.ConsumedCapacity, 3)

		cc0, cc1, cc2 := out.ConsumedCapacity[0], out.ConsumedCapacity[1], out.ConsumedCapacity[2]

		assert.Equal(t, tableA, aws.ToString(cc0.TableName), "statement 0: INSERT into tableA")
		assert.NotNil(t, cc0.Table)
		assert.Contains(t, cc0.GlobalSecondaryIndexes, ccGSIName)
		assert.Contains(t, cc0.LocalSecondaryIndexes, ccLSIName)

		assert.Equal(t, tableB, aws.ToString(cc1.TableName), "statement 1: INSERT into tableB")
		assert.NotNil(t, cc1.Table)
		assert.Contains(t, cc1.GlobalSecondaryIndexes, ccGSIName)
		assert.Nil(t, cc1.LocalSecondaryIndexes)

		assert.Equal(t, tableA, aws.ToString(cc2.TableName), "statement 2: UPDATE on tableA")
		assert.NotNil(t, cc2.Table)
		assert.Nil(t, cc2.GlobalSecondaryIndexes)
		assert.Contains(t, cc2.LocalSecondaryIndexes, ccLSIName)
	})

	t.Run("none", func(t *testing.T) {
		t.Parallel()

		client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
		ctx := t.Context()

		const tableName = "BECC_None"

		_, err := client.CreateTable(ctx, ccTableInput(tableName))
		require.NoError(t, err)

		stmt := fmt.Sprintf(`INSERT INTO %q VALUE {'pk':'k1','sk':'s1','val':'x'}`, tableName)

		out, err := client.BatchExecuteStatement(ctx, &sdk.BatchExecuteStatementInput{
			Statements:             []types.BatchStatementRequest{{Statement: aws.String(stmt)}},
			ReturnConsumedCapacity: types.ReturnConsumedCapacityNone,
		})
		require.NoError(t, err)
		assert.Empty(t, out.ConsumedCapacity)
	})
}
