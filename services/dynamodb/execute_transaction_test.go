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

func TestExecuteTransaction_EmptyStatements(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.ExecuteTransaction(t.Context(), &sdk.ExecuteTransactionInput{
		TransactStatements: []types.ParameterizedStatement{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one")
}

func TestExecuteTransaction_TooManyStatements(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	createSimplePPRTable(t, db, "TxnTable")

	stmts := make([]types.ParameterizedStatement, 101)
	for i := range stmts {
		stmt := fmt.Sprintf("SELECT * FROM \"TxnTable\" WHERE pk = '%d'", i)
		stmts[i] = types.ParameterizedStatement{Statement: &stmt}
	}

	_, err := db.ExecuteTransaction(t.Context(), &sdk.ExecuteTransactionInput{
		TransactStatements: stmts,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Too many statements")
}

func TestExecuteTransaction_ValidStatement(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	createSimplePPRTable(t, db, "TxnSelectTable")
	ctx := t.Context()

	// Pre-populate an item.
	_, err := db.PutItem(ctx, &sdk.PutItemInput{
		TableName: aws.String("TxnSelectTable"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "key1"},
		},
	})
	require.NoError(t, err)

	stmt := "SELECT * FROM \"TxnSelectTable\" WHERE pk = 'key1'"
	out, err := db.ExecuteTransaction(ctx, &sdk.ExecuteTransactionInput{
		TransactStatements: []types.ParameterizedStatement{
			{Statement: &stmt},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Responses, 1)
}

// TestExecuteTransaction_Atomicity verifies rollback when any statement fails.
func TestExecuteTransaction_Atomicity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantRollback map[string]string // pkVal → original val["S"] value to check
		seed         []map[string]any
		stmts        []string
		wantCode     int
	}{
		{
			name: "single statement succeeds",
			seed: []map[string]any{
				{"pk": map[string]string{"S": "x"}, "val": map[string]string{"S": "old"}},
			},
			stmts:    []string{`UPDATE "TXNTBL" SET val='new' WHERE pk='x'`},
			wantCode: 200,
		},
		{
			name: "second statement fails rolls back first",
			seed: []map[string]any{
				{"pk": map[string]string{"S": "a"}, "val": map[string]string{"S": "original"}},
			},
			stmts: []string{
				`UPDATE "TXNTBL" SET val='modified' WHERE pk='a'`,
				`UPDATE "DOES_NOT_EXIST" SET val='x' WHERE pk='a'`,
			},
			wantCode:     400,
			wantRollback: map[string]string{"a": "original"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerWithTable(t, "TXNTBL")
			for _, item := range tc.seed {
				seedItemViaHandler(t, h, "TXNTBL", item)
			}

			stmtList := make([]map[string]any, len(tc.stmts))
			for i, s := range tc.stmts {
				stmtList[i] = map[string]any{"Statement": s}
			}
			code, _ := invokeOp(t, h, "ExecuteTransaction", map[string]any{
				"TransactStatements": stmtList,
			})
			assert.Equal(t, tc.wantCode, code)

			for pkVal, origVal := range tc.wantRollback {
				item := getItemAttrsViaHandler(t, h, "TXNTBL", pkVal)
				require.NotNil(t, item, "item pk=%s must exist after rollback", pkVal)
				valAttr, ok := item["val"].(map[string]any)
				require.True(t, ok, "val must be a map")
				assert.Equal(t, origVal, valAttr["S"], "val must be rolled back for pk=%s", pkVal)
			}
		})
	}
}

// TestExecuteTransaction_ConsumedCapacity_TableDriven drives a mixed PartiQL
// transaction (INSERT, UPDATE, DELETE -- ExecuteTransaction cannot mix reads
// and writes in one transaction, dynamodb SDK api_op_ExecuteTransaction.go:14-17)
// through the real typed client and asserts:
//   - INDEXES returns one ConsumedCapacity entry per statement (not merged
//     per table, even though all three statements target the same table),
//     each carrying the correct GSI/LSI membership for that statement's item.
//   - the entries are ordered exactly as the statements were, not grouped.
//   - TOTAL returns the same entry count with no Table/GSI/LSI breakdown.
//   - NONE returns no ConsumedCapacity.
func TestExecuteTransaction_ConsumedCapacity_TableDriven(t *testing.T) {
	t.Parallel()

	const (
		gsiName = "gsi1"
		lsiName = "lsi1"
	)

	type want struct {
		wantGSI []bool
		wantLSI []bool
		wantLen int
		wantNil bool
	}

	tests := []struct {
		name  string
		reqCC types.ReturnConsumedCapacity
		want  want
	}{
		{
			name:  "indexes",
			reqCC: types.ReturnConsumedCapacityIndexes,
			want: want{
				wantLen: 3,
				// statement order: INSERT (both indexes), UPDATE (GSI only,
				// pre-seeded item has no lsi_sk), DELETE (LSI only,
				// pre-seeded item has no gsi_pk).
				wantGSI: []bool{true, true, false},
				wantLSI: []bool{true, false, true},
			},
		},
		{
			name:  "total",
			reqCC: types.ReturnConsumedCapacityTotal,
			want:  want{wantLen: 3},
		},
		{
			name:  "none",
			reqCC: types.ReturnConsumedCapacityNone,
			want:  want{wantNil: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))
			ctx := t.Context()

			tableName := "ExecTxnCC_" + tt.name

			_, err := client.CreateTable(ctx, &sdk.CreateTableInput{
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
						IndexName: aws.String(gsiName),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				LocalSecondaryIndexes: []types.LocalSecondaryIndex{
					{
						IndexName: aws.String(lsiName),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
							{AttributeName: aws.String("lsi_sk"), KeyType: types.KeyTypeRange},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			// UPDATE target: has gsi_pk, no lsi_sk.
			_, err = client.PutItem(ctx, &sdk.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"pk":     &types.AttributeValueMemberS{Value: "k2"},
					"sk":     &types.AttributeValueMemberS{Value: "s2"},
					"gsi_pk": &types.AttributeValueMemberS{Value: "g2"},
					"val":    &types.AttributeValueMemberS{Value: "before"},
				},
			})
			require.NoError(t, err)

			// DELETE target: has lsi_sk, no gsi_pk.
			_, err = client.PutItem(ctx, &sdk.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"pk":     &types.AttributeValueMemberS{Value: "k3"},
					"sk":     &types.AttributeValueMemberS{Value: "s3"},
					"lsi_sk": &types.AttributeValueMemberS{Value: "l3"},
					"val":    &types.AttributeValueMemberS{Value: "doomed"},
				},
			})
			require.NoError(t, err)

			insertStmt := fmt.Sprintf(
				`INSERT INTO %q VALUE {'pk':'k1','sk':'s1','gsi_pk':'g1','lsi_sk':'l1','val':'inserted'}`,
				tableName,
			)
			updateStmt := fmt.Sprintf(`UPDATE %q SET val = 'updated' WHERE pk = 'k2' AND sk = 's2'`, tableName)
			deleteStmt := fmt.Sprintf(`DELETE FROM %q WHERE pk = 'k3' AND sk = 's3'`, tableName)

			out, err := client.ExecuteTransaction(ctx, &sdk.ExecuteTransactionInput{
				TransactStatements: []types.ParameterizedStatement{
					{Statement: aws.String(insertStmt)},
					{Statement: aws.String(updateStmt)},
					{Statement: aws.String(deleteStmt)},
				},
				ReturnConsumedCapacity: tt.reqCC,
			})
			require.NoError(t, err)
			require.Len(t, out.Responses, 3)

			if tt.want.wantNil {
				assert.Empty(t, out.ConsumedCapacity)

				return
			}

			require.Len(t, out.ConsumedCapacity, tt.want.wantLen)

			for i := range out.ConsumedCapacity {
				cc := &out.ConsumedCapacity[i]
				assert.Equal(t, tableName, aws.ToString(cc.TableName))
				assert.Positive(t, aws.ToFloat64(cc.CapacityUnits))
			}

			if tt.reqCC != types.ReturnConsumedCapacityIndexes {
				for i := range out.ConsumedCapacity {
					cc := &out.ConsumedCapacity[i]
					assert.Nil(t, cc.Table)
					assert.Nil(t, cc.GlobalSecondaryIndexes)
					assert.Nil(t, cc.LocalSecondaryIndexes)
				}

				return
			}

			for i, wantGSI := range tt.want.wantGSI {
				cc := &out.ConsumedCapacity[i]
				assert.NotNil(t, cc.Table, "statement %d: Table breakdown", i)

				if wantGSI {
					assert.Contains(t, cc.GlobalSecondaryIndexes, gsiName, "statement %d", i)
				} else {
					assert.Nil(t, cc.GlobalSecondaryIndexes, "statement %d", i)
				}
			}

			for i, wantLSI := range tt.want.wantLSI {
				cc := &out.ConsumedCapacity[i]

				if wantLSI {
					assert.Contains(t, cc.LocalSecondaryIndexes, lsiName, "statement %d", i)
				} else {
					assert.Nil(t, cc.LocalSecondaryIndexes, "statement %d", i)
				}
			}
		})
	}
}
