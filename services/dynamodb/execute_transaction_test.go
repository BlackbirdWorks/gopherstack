package dynamodb_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
