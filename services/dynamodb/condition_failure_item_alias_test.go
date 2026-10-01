package dynamodb_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestConditionFailureItem_DoesNotAliasStoredItem(t *testing.T) {
	t.Parallel()

	const tbl = "alias-tbl"

	key := map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}
	failCond := aws.String("attribute_not_exists(pk)")
	xVal := map[string]types.AttributeValue{":x": &types.AttributeValueMemberN{Value: "1"}}
	allOld := types.ReturnValuesOnConditionCheckFailureAllOld

	tests := []struct {
		run  func(db *dynamodb.InMemoryDB) error
		name string
	}{
		{name: "transact_condition_check", run: func(db *dynamodb.InMemoryDB) error {
			_, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{{ConditionCheck: &types.ConditionCheck{
					TableName: aws.String(tbl), Key: key, ConditionExpression: failCond,
					ReturnValuesOnConditionCheckFailure: allOld,
				}}},
			})

			return err
		}},
		{name: "transact_put", run: func(db *dynamodb.InMemoryDB) error {
			_, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{{Put: &types.Put{
					TableName: aws.String(tbl), Item: key, ConditionExpression: failCond,
					ReturnValuesOnConditionCheckFailure: allOld,
				}}},
			})

			return err
		}},
		{name: "put_item", run: func(db *dynamodb.InMemoryDB) error {
			_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
				TableName: aws.String(tbl), Item: key, ConditionExpression: failCond,
				ReturnValuesOnConditionCheckFailure: allOld,
			})

			return err
		}},
		{name: "update_item", run: func(db *dynamodb.InMemoryDB) error {
			_, err := db.UpdateItem(t.Context(), &sdk.UpdateItemInput{
				TableName:                           aws.String(tbl),
				Key:                                 key,
				ConditionExpression:                 failCond,
				UpdateExpression:                    aws.String("SET x = :x"),
				ExpressionAttributeValues:           xVal,
				ReturnValuesOnConditionCheckFailure: allOld,
			})

			return err
		}},
		{name: "delete_item", run: func(db *dynamodb.InMemoryDB) error {
			_, err := db.DeleteItem(t.Context(), &sdk.DeleteItemInput{
				TableName: aws.String(tbl), Key: key, ConditionExpression: failCond,
				ReturnValuesOnConditionCheckFailure: allOld,
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			createTableHelper(t, db, tbl, "pk")

			_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
				TableName: aws.String(tbl),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "a"},
					"v":  &types.AttributeValueMemberS{Value: "1"},
					"m": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
						"n": &types.AttributeValueMemberS{Value: "deep"},
					}},
				},
			})
			require.NoError(t, err)

			runErr := tt.run(db)
			ddbErr, ok := errors.AsType[*dynamodb.Error](runErr)
			require.True(t, ok, "got %T: %v", runErr, runErr)

			got := ddbErr.Item
			if len(ddbErr.CancellationReasons) > 0 {
				got = ddbErr.CancellationReasons[0].Item
			}

			item, ok := got.(map[string]any)
			require.True(t, ok, "expected item map, got %T", got)

			nested, ok := item["m"].(map[string]any)["M"].(map[string]any)["n"].(map[string]any)
			require.True(t, ok)

			nested["S"] = "mutated"
			item["v"] = map[string]any{"S": "mutated"}
			item["extra"] = map[string]any{"S": "x"}

			out, err := db.GetItem(t.Context(), &sdk.GetItemInput{TableName: aws.String(tbl), Key: key})
			require.NoError(t, err)
			assert.Len(t, out.Item, 3)

			v, ok := out.Item["v"].(*types.AttributeValueMemberS)
			require.True(t, ok)
			assert.Equal(t, "1", v.Value)

			m, ok := out.Item["m"].(*types.AttributeValueMemberM)
			require.True(t, ok)

			n, ok := m.Value["n"].(*types.AttributeValueMemberS)
			require.True(t, ok)
			assert.Equal(t, "deep", n.Value)
		})
	}
}
