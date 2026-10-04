package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

func reuseKey(seq string) map[string]sdktypes.AttributeValue {
	return map[string]sdktypes.AttributeValue{
		"id":  &sdktypes.AttributeValueMemberS{Value: "a"},
		"seq": &sdktypes.AttributeValueMemberN{Value: seq},
	}
}

func TestFindMatchForPutSDKAgreesWithWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		item map[string]sdktypes.AttributeValue
		name string
		hit  bool
	}{
		{
			name: "hit",
			item: reuseKey("1"),
			hit:  true,
		},
		{
			name: "miss sort key",
			item: reuseKey("2"),
		},
		{
			name: "missing sort key",
			item: map[string]sdktypes.AttributeValue{"id": &sdktypes.AttributeValueMemberS{Value: "a"}},
		},
		{name: "empty", item: map[string]sdktypes.AttributeValue{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newSecIdxTestDB(t)
			createSecIdxTable(t, db)

			_, err := db.PutItem(t.Context(), &sdkdynamodb.PutItemInput{
				TableName: aws.String(secIdxTableName),
				Item:      reuseKey("1"),
			})
			require.NoError(t, err)

			table, err := db.getTable(t.Context(), secIdxTableName)
			require.NoError(t, err)

			table.mu.RLock("test")
			defer table.mu.RUnlock()

			wantItem, wantIdx := db.findMatchForPut(table, models.FromSDKItem(tt.item))
			gotItem, gotIdx := db.findMatchForPutSDK(table, tt.item)

			assert.Equal(t, wantIdx, gotIdx)
			assert.Equal(t, wantItem, gotItem)
			assert.Equal(t, tt.hit, gotIdx != -1)
		})
	}
}

func TestUpdateItemSharedValuesNotAliased(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	key := reuseKey("1")

	out, err := db.UpdateItem(t.Context(), &sdkdynamodb.UpdateItemInput{
		TableName:        aws.String(secIdxTableName),
		Key:              key,
		UpdateExpression: aws.String("SET m1 = :m, m2 = :m"),
		ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
			":m": &sdktypes.AttributeValueMemberM{Value: map[string]sdktypes.AttributeValue{
				"k": &sdktypes.AttributeValueMemberS{Value: "v"},
			}},
		},
		ReturnValues: sdktypes.ReturnValueAllNew,
	})
	require.NoError(t, err)

	returned := out.Attributes["m1"].(*sdktypes.AttributeValueMemberM)
	returned.Value["k"] = &sdktypes.AttributeValueMemberS{Value: "mutated"}

	got, err := db.GetItem(t.Context(), &sdkdynamodb.GetItemInput{TableName: aws.String(secIdxTableName), Key: key})
	require.NoError(t, err)

	for _, attr := range []string{"m1", "m2"} {
		m := got.Item[attr].(*sdktypes.AttributeValueMemberM).Value["k"].(*sdktypes.AttributeValueMemberS)
		assert.Equal(t, "v", m.Value, attr)
	}
}
