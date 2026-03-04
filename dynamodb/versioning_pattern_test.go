package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type updateStep struct {
	input          models.UpdateItemInput
	wantContain    []string
	wantNotContain []string
	wantEqualN     map[string]string
	wantEqualS     map[string]string
}

// TestUpdateItem_VersioningPattern tests UpdateItem UPDATED_NEW return value behaviour
// across versioning and upsert scenarios.
func TestUpdateItem_VersioningPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		steps []updateStep
	}{
		{
			name: "versioning_pattern_new_then_existing",
			steps: []updateStep{
				{
					input: models.UpdateItemInput{
						TableName:                "TestTable",
						Key:                      map[string]any{"pk": map[string]any{"S": "item1"}},
						UpdateExpression:         "SET version = :v, #data = :data",
						ExpressionAttributeNames: map[string]string{"#data": "data"},
						ExpressionAttributeValues: map[string]any{
							":v":    map[string]any{"N": "1"},
							":data": map[string]any{"S": "first version"},
						},
						ReturnValues: "UPDATED_NEW",
					},
					wantContain:    []string{"version", "data"},
					wantNotContain: []string{"pk"},
					wantEqualN:     map[string]string{"version": "1"},
					wantEqualS:     map[string]string{"data": "first version"},
				},
				{
					input: models.UpdateItemInput{
						TableName:                "TestTable",
						Key:                      map[string]any{"pk": map[string]any{"S": "item1"}},
						UpdateExpression:         "SET version = :v, #data = :data",
						ExpressionAttributeNames: map[string]string{"#data": "data"},
						ExpressionAttributeValues: map[string]any{
							":v":    map[string]any{"N": "2"},
							":data": map[string]any{"S": "second version"},
						},
						ReturnValues: "UPDATED_NEW",
					},
					wantContain:    []string{"version", "data"},
					wantNotContain: []string{"pk"},
					wantEqualN:     map[string]string{"version": "2"},
					wantEqualS:     map[string]string{"data": "second version"},
				},
				{
					input: models.UpdateItemInput{
						TableName:        "TestTable",
						Key:              map[string]any{"pk": map[string]any{"S": "item1"}},
						UpdateExpression: "SET version = :v, details = :details",
						ExpressionAttributeValues: map[string]any{
							":v": map[string]any{"N": "3"},
							":details": map[string]any{"M": map[string]any{
								"author": map[string]any{"S": "test"},
							}},
						},
						ReturnValues: "UPDATED_NEW",
					},
					wantContain:    []string{"version", "details"},
					wantNotContain: []string{"pk", "data"},
					wantEqualN:     map[string]string{"version": "3"},
				},
			},
		},
		{
			name: "blank_to_upsert",
			steps: []updateStep{
				{
					input: models.UpdateItemInput{
						TableName:        "TestTable",
						Key:              map[string]any{"pk": map[string]any{"S": "newdoc"}},
						UpdateExpression: "SET version = :v, content = :content, created = :ts",
						ExpressionAttributeValues: map[string]any{
							":v":       map[string]any{"N": "1"},
							":content": map[string]any{"S": "initial content"},
							":ts":      map[string]any{"N": "1234567890"},
						},
						ReturnValues: "UPDATED_NEW",
					},
					wantContain:    []string{"version", "content", "created"},
					wantNotContain: []string{"pk"},
				},
				{
					input: models.UpdateItemInput{
						TableName:        "TestTable",
						Key:              map[string]any{"pk": map[string]any{"S": "newdoc"}},
						UpdateExpression: "SET version = :v, content = :content, modified = :ts",
						ExpressionAttributeValues: map[string]any{
							":v":       map[string]any{"N": "2"},
							":content": map[string]any{"S": "updated content"},
							":ts":      map[string]any{"N": "1234567999"},
						},
						ReturnValues: "UPDATED_NEW",
					},
					wantContain:    []string{"version", "content", "modified"},
					wantNotContain: []string{"pk", "created"},
					wantEqualN:     map[string]string{"version": "2"},
					wantEqualS:     map[string]string{"content": "updated content"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			db := dynamodb.NewInMemoryDB()
			ctInput := models.CreateTableInput{
				TableName: "TestTable",
				KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
			}
			_, err := db.CreateTable(ctx, models.ToSDKCreateTableInput(&ctInput))
			require.NoError(t, err)

			for i, step := range tt.steps {
				sdkInput, convErr := models.ToSDKUpdateItemInput(&step.input)
				require.NoError(t, convErr, "step %d: ToSDKUpdateItemInput failed", i)

				res, updateErr := db.UpdateItem(ctx, sdkInput)
				require.NoError(t, updateErr, "step %d: UpdateItem failed", i)
				require.NotNil(t, res.Attributes, "step %d: UPDATED_NEW should return attributes", i)

				attrs := models.FromSDKItem(res.Attributes)

				for _, key := range step.wantContain {
					assert.Contains(t, attrs, key, "step %d: expected key %q in attributes", i, key)
				}
				for _, key := range step.wantNotContain {
					assert.NotContains(t, attrs, key, "step %d: unexpected key %q in attributes", i, key)
				}
				for key, wantVal := range step.wantEqualN {
					require.Contains(t, attrs, key, "step %d: key %q missing for N assertion", i, key)
					assert.Equal(t, wantVal, attrs[key].(map[string]any)["N"], "step %d: N value mismatch for %q", i, key)
				}
				for key, wantVal := range step.wantEqualS {
					require.Contains(t, attrs, key, "step %d: key %q missing for S assertion", i, key)
					assert.Equal(t, wantVal, attrs[key].(map[string]any)["S"], "step %d: S value mismatch for %q", i, key)
				}
			}
		})
	}
}
