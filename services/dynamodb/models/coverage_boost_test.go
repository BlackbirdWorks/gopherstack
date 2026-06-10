package models_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFromSDKUpdateTableOutput covers the 0% FromSDKUpdateTableOutput function.
func TestFromSDKUpdateTableOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input           *dynamodb_sdk.UpdateTableOutput
		name            string
		wantTableName   string
		wantTableStatus string
	}{
		{
			name: "active_table",
			input: &dynamodb_sdk.UpdateTableOutput{
				TableDescription: &types.TableDescription{
					TableName:   aws.String("MyTable"),
					TableStatus: types.TableStatusActive,
					ItemCount:   aws.Int64(42),
				},
			},
			wantTableName:   "MyTable",
			wantTableStatus: "ACTIVE",
		},
		{
			name: "nil_table_description",
			input: &dynamodb_sdk.UpdateTableOutput{
				TableDescription: nil,
			},
			wantTableName:   "",
			wantTableStatus: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKUpdateTableOutput(tt.input)

			require.NotNil(t, result)
			assert.Equal(t, tt.wantTableName, result.TableDescription.TableName)
			assert.Equal(t, tt.wantTableStatus, result.TableDescription.TableStatus)
		})
	}
}

// TestFromSDKTagResourceOutput covers the 0% FromSDKTagResourceOutput function.
func TestFromSDKTagResourceOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *dynamodb_sdk.TagResourceOutput
		name  string
	}{
		{
			name:  "nil_output_returns_empty",
			input: nil,
		},
		{
			name:  "empty_output_returns_empty",
			input: &dynamodb_sdk.TagResourceOutput{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKTagResourceOutput(tt.input)

			require.NotNil(t, result)
			assert.Equal(t, &models.TagResourceOutput{}, result)
		})
	}
}

// TestFromSDKUntagResourceOutput covers the 0% FromSDKUntagResourceOutput function.
func TestFromSDKUntagResourceOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *dynamodb_sdk.UntagResourceOutput
		name  string
	}{
		{
			name:  "nil_output_returns_empty",
			input: nil,
		},
		{
			name:  "empty_output_returns_empty",
			input: &dynamodb_sdk.UntagResourceOutput{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKUntagResourceOutput(tt.input)

			require.NotNil(t, result)
			assert.Equal(t, &models.UntagResourceOutput{}, result)
		})
	}
}

// TestToSDKListTagsOfResourceInput covers the 0% ToSDKListTagsOfResourceInput function.
func TestToSDKListTagsOfResourceInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input         *models.ListTagsOfResourceInput
		name          string
		wantNextToken bool
		wantErr       bool
	}{
		{
			name: "with_next_token",
			input: &models.ListTagsOfResourceInput{
				ResourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable",
				NextToken:   "some-token",
			},
			wantNextToken: true,
		},
		{
			name: "without_next_token",
			input: &models.ListTagsOfResourceInput{
				ResourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable",
				NextToken:   "",
			},
			wantNextToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKListTagsOfResourceInput(tt.input)

			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.input.ResourceArn, *got.ResourceArn)

			if tt.wantNextToken {
				require.NotNil(t, got.NextToken)
				assert.Equal(t, tt.input.NextToken, *got.NextToken)
			} else {
				assert.Nil(t, got.NextToken)
			}
		})
	}
}

// TestFromSDKListTagsOfResourceOutput covers the 0% FromSDKListTagsOfResourceOutput function.
func TestFromSDKListTagsOfResourceOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input         *dynamodb_sdk.ListTagsOfResourceOutput
		name          string
		wantNextToken string
		wantTagsLen   int
	}{
		{
			name: "with_tags_and_next_token",
			input: &dynamodb_sdk.ListTagsOfResourceOutput{
				Tags: []types.Tag{
					{Key: aws.String("Env"), Value: aws.String("prod")},
					{Key: aws.String("Owner"), Value: aws.String("team")},
				},
				NextToken: aws.String("next-page-token"),
			},
			wantTagsLen:   2,
			wantNextToken: "next-page-token",
		},
		{
			name: "with_tags_no_next_token",
			input: &dynamodb_sdk.ListTagsOfResourceOutput{
				Tags: []types.Tag{
					{Key: aws.String("Env"), Value: aws.String("dev")},
				},
			},
			wantTagsLen:   1,
			wantNextToken: "",
		},
		{
			name: "empty_tags",
			input: &dynamodb_sdk.ListTagsOfResourceOutput{
				Tags: []types.Tag{},
			},
			wantTagsLen:   0,
			wantNextToken: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKListTagsOfResourceOutput(tt.input)

			require.NotNil(t, result)
			assert.Len(t, result.Tags, tt.wantTagsLen)
			assert.Equal(t, tt.wantNextToken, result.NextToken)

			if tt.wantTagsLen > 0 {
				assert.Equal(t, *tt.input.Tags[0].Key, result.Tags[0].Key)
				assert.Equal(t, *tt.input.Tags[0].Value, result.Tags[0].Value)
			}
		})
	}
}

// TestToSDKGetItemInputWithProjection covers the ProjectionExpression branch.
func TestToSDKGetItemInputWithProjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input                *models.GetItemInput
		name                 string
		wantProjectionNotNil bool
		wantErr              bool
	}{
		{
			name: "with_projection_expression",
			input: &models.GetItemInput{
				TableName:            "test-table",
				Key:                  map[string]any{"pk": map[string]any{"S": "v1"}},
				ProjectionExpression: "pk, sk",
				ExpressionAttributeNames: map[string]string{
					"#pk": "pk",
				},
			},
			wantProjectionNotNil: true,
		},
		{
			name: "without_projection_expression",
			input: &models.GetItemInput{
				TableName: "test-table",
				Key:       map[string]any{"pk": map[string]any{"S": "v1"}},
			},
			wantProjectionNotNil: false,
		},
		{
			name: "invalid_key_returns_error",
			input: &models.GetItemInput{
				TableName: "test-table",
				Key:       map[string]any{"pk": "not-an-attr-map"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKGetItemInput(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)

			if tt.wantProjectionNotNil {
				assert.NotNil(t, got.ProjectionExpression)
			} else {
				assert.Nil(t, got.ProjectionExpression)
			}
		})
	}
}

// TestToSDKDeleteItemInputWithExpressionValues covers the ExpressionAttributeValues branch.
func TestToSDKDeleteItemInputWithExpressionValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *models.DeleteItemInput
		name    string
		wantErr bool
	}{
		{
			name: "with_condition_and_expression_values",
			input: &models.DeleteItemInput{
				TableName:           "test-table",
				Key:                 map[string]any{"pk": map[string]any{"S": "v1"}},
				ConditionExpression: "attribute_exists(pk) AND sk = :sk",
				ExpressionAttributeNames: map[string]string{
					"#pk": "pk",
				},
				ExpressionAttributeValues: map[string]any{
					":sk": map[string]any{"S": "sort-key"},
				},
			},
			wantErr: false,
		},
		{
			name: "minimal_key_only",
			input: &models.DeleteItemInput{
				TableName: "test-table",
				Key:       map[string]any{"pk": map[string]any{"S": "v1"}},
			},
			wantErr: false,
		},
		{
			name: "invalid_key",
			input: &models.DeleteItemInput{
				TableName: "test-table",
				Key:       map[string]any{"pk": "invalid"},
			},
			wantErr: true,
		},
		{
			name: "invalid_expression_attribute_values",
			input: &models.DeleteItemInput{
				TableName: "test-table",
				Key:       map[string]any{"pk": map[string]any{"S": "v1"}},
				ExpressionAttributeValues: map[string]any{
					":v": "not-an-attr-map",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKDeleteItemInput(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.input.TableName, *got.TableName)
		})
	}
}

// TestFromSDKScanOutputWithLastEvaluatedKey covers the LastEvaluatedKey branch.
func TestFromSDKScanOutputWithLastEvaluatedKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input           *dynamodb_sdk.ScanOutput
		name            string
		wantLastEvalKey bool
		wantItemsLen    int
	}{
		{
			name: "with_last_evaluated_key",
			input: &dynamodb_sdk.ScanOutput{
				Count:        2,
				ScannedCount: 2,
				Items: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: "item1"}},
					{"pk": &types.AttributeValueMemberS{Value: "item2"}},
				},
				LastEvaluatedKey: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "item2"},
				},
			},
			wantItemsLen:    2,
			wantLastEvalKey: true,
		},
		{
			name: "empty_items_no_last_key",
			input: &dynamodb_sdk.ScanOutput{
				Count:        0,
				ScannedCount: 0,
				Items:        []map[string]types.AttributeValue{},
			},
			wantItemsLen:    0,
			wantLastEvalKey: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKScanOutput(tt.input)

			require.NotNil(t, result)
			assert.Len(t, result.Items, tt.wantItemsLen)

			if tt.wantLastEvalKey {
				assert.NotNil(t, result.LastEvaluatedKey)
			} else {
				assert.Nil(t, result.LastEvaluatedKey)
			}
		})
	}
}

// TestToSDKBatchGetItemInputError covers error paths in ToSDKBatchGetItemInput.
func TestToSDKBatchGetItemInputError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *models.BatchGetItemInput
		name    string
		wantErr bool
	}{
		{
			name: "invalid_key_in_batch",
			input: &models.BatchGetItemInput{
				RequestItems: map[string]models.KeysAndAttributes{
					"table1": {
						Keys: []map[string]any{
							{"pk": "not-an-attr-map"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "valid_with_projection",
			input: &models.BatchGetItemInput{
				RequestItems: map[string]models.KeysAndAttributes{
					"table1": {
						Keys: []map[string]any{
							{"pk": map[string]any{"S": "v1"}},
						},
						AttributesToGet:          []string{"pk", "sk"},
						ConsistentRead:           aws.Bool(true),
						ExpressionAttributeNames: map[string]string{"#pk": "pk"},
						ProjectionExpression:     "pk",
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKBatchGetItemInput(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}

// TestToSDKBatchWriteItemInputErrors covers error paths in ToSDKBatchWriteItemInput.
func TestToSDKBatchWriteItemInputErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *models.BatchWriteItemInput
		name    string
		wantErr bool
	}{
		{
			name: "invalid_put_item",
			input: &models.BatchWriteItemInput{
				RequestItems: map[string][]models.WriteRequest{
					"table1": {
						{
							PutRequest: &models.PutRequest{
								Item: map[string]any{"pk": "invalid"},
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_delete_key",
			input: &models.BatchWriteItemInput{
				RequestItems: map[string][]models.WriteRequest{
					"table1": {
						{
							DeleteRequest: &models.DeleteRequest{
								Key: map[string]any{"pk": "invalid"},
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKBatchWriteItemInput(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}

// TestToSDKTransactWriteItemsInputErrors covers error paths in transact write item conversion.
func TestToSDKTransactWriteItemsInputErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *models.TransactWriteItemsInput
		name    string
		wantErr bool
	}{
		{
			name: "invalid_put_item",
			input: &models.TransactWriteItemsInput{
				TransactItems: []models.TransactWriteItem{
					{
						Put: &models.PutItemInput{
							TableName: "t1",
							Item:      map[string]any{"pk": "invalid"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_delete_item",
			input: &models.TransactWriteItemsInput{
				TransactItems: []models.TransactWriteItem{
					{
						Delete: &models.DeleteItemInput{
							TableName: "t1",
							Key:       map[string]any{"pk": "invalid"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_update_item",
			input: &models.TransactWriteItemsInput{
				TransactItems: []models.TransactWriteItem{
					{
						Update: &models.UpdateItemInput{
							TableName: "t1",
							Key:       map[string]any{"pk": "invalid"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_condition_check_key",
			input: &models.TransactWriteItemsInput{
				TransactItems: []models.TransactWriteItem{
					{
						ConditionCheck: &models.ConditionCheckInput{
							TableName:           "t1",
							Key:                 map[string]any{"pk": "invalid"},
							ConditionExpression: "attribute_exists(pk)",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_condition_check_expression_values",
			input: &models.TransactWriteItemsInput{
				TransactItems: []models.TransactWriteItem{
					{
						ConditionCheck: &models.ConditionCheckInput{
							TableName:           "t1",
							Key:                 map[string]any{"pk": map[string]any{"S": "v1"}},
							ConditionExpression: "pk = :v",
							ExpressionAttributeValues: map[string]any{
								":v": "invalid",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "with_client_request_token",
			input: &models.TransactWriteItemsInput{
				ClientRequestToken: "token-abc-123",
				TransactItems: []models.TransactWriteItem{
					{
						Put: &models.PutItemInput{
							TableName: "t1",
							Item:      map[string]any{"pk": map[string]any{"S": "v1"}},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKTransactWriteItemsInput(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}

// TestToSDKTransactGetItemsInputError covers error paths in transact get items.
func TestToSDKTransactGetItemsInputError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *models.TransactGetItemsInput
		name    string
		wantErr bool
	}{
		{
			name: "invalid_get_key",
			input: &models.TransactGetItemsInput{
				TransactItems: []models.TransactGetItem{
					{
						Get: &models.GetItemInput{
							TableName: "t1",
							Key:       map[string]any{"pk": "invalid"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "nil_get_item_skipped",
			input: &models.TransactGetItemsInput{
				TransactItems: []models.TransactGetItem{
					{Get: nil},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKTransactGetItemsInput(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}

// TestFromSDKAttributeValueDefault covers the nil/default case.
func TestFromSDKAttributeValueDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   types.AttributeValue
		name    string
		wantNil bool
	}{
		{
			name:    "nil_input_returns_nil",
			input:   nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKAttributeValue(tt.input)

			if tt.wantNil {
				assert.Nil(t, result)
			}
		})
	}
}

// TestToSDKUpdateTableInputWithReplicaUpdates covers replica update branches.
func TestToSDKUpdateTableInputWithReplicaUpdates(t *testing.T) {
	t.Parallel()

	rcu := int64(5)

	tests := []struct {
		input   *models.UpdateTableInput
		name    string
		wantErr bool
	}{
		{
			name: "replica_create",
			input: &models.UpdateTableInput{
				TableName: "global-table",
				ReplicaUpdates: []models.ReplicaUpdate{
					{
						Create: &models.CreateReplicationGroupMemberAction{
							RegionName: "eu-west-1",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "replica_update_with_provisioned_rcu",
			input: &models.UpdateTableInput{
				TableName: "global-table",
				ReplicaUpdates: []models.ReplicaUpdate{
					{
						Update: &models.UpdateReplicationGroupMemberAction{
							RegionName:                   "eu-west-1",
							TableClassOverride:           "STANDARD",
							ProvisionedReadCapacityUnits: &rcu,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "replica_update_without_provisioned_rcu",
			input: &models.UpdateTableInput{
				TableName: "global-table",
				ReplicaUpdates: []models.ReplicaUpdate{
					{
						Update: &models.UpdateReplicationGroupMemberAction{
							RegionName:         "ap-southeast-1",
							TableClassOverride: "STANDARD_INFREQUENT_ACCESS",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "replica_delete",
			input: &models.UpdateTableInput{
				TableName: "global-table",
				ReplicaUpdates: []models.ReplicaUpdate{
					{
						Delete: &models.DeleteReplicationGroupMemberAction{
							RegionName: "us-west-2",
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKUpdateTableInput(tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.input.TableName, *got.TableName)
			assert.NotEmpty(t, got.ReplicaUpdates)
		})
	}
}

// TestToSDKAttributeValueErrorPaths covers error paths in type converters.
func TestToSDKAttributeValueErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   any
		wantErr error
		name    string
	}{
		{
			name:    "S_type_with_non_string",
			input:   map[string]any{"S": 123},
			wantErr: models.ErrInvalidTypeS,
		},
		{
			name:    "N_type_with_non_string",
			input:   map[string]any{"N": 123},
			wantErr: models.ErrInvalidTypeN,
		},
		{
			name:    "B_type_with_invalid_base64",
			input:   map[string]any{"B": "!!!not-valid-base64!!!"},
			wantErr: models.ErrInvalidTypeB,
		},
		{
			name:    "NS_type_with_non_slice",
			input:   map[string]any{"NS": "not-a-slice"},
			wantErr: models.ErrInvalidTypeNS,
		},
		{
			name:    "NS_type_with_non_string_item",
			input:   map[string]any{"NS": []any{123}},
			wantErr: models.ErrInvalidStringInNS,
		},
		{
			name:    "BS_type_with_non_slice",
			input:   map[string]any{"BS": "not-a-slice"},
			wantErr: models.ErrInvalidTypeBS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := models.ToSDKAttributeValue(tt.input)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestFromSDKTableDescriptionWithReplicas covers the replica branch in FromSDKTableDescription.
func TestFromSDKTableDescriptionWithReplicas(t *testing.T) {
	t.Parallel()

	rcu := int64(5)

	tests := []struct {
		input          *types.TableDescription
		name           string
		wantReplicaLen int
	}{
		{
			name: "with_replicas_including_table_class_and_rcu",
			input: &types.TableDescription{
				TableName:   aws.String("GlobalTable"),
				TableStatus: types.TableStatusActive,
				Replicas: []types.ReplicaDescription{
					{
						RegionName:    aws.String("eu-west-1"),
						ReplicaStatus: types.ReplicaStatusActive,
						ReplicaTableClassSummary: &types.TableClassSummary{
							TableClass: types.TableClassStandard,
						},
						ProvisionedThroughputOverride: &types.ProvisionedThroughputOverride{
							ReadCapacityUnits: &rcu,
						},
					},
					{
						RegionName:    aws.String("ap-southeast-1"),
						ReplicaStatus: types.ReplicaStatusCreating,
					},
				},
				BillingModeSummary: &types.BillingModeSummary{
					BillingMode: types.BillingModePayPerRequest,
				},
			},
			wantReplicaLen: 2,
		},
		{
			name: "with_empty_replicas_returns_nil",
			input: &types.TableDescription{
				TableName:   aws.String("SimpleTable"),
				TableStatus: types.TableStatusActive,
				Replicas:    []types.ReplicaDescription{},
			},
			wantReplicaLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKTableDescription(tt.input)

			assert.Equal(t, *tt.input.TableName, result.TableName)
			assert.Len(t, result.Replicas, tt.wantReplicaLen)

			if tt.wantReplicaLen == 0 {
				assert.Nil(t, result.Replicas)
			}
		})
	}
}
