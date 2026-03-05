package models_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- FromSDKGetItemOutput ---

func TestFromSDKGetItemOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    *dynamodb.GetItemOutput
		name     string
		wantItem bool
	}{
		{
			name: "with_item",
			input: &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "user1"},
				},
			},
			wantItem: true,
		},
		{
			name:     "empty_output",
			input:    &dynamodb.GetItemOutput{},
			wantItem: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := models.FromSDKGetItemOutput(tt.input)

			require.NotNil(t, got)
			if tt.wantItem {
				assert.NotEmpty(t, got.Item)
				assert.Equal(t, map[string]any{"S": "user1"}, got.Item["pk"])
			} else {
				assert.Empty(t, got.Item)
			}
		})
	}
}

// --- FromSDKDeleteItemOutput ---

func TestFromSDKDeleteItemOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *dynamodb.DeleteItemOutput
		name  string
	}{
		{
			name:  "always_returns_empty_struct",
			input: &dynamodb.DeleteItemOutput{},
		},
		{
			name:  "nil_input_ignored",
			input: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := models.FromSDKDeleteItemOutput(tt.input)
			require.NotNil(t, got)
			assert.Equal(t, &models.DeleteItemOutput{}, got)
		})
	}
}

// --- FromSDKUpdateItemOutput ---

func TestFromSDKUpdateItemOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input                     *dynamodb.UpdateItemOutput
		name                      string
		wantAttributes            bool
		wantConsumedCapacity      bool
		wantItemCollectionMetrics bool
	}{
		{
			name: "with_all_fields",
			input: &dynamodb.UpdateItemOutput{
				Attributes: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "v1"},
				},
				ConsumedCapacity: &types.ConsumedCapacity{TableName: aws.String("t1")},
				ItemCollectionMetrics: &types.ItemCollectionMetrics{
					ItemCollectionKey: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "v1"},
					},
					SizeEstimateRangeGB: []float64{0.1},
				},
			},
			wantAttributes:            true,
			wantConsumedCapacity:      true,
			wantItemCollectionMetrics: true,
		},
		{
			name:                      "empty_output",
			input:                     &dynamodb.UpdateItemOutput{},
			wantAttributes:            false,
			wantConsumedCapacity:      false,
			wantItemCollectionMetrics: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := models.FromSDKUpdateItemOutput(tt.input)

			require.NotNil(t, got)
			if tt.wantAttributes {
				assert.NotEmpty(t, got.Attributes)
			} else {
				assert.Empty(t, got.Attributes)
			}
			if tt.wantConsumedCapacity {
				assert.NotNil(t, got.ConsumedCapacity)
			} else {
				assert.Nil(t, got.ConsumedCapacity)
			}
			if tt.wantItemCollectionMetrics {
				assert.NotNil(t, got.ItemCollectionMetrics)
			} else {
				assert.Nil(t, got.ItemCollectionMetrics)
			}
		})
	}
}

// --- ToSDKPutItemInput (expression attribute values branch) ---

func TestToSDKPutItemInputWithExpressionValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *models.PutItemInput
		name    string
		wantErr bool
	}{
		{
			name: "with_expression_attribute_values",
			input: &models.PutItemInput{
				TableName: "test-table",
				Item: map[string]any{
					"pk": map[string]any{"S": "v1"},
				},
				ConditionExpression: "attribute_not_exists(pk)",
				ExpressionAttributeValues: map[string]any{
					":v": map[string]any{"S": "value"},
				},
			},
			wantErr: false,
		},
		{
			name: "with_return_values",
			input: &models.PutItemInput{
				TableName: "test-table",
				Item: map[string]any{
					"pk": map[string]any{"S": "v1"},
				},
				ReturnValues:                models.ReturnValuesAllOld,
				ReturnConsumedCapacity:      "TOTAL",
				ReturnItemCollectionMetrics: "SIZE",
			},
			wantErr: false,
		},
		{
			name: "invalid_expression_attribute_values",
			input: &models.PutItemInput{
				TableName: "test-table",
				Item: map[string]any{
					"pk": map[string]any{"S": "v1"},
				},
				ExpressionAttributeValues: map[string]any{
					":v": "not-an-attribute-map",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_item",
			input: &models.PutItemInput{
				TableName: "test-table",
				Item: map[string]any{
					"pk": "invalid",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKPutItemInput(tt.input)

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

// --- ToSDKUpdateItemInput (expression attribute values branch) ---

func TestToSDKUpdateItemInputCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *models.UpdateItemInput
		name    string
		wantErr bool
	}{
		{
			name: "with_all_fields",
			input: &models.UpdateItemInput{
				TableName:                "test-table",
				Key:                      map[string]any{"pk": map[string]any{"S": "v1"}},
				UpdateExpression:         "SET #n = :v",
				ConditionExpression:      "attribute_exists(pk)",
				ExpressionAttributeNames: map[string]string{"#n": "name"},
				ExpressionAttributeValues: map[string]any{
					":v": map[string]any{"S": "new-value"},
				},
				ReturnValues:                models.ReturnValuesAllNew,
				ReturnConsumedCapacity:      "TOTAL",
				ReturnItemCollectionMetrics: "SIZE",
			},
			wantErr: false,
		},
		{
			name: "minimal_fields",
			input: &models.UpdateItemInput{
				TableName: "test-table",
				Key:       map[string]any{"pk": map[string]any{"S": "v1"}},
			},
			wantErr: false,
		},
		{
			name: "invalid_key",
			input: &models.UpdateItemInput{
				TableName: "test-table",
				Key:       map[string]any{"pk": "invalid"},
			},
			wantErr: true,
		},
		{
			name: "invalid_expression_attribute_values",
			input: &models.UpdateItemInput{
				TableName: "test-table",
				Key:       map[string]any{"pk": map[string]any{"S": "v1"}},
				ExpressionAttributeValues: map[string]any{
					":v": "not-an-attribute-map",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKUpdateItemInput(tt.input)

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

// --- ToSDKScanInput (full branch coverage) ---

func TestToSDKScanInputCoverage(t *testing.T) {
	t.Parallel()

	limit := int32(10)
	segment := int32(0)
	totalSegments := int32(2)

	tests := []struct {
		input   *models.ScanInput
		name    string
		wantErr bool
	}{
		{
			name: "with_all_optional_fields",
			input: &models.ScanInput{
				TableName:                "test-table",
				IndexName:                "my-index",
				FilterExpression:         "#pk = :v",
				ProjectionExpression:     "pk, sk",
				ExpressionAttributeNames: map[string]string{"#pk": "pk"},
				ExpressionAttributeValues: map[string]any{
					":v": map[string]any{"S": "val"},
				},
				ExclusiveStartKey: map[string]any{
					"pk": map[string]any{"S": "last-key"},
				},
				Limit:         &limit,
				Segment:       &segment,
				TotalSegments: &totalSegments,
			},
			wantErr: false,
		},
		{
			name: "invalid_expression_attribute_values",
			input: &models.ScanInput{
				TableName: "test-table",
				ExpressionAttributeValues: map[string]any{
					":v": "invalid",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_exclusive_start_key",
			input: &models.ScanInput{
				TableName: "test-table",
				ExclusiveStartKey: map[string]any{
					"pk": "invalid",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKScanInput(tt.input)

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

// --- ToSDKQueryInput (full branch coverage) ---

func TestToSDKQueryInputCoverage(t *testing.T) {
	t.Parallel()

	scanForward := true

	tests := []struct {
		input   *models.QueryInput
		name    string
		wantErr bool
	}{
		{
			name: "with_all_optional_fields",
			input: &models.QueryInput{
				TableName:              "test-table",
				IndexName:              "my-index",
				KeyConditionExpression: "pk = :pk",
				FilterExpression:       "sk > :sk",
				ProjectionExpression:   "pk, sk",
				ExpressionAttributeNames: map[string]string{
					"#pk": "pk",
				},
				ExpressionAttributeValues: map[string]any{
					":pk": map[string]any{"S": "user1"},
				},
				ExclusiveStartKey: map[string]any{
					"pk": map[string]any{"S": "last-pk"},
				},
				ScanIndexForward: &scanForward,
				Limit:            5,
			},
			wantErr: false,
		},
		{
			name: "zero_limit_not_set",
			input: &models.QueryInput{
				TableName: "test-table",
				Limit:     0,
			},
			wantErr: false,
		},
		{
			name: "invalid_expression_attribute_values",
			input: &models.QueryInput{
				TableName: "test-table",
				ExpressionAttributeValues: map[string]any{
					":v": "invalid",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_exclusive_start_key",
			input: &models.QueryInput{
				TableName: "test-table",
				ExclusiveStartKey: map[string]any{
					"pk": "invalid",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKQueryInput(tt.input)

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

// --- ToSDKCreateTableInput (full branch coverage) ---

func TestToSDKCreateTableInputCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input              *models.CreateTableInput
		name               string
		wantStreamSpec     bool
		wantProvThroughput bool
		wantGSIs           bool
		wantLSIs           bool
	}{
		{
			name: "with_stream_spec",
			input: &models.CreateTableInput{
				TableName: "StreamTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: models.KeyTypeHash},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
				StreamSpecification: map[string]any{
					"StreamEnabled":  true,
					"StreamViewType": "NEW_AND_OLD_IMAGES",
				},
			},
			wantStreamSpec:     true,
			wantProvThroughput: false,
		},
		{
			name: "with_gsi_and_lsi",
			input: &models.CreateTableInput{
				TableName: "IndexTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: models.KeyTypeHash},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
					{AttributeName: "sk", AttributeType: "S"},
				},
				ProvisionedThroughput: map[string]any{
					"ReadCapacityUnits":  int64(5),
					"WriteCapacityUnits": int64(5),
				},
				GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
					{
						IndexName: "GSI1",
						KeySchema: []models.KeySchemaElement{
							{AttributeName: "sk", KeyType: models.KeyTypeHash},
						},
						Projection: models.Projection{ProjectionType: "ALL"},
						ProvisionedThroughput: models.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(5),
							WriteCapacityUnits: aws.Int64(5),
						},
					},
				},
				LocalSecondaryIndexes: []models.LocalSecondaryIndex{
					{
						IndexName: "LSI1",
						KeySchema: []models.KeySchemaElement{
							{AttributeName: "pk", KeyType: models.KeyTypeHash},
							{AttributeName: "sk", KeyType: models.KeyTypeRange},
						},
						Projection: models.Projection{ProjectionType: "ALL"},
					},
				},
			},
			wantStreamSpec:     false,
			wantProvThroughput: true,
			wantGSIs:           true,
			wantLSIs:           true,
		},
		{
			name: "no_stream_spec_nil_prov_throughput",
			input: &models.CreateTableInput{
				TableName: "PlainTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: models.KeyTypeHash},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
			},
			wantStreamSpec:     false,
			wantProvThroughput: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := models.ToSDKCreateTableInput(tt.input)

			require.NotNil(t, got)
			assert.Equal(t, tt.input.TableName, *got.TableName)

			if tt.wantStreamSpec {
				require.NotNil(t, got.StreamSpecification)
				assert.True(t, *got.StreamSpecification.StreamEnabled)
			} else {
				assert.Nil(t, got.StreamSpecification)
			}

			if tt.wantProvThroughput {
				assert.NotNil(t, got.ProvisionedThroughput)
			} else {
				assert.Nil(t, got.ProvisionedThroughput)
			}

			if tt.wantGSIs {
				assert.NotEmpty(t, got.GlobalSecondaryIndexes)
			}
			if tt.wantLSIs {
				assert.NotEmpty(t, got.LocalSecondaryIndexes)
			}
		})
	}
}

// --- ToSDKUpdateTableInput ---

func TestToSDKUpdateTableInput(t *testing.T) {
	t.Parallel()

	rcu := int64(10)
	wcu := int64(5)

	tests := []struct {
		input   *models.UpdateTableInput
		name    string
		wantErr bool
	}{
		{
			name: "minimal_table_name_only",
			input: &models.UpdateTableInput{
				TableName: "my-table",
			},
			wantErr: false,
		},
		{
			name: "with_attribute_definitions",
			input: &models.UpdateTableInput{
				TableName: "my-table",
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
			},
			wantErr: false,
		},
		{
			name: "with_provisioned_throughput",
			input: &models.UpdateTableInput{
				TableName: "my-table",
				ProvisionedThroughput: &models.ProvisionedThroughput{
					ReadCapacityUnits:  &rcu,
					WriteCapacityUnits: &wcu,
				},
			},
			wantErr: false,
		},
		{
			name: "with_stream_specification",
			input: &models.UpdateTableInput{
				TableName: "my-table",
				StreamSpecification: &models.StreamSpecificationInput{
					StreamEnabled:  true,
					StreamViewType: "NEW_AND_OLD_IMAGES",
				},
			},
			wantErr: false,
		},
		{
			name: "with_gsi_create",
			input: &models.UpdateTableInput{
				TableName: "my-table",
				GlobalSecondaryIndexUpdates: []models.GlobalSecondaryIndexUpdate{
					{
						Create: &models.CreateGlobalSecondaryIndexAction{
							IndexName: "NewGSI",
							KeySchema: []models.KeySchemaElement{
								{AttributeName: "sk", KeyType: models.KeyTypeHash},
							},
							Projection: models.Projection{ProjectionType: "ALL"},
							ProvisionedThroughput: &models.ProvisionedThroughput{
								ReadCapacityUnits:  &rcu,
								WriteCapacityUnits: &wcu,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "with_gsi_create_no_provisioned_throughput",
			input: &models.UpdateTableInput{
				TableName: "my-table",
				GlobalSecondaryIndexUpdates: []models.GlobalSecondaryIndexUpdate{
					{
						Create: &models.CreateGlobalSecondaryIndexAction{
							IndexName: "NewGSI",
							KeySchema: []models.KeySchemaElement{
								{AttributeName: "sk", KeyType: models.KeyTypeHash},
							},
							Projection: models.Projection{ProjectionType: "ALL"},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "with_gsi_update",
			input: &models.UpdateTableInput{
				TableName: "my-table",
				GlobalSecondaryIndexUpdates: []models.GlobalSecondaryIndexUpdate{
					{
						Update: &models.UpdateGlobalSecondaryIndexAction{
							IndexName: "ExistingGSI",
							ProvisionedThroughput: models.ProvisionedThroughput{
								ReadCapacityUnits:  &rcu,
								WriteCapacityUnits: &wcu,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "with_gsi_delete",
			input: &models.UpdateTableInput{
				TableName: "my-table",
				GlobalSecondaryIndexUpdates: []models.GlobalSecondaryIndexUpdate{
					{
						Delete: &models.DeleteGlobalSecondaryIndexAction{
							IndexName: "OldGSI",
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
		})
	}
}

// --- ToSDKTagResourceInput ---

func TestToSDKTagResourceInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    *models.TagResourceInput
		name     string
		wantTags int
	}{
		{
			name: "with_multiple_tags",
			input: &models.TagResourceInput{
				ResourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable",
				Tags: []models.Tag{
					{Key: "Environment", Value: "production"},
					{Key: "Owner", Value: "team-a"},
				},
			},
			wantTags: 2,
		},
		{
			name: "with_no_tags",
			input: &models.TagResourceInput{
				ResourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable",
				Tags:        []models.Tag{},
			},
			wantTags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKTagResourceInput(tt.input)

			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.input.ResourceArn, *got.ResourceArn)
			assert.Len(t, got.Tags, tt.wantTags)
			if tt.wantTags > 0 {
				assert.Equal(t, tt.input.Tags[0].Key, *got.Tags[0].Key)
				assert.Equal(t, tt.input.Tags[0].Value, *got.Tags[0].Value)
			}
		})
	}
}

// --- ToSDKUntagResourceInput ---

func TestToSDKUntagResourceInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    *models.UntagResourceInput
		name     string
		wantKeys int
	}{
		{
			name: "with_multiple_tag_keys",
			input: &models.UntagResourceInput{
				ResourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable",
				TagKeys:     []string{"Environment", "Owner"},
			},
			wantKeys: 2,
		},
		{
			name: "with_empty_tag_keys",
			input: &models.UntagResourceInput{
				ResourceArn: "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable",
				TagKeys:     []string{},
			},
			wantKeys: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := models.ToSDKUntagResourceInput(tt.input)

			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.input.ResourceArn, *got.ResourceArn)
			assert.Len(t, got.TagKeys, tt.wantKeys)
		})
	}
}

// --- convertBoolType error path (via ToSDKAttributeValue) ---

func TestToSDKAttributeValueBoolErrorPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   any
		wantErr error
		name    string
	}{
		{
			name:    "bool_type_with_non_bool_value",
			input:   map[string]any{"BOOL": "not-a-bool"},
			wantErr: models.ErrInvalidTypeBOOL,
		},
		{
			name:    "null_type_with_non_bool_value",
			input:   map[string]any{"NULL": "not-a-bool"},
			wantErr: models.ErrInvalidTypeNULL,
		},
		{
			name:    "map_type_with_non_map_value",
			input:   map[string]any{"M": "not-a-map"},
			wantErr: models.ErrInvalidTypeM,
		},
		{
			name:    "list_type_with_non_slice_value",
			input:   map[string]any{"L": "not-a-slice"},
			wantErr: models.ErrInvalidTypeL,
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
