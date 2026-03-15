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

func TestToSDKCreateTableInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		input              *models.CreateTableInput
		wantTableName      string
		wantKeySchemaLen   int
		wantAttrDefsLen    int
		wantProvThroughput bool
	}{
		{
			name: "valid_create_table_input",
			input: &models.CreateTableInput{
				TableName: "TestTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: models.KeyTypeHash},
					{AttributeName: "sk", KeyType: models.KeyTypeRange},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
					{AttributeName: "sk", AttributeType: "S"},
				},
				ProvisionedThroughput: map[string]any{
					"ReadCapacityUnits":  int64(5),
					"WriteCapacityUnits": int64(5),
				},
			},
			wantTableName:      "TestTable",
			wantKeySchemaLen:   2,
			wantAttrDefsLen:    2,
			wantProvThroughput: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			output := models.ToSDKCreateTableInput(tt.input)

			assert.Equal(t, tt.wantTableName, aws.ToString(output.TableName))
			assert.Len(t, output.KeySchema, tt.wantKeySchemaLen)
			assert.Len(t, output.AttributeDefinitions, tt.wantAttrDefsLen)
			if tt.wantProvThroughput {
				assert.NotNil(t, output.ProvisionedThroughput)
			}
		})
	}
}

func TestFromSDKCreateTableOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		input           *dynamodb_sdk.CreateTableOutput
		wantTableName   string
		wantTableStatus string
		wantItemCount   int
	}{
		{
			name: "creating_table",
			input: &dynamodb_sdk.CreateTableOutput{
				TableDescription: &types.TableDescription{
					TableName:   aws.String("TestTable"),
					TableStatus: types.TableStatusCreating,
					ItemCount:   aws.Int64(0),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					},
					AttributeDefinitions: []types.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					},
				},
			},
			wantTableName:   "TestTable",
			wantTableStatus: "CREATING",
			wantItemCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKCreateTableOutput(tt.input)

			require.NotNil(t, result)
			assert.Equal(t, tt.wantTableName, result.TableDescription.TableName)
			assert.Equal(t, tt.wantTableStatus, result.TableDescription.TableStatus)
			assert.Equal(t, tt.wantItemCount, result.TableDescription.ItemCount)
		})
	}
}

func TestToSDKDeleteTableInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         *models.DeleteTableInput
		wantTableName string
	}{
		{
			name:          "valid_delete_table_input",
			input:         &models.DeleteTableInput{TableName: "TestTable"},
			wantTableName: "TestTable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			output := models.ToSDKDeleteTableInput(tt.input)

			assert.Equal(t, tt.wantTableName, aws.ToString(output.TableName))
		})
	}
}

func TestFromSDKDeleteTableOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		input           *dynamodb_sdk.DeleteTableOutput
		wantTableName   string
		wantTableStatus string
	}{
		{
			name: "deleting_table",
			input: &dynamodb_sdk.DeleteTableOutput{
				TableDescription: &types.TableDescription{
					TableName:   aws.String("TestTable"),
					TableStatus: types.TableStatusDeleting,
				},
			},
			wantTableName:   "TestTable",
			wantTableStatus: "DELETING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKDeleteTableOutput(tt.input)

			require.NotNil(t, result)
			assert.Equal(t, tt.wantTableName, result.TableDescription.TableName)
			assert.Equal(t, tt.wantTableStatus, result.TableDescription.TableStatus)
		})
	}
}

func TestToSDKDescribeTableInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         *models.DescribeTableInput
		wantTableName string
	}{
		{
			name:          "valid_describe_table_input",
			input:         &models.DescribeTableInput{TableName: "TestTable"},
			wantTableName: "TestTable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			output := models.ToSDKDescribeTableInput(tt.input)

			assert.Equal(t, tt.wantTableName, aws.ToString(output.TableName))
		})
	}
}

func TestFromSDKDescribeTableOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		input           *dynamodb_sdk.DescribeTableOutput
		wantTableName   string
		wantTableStatus string
		wantItemCount   int
	}{
		{
			name: "active_table",
			input: &dynamodb_sdk.DescribeTableOutput{
				Table: &types.TableDescription{
					TableName:   aws.String("TestTable"),
					TableStatus: types.TableStatusActive,
					ItemCount:   aws.Int64(100),
				},
			},
			wantTableName:   "TestTable",
			wantTableStatus: "ACTIVE",
			wantItemCount:   100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKDescribeTableOutput(tt.input)

			require.NotNil(t, result)
			assert.Equal(t, tt.wantTableName, result.Table.TableName)
			assert.Equal(t, tt.wantTableStatus, result.Table.TableStatus)
			assert.Equal(t, tt.wantItemCount, result.Table.ItemCount)
		})
	}
}

func TestToSDKListTablesInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input                   *models.ListTablesInput
		name                    string
		wantExclusiveStartTable string
		wantLimit               int32
	}{
		{
			name:      "normal_limit",
			input:     &models.ListTablesInput{Limit: 10},
			wantLimit: int32(10),
		},
		{
			name:      "limit_larger_than_int32_max_clamped",
			input:     &models.ListTablesInput{Limit: 3000000000},
			wantLimit: int32(2147483647),
		},
		{
			name:                    "exclusive_start_table_name_set",
			input:                   &models.ListTablesInput{Limit: 5, ExclusiveStartTableName: "my-table"},
			wantLimit:               int32(5),
			wantExclusiveStartTable: "my-table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			output := models.ToSDKListTablesInput(tt.input)

			require.NotNil(t, output.Limit)
			assert.Equal(t, tt.wantLimit, *output.Limit)

			if tt.wantExclusiveStartTable != "" {
				require.NotNil(t, output.ExclusiveStartTableName)
				assert.Equal(t, tt.wantExclusiveStartTable, *output.ExclusiveStartTableName)
			} else {
				assert.Nil(t, output.ExclusiveStartTableName)
			}
		})
	}
}

func TestFromSDKListTablesOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input                  *dynamodb_sdk.ListTablesOutput
		name                   string
		wantLastEvaluatedTable string
		wantTableNames         []string
	}{
		{
			name:           "multiple_tables",
			input:          &dynamodb_sdk.ListTablesOutput{TableNames: []string{"table1", "table2", "table3"}},
			wantTableNames: []string{"table1", "table2", "table3"},
		},
		{
			name: "with_last_evaluated_table_name",
			input: &dynamodb_sdk.ListTablesOutput{
				TableNames:             []string{"table1"},
				LastEvaluatedTableName: aws.String("table1"),
			},
			wantTableNames:         []string{"table1"},
			wantLastEvaluatedTable: "table1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKListTablesOutput(tt.input)

			require.NotNil(t, result)
			assert.Equal(t, tt.wantTableNames, result.TableNames)
			assert.Equal(t, tt.wantLastEvaluatedTable, result.LastEvaluatedTableName)
		})
	}
}

func TestFromSDKUpdateTimeToLiveOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        *dynamodb_sdk.UpdateTimeToLiveOutput
		wantAttrName string
		wantEnabled  bool
	}{
		{
			name: "ttl_enabled",
			input: &dynamodb_sdk.UpdateTimeToLiveOutput{
				TimeToLiveSpecification: &types.TimeToLiveSpecification{
					AttributeName: aws.String("ttl"),
					Enabled:       aws.Bool(true),
				},
			},
			wantAttrName: "ttl",
			wantEnabled:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKUpdateTimeToLiveOutput(tt.input)

			require.NotNil(t, result)
			assert.Equal(t, tt.wantAttrName, result.TimeToLiveSpecification.AttributeName)
			assert.Equal(t, tt.wantEnabled, result.TimeToLiveSpecification.Enabled)
		})
	}
}

func TestToSDKDescribeTimeToLiveInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         *models.DescribeTimeToLiveInput
		wantTableName string
	}{
		{
			name:          "valid_describe_ttl_input",
			input:         &models.DescribeTimeToLiveInput{TableName: "TestTable"},
			wantTableName: "TestTable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			output := models.ToSDKDescribeTimeToLiveInput(tt.input)

			assert.Equal(t, tt.wantTableName, aws.ToString(output.TableName))
		})
	}
}

func TestFromSDKTableDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input                 *types.TableDescription
		name                  string
		wantTableName         string
		wantTableStatus       string
		wantLatestStreamArn   string
		wantLatestStreamLabel string
		wantStreamViewType    string
		wantItemCount         int
		wantKeySchemaLen      int
		wantAttrDefsLen       int
		wantEmpty             bool
		wantStreamEnabled     bool
	}{
		{
			name: "active_table_with_schema",
			input: &types.TableDescription{
				TableName:   aws.String("TestTable"),
				TableStatus: types.TableStatusActive,
				ItemCount:   aws.Int64(50),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
			},
			wantTableName:    "TestTable",
			wantTableStatus:  "ACTIVE",
			wantItemCount:    50,
			wantKeySchemaLen: 1,
			wantAttrDefsLen:  1,
		},
		{
			name: "streaming_enabled_returns_stream_fields",
			input: &types.TableDescription{
				TableName:   aws.String("StreamTable"),
				TableStatus: types.TableStatusActive,
				LatestStreamArn: aws.String(
					"arn:aws:dynamodb:us-east-1:000000000000:table/StreamTable/stream/2024-01-01T00:00:00.000",
				),
				LatestStreamLabel: aws.String("2024-01-01T00:00:00.000"),
				StreamSpecification: &types.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: types.StreamViewTypeNewImage,
				},
			},
			wantTableName:         "StreamTable",
			wantTableStatus:       "ACTIVE",
			wantLatestStreamArn:   "arn:aws:dynamodb:us-east-1:000000000000:table/StreamTable/stream/2024-01-01T00:00:00.000",
			wantLatestStreamLabel: "2024-01-01T00:00:00.000",
			wantStreamEnabled:     true,
			wantStreamViewType:    "NEW_IMAGE",
		},
		{
			name:      "nil_returns_empty_struct",
			input:     nil,
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKTableDescription(tt.input)

			if tt.wantEmpty {
				assert.Equal(t, models.TableDescription{}, result)

				return
			}

			assert.Equal(t, tt.wantTableName, result.TableName)
			assert.Equal(t, tt.wantTableStatus, result.TableStatus)
			assert.Equal(t, tt.wantItemCount, result.ItemCount)
			assert.Len(t, result.KeySchema, tt.wantKeySchemaLen)
			assert.Len(t, result.AttributeDefinitions, tt.wantAttrDefsLen)
			assert.Equal(t, tt.wantLatestStreamArn, result.LatestStreamArn)
			assert.Equal(t, tt.wantLatestStreamLabel, result.LatestStreamLabel)

			if !tt.wantStreamEnabled {
				assert.Nil(t, result.StreamSpecification)

				return
			}

			require.NotNil(t, result.StreamSpecification)
			assert.True(t, result.StreamSpecification.StreamEnabled)
			assert.Equal(t, tt.wantStreamViewType, result.StreamSpecification.StreamViewType)
		})
	}
}

func TestFromSDKConsumedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input             *types.ConsumedCapacity
		name              string
		wantTableName     string
		wantCapacityUnits float64
		wantNil           bool
	}{
		{
			name: "valid_consumed_capacity",
			input: &types.ConsumedCapacity{
				TableName:          aws.String("TestTable"),
				CapacityUnits:      aws.Float64(1.5),
				ReadCapacityUnits:  aws.Float64(1.0),
				WriteCapacityUnits: aws.Float64(0.5),
			},
			wantTableName:     "TestTable",
			wantCapacityUnits: 1.5,
		},
		{
			name:    "nil_returns_nil",
			input:   nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKConsumedCapacity(tt.input)

			if tt.wantNil {
				assert.Nil(t, result)

				return
			}

			require.NotNil(t, result)
			assert.Equal(t, tt.wantTableName, result.TableName)
			assert.InEpsilon(t, tt.wantCapacityUnits, result.CapacityUnits, 0.0001)
		})
	}
}

func TestFromSDKItemCollectionMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input                    *types.ItemCollectionMetrics
		name                     string
		wantSizeEstimateRangeLen int
		wantNil                  bool
	}{
		{
			name: "valid_item_collection_metrics",
			input: &types.ItemCollectionMetrics{
				ItemCollectionKey: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "test"},
				},
				SizeEstimateRangeGB: []float64{0.1, 0.5},
			},
			wantSizeEstimateRangeLen: 2,
		},
		{
			name:    "nil_returns_nil",
			input:   nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKItemCollectionMetrics(tt.input)

			if tt.wantNil {
				assert.Nil(t, result)

				return
			}

			require.NotNil(t, result)
			assert.NotNil(t, result.ItemCollectionKey)
			assert.Len(t, result.SizeEstimateRangeGB, tt.wantSizeEstimateRangeLen)
		})
	}
}

func TestFromSDKGlobalSecondaryIndexDescriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantIndexName   string
		wantIndexStatus string
		input           []types.GlobalSecondaryIndexDescription
		wantLen         int
		wantItemCount   int
	}{
		{
			name: "single_active_gsi",
			input: []types.GlobalSecondaryIndexDescription{
				{
					IndexName:   aws.String("GSI1"),
					IndexStatus: types.IndexStatusActive,
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("gsi1pk"), KeyType: types.KeyTypeHash},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
					ProvisionedThroughput: &types.ProvisionedThroughputDescription{
						ReadCapacityUnits:  aws.Int64(5),
						WriteCapacityUnits: aws.Int64(5),
					},
					ItemCount: aws.Int64(100),
				},
			},
			wantLen:         1,
			wantIndexName:   "GSI1",
			wantIndexStatus: "ACTIVE",
			wantItemCount:   100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKGlobalSecondaryIndexDescriptions(tt.input)

			require.Len(t, result, tt.wantLen)
			assert.Equal(t, tt.wantIndexName, result[0].IndexName)
			assert.Equal(t, tt.wantIndexStatus, result[0].IndexStatus)
			assert.Equal(t, tt.wantItemCount, result[0].ItemCount)
		})
	}
}

func TestFromSDKLocalSecondaryIndexDescriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		wantIndexName      string
		input              []types.LocalSecondaryIndexDescription
		wantLen            int
		wantIndexSizeBytes int64
		wantItemCount      int
	}{
		{
			name: "single_lsi",
			input: []types.LocalSecondaryIndexDescription{
				{
					IndexName: aws.String("LSI1"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String("lsi1sk"), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
					IndexSizeBytes: aws.Int64(1024),
					ItemCount:      aws.Int64(50),
				},
			},
			wantLen:            1,
			wantIndexName:      "LSI1",
			wantIndexSizeBytes: 1024,
			wantItemCount:      50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKLocalSecondaryIndexDescriptions(tt.input)

			require.Len(t, result, tt.wantLen)
			assert.Equal(t, tt.wantIndexName, result[0].IndexName)
			assert.Equal(t, tt.wantIndexSizeBytes, result[0].IndexSizeBytes)
			assert.Equal(t, tt.wantItemCount, result[0].ItemCount)
		})
	}
}

func TestFromSDKProvisionedThroughputDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input             *types.ProvisionedThroughputDescription
		name              string
		wantReadCapacity  int
		wantWriteCapacity int
		wantNil           bool
	}{
		{
			name: "valid_throughput_description",
			input: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  aws.Int64(10),
				WriteCapacityUnits: aws.Int64(5),
			},
			wantReadCapacity:  10,
			wantWriteCapacity: 5,
		},
		{
			name:    "nil_returns_nil",
			input:   nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := models.FromSDKProvisionedThroughputDescription(tt.input)

			if tt.wantNil {
				assert.Nil(t, result)

				return
			}

			require.NotNil(t, result)
			assert.Equal(t, tt.wantReadCapacity, result.ReadCapacityUnits)
			assert.Equal(t, tt.wantWriteCapacity, result.WriteCapacityUnits)
		})
	}
}
