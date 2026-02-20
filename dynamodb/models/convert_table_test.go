package models_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToSDKCreateTableInput(t *testing.T) {
	t.Parallel()

	input := &models.CreateTableInput{
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
	}

	output := models.ToSDKCreateTableInput(input)

	assert.Equal(t, "TestTable", aws.ToString(output.TableName))
	assert.Len(t, output.KeySchema, 2)
	assert.Len(t, output.AttributeDefinitions, 2)
	assert.NotNil(t, output.ProvisionedThroughput)
}

func TestFromSDKCreateTableOutput(t *testing.T) {
	t.Parallel()

	output := &dynamodb_sdk.CreateTableOutput{
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
	}

	result := models.FromSDKCreateTableOutput(output)

	assert.NotNil(t, result)
	assert.Equal(t, "TestTable", result.TableDescription.TableName)
	assert.Equal(t, "CREATING", result.TableDescription.TableStatus)
	assert.Equal(t, 0, result.TableDescription.ItemCount)
}

func TestToSDKDeleteTableInput(t *testing.T) {
	t.Parallel()

	input := &models.DeleteTableInput{
		TableName: "TestTable",
	}

	output := models.ToSDKDeleteTableInput(input)

	assert.Equal(t, "TestTable", aws.ToString(output.TableName))
}

func TestFromSDKDeleteTableOutput(t *testing.T) {
	t.Parallel()

	output := &dynamodb_sdk.DeleteTableOutput{
		TableDescription: &types.TableDescription{
			TableName:   aws.String("TestTable"),
			TableStatus: types.TableStatusDeleting,
		},
	}

	result := models.FromSDKDeleteTableOutput(output)

	assert.NotNil(t, result)
	assert.Equal(t, "TestTable", result.TableDescription.TableName)
	assert.Equal(t, "DELETING", result.TableDescription.TableStatus)
}

func TestToSDKDescribeTableInput(t *testing.T) {
	t.Parallel()

	input := &models.DescribeTableInput{
		TableName: "TestTable",
	}

	output := models.ToSDKDescribeTableInput(input)

	assert.Equal(t, "TestTable", aws.ToString(output.TableName))
}

func TestFromSDKDescribeTableOutput(t *testing.T) {
	t.Parallel()

	output := &dynamodb_sdk.DescribeTableOutput{
		Table: &types.TableDescription{
			TableName:   aws.String("TestTable"),
			TableStatus: types.TableStatusActive,
			ItemCount:   aws.Int64(100),
		},
	}

	result := models.FromSDKDescribeTableOutput(output)

	assert.NotNil(t, result)
	assert.Equal(t, "TestTable", result.Table.TableName)
	assert.Equal(t, "ACTIVE", result.Table.TableStatus)
	assert.Equal(t, 100, result.Table.ItemCount)
}

func TestToSDKListTablesInput(t *testing.T) {
	t.Parallel()

	input := &models.ListTablesInput{Limit: 10}
	output := models.ToSDKListTablesInput(input)

	assert.NotNil(t, output.Limit)
	assert.Equal(t, int32(10), *output.Limit)
}

func TestFromSDKListTablesOutput(t *testing.T) {
	t.Parallel()

	output := &dynamodb_sdk.ListTablesOutput{
		TableNames: []string{"table1", "table2", "table3"},
	}

	result := models.FromSDKListTablesOutput(output)

	assert.NotNil(t, result)
	assert.Equal(t, []string{"table1", "table2", "table3"}, result.TableNames)
}

func TestToSDKListTablesInputLarge(t *testing.T) {
	t.Parallel()

	input := &models.ListTablesInput{Limit: 3000000000} // Larger than int32
	got := models.ToSDKListTablesInput(input)
	assert.Equal(t, int32(2147483647), *got.Limit)
}

func TestFromSDKUpdateTimeToLiveOutput(t *testing.T) {
	t.Parallel()

	output := &dynamodb_sdk.UpdateTimeToLiveOutput{
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("ttl"),
			Enabled:       aws.Bool(true),
		},
	}

	result := models.FromSDKUpdateTimeToLiveOutput(output)

	assert.NotNil(t, result)
	assert.Equal(t, "ttl", result.TimeToLiveSpecification.AttributeName)
	assert.True(t, result.TimeToLiveSpecification.Enabled)
}

func TestToSDKDescribeTimeToLiveInput(t *testing.T) {
	t.Parallel()

	input := &models.DescribeTimeToLiveInput{
		TableName: "TestTable",
	}

	output := models.ToSDKDescribeTimeToLiveInput(input)

	assert.Equal(t, "TestTable", aws.ToString(output.TableName))
}

func TestFromSDKTableDescription(t *testing.T) {
	t.Parallel()

	td := &types.TableDescription{
		TableName:   aws.String("TestTable"),
		TableStatus: types.TableStatusActive,
		ItemCount:   aws.Int64(50),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
	}

	result := models.FromSDKTableDescription(td)

	assert.Equal(t, "TestTable", result.TableName)
	assert.Equal(t, "ACTIVE", result.TableStatus)
	assert.Equal(t, 50, result.ItemCount)
	assert.Len(t, result.KeySchema, 1)
	assert.Len(t, result.AttributeDefinitions, 1)
}

func TestFromSDKTableDescriptionNil(t *testing.T) {
	t.Parallel()

	result := models.FromSDKTableDescription(nil)

	assert.Equal(t, models.TableDescription{}, result)
}

func TestFromSDKConsumedCapacity(t *testing.T) {
	t.Parallel()

	cc := &types.ConsumedCapacity{
		TableName:          aws.String("TestTable"),
		CapacityUnits:      aws.Float64(1.5),
		ReadCapacityUnits:  aws.Float64(1.0),
		WriteCapacityUnits: aws.Float64(0.5),
	}

	result := models.FromSDKConsumedCapacity(cc)

	assert.NotNil(t, result)
	assert.Equal(t, "TestTable", result.TableName)
	assert.InEpsilon(t, 1.5, result.CapacityUnits, 0.0001)
}

func TestFromSDKConsumedCapacityNil(t *testing.T) {
	t.Parallel()

	result := models.FromSDKConsumedCapacity(nil)

	assert.Nil(t, result)
}

func TestFromSDKItemCollectionMetrics(t *testing.T) {
	t.Parallel()

	icm := &types.ItemCollectionMetrics{
		ItemCollectionKey: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "test"},
		},
		SizeEstimateRangeGB: []float64{0.1, 0.5},
	}

	result := models.FromSDKItemCollectionMetrics(icm)

	assert.NotNil(t, result)
	assert.NotNil(t, result.ItemCollectionKey)
	assert.Len(t, result.SizeEstimateRangeGB, 2)
}

func TestFromSDKItemCollectionMetricsNil(t *testing.T) {
	t.Parallel()

	result := models.FromSDKItemCollectionMetrics(nil)

	assert.Nil(t, result)
}

func TestFromSDKGlobalSecondaryIndexDescriptions(t *testing.T) {
	t.Parallel()

	gsis := []types.GlobalSecondaryIndexDescription{
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
	}

	result := models.FromSDKGlobalSecondaryIndexDescriptions(gsis)

	require.Len(t, result, 1)
	assert.Equal(t, "GSI1", result[0].IndexName)
	assert.Equal(t, "ACTIVE", result[0].IndexStatus)
	assert.Equal(t, 100, result[0].ItemCount)
}

func TestFromSDKLocalSecondaryIndexDescriptions(t *testing.T) {
	t.Parallel()

	lsis := []types.LocalSecondaryIndexDescription{
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
	}

	result := models.FromSDKLocalSecondaryIndexDescriptions(lsis)

	require.Len(t, result, 1)
	assert.Equal(t, "LSI1", result[0].IndexName)
	assert.Equal(t, int64(1024), result[0].IndexSizeBytes)
	assert.Equal(t, 50, result[0].ItemCount)
}

func TestFromSDKProvisionedThroughputDescription(t *testing.T) {
	t.Parallel()

	ptd := &types.ProvisionedThroughputDescription{
		ReadCapacityUnits:  aws.Int64(10),
		WriteCapacityUnits: aws.Int64(5),
	}

	result := models.FromSDKProvisionedThroughputDescription(ptd)

	assert.NotNil(t, result)
	assert.Equal(t, 10, result.ReadCapacityUnits)
	assert.Equal(t, 5, result.WriteCapacityUnits)
}

func TestFromSDKProvisionedThroughputDescriptionNil(t *testing.T) {
	t.Parallel()

	result := models.FromSDKProvisionedThroughputDescription(nil)

	assert.Nil(t, result)
}
