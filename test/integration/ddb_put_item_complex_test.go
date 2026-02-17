package integration_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_PutItem_Complex(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)
	ctx := t.Context()
	tableName := "PutItemComplexTest-" + uuid.NewString()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	time.Sleep(100 * time.Millisecond)

	complexItem := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "complex-1"},
		"metadata": &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{
				"name": &types.AttributeValueMemberS{Value: "Deep Object"},
				"tags": &types.AttributeValueMemberL{
					Value: []types.AttributeValue{
						&types.AttributeValueMemberS{Value: "tag1"},
						&types.AttributeValueMemberS{Value: "tag2"},
					},
				},
				"stats": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"count":  &types.AttributeValueMemberN{Value: "42"},
						"active": &types.AttributeValueMemberBOOL{Value: true},
					},
				},
			},
		},
		"binary_data": &types.AttributeValueMemberB{Value: []byte("hello world")},
		"null_field":  &types.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      complexItem,
	})
	require.NoError(t, err)

	// Verify the item
	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "complex-1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Item)

	expected := map[string]any{
		"pk": "complex-1",
		"metadata": map[string]any{
			"name": "Deep Object",
			"tags": []any{"tag1", "tag2"},
			"stats": map[string]any{
				"count":  "42",
				"active": true,
			},
		},
		"binary_data": "aGVsbG8gd29ybGQ=",
		"null_field":  nil,
	}

	AssertItem(t, getOut.Item, expected)
}

func TestIntegration_DDB_PutItem_CompositeComplex(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)
	ctx := t.Context()
	tableName := "PutItemCompositeComplexTest-" + uuid.NewString()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("name"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("version"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("name"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("version"), KeyType: types.KeyTypeRange},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	time.Sleep(100 * time.Millisecond)

	itemName := "multi-version-item"
	for i := 1; i <= 5; i++ {
		versionStr := strconv.Itoa(i)
		valStr := fmt.Sprintf("value-%d", i)

		item := map[string]types.AttributeValue{
			"name":    &types.AttributeValueMemberS{Value: itemName},
			"version": &types.AttributeValueMemberN{Value: versionStr},
			"data": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"sub_val": &types.AttributeValueMemberS{Value: valStr},
					"nested": &types.AttributeValueMemberM{
						Value: map[string]types.AttributeValue{
							"index": &types.AttributeValueMemberN{Value: versionStr},
						},
					},
				},
			},
		}

		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		require.NoError(t, err, "PutItem failed for version %d", i)
	}

	// Verify we have 5 items
	scanOut, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(5), scanOut.Count, "Should have exactly 5 items")
	assert.Len(t, scanOut.Items, 5)

	// Verify each item has the correct data
	for _, item := range scanOut.Items {
		name := item["name"].(*types.AttributeValueMemberS).Value
		version := item["version"].(*types.AttributeValueMemberN).Value
		assert.Equal(t, itemName, name)

		data := item["data"].(*types.AttributeValueMemberM).Value
		subVal := data["sub_val"].(*types.AttributeValueMemberS).Value
		assert.Equal(t, "value-"+version, subVal)

		nested := data["nested"].(*types.AttributeValueMemberM).Value
		index := nested["index"].(*types.AttributeValueMemberN).Value
		assert.Equal(t, version, index)
	}
}
