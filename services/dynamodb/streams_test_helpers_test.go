package dynamodb_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// makeCreateTableInput creates a minimal CreateTableInput for a table with a string partition key.
// If sk is non-empty, a string sort key is added.
func makeCreateTableInput(name, pk string) *dynamodb.CreateTableInput {
	keySchema := []models.KeySchemaElement{
		{AttributeName: pk, KeyType: models.KeyTypeHash},
	}
	attrDefs := []models.AttributeDefinition{
		{AttributeName: pk, AttributeType: "S"},
	}

	input := models.CreateTableInput{
		TableName:            name,
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
	}

	sdkInput := models.ToSDKCreateTableInput(&input)
	rc := int64(5)
	wc := int64(5)
	sdkInput.ProvisionedThroughput = &types.ProvisionedThroughput{
		ReadCapacityUnits:  &rc,
		WriteCapacityUnits: &wc,
	}

	return sdkInput
}

// makePutItem creates a PutItemInput with a single string partition key attribute.
func makePutItem(tableName, pkAttr, pkVal string) *dynamodb.PutItemInput {
	return &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			pkAttr: &types.AttributeValueMemberS{Value: pkVal},
		},
	}
}

// makePutItemN creates a PutItemInput with a numeric (string-encoded) partition key named "pk".
func makePutItemN(tableName string, pkNum int) *dynamodb.PutItemInput {
	return &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("key-%d", pkNum)},
		},
	}
}

// makeDeleteItem creates a DeleteItemInput for a single-key table.
func makeDeleteItem(tableName, pkAttr, pkVal string) *dynamodb.DeleteItemInput {
	return &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			pkAttr: &types.AttributeValueMemberS{Value: pkVal},
		},
	}
}
