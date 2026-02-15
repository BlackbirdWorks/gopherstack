package dynamodb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestSDKJSONMarshaling(t *testing.T) {
	// Test basic input struct
	input := &dynamodb.CreateTableInput{
		TableName: aws.String("TestTable"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	b, err := json.Marshal(input)
	require.NoError(t, err)
	fmt.Printf("CreateTableInput JSON: %s\n", string(b))

	// Test output struct with AttributeValue
	output := &dynamodb.PutItemOutput{
		Attributes: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "123"},
			"Count": &types.AttributeValueMemberN{Value: "10"},
		},
	}
	b, err = json.Marshal(output)
	require.NoError(t, err)
	fmt.Printf("PutItemOutput JSON: %s\n", string(b))
}
