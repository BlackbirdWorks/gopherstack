package dynamodb_test

import (
	"Gopherstack/dynamodb/models"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestSDKJSONMarshaling(t *testing.T) {
	t.Parallel()

	input := &models.CreateTableInput{
		TableName: "TestTable",
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "PK", KeyType: models.KeyTypeHash},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "PK", AttributeType: "S"},
		},
		ProvisionedThroughput: &models.ProvisionedThroughput{
			ReadCapacityUnits:  new(int64(5)),
			WriteCapacityUnits: new(int64(5)),
		},
	}

	b, err := json.Marshal(input)
	require.NoError(t, err)
	t.Logf("CreateTableInput JSON: %s", string(b))

	output := &dynamodb.PutItemOutput{
		Attributes: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "123"},
			"Count": &types.AttributeValueMemberN{Value: "10"},
		},
	}
	b, err = json.Marshal(output)
	require.NoError(t, err)
	t.Logf("PutItemOutput JSON: %s", string(b))
}
