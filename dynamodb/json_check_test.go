package dynamodb_test

import (
	"encoding/json"
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestSDKJSONMarshaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input any
		name  string
	}{
		{
			name: "CreateTableInput",
			input: &models.CreateTableInput{
				TableName: "TestTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "PK", KeyType: models.KeyTypeHash},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "PK", AttributeType: "S"},
				},
				ProvisionedThroughput: &models.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
		{
			name: "PutItemOutput",
			input: &dynamodb.PutItemOutput{
				Attributes: map[string]types.AttributeValue{
					"PK":    &types.AttributeValueMemberS{Value: "123"},
					"Count": &types.AttributeValueMemberN{Value: "10"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tt.input)
			require.NoError(t, err)
			require.NotEmpty(t, b)
			t.Logf("%s JSON: %s", tt.name, string(b))
		})
	}
}
