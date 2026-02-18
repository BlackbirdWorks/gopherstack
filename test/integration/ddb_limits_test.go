package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_ValidationAndLimits(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	createTable := func(t *testing.T) string {
		t.Helper()
		tableName := "Limits_" + uuid.NewString()
		_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
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
			client.DeleteTable(
				context.Background(),
				&dynamodb.DeleteTableInput{TableName: aws.String(tableName)},
			)
		})

		return tableName
	}

	tests := []struct {
		item    func() map[string]types.AttributeValue
		name    string
		wantErr bool
	}{
		{
			name: "ItemSizeLimit_Over400KB",
			item: func() map[string]types.AttributeValue {
				return map[string]types.AttributeValue{
					"pk":   &types.AttributeValueMemberS{Value: "largeItem"},
					"data": &types.AttributeValueMemberS{Value: strings.Repeat("a", 410000)},
				}
			},
			wantErr: true,
		},
		{
			name: "MissingKey_ReturnsError",
			item: func() map[string]types.AttributeValue {
				return map[string]types.AttributeValue{
					"other": &types.AttributeValueMemberS{Value: "val"},
				}
			},
			wantErr: true,
		},
		{
			name: "ValidNumberType_Succeeds",
			item: func() map[string]types.AttributeValue {
				return map[string]types.AttributeValue{
					"pk":  &types.AttributeValueMemberS{Value: "item1"},
					"num": &types.AttributeValueMemberN{Value: "123"},
				}
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tableName := createTable(t)

			_, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item:      tt.item(),
			})

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
