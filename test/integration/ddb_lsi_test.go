package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_LocalSecondaryIndex(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	tableName := "LSI_Test_" + uuid.NewString()

	_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("lsi_sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("LSI_1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("lsi_sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
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
	time.Sleep(10 * time.Millisecond)

	// Seed: PK=A, SK=1/lsi_sk=50, SK=2/lsi_sk=40, SK=3/lsi_sk=30
	for _, v := range []struct{ sk, lsiSk string }{{"1", "50"}, {"2", "40"}, {"3", "30"}} {
		_, pErr := client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":     &types.AttributeValueMemberS{Value: "A"},
				"sk":     &types.AttributeValueMemberN{Value: v.sk},
				"lsi_sk": &types.AttributeValueMemberN{Value: v.lsiSk},
				"data":   &types.AttributeValueMemberS{Value: "some data"},
			},
		})
		require.NoError(t, pErr)
	}

	tests := []struct {
		indexName *string
		wantFirst map[string]string
		wantLast  map[string]string
		name      string
	}{
		{
			name:      "QueryTable_OrderedBySK",
			indexName: nil,
			wantFirst: map[string]string{"sk": "1"},
			wantLast:  map[string]string{"sk": "3"},
		},
		{
			name:      "QueryLSI_OrderedByLsiSK",
			indexName: aws.String("LSI_1"),
			wantFirst: map[string]string{"lsi_sk": "30", "sk": "3"},
			wantLast:  map[string]string{"lsi_sk": "50", "sk": "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, queryErr := client.Query(t.Context(), &dynamodb.QueryInput{
				TableName:              aws.String(tableName),
				IndexName:              tt.indexName,
				KeyConditionExpression: aws.String("pk = :pk"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk": &types.AttributeValueMemberS{Value: "A"},
				},
			})
			require.NoError(t, queryErr)
			assert.Len(t, out.Items, 3)

			for attr, want := range tt.wantFirst {
				assert.Equal(t, want, out.Items[0][attr].(*types.AttributeValueMemberN).Value)
			}
			for attr, want := range tt.wantLast {
				assert.Equal(t, want, out.Items[2][attr].(*types.AttributeValueMemberN).Value)
			}
		})
	}
}
