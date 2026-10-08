package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestMetrics_IndexWriteCapacityForBatchAndTransact(t *testing.T) {
	t.Parallel()

	key := map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}
	item := map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "a"},
		"email": &types.AttributeValueMemberS{Value: "a@x"},
	}

	tests := []struct {
		run     func(t *testing.T, db *dynamodb.InMemoryDB)
		name    string
		seeded  bool
		wantGSI bool
	}{
		{
			name: "batch put", wantGSI: true,
			run: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				_, err := db.BatchWriteItem(t.Context(), &sdk.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{"m": {{PutRequest: &types.PutRequest{Item: item}}}},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "batch delete", seeded: true, wantGSI: true,
			run: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				_, err := db.BatchWriteItem(t.Context(), &sdk.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						"m": {{DeleteRequest: &types.DeleteRequest{Key: key}}},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "transact delete", seeded: true, wantGSI: true,
			run: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				_, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
					TransactItems: []types.TransactWriteItem{
						{Delete: &types.Delete{TableName: aws.String("m"), Key: key}},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "batch delete of missing item", wantGSI: false,
			run: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				_, err := db.BatchWriteItem(t.Context(), &sdk.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						"m": {{DeleteRequest: &types.DeleteRequest{Key: key}}},
					},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			db.SetDefaultRegion("us-east-1")

			_, err := db.CreateTable(t.Context(), &sdk.CreateTableInput{
				TableName: aws.String("m"),
				KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("email"), AttributeType: types.ScalarAttributeTypeS},
				},
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
					{
						IndexName: aws.String("by-email"),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("email"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			if tt.seeded {
				_, err = db.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String("m"), Item: item})
				require.NoError(t, err)
			}

			rec := &metricRecorder{}
			db.SetMetricEmitter(rec)

			tt.run(t, db)

			assert.Equal(t, tt.wantGSI, rec.has("ConsumedWriteCapacityUnits",
				map[string]string{"TableName": "m", "GlobalSecondaryIndexName": "by-email"}))
		})
	}
}
