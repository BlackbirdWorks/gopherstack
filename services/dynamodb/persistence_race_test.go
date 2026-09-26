package dynamodb_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestSnapshot_RacesWithItemAndTableWrites reproduces gopherstack-fwd0g: Snapshot marshals tables under db.mu.RLock.
// PutItem/UpdateTable mutate table fields under table.mu alone; run with -race.
func TestSnapshot_RacesWithItemAndTableWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, db *dynamodb.InMemoryDB, tableName string)
		name   string
	}{
		{
			name: "concurrent_snapshot_and_put_item",
			mutate: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()

				for i := range 200 {
					_, err := db.PutItem(t.Context(), &sdkddb.PutItemInput{
						TableName: aws.String(tableName),
						Item: map[string]sdktypes.AttributeValue{
							"pk": &sdktypes.AttributeValueMemberS{Value: "item-" + strconv.Itoa(i)},
						},
					})
					require.NoError(t, err)
				}
			},
		},
		{
			name: "concurrent_snapshot_and_update_table",
			mutate: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()

				for range 200 {
					_, err := db.UpdateTable(t.Context(), &sdkddb.UpdateTableInput{
						TableName: aws.String(tableName),
						ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
							ReadCapacityUnits:  aws.Int64(5),
							WriteCapacityUnits: aws.Int64(5),
						},
					})
					require.NoError(t, err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newTestDBWithCleanup(t)
			const tableName = "race-snapshot-table"

			_, err := db.CreateTable(t.Context(), &sdkddb.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []sdktypes.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: sdktypes.KeyTypeHash},
				},
				AttributeDefinitions: []sdktypes.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: sdktypes.ScalarAttributeTypeS},
				},
				ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			})
			require.NoError(t, err)

			var wg sync.WaitGroup

			stop := make(chan struct{})

			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}

					_ = db.Snapshot(t.Context())
				}
			})

			tt.mutate(t, db, tableName)
			close(stop)
			wg.Wait()
		})
	}
}
