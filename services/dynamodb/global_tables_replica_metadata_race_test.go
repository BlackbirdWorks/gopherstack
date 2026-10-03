package dynamodb_test

import (
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestGlobalTableReplicaMetadataRace checks replica metadata writes take table.mu,
// which DescribeTable reads under.
func TestGlobalTableReplicaMetadataRace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, db *dynamodb.InMemoryDB)
		name   string
	}{
		{
			name: "CreateGlobalTable_adopts_existing_replica",
			mutate: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()

				_, err := db.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
					GlobalTableName: aws.String("RaceTable"),
					ReplicationGroup: []types.Replica{
						{RegionName: aws.String("us-east-1")},
						{RegionName: aws.String("us-west-2")},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "UpdateGlobalTable_adds_replica_region",
			mutate: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()

				_, err := db.CreateGlobalTable(t.Context(), &sdk.CreateGlobalTableInput{
					GlobalTableName:  aws.String("RaceTable"),
					ReplicationGroup: []types.Replica{{RegionName: aws.String("us-east-1")}},
				})
				require.NoError(t, err)

				_, err = db.UpdateGlobalTable(t.Context(), &sdk.UpdateGlobalTableInput{
					GlobalTableName: aws.String("RaceTable"),
					ReplicaUpdates: []types.ReplicaUpdate{
						{Create: &types.CreateReplicaAction{
							RegionName: aws.String("us-west-2"),
						}},
					},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newTestDBWithCleanup(t)

			// Pre-create the table in us-west-2 so the mutate step's
			// global-table op adopts an *existing* Table (the in-place
			// "existing.GlobalTableName = name" / "t.Replicas = ..." path)
			// instead of building a fresh, not-yet-published one.
			_, err := db.CreateTableInRegion(t.Context(), &sdk.CreateTableInput{
				TableName: aws.String("RaceTable"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: types.BillingModePayPerRequest,
			}, "us-west-2")
			require.NoError(t, err)

			readCtx := dynamodb.WithRegion(t.Context(), "us-west-2")

			var wg sync.WaitGroup

			stop := make(chan struct{})

			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}

					_, _ = db.DescribeTable(readCtx, &sdk.DescribeTableInput{
						TableName: aws.String("RaceTable"),
					})
				}
			})

			tt.mutate(t, db)
			close(stop)
			wg.Wait()
		})
	}
}
