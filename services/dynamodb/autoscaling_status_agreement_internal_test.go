package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// TestUpdateAndDescribeTableReplicaAutoScaling_AgreeOnReplicaStatus proves
// UpdateTableReplicaAutoScaling and DescribeTableReplicaAutoScaling report
// the same per-replica ReplicaStatus. Nothing in this emulator's normal
// lifecycle ever sets a replica to anything but ACTIVE, so the replica
// status is forced directly on the stored table -- otherwise the two ops
// would coincidentally agree (both ACTIVE) whether or not
// UpdateTableReplicaAutoScaling still hardcoded the value.
func TestUpdateAndDescribeTableReplicaAutoScaling_AgreeOnReplicaStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		replicaStatus string
	}{
		{name: "active replica", replicaStatus: "ACTIVE"},
		{name: "updating replica", replicaStatus: "UPDATING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := NewInMemoryDB()
			ctx := t.Context()
			tableName := "as-replica-" + tt.name

			rc, wc := int64(5), int64(5)
			_, err := db.CreateTable(ctx, &sdkdynamodb.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []sdktypes.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: sdktypes.KeyTypeHash},
				},
				AttributeDefinitions: []sdktypes.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: sdktypes.ScalarAttributeTypeS},
				},
				ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
					ReadCapacityUnits:  &rc,
					WriteCapacityUnits: &wc,
				},
			})
			require.NoError(t, err)

			db.mu.RLock("test.AgreeOnReplicaStatus")
			tbl, ok := db.tables.Get(tableKey(db.defaultRegion, tableName))
			db.mu.RUnlock()
			require.True(t, ok)

			tbl.mu.Lock("test.AgreeOnReplicaStatus.table")
			tbl.Replicas = []models.ReplicaDescription{
				{RegionName: "us-west-2", ReplicaStatus: tt.replicaStatus},
			}
			tbl.mu.Unlock()

			updOut, err := db.UpdateTableReplicaAutoScaling(ctx, &sdkdynamodb.UpdateTableReplicaAutoScalingInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)

			descOut, err := db.DescribeTableReplicaAutoScaling(ctx, &sdkdynamodb.DescribeTableReplicaAutoScalingInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)

			updReplicas := updOut.TableAutoScalingDescription.Replicas
			descReplicas := descOut.TableAutoScalingDescription.Replicas

			require.Len(t, updReplicas, 1)
			require.Len(t, descReplicas, 1)
			assert.Equal(t, string(descReplicas[0].ReplicaStatus), string(updReplicas[0].ReplicaStatus),
				"Update and Describe must agree on ReplicaStatus")
			assert.Equal(t, tt.replicaStatus, string(updReplicas[0].ReplicaStatus))
		})
	}
}

// TestTableAutoScaling_TableStatusSourcedFromSameField proves
// applyAutoScalingSettingsLocked (used by UpdateTableReplicaAutoScaling) and
// replicaAutoScalingDescriptionsRLocked (used by
// DescribeTableReplicaAutoScaling) both read table.Status rather than one of
// them hardcoding a literal. This exercises the two locked helpers directly
// instead of going through the top-level ops: getTable refuses any table
// whose Status isn't ACTIVE (or empty) before either op's body runs, so a
// non-ACTIVE TableStatus can never reach these code paths through the public
// API today -- but the two helpers must still agree in principle, since a
// looser getTable gate (or a future caller bypassing it) would otherwise
// resurface the original contradiction.
func TestTableAutoScaling_TableStatusSourcedFromSameField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tableStatus string
	}{
		{name: "active", tableStatus: "ACTIVE"},
		{name: "updating", tableStatus: "UPDATING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			table := &Table{
				Name:   "direct-table",
				Status: tt.tableStatus,
				mu:     lockmetrics.New("test.table"),
			}

			_, updStatus, _, _ := applyAutoScalingSettingsLocked(
				table,
				&sdkdynamodb.UpdateTableReplicaAutoScalingInput{TableName: aws.String(table.Name)},
			)
			descStatus, _ := replicaAutoScalingDescriptionsRLocked(table)

			assert.Equal(t, descStatus, updStatus, "both helpers must agree on TableStatus")
			assert.Equal(t, tt.tableStatus, updStatus)
			assert.Equal(t, tt.tableStatus, descStatus)
		})
	}
}
