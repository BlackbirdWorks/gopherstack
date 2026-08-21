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

// TestUpdateTableReplicaAutoScaling_WriteAndGSIUpdatesDontClobberEachOther
// guards against gopherstack-1vv2/c8ge: ProvisionedWriteCapacityAutoScalingUpdate
// and GlobalSecondaryIndexUpdates are independently optional on the real input
// (api_op_UpdateTableReplicaAutoScaling.go) -- a caller may update one without
// mentioning the other. mergeAutoScalingSettingsFromInput used to be
// autoScalingSettingsFromInput, building a brand-new autoScalingSettings from
// only the current call's fields and assigning it wholesale over
// table.AutoScaling, so a call updating only the GSI settings silently wiped
// whatever an earlier call had set for write-capacity autoscaling, and vice
// versa.
func TestUpdateTableReplicaAutoScaling_WriteAndGSIUpdatesDontClobberEachOther(t *testing.T) {
	t.Parallel()

	db := NewInMemoryDB()
	ctx := t.Context()
	tableName := "as-no-clobber"

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

	_, err = db.UpdateTableReplicaAutoScaling(ctx, &sdkdynamodb.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String(tableName),
		ProvisionedWriteCapacityAutoScalingUpdate: &sdktypes.AutoScalingSettingsUpdate{
			MinimumUnits: aws.Int64(5),
			MaximumUnits: aws.Int64(500),
		},
	})
	require.NoError(t, err)

	table, ok := db.tables.Get(tableKey(db.defaultRegion, tableName))
	require.True(t, ok)
	require.NotNil(t, table.AutoScaling)
	require.NotNil(t, table.AutoScaling.Write, "write-capacity settings must be stored")
	assert.Equal(t, int64(5), aws.ToInt64(table.AutoScaling.Write.MinCapacity))
	assert.Equal(t, int64(500), aws.ToInt64(table.AutoScaling.Write.MaxCapacity))

	// Second call only mentions a GSI update -- ProvisionedWriteCapacityAutoScalingUpdate
	// is entirely absent, matching a real client updating just one index.
	_, err = db.UpdateTableReplicaAutoScaling(ctx, &sdkdynamodb.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String(tableName),
		GlobalSecondaryIndexUpdates: []sdktypes.GlobalSecondaryIndexAutoScalingUpdate{
			{
				IndexName: aws.String("gsi1"),
				ProvisionedWriteCapacityAutoScalingUpdate: &sdktypes.AutoScalingSettingsUpdate{
					MinimumUnits: aws.Int64(2),
					MaximumUnits: aws.Int64(20),
				},
			},
		},
	})
	require.NoError(t, err)

	table, ok = db.tables.Get(tableKey(db.defaultRegion, tableName))
	require.True(t, ok)
	require.NotNil(
		t,
		table.AutoScaling.Write,
		"write-capacity settings set by the first call must survive an Update that never mentioned them",
	)
	assert.Equal(t, int64(5), aws.ToInt64(table.AutoScaling.Write.MinCapacity))
	assert.Equal(t, int64(500), aws.ToInt64(table.AutoScaling.Write.MaxCapacity))

	require.NotNil(t, table.AutoScaling.GlobalSecondaryIndexes["gsi1"], "the second call's own GSI update must apply")
	assert.Equal(t, int64(2), aws.ToInt64(table.AutoScaling.GlobalSecondaryIndexes["gsi1"].MinCapacity))
	assert.Equal(t, int64(20), aws.ToInt64(table.AutoScaling.GlobalSecondaryIndexes["gsi1"].MaxCapacity))
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
