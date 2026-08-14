// Package dynamodb implements the AWS DynamoDB mock service.
// autoscaling.go implements UpdateTableReplicaAutoScaling: it persists the
// requested auto-scaling configuration so DescribeTableReplicaAutoScaling can
// round-trip the values without simulating real scaling.
package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// autoScalingSettingsFromInput converts an UpdateTableReplicaAutoScalingInput
// into the persisted shape so the next DescribeTableReplicaAutoScaling can
// round-trip the values without simulating real scaling.
func autoScalingSettingsFromInput(
	input *dynamodb.UpdateTableReplicaAutoScalingInput,
) *autoScalingSettings {
	s := &autoScalingSettings{}

	if input.ProvisionedWriteCapacityAutoScalingUpdate != nil {
		s.Write = throughputFromUpdate(input.ProvisionedWriteCapacityAutoScalingUpdate)
	}

	if len(input.GlobalSecondaryIndexUpdates) > 0 {
		s.GlobalSecondaryIndexes = make(
			map[string]*autoScalingThroughput,
			len(input.GlobalSecondaryIndexUpdates),
		)
		for _, g := range input.GlobalSecondaryIndexUpdates {
			if g.IndexName == nil {
				continue
			}
			s.GlobalSecondaryIndexes[*g.IndexName] = throughputFromUpdate(
				g.ProvisionedWriteCapacityAutoScalingUpdate,
			)
		}
	}

	return s
}

// throughputFromUpdate translates the SDK AutoScalingSettingsUpdate struct
// into the persisted shape. Returns nil when no fields were supplied so the
// caller can distinguish "explicitly cleared" from "untouched".
func throughputFromUpdate(u *types.AutoScalingSettingsUpdate) *autoScalingThroughput {
	if u == nil {
		return nil
	}

	out := &autoScalingThroughput{
		MinCapacity: u.MinimumUnits,
		MaxCapacity: u.MaximumUnits,
	}
	if u.AutoScalingDisabled != nil {
		out.Disabled = *u.AutoScalingDisabled
	}
	if u.ScalingPolicyUpdate != nil &&
		u.ScalingPolicyUpdate.TargetTrackingScalingPolicyConfiguration != nil {
		out.TargetUtilizPct = u.ScalingPolicyUpdate.TargetTrackingScalingPolicyConfiguration.TargetValue
	}

	return out
}

// applyAutoScalingSettingsLocked sets table.AutoScaling from input and
// snapshots the table's name, status, replica list, and the just-applied
// write-capacity settings, all under a single defer-protected table.mu.Lock.
func applyAutoScalingSettingsLocked(
	table *Table,
	input *dynamodb.UpdateTableReplicaAutoScalingInput,
) (string, string, []models.ReplicaDescription, *autoScalingThroughput) {
	table.mu.Lock("UpdateTableReplicaAutoScaling")
	defer table.mu.Unlock()

	table.AutoScaling = autoScalingSettingsFromInput(input)
	replicas := make([]models.ReplicaDescription, len(table.Replicas))
	copy(replicas, table.Replicas)

	return table.Name, table.Status, replicas, table.AutoScaling.Write
}

// --- UpdateTableReplicaAutoScaling ---

// UpdateTableReplicaAutoScaling persists the autoscaling settings for a table's replicas
// so DescribeTableReplicaAutoScaling can round-trip the configured values.
func (db *InMemoryDB) UpdateTableReplicaAutoScaling(
	ctx context.Context,
	input *dynamodb.UpdateTableReplicaAutoScalingInput,
) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	tableName, tableStatus, replicas, write := applyAutoScalingSettingsLocked(table, input)

	replicaDescs := make([]types.ReplicaAutoScalingDescription, 0, len(replicas))

	for _, r := range replicas {
		region := r.RegionName

		replicaDescs = append(replicaDescs, types.ReplicaAutoScalingDescription{
			RegionName:    &region,
			ReplicaStatus: types.ReplicaStatusActive,
			ReplicaProvisionedWriteCapacityAutoScalingSettings: sdkAutoScalingSettingsDescription(write),
		})
	}

	return &dynamodb.UpdateTableReplicaAutoScalingOutput{
		TableAutoScalingDescription: &types.TableAutoScalingDescription{
			TableName:   &tableName,
			TableStatus: types.TableStatus(tableStatus),
			Replicas:    replicaDescs,
		},
	}, nil
}

// sdkAutoScalingSettingsDescription converts a persisted autoScalingThroughput
// into the SDK description type, or nil if t is nil (no settings configured).
func sdkAutoScalingSettingsDescription(t *autoScalingThroughput) *types.AutoScalingSettingsDescription {
	if t == nil {
		return nil
	}

	disabled := t.Disabled

	return &types.AutoScalingSettingsDescription{
		MinimumUnits:        t.MinCapacity,
		MaximumUnits:        t.MaxCapacity,
		AutoScalingDisabled: &disabled,
	}
}

// --- DescribeTableReplicaAutoScaling ---

// replicaAutoScalingDescriptionsRLocked copies table.Replicas, along with the
// table's write-capacity autoscaling settings (applied uniformly to every
// replica -- this emulator doesn't model per-replica overrides), into the SDK
// description type under a defer-protected table.mu.RLock.
func replicaAutoScalingDescriptionsRLocked(table *Table) []types.ReplicaAutoScalingDescription {
	table.mu.RLock(opDescribeTableReplicaAutoScaling)
	defer table.mu.RUnlock()

	var write *types.AutoScalingSettingsDescription
	if table.AutoScaling != nil {
		write = sdkAutoScalingSettingsDescription(table.AutoScaling.Write)
	}

	replicas := make([]types.ReplicaAutoScalingDescription, 0, len(table.Replicas))
	for _, r := range table.Replicas {
		region := r.RegionName
		status := r.ReplicaStatus

		replicas = append(replicas, types.ReplicaAutoScalingDescription{
			RegionName:    &region,
			ReplicaStatus: types.ReplicaStatus(status),
			ReplicaProvisionedWriteCapacityAutoScalingSettings: write,
		})
	}

	return replicas
}

// DescribeTableReplicaAutoScaling returns the autoscaling settings for a
// table's replicas. It satisfies the StorageBackend interface using official
// AWS SDK v2 types.
func (db *InMemoryDB) DescribeTableReplicaAutoScaling(
	ctx context.Context,
	input *dynamodb.DescribeTableReplicaAutoScalingInput,
) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error) {
	tableName := aws.ToString(input.TableName)
	if tableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	replicas := replicaAutoScalingDescriptionsRLocked(table)

	return &dynamodb.DescribeTableReplicaAutoScalingOutput{
		TableAutoScalingDescription: &types.TableAutoScalingDescription{
			TableName:   &tableName,
			TableStatus: types.TableStatus(models.TableStatusActive),
			Replicas:    replicas,
		},
	}, nil
}
