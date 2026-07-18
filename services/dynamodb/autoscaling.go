// Package dynamodb implements the AWS DynamoDB mock service.
// autoscaling.go implements UpdateTableReplicaAutoScaling: it persists the
// requested auto-scaling configuration so DescribeTableReplicaAutoScaling can
// round-trip the values without simulating real scaling.
package dynamodb

import (
	"context"

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
// snapshots the table's name, status, and replica list, all under a single
// defer-protected table.mu.Lock.
func applyAutoScalingSettingsLocked(
	table *Table,
	input *dynamodb.UpdateTableReplicaAutoScalingInput,
) (string, string, []models.ReplicaDescription) {
	table.mu.Lock("UpdateTableReplicaAutoScaling")
	defer table.mu.Unlock()

	table.AutoScaling = autoScalingSettingsFromInput(input)
	replicas := make([]models.ReplicaDescription, len(table.Replicas))
	copy(replicas, table.Replicas)

	return table.Name, table.Status, replicas
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

	tableName, tableStatus, replicas := applyAutoScalingSettingsLocked(table, input)

	replicaDescs := make([]types.ReplicaAutoScalingDescription, 0, len(replicas))

	for _, r := range replicas {
		region := r.RegionName

		replicaDescs = append(replicaDescs, types.ReplicaAutoScalingDescription{
			RegionName:    &region,
			ReplicaStatus: types.ReplicaStatusActive,
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
