// Package dynamodb implements the AWS DynamoDB mock service.
// autoscaling.go implements UpdateTableReplicaAutoScaling: it persists the
// requested auto-scaling configuration so DescribeTableReplicaAutoScaling can
// round-trip the values without simulating real scaling.
package dynamodb

import (
	"context"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// mergeAutoScalingSettingsFromInput merges an UpdateTableReplicaAutoScalingInput
// into existing (nil if this table has never been updated before), so the next
// DescribeTableReplicaAutoScaling can round-trip the values without simulating
// real scaling.
//
// GlobalSecondaryIndexUpdates and ProvisionedWriteCapacityAutoScalingUpdate are
// independently optional on the real input (api_op_UpdateTableReplicaAutoScaling.go):
// a caller may update only one index's auto scaling settings without
// mentioning table-level write capacity, or vice versa. Building a fresh
// autoScalingSettings from only this call's fields and assigning it wholesale
// (the previous behavior) silently wiped whatever an earlier call had set for
// the field this call omitted.
func mergeAutoScalingSettingsFromInput(
	existing *autoScalingSettings,
	input *dynamodb.UpdateTableReplicaAutoScalingInput,
) *autoScalingSettings {
	s := existing
	if s == nil {
		s = &autoScalingSettings{}
	}

	if input.ProvisionedWriteCapacityAutoScalingUpdate != nil {
		s.Write = throughputFromUpdate(input.ProvisionedWriteCapacityAutoScalingUpdate)
	}

	if len(input.GlobalSecondaryIndexUpdates) > 0 {
		if s.GlobalSecondaryIndexes == nil {
			s.GlobalSecondaryIndexes = make(
				map[string]*autoScalingThroughput,
				len(input.GlobalSecondaryIndexUpdates),
			)
		}
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

// mergeReplicaAutoScalingFromUpdates merges UpdateTableReplicaAutoScalingInput's
// ReplicaUpdates (types.ReplicaAutoScalingUpdate, keyed by RegionName) into
// existing, the same merge-not-replace treatment mergeAutoScalingSettingsFromInput
// gives table-level settings: a call updating one replica's read capacity must
// not disturb another replica's, or that replica's own GSI settings.
func mergeReplicaAutoScalingFromUpdates(
	existing map[string]*replicaAutoScalingSettings,
	updates []types.ReplicaAutoScalingUpdate,
) map[string]*replicaAutoScalingSettings {
	if len(updates) == 0 {
		return existing
	}

	out := existing
	if out == nil {
		out = make(map[string]*replicaAutoScalingSettings, len(updates))
	}

	for _, u := range updates {
		region := aws.ToString(u.RegionName)
		if region == "" {
			continue
		}

		out[region] = mergeOneReplicaAutoScalingUpdate(out[region], u)
	}

	return out
}

// mergeOneReplicaAutoScalingUpdate merges a single ReplicaAutoScalingUpdate
// into rs (nil if this replica has never been updated before).
func mergeOneReplicaAutoScalingUpdate(
	rs *replicaAutoScalingSettings,
	u types.ReplicaAutoScalingUpdate,
) *replicaAutoScalingSettings {
	if rs == nil {
		rs = &replicaAutoScalingSettings{}
	}

	if u.ReplicaProvisionedReadCapacityAutoScalingUpdate != nil {
		rs.Read = throughputFromUpdate(u.ReplicaProvisionedReadCapacityAutoScalingUpdate)
	}

	if len(u.ReplicaGlobalSecondaryIndexUpdates) > 0 {
		rs.GlobalSecondaryIndexes = mergeReplicaGSIAutoScalingUpdates(
			rs.GlobalSecondaryIndexes,
			u.ReplicaGlobalSecondaryIndexUpdates,
		)
	}

	return rs
}

// mergeReplicaGSIAutoScalingUpdates merges a replica's per-GSI read-capacity
// updates into existing (nil if none stored yet).
func mergeReplicaGSIAutoScalingUpdates(
	existing map[string]*autoScalingThroughput,
	updates []types.ReplicaGlobalSecondaryIndexAutoScalingUpdate,
) map[string]*autoScalingThroughput {
	out := existing
	if out == nil {
		out = make(map[string]*autoScalingThroughput, len(updates))
	}

	for _, g := range updates {
		if g.IndexName == nil {
			continue
		}
		out[*g.IndexName] = throughputFromUpdate(g.ProvisionedReadCapacityAutoScalingUpdate)
	}

	return out
}

// throughputFromUpdate translates the SDK AutoScalingSettingsUpdate struct
// into the persisted shape. Returns nil when no fields were supplied so the
// caller can distinguish "explicitly cleared" from "untouched". Echoes
// AutoScalingRoleArn/ScalingPolicyUpdate back exactly as supplied -- this
// emulator has no IAM-role or scaling-policy engine, so it is not fabricating
// a value, only round-tripping the caller's own input.
func throughputFromUpdate(u *types.AutoScalingSettingsUpdate) *autoScalingThroughput {
	if u == nil {
		return nil
	}

	out := &autoScalingThroughput{
		MinCapacity: u.MinimumUnits,
		MaxCapacity: u.MaximumUnits,
		RoleArn:     u.AutoScalingRoleArn,
	}
	if u.AutoScalingDisabled != nil {
		out.Disabled = *u.AutoScalingDisabled
	}
	if u.ScalingPolicyUpdate != nil {
		out.PolicyName = u.ScalingPolicyUpdate.PolicyName
		if tt := u.ScalingPolicyUpdate.TargetTrackingScalingPolicyConfiguration; tt != nil {
			out.TargetUtilizPct = tt.TargetValue
			out.DisableScaleIn = tt.DisableScaleIn
			out.ScaleInCooldown = tt.ScaleInCooldown
			out.ScaleOutCooldown = tt.ScaleOutCooldown
		}
	}

	return out
}

// validateAutoScalingSettingsUpdate rejects MinimumUnits > MaximumUnits when
// both are supplied. Real DynamoDB documents the two as independent bounds
// (API_AutoScalingSettingsUpdate.html) but publishes no verbatim rejection
// string for an inverted range; this wording is our own, disclosed the same
// way as this file's other undocumented-error-text validations.
func validateAutoScalingSettingsUpdate(u *types.AutoScalingSettingsUpdate) error {
	if u == nil {
		return nil
	}
	if u.MinimumUnits != nil && u.MaximumUnits != nil && *u.MinimumUnits > *u.MaximumUnits {
		return NewValidationException("MinimumUnits must be less than or equal to MaximumUnits")
	}

	return nil
}

// autoScalingUpdateRequestsSettings reports whether input asks to change any
// autoscaling configuration at all. A bare TableName (as a real client sends
// to refresh replica status) must not trip the PROVISIONED-only gate below.
func autoScalingUpdateRequestsSettings(input *dynamodb.UpdateTableReplicaAutoScalingInput) bool {
	return input.ProvisionedWriteCapacityAutoScalingUpdate != nil ||
		len(input.GlobalSecondaryIndexUpdates) > 0 ||
		len(input.ReplicaUpdates) > 0
}

// validateAutoScalingUpdateInput checks every AutoScalingSettingsUpdate the
// input carries -- table-level, per-GSI, per-replica, and per-replica-per-GSI.
func validateAutoScalingUpdateInput(input *dynamodb.UpdateTableReplicaAutoScalingInput) error {
	if err := validateAutoScalingSettingsUpdate(input.ProvisionedWriteCapacityAutoScalingUpdate); err != nil {
		return err
	}

	for _, g := range input.GlobalSecondaryIndexUpdates {
		if err := validateAutoScalingSettingsUpdate(g.ProvisionedWriteCapacityAutoScalingUpdate); err != nil {
			return err
		}
	}

	for _, r := range input.ReplicaUpdates {
		if err := validateAutoScalingSettingsUpdate(r.ReplicaProvisionedReadCapacityAutoScalingUpdate); err != nil {
			return err
		}
		for _, g := range r.ReplicaGlobalSecondaryIndexUpdates {
			if err := validateAutoScalingSettingsUpdate(g.ProvisionedReadCapacityAutoScalingUpdate); err != nil {
				return err
			}
		}
	}

	return nil
}

// tableHasReplicaRegion reports whether table.Replicas already contains
// region. Callers must hold table.mu.
func tableHasReplicaRegion(table *Table, region string) bool {
	for _, r := range table.Replicas {
		if r.RegionName == region {
			return true
		}
	}

	return false
}

// applyAutoScalingSettingsLocked validates and applies input under a single
// defer-protected table.mu.Lock, returning the table's name and status.
func applyAutoScalingSettingsLocked(
	table *Table,
	input *dynamodb.UpdateTableReplicaAutoScalingInput,
) (string, string, error) {
	table.mu.Lock("UpdateTableReplicaAutoScaling")
	defer table.mu.Unlock()

	if autoScalingUpdateRequestsSettings(input) && isOnDemandTable(table.BillingMode) {
		return "", "", NewValidationException(
			"AutoScaling is not available for tables with PAY_PER_REQUEST billing mode",
		)
	}

	if err := validateAutoScalingUpdateInput(input); err != nil {
		return "", "", err
	}

	for _, r := range input.ReplicaUpdates {
		region := aws.ToString(r.RegionName)
		if region != "" && !tableHasReplicaRegion(table, region) {
			return "", "", NewResourceNotFoundException(
				fmt.Sprintf("Replica not found for region: %s", region),
			)
		}
	}

	table.AutoScaling = mergeAutoScalingSettingsFromInput(table.AutoScaling, input)
	table.ReplicaAutoScaling = mergeReplicaAutoScalingFromUpdates(
		table.ReplicaAutoScaling,
		input.ReplicaUpdates,
	)

	return table.Name, table.Status, nil
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

	tableName, _, applyErr := applyAutoScalingSettingsLocked(table, input)
	if applyErr != nil {
		return nil, applyErr
	}

	tableStatus, replicaDescs := replicaAutoScalingDescriptionsRLocked(table)

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
// AutoScalingRoleArn and ScalingPolicies are echoed back exactly as the
// client supplied them on the matching Update call.
func sdkAutoScalingSettingsDescription(
	t *autoScalingThroughput,
) *types.AutoScalingSettingsDescription {
	if t == nil {
		return nil
	}

	disabled := t.Disabled

	desc := &types.AutoScalingSettingsDescription{
		MinimumUnits:        t.MinCapacity,
		MaximumUnits:        t.MaxCapacity,
		AutoScalingDisabled: &disabled,
		AutoScalingRoleArn:  t.RoleArn,
	}

	if t.TargetUtilizPct != nil {
		desc.ScalingPolicies = []types.AutoScalingPolicyDescription{
			{
				PolicyName: t.PolicyName,
				TargetTrackingScalingPolicyConfiguration: &types.AutoScalingTargetTrackingScalingPolicyConfigurationDescription{
					TargetValue:      t.TargetUtilizPct,
					DisableScaleIn:   t.DisableScaleIn,
					ScaleInCooldown:  t.ScaleInCooldown,
					ScaleOutCooldown: t.ScaleOutCooldown,
				},
			},
		}
	}

	return desc
}

// --- DescribeTableReplicaAutoScaling ---

// buildReplicaGSIAutoScalingDescriptions merges per-index write settings
// (table-wide, from table.AutoScaling.GlobalSecondaryIndexes) with per-index
// read settings (per-replica, from table.ReplicaAutoScaling[region]) into one
// sorted ReplicaGlobalSecondaryIndexAutoScalingDescription list.
func buildReplicaGSIAutoScalingDescriptions(
	write, read map[string]*types.AutoScalingSettingsDescription,
) []types.ReplicaGlobalSecondaryIndexAutoScalingDescription {
	if len(write) == 0 && len(read) == 0 {
		return nil
	}

	names := make(map[string]struct{}, len(write)+len(read))
	for name := range write {
		names[name] = struct{}{}
	}
	for name := range read {
		names[name] = struct{}{}
	}

	out := make([]types.ReplicaGlobalSecondaryIndexAutoScalingDescription, 0, len(names))
	for name := range names {
		idxName := name
		out = append(out, types.ReplicaGlobalSecondaryIndexAutoScalingDescription{
			IndexName:   &idxName,
			IndexStatus: types.IndexStatusActive,
			ProvisionedWriteCapacityAutoScalingSettings: write[name],
			ProvisionedReadCapacityAutoScalingSettings:  read[name],
		})
	}
	sort.Slice(out, func(i, j int) bool { return *out[i].IndexName < *out[j].IndexName })

	return out
}

// replicaAutoScalingDescriptionsRLocked copies table.Status and table.Replicas,
// along with the table's write-capacity autoscaling settings (applied
// uniformly to every replica -- this emulator doesn't model per-replica write
// overrides, matching AWS's own v1 "one write capacity per global table" model)
// and each replica's own read-capacity settings from table.ReplicaAutoScaling,
// into the SDK description type under a defer-protected table.mu.RLock.
func replicaAutoScalingDescriptionsRLocked(
	table *Table,
) (string, []types.ReplicaAutoScalingDescription) {
	table.mu.RLock(opDescribeTableReplicaAutoScaling)
	defer table.mu.RUnlock()

	var write *types.AutoScalingSettingsDescription
	gsiWrite := map[string]*types.AutoScalingSettingsDescription{}
	if table.AutoScaling != nil {
		write = sdkAutoScalingSettingsDescription(table.AutoScaling.Write)
		for name, throughput := range table.AutoScaling.GlobalSecondaryIndexes {
			gsiWrite[name] = sdkAutoScalingSettingsDescription(throughput)
		}
	}

	replicas := make([]types.ReplicaAutoScalingDescription, 0, len(table.Replicas))
	for _, r := range table.Replicas {
		region := r.RegionName
		status := r.ReplicaStatus

		var read *types.AutoScalingSettingsDescription
		gsiRead := map[string]*types.AutoScalingSettingsDescription{}
		if rs := table.ReplicaAutoScaling[region]; rs != nil {
			read = sdkAutoScalingSettingsDescription(rs.Read)
			for name, throughput := range rs.GlobalSecondaryIndexes {
				gsiRead[name] = sdkAutoScalingSettingsDescription(throughput)
			}
		}

		replicas = append(replicas, types.ReplicaAutoScalingDescription{
			RegionName:    &region,
			ReplicaStatus: types.ReplicaStatus(status),
			ReplicaProvisionedWriteCapacityAutoScalingSettings: write,
			ReplicaProvisionedReadCapacityAutoScalingSettings:  read,
			GlobalSecondaryIndexes: buildReplicaGSIAutoScalingDescriptions(
				gsiWrite,
				gsiRead,
			),
		})
	}

	return table.Status, replicas
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

	tableStatus, replicas := replicaAutoScalingDescriptionsRLocked(table)

	return &dynamodb.DescribeTableReplicaAutoScalingOutput{
		TableAutoScalingDescription: &types.TableAutoScalingDescription{
			TableName:   &tableName,
			TableStatus: types.TableStatus(tableStatus),
			Replicas:    replicas,
		},
	}, nil
}
