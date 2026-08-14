// Package dynamodb implements the AWS DynamoDB mock service.
// handler_autoscaling.go implements the wire-JSON handler for
// UpdateTableReplicaAutoScaling. Routing (dispatchExtraOps) stays in
// handler.go; this is the leaf implementation it calls into. Backend logic
// lives in autoscaling.go.
package dynamodb

import (
	"context"
	"encoding/json"

	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// autoScalingSettingsUpdateWire is the wire format for
// types.AutoScalingSettingsUpdate (see serializers.go's
// awsAwsjson10_serializeDocumentAutoScalingSettingsUpdate). AutoScalingRoleArn
// is omitted: this emulator does not model IAM roles for scaling policies.
type autoScalingSettingsUpdateWire struct {
	ScalingPolicyUpdate *autoScalingPolicyUpdateWire `json:"ScalingPolicyUpdate,omitempty"`
	MinimumUnits        *int64                       `json:"MinimumUnits,omitempty"`
	MaximumUnits        *int64                       `json:"MaximumUnits,omitempty"`
	AutoScalingDisabled *bool                        `json:"AutoScalingDisabled,omitempty"`
}

type autoScalingPolicyUpdateWire struct {
	TargetTracking *autoScalingTargetTrackingUpdateWire `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	PolicyName     string                               `json:"PolicyName,omitempty"`
}

type autoScalingTargetTrackingUpdateWire struct {
	TargetValue    float64 `json:"TargetValue"`
	DisableScaleIn bool    `json:"DisableScaleIn,omitempty"`
}

// gsiAutoScalingUpdateWire is the wire format for
// types.GlobalSecondaryIndexAutoScalingUpdate.
type gsiAutoScalingUpdateWire struct {
	WriteCapacityUpdate *autoScalingSettingsUpdateWire `json:"ProvisionedWriteCapacityAutoScalingUpdate,omitempty"`
	IndexName           string                         `json:"IndexName,omitempty"`
}

// updateTableReplicaAutoScalingInput is the wire format for
// UpdateTableReplicaAutoScaling. ReplicaUpdates (per-replica overrides) is
// deliberately not modeled: this emulator's replica lifecycle is owned by
// UpdateGlobalTable/CreateGlobalTable, and per-replica autoscaling overrides
// don't map onto that model without a larger redesign.
type updateTableReplicaAutoScalingInput struct {
	WriteCapacityUpdate         *autoScalingSettingsUpdateWire `json:"ProvisionedWriteCapacityAutoScalingUpdate,omitempty"`
	TableName                   string                         `json:"TableName"`
	GlobalSecondaryIndexUpdates []gsiAutoScalingUpdateWire     `json:"GlobalSecondaryIndexUpdates,omitempty"`
}

// toSDKAutoScalingSettingsUpdate converts the wire form to the SDK type. w may
// be nil, matching an omitted request member.
func toSDKAutoScalingSettingsUpdate(w *autoScalingSettingsUpdateWire) *types.AutoScalingSettingsUpdate {
	if w == nil {
		return nil
	}

	out := &types.AutoScalingSettingsUpdate{
		MinimumUnits:        w.MinimumUnits,
		MaximumUnits:        w.MaximumUnits,
		AutoScalingDisabled: w.AutoScalingDisabled,
	}

	if w.ScalingPolicyUpdate != nil && w.ScalingPolicyUpdate.TargetTracking != nil {
		tt := w.ScalingPolicyUpdate.TargetTracking
		out.ScalingPolicyUpdate = &types.AutoScalingPolicyUpdate{
			PolicyName: ptrconv.NilIfEmpty(w.ScalingPolicyUpdate.PolicyName),
			TargetTrackingScalingPolicyConfiguration: &types.AutoScalingTargetTrackingScalingPolicyConfigurationUpdate{
				TargetValue:    &tt.TargetValue,
				DisableScaleIn: &tt.DisableScaleIn,
			},
		}
	}

	return out
}

// toSDKGlobalSecondaryIndexAutoScalingUpdates converts the wire slice to SDK form.
func toSDKGlobalSecondaryIndexAutoScalingUpdates(
	w []gsiAutoScalingUpdateWire,
) []types.GlobalSecondaryIndexAutoScalingUpdate {
	if len(w) == 0 {
		return nil
	}

	out := make([]types.GlobalSecondaryIndexAutoScalingUpdate, len(w))
	for i, g := range w {
		indexName := g.IndexName
		out[i] = types.GlobalSecondaryIndexAutoScalingUpdate{
			IndexName: &indexName,
			ProvisionedWriteCapacityAutoScalingUpdate: toSDKAutoScalingSettingsUpdate(g.WriteCapacityUpdate),
		}
	}

	return out
}

// autoScalingSettingsDescWire is the wire format for
// types.AutoScalingSettingsDescription, trimmed to the members this emulator
// tracks (min/max/disabled). AutoScalingRoleArn and ScalingPolicies'
// full nested policy list are not modeled.
type autoScalingSettingsDescWire struct {
	MinimumUnits        *int64 `json:"MinimumUnits,omitempty"`
	MaximumUnits        *int64 `json:"MaximumUnits,omitempty"`
	AutoScalingDisabled *bool  `json:"AutoScalingDisabled,omitempty"`
}

// autoScalingSettingsDescWireFromStored builds the wire description from the
// persisted autoScalingThroughput, or nil if t is nil.
func autoScalingSettingsDescWireFromStored(t *autoScalingThroughput) *autoScalingSettingsDescWire {
	if t == nil {
		return nil
	}

	disabled := t.Disabled

	return &autoScalingSettingsDescWire{
		MinimumUnits:        t.MinCapacity,
		MaximumUnits:        t.MaxCapacity,
		AutoScalingDisabled: &disabled,
	}
}

// replicaAutoScalingDescWire is the wire format for
// types.ReplicaAutoScalingDescription, trimmed to the members this emulator
// tracks (GlobalSecondaryIndexes and the read-capacity settings are not
// modeled).
type replicaAutoScalingDescWire struct {
	WriteCapAutoScaling *autoScalingSettingsDescWire `json:"ReplicaProvisionedWriteCapacityAutoScalingSettings,omitempty"`
	RegionName          string                       `json:"RegionName,omitempty"`
	ReplicaStatus       string                       `json:"ReplicaStatus,omitempty"`
}

type tableAutoScalingDescWire struct {
	TableName   string                       `json:"TableName,omitempty"`
	TableStatus string                       `json:"TableStatus,omitempty"`
	Replicas    []replicaAutoScalingDescWire `json:"Replicas,omitempty"`
}

type updateTableReplicaAutoScalingOutput struct {
	TableAutoScalingDescription tableAutoScalingDescWire `json:"TableAutoScalingDescription"`
}

func (h *DynamoDBHandler) handleUpdateTableReplicaAutoScaling(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req updateTableReplicaAutoScalingInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.UpdateTableReplicaAutoScaling(
		ctx,
		&sdkDDB.UpdateTableReplicaAutoScalingInput{
			TableName: &req.TableName,
			ProvisionedWriteCapacityAutoScalingUpdate: toSDKAutoScalingSettingsUpdate(req.WriteCapacityUpdate),
			GlobalSecondaryIndexUpdates: toSDKGlobalSecondaryIndexAutoScalingUpdates(
				req.GlobalSecondaryIndexUpdates,
			),
		},
	)
	if err != nil {
		return nil, err
	}

	return &updateTableReplicaAutoScalingOutput{
		TableAutoScalingDescription: buildTableAutoScalingDescWire(out.TableAutoScalingDescription),
	}, nil
}

// buildTableAutoScalingDescWire converts the SDK TableAutoScalingDescription
// to the wire shape, shared with DescribeTableReplicaAutoScaling.
func buildTableAutoScalingDescWire(d *types.TableAutoScalingDescription) tableAutoScalingDescWire {
	if d == nil {
		return tableAutoScalingDescWire{}
	}

	desc := tableAutoScalingDescWire{
		TableName:   ptrconv.String(d.TableName),
		TableStatus: string(d.TableStatus),
		Replicas:    make([]replicaAutoScalingDescWire, 0, len(d.Replicas)),
	}

	for _, r := range d.Replicas {
		desc.Replicas = append(desc.Replicas, replicaAutoScalingDescWire{
			RegionName:    ptrconv.String(r.RegionName),
			ReplicaStatus: string(r.ReplicaStatus),
			WriteCapAutoScaling: autoScalingSettingsDescWireFromSDK(
				r.ReplicaProvisionedWriteCapacityAutoScalingSettings,
			),
		})
	}

	return desc
}

// autoScalingSettingsDescWireFromSDK converts the SDK description to the wire
// shape, trimmed the same way autoScalingSettingsDescWire is.
func autoScalingSettingsDescWireFromSDK(d *types.AutoScalingSettingsDescription) *autoScalingSettingsDescWire {
	if d == nil {
		return nil
	}

	return &autoScalingSettingsDescWire{
		MinimumUnits:        d.MinimumUnits,
		MaximumUnits:        d.MaximumUnits,
		AutoScalingDisabled: d.AutoScalingDisabled,
	}
}
