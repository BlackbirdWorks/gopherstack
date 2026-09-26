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
// awsAwsjson10_serializeDocumentAutoScalingSettingsUpdate).
type autoScalingSettingsUpdateWire struct {
	ScalingPolicyUpdate *autoScalingPolicyUpdateWire `json:"ScalingPolicyUpdate,omitempty"`
	AutoScalingRoleArn  *string                      `json:"AutoScalingRoleArn,omitempty"`
	MinimumUnits        *int64                       `json:"MinimumUnits,omitempty"`
	MaximumUnits        *int64                       `json:"MaximumUnits,omitempty"`
	AutoScalingDisabled *bool                        `json:"AutoScalingDisabled,omitempty"`
}

type autoScalingPolicyUpdateWire struct {
	TargetTracking *autoScalingTargetTrackingUpdateWire `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	PolicyName     string                               `json:"PolicyName,omitempty"`
}

type autoScalingTargetTrackingUpdateWire struct {
	TargetValue      float64 `json:"TargetValue"`
	DisableScaleIn   bool    `json:"DisableScaleIn,omitempty"`
	ScaleInCooldown  int32   `json:"ScaleInCooldown,omitempty"`
	ScaleOutCooldown int32   `json:"ScaleOutCooldown,omitempty"`
}

// gsiAutoScalingUpdateWire is the wire format for
// types.GlobalSecondaryIndexAutoScalingUpdate.
type gsiAutoScalingUpdateWire struct {
	WriteCapacityUpdate *autoScalingSettingsUpdateWire `json:"ProvisionedWriteCapacityAutoScalingUpdate,omitempty"`
	IndexName           string                         `json:"IndexName,omitempty"`
}

// replicaGSIAutoScalingUpdateWire is the wire format for
// types.ReplicaGlobalSecondaryIndexAutoScalingUpdate.
type replicaGSIAutoScalingUpdateWire struct {
	ReadCapacityUpdate *autoScalingSettingsUpdateWire `json:"ProvisionedReadCapacityAutoScalingUpdate,omitempty"`
	IndexName          string                         `json:"IndexName,omitempty"`
}

// replicaAutoScalingUpdateWire is the wire format for
// types.ReplicaAutoScalingUpdate.
type replicaAutoScalingUpdateWire struct {
	ReadCapacityUpdate *autoScalingSettingsUpdateWire    `json:"ReplicaProvisionedReadCapacityAutoScalingUpdate,omitempty"`
	RegionName         string                            `json:"RegionName"`
	GSIUpdates         []replicaGSIAutoScalingUpdateWire `json:"ReplicaGlobalSecondaryIndexUpdates,omitempty"`
}

// updateTableReplicaAutoScalingInput is the wire format for
// UpdateTableReplicaAutoScaling.
type updateTableReplicaAutoScalingInput struct {
	WriteCapacityUpdate         *autoScalingSettingsUpdateWire `json:"ProvisionedWriteCapacityAutoScalingUpdate,omitempty"`
	TableName                   string                         `json:"TableName"`
	GlobalSecondaryIndexUpdates []gsiAutoScalingUpdateWire     `json:"GlobalSecondaryIndexUpdates,omitempty"`
	ReplicaUpdates              []replicaAutoScalingUpdateWire `json:"ReplicaUpdates,omitempty"`
}

// int32PtrIfNonZero returns nil for a zero value so an omitted wire field
// (Go zero value) doesn't turn into an explicit SDK zero-cooldown.
func int32PtrIfNonZero(v int32) *int32 {
	if v == 0 {
		return nil
	}

	return &v
}

// int32Val dereferences an *int32, returning 0 for nil.
func int32Val(v *int32) int32 {
	if v == nil {
		return 0
	}

	return *v
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
		AutoScalingRoleArn:  w.AutoScalingRoleArn,
	}

	if w.ScalingPolicyUpdate != nil && w.ScalingPolicyUpdate.TargetTracking != nil {
		tt := w.ScalingPolicyUpdate.TargetTracking
		out.ScalingPolicyUpdate = &types.AutoScalingPolicyUpdate{
			PolicyName: ptrconv.NilIfEmpty(w.ScalingPolicyUpdate.PolicyName),
			TargetTrackingScalingPolicyConfiguration: &types.AutoScalingTargetTrackingScalingPolicyConfigurationUpdate{
				TargetValue:      &tt.TargetValue,
				DisableScaleIn:   &tt.DisableScaleIn,
				ScaleInCooldown:  int32PtrIfNonZero(tt.ScaleInCooldown),
				ScaleOutCooldown: int32PtrIfNonZero(tt.ScaleOutCooldown),
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

// toSDKReplicaAutoScalingUpdates converts the wire ReplicaUpdates slice to SDK form.
func toSDKReplicaAutoScalingUpdates(w []replicaAutoScalingUpdateWire) []types.ReplicaAutoScalingUpdate {
	if len(w) == 0 {
		return nil
	}

	out := make([]types.ReplicaAutoScalingUpdate, len(w))
	for i, r := range w {
		regionName := r.RegionName
		out[i] = types.ReplicaAutoScalingUpdate{
			RegionName: &regionName,
			ReplicaProvisionedReadCapacityAutoScalingUpdate: toSDKAutoScalingSettingsUpdate(r.ReadCapacityUpdate),
			ReplicaGlobalSecondaryIndexUpdates:              toSDKReplicaGSIAutoScalingUpdates(r.GSIUpdates),
		}
	}

	return out
}

// toSDKReplicaGSIAutoScalingUpdates converts the wire per-replica GSI slice to SDK form.
func toSDKReplicaGSIAutoScalingUpdates(
	w []replicaGSIAutoScalingUpdateWire,
) []types.ReplicaGlobalSecondaryIndexAutoScalingUpdate {
	if len(w) == 0 {
		return nil
	}

	out := make([]types.ReplicaGlobalSecondaryIndexAutoScalingUpdate, len(w))
	for i, g := range w {
		indexName := g.IndexName
		out[i] = types.ReplicaGlobalSecondaryIndexAutoScalingUpdate{
			IndexName:                                &indexName,
			ProvisionedReadCapacityAutoScalingUpdate: toSDKAutoScalingSettingsUpdate(g.ReadCapacityUpdate),
		}
	}

	return out
}

// autoScalingSettingsDescWire is the wire format for
// types.AutoScalingSettingsDescription.
type autoScalingSettingsDescWire struct {
	MinimumUnits        *int64                      `json:"MinimumUnits,omitempty"`
	MaximumUnits        *int64                      `json:"MaximumUnits,omitempty"`
	AutoScalingRoleArn  *string                     `json:"AutoScalingRoleArn,omitempty"`
	AutoScalingDisabled *bool                       `json:"AutoScalingDisabled,omitempty"`
	ScalingPolicies     []autoScalingPolicyDescWire `json:"ScalingPolicies,omitempty"`
}

// autoScalingPolicyDescWire is the wire format for
// types.AutoScalingPolicyDescription.
type autoScalingPolicyDescWire struct {
	TargetTracking *autoScalingTargetTrackingDescWire `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	PolicyName     string                             `json:"PolicyName,omitempty"`
}

// autoScalingTargetTrackingDescWire is the wire format for
// types.AutoScalingTargetTrackingScalingPolicyConfigurationDescription.
type autoScalingTargetTrackingDescWire struct {
	TargetValue      float64 `json:"TargetValue"`
	DisableScaleIn   bool    `json:"DisableScaleIn,omitempty"`
	ScaleInCooldown  int32   `json:"ScaleInCooldown,omitempty"`
	ScaleOutCooldown int32   `json:"ScaleOutCooldown,omitempty"`
}

// replicaGSIAutoScalingDescWire is the wire format for
// types.ReplicaGlobalSecondaryIndexAutoScalingDescription.
type replicaGSIAutoScalingDescWire struct {
	WriteCap    *autoScalingSettingsDescWire `json:"ProvisionedWriteCapacityAutoScalingSettings,omitempty"`
	ReadCap     *autoScalingSettingsDescWire `json:"ProvisionedReadCapacityAutoScalingSettings,omitempty"`
	IndexName   string                       `json:"IndexName,omitempty"`
	IndexStatus string                       `json:"IndexStatus,omitempty"`
}

// replicaAutoScalingDescWire is the wire format for
// types.ReplicaAutoScalingDescription.
type replicaAutoScalingDescWire struct {
	WriteCap      *autoScalingSettingsDescWire    `json:"ReplicaProvisionedWriteCapacityAutoScalingSettings,omitempty"`
	ReadCap       *autoScalingSettingsDescWire    `json:"ReplicaProvisionedReadCapacityAutoScalingSettings,omitempty"`
	RegionName    string                          `json:"RegionName,omitempty"`
	ReplicaStatus string                          `json:"ReplicaStatus,omitempty"`
	GSIs          []replicaGSIAutoScalingDescWire `json:"GlobalSecondaryIndexes,omitempty"`
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
			ReplicaUpdates: toSDKReplicaAutoScalingUpdates(req.ReplicaUpdates),
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
		var gsis []replicaGSIAutoScalingDescWire
		if len(r.GlobalSecondaryIndexes) > 0 {
			gsis = make([]replicaGSIAutoScalingDescWire, 0, len(r.GlobalSecondaryIndexes))
			for _, g := range r.GlobalSecondaryIndexes {
				gsis = append(gsis, replicaGSIAutoScalingDescWire{
					IndexName:   ptrconv.String(g.IndexName),
					IndexStatus: string(g.IndexStatus),
					WriteCap: autoScalingSettingsDescWireFromSDK(
						g.ProvisionedWriteCapacityAutoScalingSettings,
					),
					ReadCap: autoScalingSettingsDescWireFromSDK(
						g.ProvisionedReadCapacityAutoScalingSettings,
					),
				})
			}
		}
		desc.Replicas = append(desc.Replicas, replicaAutoScalingDescWire{
			RegionName:    ptrconv.String(r.RegionName),
			ReplicaStatus: string(r.ReplicaStatus),
			WriteCap: autoScalingSettingsDescWireFromSDK(
				r.ReplicaProvisionedWriteCapacityAutoScalingSettings,
			),
			ReadCap: autoScalingSettingsDescWireFromSDK(
				r.ReplicaProvisionedReadCapacityAutoScalingSettings,
			),
			GSIs: gsis,
		})
	}

	return desc
}

// autoScalingSettingsDescWireFromSDK converts the SDK description to the wire shape.
func autoScalingSettingsDescWireFromSDK(d *types.AutoScalingSettingsDescription) *autoScalingSettingsDescWire {
	if d == nil {
		return nil
	}

	out := &autoScalingSettingsDescWire{
		MinimumUnits:        d.MinimumUnits,
		MaximumUnits:        d.MaximumUnits,
		AutoScalingDisabled: d.AutoScalingDisabled,
		AutoScalingRoleArn:  d.AutoScalingRoleArn,
	}

	if len(d.ScalingPolicies) > 0 {
		out.ScalingPolicies = make([]autoScalingPolicyDescWire, 0, len(d.ScalingPolicies))
		for _, p := range d.ScalingPolicies {
			pw := autoScalingPolicyDescWire{PolicyName: ptrconv.String(p.PolicyName)}
			if tt := p.TargetTrackingScalingPolicyConfiguration; tt != nil {
				pw.TargetTracking = &autoScalingTargetTrackingDescWire{
					TargetValue:      ptrconv.Float64(tt.TargetValue),
					DisableScaleIn:   ptrconv.Bool(tt.DisableScaleIn),
					ScaleInCooldown:  int32Val(tt.ScaleInCooldown),
					ScaleOutCooldown: int32Val(tt.ScaleOutCooldown),
				}
			}
			out.ScalingPolicies = append(out.ScalingPolicies, pw)
		}
	}

	return out
}
