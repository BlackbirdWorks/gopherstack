// Package dynamodb implements the AWS DynamoDB mock service.
// handler_global_tables.go implements the wire-JSON handlers for the global
// tables family (Create/Describe/List/Update GlobalTable[Settings]). Routing
// (dispatchExtraOps) stays in handler.go; these are the leaf implementations
// it calls into. Backend logic lives in global_tables.go.
package dynamodb

import (
	"context"
	"encoding/json"

	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

type globalTableReplicaWire struct {
	RegionName string `json:"RegionName"`
}

type globalTableDescriptionWire struct {
	GlobalTableArn    string                   `json:"GlobalTableArn,omitempty"`
	GlobalTableName   string                   `json:"GlobalTableName,omitempty"`
	GlobalTableStatus string                   `json:"GlobalTableStatus,omitempty"`
	ReplicationGroup  []globalTableReplicaWire `json:"ReplicationGroup,omitempty"`
	CreationDateTime  float64                  `json:"CreationDateTime,omitempty"`
}

type createGlobalTableInput struct {
	GlobalTableName  string                   `json:"GlobalTableName"`
	ReplicationGroup []globalTableReplicaWire `json:"ReplicationGroup"`
}

type createGlobalTableOutput struct {
	GlobalTableDescription globalTableDescriptionWire `json:"GlobalTableDescription"`
}

// updateGlobalTableReplicaActionWire wraps a Create or Delete action for a single region.
type updateGlobalTableReplicaActionWire struct {
	RegionName string `json:"RegionName,omitempty"`
}

// updateGlobalTableReplicaUpdateWire represents a single Create or Delete replica action.
type updateGlobalTableReplicaUpdateWire struct {
	Create *updateGlobalTableReplicaActionWire `json:"Create,omitempty"`
	Delete *updateGlobalTableReplicaActionWire `json:"Delete,omitempty"`
}

type updateGlobalTableInput struct {
	GlobalTableName string                               `json:"GlobalTableName"`
	ReplicaUpdates  []updateGlobalTableReplicaUpdateWire `json:"ReplicaUpdates"`
}

type updateGlobalTableOutput struct {
	GlobalTableDescription globalTableDescriptionWire `json:"GlobalTableDescription"`
}

type describeGlobalTableInput struct {
	GlobalTableName string `json:"GlobalTableName"`
}

type describeGlobalTableOutput struct {
	GlobalTableDescription globalTableDescriptionWire `json:"GlobalTableDescription"`
}

type describeGlobalTableSettingsInput struct {
	GlobalTableName string `json:"GlobalTableName"`
}

type describeGlobalTableSettingsOutput struct {
	GlobalTableName string                    `json:"GlobalTableName,omitempty"`
	ReplicaSettings []replicaSettingsDescWire `json:"ReplicaSettings,omitempty"`
}

type globalTableWire struct {
	GlobalTableName  string                   `json:"GlobalTableName,omitempty"`
	ReplicationGroup []globalTableReplicaWire `json:"ReplicationGroup,omitempty"`
}

type listGlobalTablesInput struct {
	ExclusiveStartGlobalTableName string `json:"ExclusiveStartGlobalTableName,omitempty"`
	RegionName                    string `json:"RegionName,omitempty"`
	Limit                         int32  `json:"Limit,omitempty"`
}

type listGlobalTablesOutput struct {
	LastEvaluatedGlobalTableName string            `json:"LastEvaluatedGlobalTableName,omitempty"`
	GlobalTables                 []globalTableWire `json:"GlobalTables"`
}

func (h *DynamoDBHandler) handleCreateGlobalTable(ctx context.Context, body []byte) (any, error) {
	var req createGlobalTableInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	replicas := make([]types.Replica, 0, len(req.ReplicationGroup))

	for _, r := range req.ReplicationGroup {
		regionName := r.RegionName
		replicas = append(replicas, types.Replica{RegionName: &regionName})
	}

	out, err := h.Backend.CreateGlobalTable(ctx, &sdkDDB.CreateGlobalTableInput{
		GlobalTableName:  &req.GlobalTableName,
		ReplicationGroup: replicas,
	})
	if err != nil {
		return nil, err
	}

	d := out.GlobalTableDescription
	wire := buildGlobalTableDescriptionWire(d)

	return &createGlobalTableOutput{GlobalTableDescription: wire}, nil
}

func (h *DynamoDBHandler) handleDescribeGlobalTable(ctx context.Context, body []byte) (any, error) {
	var req describeGlobalTableInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DescribeGlobalTable(ctx, &sdkDDB.DescribeGlobalTableInput{
		GlobalTableName: &req.GlobalTableName,
	})
	if err != nil {
		return nil, err
	}

	wire := buildGlobalTableDescriptionWire(out.GlobalTableDescription)

	return &describeGlobalTableOutput{GlobalTableDescription: wire}, nil
}

func (h *DynamoDBHandler) handleDescribeGlobalTableSettings(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req describeGlobalTableSettingsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DescribeGlobalTableSettings(ctx, &sdkDDB.DescribeGlobalTableSettingsInput{
		GlobalTableName: &req.GlobalTableName,
	})
	if err != nil {
		return nil, err
	}

	replicaSettings := make([]replicaSettingsDescWire, 0, len(out.ReplicaSettings))
	for _, rs := range out.ReplicaSettings {
		replicaSettings = append(replicaSettings, replicaSettingsDescWireFromSDK(rs))
	}

	return &describeGlobalTableSettingsOutput{
		GlobalTableName: ptrconv.String(out.GlobalTableName),
		ReplicaSettings: replicaSettings,
	}, nil
}

func (h *DynamoDBHandler) handleListGlobalTables(ctx context.Context, body []byte) (any, error) {
	var req listGlobalTablesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, err
		}
	}

	sdkInput := &sdkDDB.ListGlobalTablesInput{}
	if req.ExclusiveStartGlobalTableName != "" {
		sdkInput.ExclusiveStartGlobalTableName = &req.ExclusiveStartGlobalTableName
	}

	if req.RegionName != "" {
		sdkInput.RegionName = &req.RegionName
	}

	if req.Limit > 0 {
		sdkInput.Limit = &req.Limit
	}

	out, err := h.Backend.ListGlobalTables(ctx, sdkInput)
	if err != nil {
		return nil, err
	}

	tables := make([]globalTableWire, 0, len(out.GlobalTables))
	for _, gt := range out.GlobalTables {
		replicas := make([]globalTableReplicaWire, 0, len(gt.ReplicationGroup))
		for _, r := range gt.ReplicationGroup {
			replicas = append(
				replicas,
				globalTableReplicaWire{RegionName: ptrconv.String(r.RegionName)},
			)
		}

		tables = append(tables, globalTableWire{
			GlobalTableName:  ptrconv.String(gt.GlobalTableName),
			ReplicationGroup: replicas,
		})
	}

	wire := &listGlobalTablesOutput{GlobalTables: tables}
	if out.LastEvaluatedGlobalTableName != nil {
		wire.LastEvaluatedGlobalTableName = *out.LastEvaluatedGlobalTableName
	}

	return wire, nil
}

func (h *DynamoDBHandler) handleUpdateGlobalTable(ctx context.Context, body []byte) (any, error) {
	var req updateGlobalTableInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	updates := make([]types.ReplicaUpdate, 0, len(req.ReplicaUpdates))
	for _, u := range req.ReplicaUpdates {
		var update types.ReplicaUpdate
		if u.Create != nil {
			regionName := u.Create.RegionName
			update.Create = &types.CreateReplicaAction{RegionName: &regionName}
		} else if u.Delete != nil {
			regionName := u.Delete.RegionName
			update.Delete = &types.DeleteReplicaAction{RegionName: &regionName}
		}

		updates = append(updates, update)
	}

	out, err := h.Backend.UpdateGlobalTable(ctx, &sdkDDB.UpdateGlobalTableInput{
		GlobalTableName: &req.GlobalTableName,
		ReplicaUpdates:  updates,
	})
	if err != nil {
		return nil, err
	}

	d := out.GlobalTableDescription
	wire := buildGlobalTableDescriptionWire(d)

	return &updateGlobalTableOutput{GlobalTableDescription: wire}, nil
}

// buildGlobalTableDescriptionWire converts the SDK GlobalTableDescription to the wire format.
func buildGlobalTableDescriptionWire(d *types.GlobalTableDescription) globalTableDescriptionWire {
	if d == nil {
		return globalTableDescriptionWire{}
	}

	replicas := make([]globalTableReplicaWire, 0, len(d.ReplicationGroup))
	for _, r := range d.ReplicationGroup {
		replicas = append(
			replicas,
			globalTableReplicaWire{RegionName: ptrconv.String(r.RegionName)},
		)
	}

	wire := globalTableDescriptionWire{
		GlobalTableName:   ptrconv.String(d.GlobalTableName),
		GlobalTableArn:    ptrconv.String(d.GlobalTableArn),
		GlobalTableStatus: string(d.GlobalTableStatus),
		ReplicationGroup:  replicas,
	}

	if d.CreationDateTime != nil {
		wire.CreationDateTime = float64(d.CreationDateTime.Unix())
	}

	return wire
}

// --- UpdateGlobalTableSettings handler ---

type billingModeSummaryWire struct {
	BillingMode string `json:"BillingMode,omitempty"`
}

type tableClassSummaryWire struct {
	TableClass string `json:"TableClass,omitempty"`
}

type gsiUpdateWire struct {
	ProvisionedReadCapacityUnits *int64 `json:"ProvisionedReadCapacityUnits,omitempty"`
	IndexName                    string `json:"IndexName"`
}

type replicaSettingsUpdateInputWire struct {
	ReplicaProvisionedReadCapacityUnits       *int64          `json:"ReplicaProvisionedReadCapacityUnits,omitempty"`
	ReplicaTableClass                         string          `json:"ReplicaTableClass,omitempty"`
	RegionName                                string          `json:"RegionName"`
	ReplicaGlobalSecondaryIndexSettingsUpdate []gsiUpdateWire `json:"ReplicaGlobalSecondaryIndexSettingsUpdate,omitempty"`
}

type updateGlobalTableSettingsInput struct {
	GlobalTableName          string                           `json:"GlobalTableName"`
	GlobalTableBillingMode   string                           `json:"GlobalTableBillingMode,omitempty"`
	ProvisionedWriteCapacity *int64                           `json:"GlobalTableProvisionedWriteCapacityUnits,omitempty"`
	ReplicaSettingsUpdate    []replicaSettingsUpdateInputWire `json:"ReplicaSettingsUpdate,omitempty"`
}

type replicaGSIDescWire struct {
	ProvisionedReadCapacityUnits  *int64 `json:"ProvisionedReadCapacityUnits,omitempty"`
	ProvisionedWriteCapacityUnits *int64 `json:"ProvisionedWriteCapacityUnits,omitempty"`
	IndexName                     string `json:"IndexName"`
	IndexStatus                   string `json:"IndexStatus,omitempty"`
}

type replicaSettingsDescWire struct {
	ReplicaBillingModeSummary            *billingModeSummaryWire `json:"ReplicaBillingModeSummary,omitempty"`
	ReplicaTableClassSummary             *tableClassSummaryWire  `json:"ReplicaTableClassSummary,omitempty"`
	ReplicaProvisionedReadCapacityUnits  *int64                  `json:"ReplicaProvisionedReadCapacityUnits,omitempty"`
	ReplicaProvisionedWriteCapacityUnits *int64                  `json:"ReplicaProvisionedWriteCapacityUnits,omitempty"`
	RegionName                           string                  `json:"RegionName"`
	ReplicaStatus                        string                  `json:"ReplicaStatus,omitempty"`
	ReplicaGlobalSecondaryIndexSettings  []replicaGSIDescWire    `json:"ReplicaGlobalSecondaryIndexSettings,omitempty"`
}

// replicaSettingsDescWireFromSDK converts the SDK ReplicaSettingsDescription
// to the wire shape. Shared by DescribeGlobalTableSettings and
// UpdateGlobalTableSettings, which return the same type -- previously each
// handler hand-rolled its own narrower conversion and only one of them
// carried ReplicaBillingModeSummary/ReplicaTableClassSummary through to JSON.
func replicaSettingsDescWireFromSDK(rs types.ReplicaSettingsDescription) replicaSettingsDescWire {
	w := replicaSettingsDescWire{
		RegionName:    ptrconv.String(rs.RegionName),
		ReplicaStatus: string(rs.ReplicaStatus),
	}

	if rs.ReplicaBillingModeSummary != nil {
		w.ReplicaBillingModeSummary = &billingModeSummaryWire{
			BillingMode: string(rs.ReplicaBillingModeSummary.BillingMode),
		}
	}

	if rs.ReplicaTableClassSummary != nil {
		w.ReplicaTableClassSummary = &tableClassSummaryWire{
			TableClass: string(rs.ReplicaTableClassSummary.TableClass),
		}
	}

	if rs.ReplicaProvisionedReadCapacityUnits != nil {
		rcu := *rs.ReplicaProvisionedReadCapacityUnits
		w.ReplicaProvisionedReadCapacityUnits = &rcu
	}

	if rs.ReplicaProvisionedWriteCapacityUnits != nil {
		wcu := *rs.ReplicaProvisionedWriteCapacityUnits
		w.ReplicaProvisionedWriteCapacityUnits = &wcu
	}

	for _, gsi := range rs.ReplicaGlobalSecondaryIndexSettings {
		gw := replicaGSIDescWire{
			IndexName:   ptrconv.String(gsi.IndexName),
			IndexStatus: string(gsi.IndexStatus),
		}
		if gsi.ProvisionedReadCapacityUnits != nil {
			rcu := *gsi.ProvisionedReadCapacityUnits
			gw.ProvisionedReadCapacityUnits = &rcu
		}
		if gsi.ProvisionedWriteCapacityUnits != nil {
			wcu := *gsi.ProvisionedWriteCapacityUnits
			gw.ProvisionedWriteCapacityUnits = &wcu
		}
		w.ReplicaGlobalSecondaryIndexSettings = append(w.ReplicaGlobalSecondaryIndexSettings, gw)
	}

	return w
}

type updateGlobalTableSettingsOutput struct {
	GlobalTableName string                    `json:"GlobalTableName"`
	ReplicaSettings []replicaSettingsDescWire `json:"ReplicaSettings,omitempty"`
}

func (h *DynamoDBHandler) handleUpdateGlobalTableSettings(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req updateGlobalTableSettingsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	sdkInput := &sdkDDB.UpdateGlobalTableSettingsInput{
		GlobalTableName:                          &req.GlobalTableName,
		GlobalTableBillingMode:                   types.BillingMode(req.GlobalTableBillingMode),
		GlobalTableProvisionedWriteCapacityUnits: req.ProvisionedWriteCapacity,
	}

	if len(req.ReplicaSettingsUpdate) > 0 {
		sdkInput.ReplicaSettingsUpdate = make(
			[]types.ReplicaSettingsUpdate,
			len(req.ReplicaSettingsUpdate),
		)
		for i, ru := range req.ReplicaSettingsUpdate {
			region := ru.RegionName
			rUpdate := types.ReplicaSettingsUpdate{
				RegionName:                          &region,
				ReplicaTableClass:                   types.TableClass(ru.ReplicaTableClass),
				ReplicaProvisionedReadCapacityUnits: ru.ReplicaProvisionedReadCapacityUnits,
			}
			if len(ru.ReplicaGlobalSecondaryIndexSettingsUpdate) > 0 {
				gsiUpdates := make(
					[]types.ReplicaGlobalSecondaryIndexSettingsUpdate,
					len(ru.ReplicaGlobalSecondaryIndexSettingsUpdate),
				)
				for j, gu := range ru.ReplicaGlobalSecondaryIndexSettingsUpdate {
					idxName := gu.IndexName
					gsiUpdates[j] = types.ReplicaGlobalSecondaryIndexSettingsUpdate{
						IndexName:                    &idxName,
						ProvisionedReadCapacityUnits: gu.ProvisionedReadCapacityUnits,
					}
				}
				rUpdate.ReplicaGlobalSecondaryIndexSettingsUpdate = gsiUpdates
			}
			sdkInput.ReplicaSettingsUpdate[i] = rUpdate
		}
	}

	out, err := h.Backend.UpdateGlobalTableSettings(ctx, sdkInput)
	if err != nil {
		return nil, err
	}

	wire := make([]replicaSettingsDescWire, 0, len(out.ReplicaSettings))
	for _, rs := range out.ReplicaSettings {
		wire = append(wire, replicaSettingsDescWireFromSDK(rs))
	}

	return &updateGlobalTableSettingsOutput{
		GlobalTableName: ptrconv.String(out.GlobalTableName),
		ReplicaSettings: wire,
	}, nil
}
