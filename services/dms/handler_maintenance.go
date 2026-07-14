package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type applyPendingMaintenanceActionInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	ApplyAction            *string `json:"ApplyAction"`
	OptInType              *string `json:"OptInType"`
}

type resourcePendingMaintenanceActionsJSON struct {
	ResourceIdentifier              string `json:"ResourceIdentifier"`
	PendingMaintenanceActionDetails []any  `json:"PendingMaintenanceActionDetails"`
}

type applyPendingMaintenanceActionOutput struct {
	ResourcePendingMaintenanceActions resourcePendingMaintenanceActionsJSON `json:"ResourcePendingMaintenanceActions"`
}

func (h *Handler) handleApplyPendingMaintenanceAction(
	ctx context.Context, in *applyPendingMaintenanceActionInput,
) (*applyPendingMaintenanceActionOutput, error) {
	instanceArn := ptrconv.String(in.ReplicationInstanceArn)
	if instanceArn == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceArn is required", ErrValidation)
	}

	ri, err := h.Backend.ApplyPendingMaintenanceAction(
		ctx,
		instanceArn,
		ptrconv.String(in.ApplyAction),
		ptrconv.String(in.OptInType),
	)
	if err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionOutput{
		ResourcePendingMaintenanceActions: resourcePendingMaintenanceActionsJSON{
			ResourceIdentifier:              ri.ReplicationInstanceArn,
			PendingMaintenanceActionDetails: []any{},
		},
	}, nil
}

type describePendingMaintenanceActionsInput struct {
	ReplicationInstanceArn *string       `json:"ReplicationInstanceArn"`
	Marker                 *string       `json:"Marker"`
	MaxRecords             *int32        `json:"MaxRecords"`
	Filters                []filterEntry `json:"Filters"`
}

type describePendingMaintenanceActionsOutput struct {
	Marker                    *string          `json:"Marker,omitempty"`
	PendingMaintenanceActions []map[string]any `json:"PendingMaintenanceActions"`
}

func (h *Handler) handleDescribePendingMaintenanceActions(
	_ context.Context, _ *describePendingMaintenanceActionsInput,
) (*describePendingMaintenanceActionsOutput, error) {
	return &describePendingMaintenanceActionsOutput{
		PendingMaintenanceActions: []map[string]any{},
	}, nil
}

// opsMaintenance returns the dispatch-table entries for the maintenance operation family.
func (h *Handler) opsMaintenance() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opApplyPendingMaintenanceAction: service.WrapOp(
			h.handleApplyPendingMaintenanceAction,
		),
		opDescribePendingMaintenanceActions: service.WrapOp(
			h.handleDescribePendingMaintenanceActions,
		),
	}
}
