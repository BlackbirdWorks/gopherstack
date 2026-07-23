package dms

import (
	"context"
	"fmt"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type applyPendingMaintenanceActionInput struct {
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
	ApplyAction            *string `json:"ApplyAction"`
	OptInType              *string `json:"OptInType"`
}

// validApplyActionsTable lazily builds the ApplyAction lookup table exactly
// once.
//
//nolint:gochecknoglobals // read-only package-level lookup table, apigatewayv2-style
var validApplyActionsTable = sync.OnceValue(func() map[string]bool {
	return map[string]bool{
		"os-upgrade":    true,
		"system-update": true,
		"db-upgrade":    true,
		"os-patch":      true,
	}
})

// validApplyActions mirrors the ApplyAction valid-values list documented on
// ApplyPendingMaintenanceActionInput.ApplyAction in the SDK.
func validApplyActions(s string) bool {
	return validApplyActionsTable()[s]
}

// validOptInTypesTable lazily builds the OptInType lookup table exactly once.
//
//nolint:gochecknoglobals // read-only package-level lookup table, apigatewayv2-style
var validOptInTypesTable = sync.OnceValue(func() map[string]bool {
	return map[string]bool{
		"immediate":        true,
		"next-maintenance": true,
		"undo-opt-in":      true,
	}
})

// validOptInTypes mirrors the OptInType valid-values list documented on
// ApplyPendingMaintenanceActionInput.OptInType in the SDK.
func validOptInTypes(s string) bool {
	return validOptInTypesTable()[s]
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

	applyAction := ptrconv.String(in.ApplyAction)
	if applyAction == "" {
		return nil, fmt.Errorf("%w: ApplyAction is required", ErrValidation)
	}

	if !validApplyActions(applyAction) {
		return nil, fmt.Errorf(
			"%w: invalid ApplyAction %q; valid: os-upgrade, system-update, db-upgrade, os-patch",
			ErrValidation,
			applyAction,
		)
	}

	optInType := ptrconv.String(in.OptInType)
	if optInType == "" {
		return nil, fmt.Errorf("%w: OptInType is required", ErrValidation)
	}

	if !validOptInTypes(optInType) {
		return nil, fmt.Errorf(
			"%w: invalid OptInType %q; valid: immediate, next-maintenance, undo-opt-in",
			ErrValidation,
			optInType,
		)
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
