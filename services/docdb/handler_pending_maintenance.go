package docdb

import (
	"context"
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleApplyPendingMaintenanceAction(ctx context.Context, vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceIdentifier")
	action := vals.Get("ApplyAction")
	optInType := vals.Get("OptInType")
	result, err := h.Backend.ApplyPendingMaintenanceAction(ctx, resourceARN, action, optInType)
	if err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionResponse{
		Xmlns: docdbXMLNS,
		Result: applyPendingMaintenanceActionResult{
			ResourcePendingMaintenanceActions: toXMLResourcePendingMaintenanceActions(result),
		},
	}, nil
}

func (h *Handler) handleDescribePendingMaintenanceActions(ctx context.Context, vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceIdentifier")
	actions := h.Backend.DescribePendingMaintenanceActions(ctx, resourceARN)
	members := make([]xmlResourcePendingMaintenanceActions, 0, len(actions))
	for _, a := range actions {
		cp := a
		members = append(members, toXMLResourcePendingMaintenanceActions(&cp))
	}

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describePendingMaintenanceActionsResponse{
		Xmlns: docdbXMLNS,
		Result: describePendingMaintenanceActionsResult{
			Marker:                    nextMarker,
			PendingMaintenanceActions: xmlResourcePendingMaintenanceActionsList{Members: members},
		},
	}, nil
}

// xmlPendingMaintenanceAction mirrors types.PendingMaintenanceAction's full
// wire shape (Action/AutoAppliedAfterDate/CurrentApplyDate/Description/
// ForcedApplyDate/OptInStatus).
type xmlPendingMaintenanceAction struct {
	Action               string `xml:"Action,omitempty"`
	Description          string `xml:"Description,omitempty"`
	OptInStatus          string `xml:"OptInStatus,omitempty"`
	AutoAppliedAfterDate string `xml:"AutoAppliedAfterDate,omitempty"`
	CurrentApplyDate     string `xml:"CurrentApplyDate,omitempty"`
	ForcedApplyDate      string `xml:"ForcedApplyDate,omitempty"`
}

type xmlPendingMaintenanceActionList struct {
	Members []xmlPendingMaintenanceAction `xml:"PendingMaintenanceAction"`
}

type xmlResourcePendingMaintenanceActions struct {
	ResourceIdentifier              string                          `xml:"ResourceIdentifier"`
	PendingMaintenanceActionDetails xmlPendingMaintenanceActionList `xml:"PendingMaintenanceActionDetails"`
}

func toXMLResourcePendingMaintenanceActions(r *ResourcePendingMaintenanceActions) xmlResourcePendingMaintenanceActions {
	details := make([]xmlPendingMaintenanceAction, 0, len(r.Actions))
	for _, a := range r.Actions {
		// PendingMaintenanceAction and xmlPendingMaintenanceAction share an
		// identical field set/order (only their XML struct tags differ,
		// which Go's conversion rules ignore), so a direct type conversion
		// is the correct, staticcheck-preferred way to build the wire type.
		details = append(details, xmlPendingMaintenanceAction(a))
	}

	return xmlResourcePendingMaintenanceActions{
		ResourceIdentifier:              r.ResourceIdentifier,
		PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{Members: details},
	}
}

type applyPendingMaintenanceActionResult struct {
	ResourcePendingMaintenanceActions xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type applyPendingMaintenanceActionResponse struct {
	XMLName xml.Name                            `xml:"ApplyPendingMaintenanceActionResponse"`
	Xmlns   string                              `xml:"xmlns,attr"`
	Result  applyPendingMaintenanceActionResult `xml:"ApplyPendingMaintenanceActionResult"`
}

type xmlResourcePendingMaintenanceActionsList struct {
	Members []xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResult struct {
	Marker                    string                                   `xml:"Marker,omitempty"`
	PendingMaintenanceActions xmlResourcePendingMaintenanceActionsList `xml:"PendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResponse struct {
	XMLName xml.Name                                `xml:"DescribePendingMaintenanceActionsResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describePendingMaintenanceActionsResult `xml:"DescribePendingMaintenanceActionsResult"`
}
