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
	if err := h.Backend.ApplyPendingMaintenanceAction(ctx, resourceARN, action, optInType); err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionResponse{
		Xmlns: docdbXMLNS,
		Result: applyPendingMaintenanceActionResult{
			ResourcePendingMaintenanceActions: xmlResourcePendingMaintenanceActions{
				ResourceIdentifier:              resourceARN,
				PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{},
			},
		},
	}, nil
}

func (h *Handler) handleDescribePendingMaintenanceActions(ctx context.Context, vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceIdentifier")
	actions := h.Backend.DescribePendingMaintenanceActions(ctx, resourceARN)
	members := make([]xmlResourcePendingMaintenanceActions, 0, len(actions))
	for _, a := range actions {
		members = append(members, xmlResourcePendingMaintenanceActions{
			ResourceIdentifier:              a.ResourceIdentifier,
			PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{},
		})
	}

	return &describePendingMaintenanceActionsResponse{
		Xmlns: docdbXMLNS,
		Result: describePendingMaintenanceActionsResult{
			PendingMaintenanceActions: xmlResourcePendingMaintenanceActionsList{Members: members},
		},
	}, nil
}

type xmlPendingMaintenanceAction struct {
	Action      string `xml:"Action"`
	OptInStatus string `xml:"OptInStatus"`
}

type xmlPendingMaintenanceActionList struct {
	Members []xmlPendingMaintenanceAction `xml:"PendingMaintenanceAction"`
}

type xmlResourcePendingMaintenanceActions struct {
	ResourceIdentifier              string                          `xml:"ResourceIdentifier"`
	PendingMaintenanceActionDetails xmlPendingMaintenanceActionList `xml:"PendingMaintenanceActionDetails"`
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
	PendingMaintenanceActions xmlResourcePendingMaintenanceActionsList `xml:"PendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResponse struct {
	XMLName xml.Name                                `xml:"DescribePendingMaintenanceActionsResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describePendingMaintenanceActionsResult `xml:"DescribePendingMaintenanceActionsResult"`
}
