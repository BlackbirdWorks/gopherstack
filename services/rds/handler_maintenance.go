package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleApplyPendingMaintenanceAction(vals url.Values) (any, error) {
	resourceID := vals.Get("ResourceIdentifier")
	applyAction := vals.Get("ApplyAction")

	if _, err := h.Backend.ApplyPendingMaintenanceAction(resourceID, applyAction); err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionResponse{
		Xmlns: rdsXMLNS,
		Result: applyPendingMaintenanceActionResult{
			ResourcePendingMaintenanceActions: xmlResourcePendingMaintenanceActions{
				ResourceIdentifier:              resourceID,
				PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{},
			},
		},
	}, nil
}

type xmlPendingMaintenanceAction struct {
	Action      string `xml:"Action"`
	Description string `xml:"Description,omitempty"`
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

type xmlPendingMaintenanceActionResourceList struct {
	Members []xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type xmlPendingActionsWrapper struct {
	Actions xmlPendingMaintenanceActionResourceList `xml:"PendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResponse struct {
	XMLName xml.Name                 `xml:"DescribePendingMaintenanceActionsResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  xmlPendingActionsWrapper `xml:"DescribePendingMaintenanceActionsResult"`
}

func (h *Handler) handleDescribePendingMaintenanceActions(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceIdentifier")
	actions := h.Backend.DescribePendingMaintenanceActions(resourceARN)
	members := make([]xmlResourcePendingMaintenanceActions, 0, len(actions))
	for _, a := range actions {
		members = append(members, xmlResourcePendingMaintenanceActions{
			ResourceIdentifier: a.ResourceIdentifier,
			PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{
				Members: []xmlPendingMaintenanceAction{
					{Action: a.Action, Description: a.Description},
				},
			},
		})
	}

	return &describePendingMaintenanceActionsResponse{
		Xmlns:  rdsXMLNS,
		Result: xmlPendingActionsWrapper{Actions: xmlPendingMaintenanceActionResourceList{Members: members}},
	}, nil
}
