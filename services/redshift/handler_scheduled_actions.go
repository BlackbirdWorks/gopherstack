package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- Scheduled Actions -----

type scheduledActionXML struct {
	ScheduledActionName        string `xml:"ScheduledActionName"`
	Schedule                   string `xml:"Schedule"`
	IamRole                    string `xml:"IamRole,omitempty"`
	ScheduledActionDescription string `xml:"ScheduledActionDescription,omitempty"`
	State                      string `xml:"State"`
}

type createScheduledActionResponse struct {
	XMLName xml.Name           `xml:"CreateScheduledActionResponse"`
	Xmlns   string             `xml:"xmlns,attr"`
	Result  scheduledActionXML `xml:"CreateScheduledActionResult"`
}

func scheduledActionToXML(a *ScheduledAction) scheduledActionXML {
	return scheduledActionXML{
		ScheduledActionName:        a.ScheduledActionName,
		Schedule:                   a.Schedule,
		IamRole:                    a.IamRole,
		ScheduledActionDescription: a.ScheduledActionDescription,
		State:                      a.State,
	}
}

func (h *Handler) handleCreateScheduledAction(vals url.Values) (any, error) {
	action, err := h.Backend.CreateScheduledAction(
		vals.Get("ScheduledActionName"),
		vals.Get("Schedule"),
		vals.Get("IamRole"),
		vals.Get("ScheduledActionDescription"),
		vals.Get("TargetAction"),
	)
	if err != nil {
		return nil, err
	}

	return &createScheduledActionResponse{
		Xmlns:  redshiftXMLNS,
		Result: scheduledActionToXML(action),
	}, nil
}

type deleteScheduledActionResponse struct {
	XMLName xml.Name `xml:"DeleteScheduledActionResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteScheduledAction(vals url.Values) (any, error) {
	name := vals.Get("ScheduledActionName")
	if err := h.Backend.DeleteScheduledAction(name); err != nil {
		return nil, err
	}

	return &deleteScheduledActionResponse{Xmlns: redshiftXMLNS}, nil
}

type describeScheduledActionsResponse struct {
	XMLName xml.Name `xml:"DescribeScheduledActionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		ScheduledActions []scheduledActionXML `xml:"ScheduledActions>ScheduledAction"`
	} `xml:"DescribeScheduledActionsResult"`
}

func (h *Handler) handleDescribeScheduledActions(vals url.Values) (any, error) {
	name := vals.Get("ScheduledActionName")
	actions, err := h.Backend.DescribeScheduledActions(name)
	if err != nil {
		return nil, err
	}

	members := make([]scheduledActionXML, 0, len(actions))

	for i := range actions {
		members = append(members, scheduledActionToXML(&actions[i]))
	}

	resp := &describeScheduledActionsResponse{Xmlns: redshiftXMLNS}
	resp.Result.ScheduledActions = members

	return resp, nil
}

type modifyScheduledActionResponse struct {
	XMLName xml.Name           `xml:"ModifyScheduledActionResponse"`
	Xmlns   string             `xml:"xmlns,attr"`
	Result  scheduledActionXML `xml:"ModifyScheduledActionResult"`
}

func (h *Handler) handleModifyScheduledAction(vals url.Values) (any, error) {
	action, err := h.Backend.ModifyScheduledAction(
		vals.Get("ScheduledActionName"),
		vals.Get("Schedule"),
		vals.Get("IamRole"),
		vals.Get("ScheduledActionDescription"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyScheduledActionResponse{
		Xmlns:  redshiftXMLNS,
		Result: scheduledActionToXML(action),
	}, nil
}
