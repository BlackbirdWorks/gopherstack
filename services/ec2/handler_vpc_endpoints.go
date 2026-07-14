package ec2

import (
	"encoding/xml"
	"net/url"
)

type createVpcEndpointConnectionNotificationResponse struct {
	XMLName                xml.Name            `xml:"CreateVpcEndpointConnectionNotificationResponse"`
	RequestID              string              `xml:"requestId"`
	ConnectionNotification connectionNotifItem `xml:"connectionNotification"`
}

type describeVpcEndpointConnectionNotificationsResponse struct {
	XMLName                   xml.Name `xml:"DescribeVpcEndpointConnectionNotificationsResponse"`
	RequestID                 string   `xml:"requestId"`
	ConnectionNotificationSet struct {
		Items []connectionNotifItem `xml:"item"`
	} `xml:"connectionNotificationSet"`
}

type modifyVpcEndpointConnectionNotificationResponse struct {
	XMLName     xml.Name `xml:"ModifyVpcEndpointConnectionNotificationResponse"`
	RequestID   string   `xml:"requestId"`
	ReturnValue bool     `xml:"return"`
}

type describeVpcEndpointConnectionsResponse struct {
	XMLName                  xml.Name `xml:"DescribeVpcEndpointConnectionsResponse"`
	RequestID                string   `xml:"requestId"`
	VpcEndpointConnectionSet struct {
		Items []vpcEndpointConnectionItem `xml:"item"`
	} `xml:"vpcEndpointConnectionSet"`
}

type vpcEndpointAssocItem struct {
	VpcEndpointID string `xml:"vpcEndpointId"`
	VPCID         string `xml:"vpcId"`
	ServiceName   string `xml:"serviceName"`
	State         string `xml:"state"`
}

type describeVpcEndpointAssociationsResponse struct {
	XMLName                   xml.Name `xml:"DescribeVpcEndpointAssociationsResponse"`
	RequestID                 string   `xml:"requestId"`
	VpcEndpointAssociationSet struct {
		Items []vpcEndpointAssocItem `xml:"item"`
	} `xml:"vpcEndpointAssociationSet"`
}

type describeVpcEndpointServicePermissionsResponse struct {
	XMLName           xml.Name `xml:"DescribeVpcEndpointServicePermissionsResponse"`
	RequestID         string   `xml:"requestId"`
	AllowedPrincipals struct {
		Items []struct {
			Principal string `xml:"principal"`
		} `xml:"item"`
	} `xml:"allowedPrincipals"`
}

func toConnectionNotifItem(n *VpcEndpointConnectionNotification) connectionNotifItem {
	item := connectionNotifItem{
		ConnectionNotificationID:    n.ConnectionNotificationID,
		ServiceID:                   n.ServiceID,
		VpcEndpointID:               n.VpcEndpointID,
		ConnectionNotificationARN:   n.ConnectionNotificationARN,
		ConnectionNotificationType:  n.ConnectionNotificationType,
		ConnectionNotificationState: n.ConnectionNotificationState,
	}
	for _, e := range n.ConnectionEvents {
		item.ConnectionEvents.Items = append(item.ConnectionEvents.Items, struct {
			Event string `xml:"item"`
		}{Event: e})
	}

	return item
}

func (h *Handler) handleCreateVpcEndpointConnectionNotification(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	endpointID := vals.Get("VpcEndpointId")
	notifARN := vals.Get("ConnectionNotificationArn")
	events := parseMemberList(vals, "ConnectionEvents.member")
	if len(events) == 0 {
		events = parseMemberList(vals, "ConnectionEvents")
	}

	notif, err := h.Backend.CreateVpcEndpointConnectionNotification(
		serviceID,
		endpointID,
		notifARN,
		events,
	)
	if err != nil {
		return nil, err
	}

	return &createVpcEndpointConnectionNotificationResponse{
		RequestID:              reqID,
		ConnectionNotification: toConnectionNotifItem(notif),
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointConnectionNotifications(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ConnectionNotificationId")
	notifs := h.Backend.DescribeVpcEndpointConnectionNotifications(ids)

	resp := &describeVpcEndpointConnectionNotificationsResponse{RequestID: reqID}
	for _, n := range notifs {
		resp.ConnectionNotificationSet.Items = append(
			resp.ConnectionNotificationSet.Items,
			toConnectionNotifItem(n),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteVpcEndpointConnectionNotifications(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ConnectionNotificationId")
	if err := h.Backend.DeleteVpcEndpointConnectionNotifications(ids); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpcEndpointConnectionNotificationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyVpcEndpointConnectionNotification(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("ConnectionNotificationId")
	notifARN := vals.Get("ConnectionNotificationArn")
	events := parseMemberList(vals, "ConnectionEvents.member")

	if _, err := h.Backend.ModifyVpcEndpointConnectionNotification(id, notifARN, events); err != nil {
		return nil, err
	}

	return &modifyVpcEndpointConnectionNotificationResponse{
		RequestID:   reqID,
		ReturnValue: true,
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointConnections(vals url.Values, reqID string) (any, error) {
	serviceIDs := parseMemberList(vals, "ServiceId")
	conns := h.Backend.DescribeVpcEndpointConnections(serviceIDs)

	resp := &describeVpcEndpointConnectionsResponse{RequestID: reqID}
	for _, c := range conns {
		resp.VpcEndpointConnectionSet.Items = append(
			resp.VpcEndpointConnectionSet.Items,
			vpcEndpointConnectionItem{
				VpcEndpointID: c.VpcEndpointID,
				ServiceID:     c.ServiceID,
				State:         c.State,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeVpcEndpointAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "VpcEndpointId")
	eps := h.Backend.DescribeVpcEndpointAssociations(ids)

	resp := &describeVpcEndpointAssociationsResponse{RequestID: reqID}
	for _, ep := range eps {
		resp.VpcEndpointAssociationSet.Items = append(
			resp.VpcEndpointAssociationSet.Items,
			vpcEndpointAssocItem{
				VpcEndpointID: ep.ID,
				VPCID:         ep.VPCID,
				ServiceName:   ep.ServiceName,
				State:         ep.State,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyVpcEndpointServicePayerResponsibility(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	payer := vals.Get("PayerResponsibility")
	if err := h.Backend.ModifyVpcEndpointServicePayerResponsibility(serviceID, payer); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcEndpointServicePayerResponsibilityResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointServicePermissions(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	principals := h.Backend.DescribeVpcEndpointServicePermissions(serviceID)

	resp := &describeVpcEndpointServicePermissionsResponse{RequestID: reqID}
	for _, p := range principals {
		resp.AllowedPrincipals.Items = append(resp.AllowedPrincipals.Items, struct {
			Principal string `xml:"principal"`
		}{Principal: p})
	}

	return resp, nil
}

func (h *Handler) handleModifyVpcEndpointServicePermissions(
	vals url.Values,
	reqID string,
) (any, error) {
	serviceID := vals.Get("ServiceId")
	add := parseMemberList(vals, "AddAllowedPrincipals.member")
	remove := parseMemberList(vals, "RemoveAllowedPrincipals.member")
	if err := h.Backend.ModifyVpcEndpointServicePermissions(serviceID, add, remove); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcEndpointServicePermissionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyVpcEndpoint(vals url.Values, reqID string) (any, error) {
	endpointID := vals.Get("VpcEndpointId")
	addSubnets := parseMemberList(vals, "AddSubnetId")
	removeSubnets := parseMemberList(vals, "RemoveSubnetId")
	if err := h.Backend.ModifyVpcEndpoint(endpointID, addSubnets, removeSubnets); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type connectionNotifItem struct {
	ConnectionNotificationID    string `xml:"connectionNotificationId"`
	ServiceID                   string `xml:"serviceId,omitempty"`
	VpcEndpointID               string `xml:"vpcEndpointId,omitempty"`
	ConnectionNotificationARN   string `xml:"connectionNotificationArn"`
	ConnectionNotificationType  string `xml:"connectionNotificationType"`
	ConnectionNotificationState string `xml:"connectionNotificationState"`
	ConnectionEvents            struct {
		Items []struct {
			Event string `xml:"item"`
		} `xml:"item"`
	} `xml:"connectionEvents"`
}

func (h *Handler) handleDeleteVpcEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpcEndpointId")

	unsuccessful, err := h.Backend.DeleteVpcEndpoints(ids)
	if err != nil {
		return nil, err
	}

	items := make([]unsuccessfulEndpointItem, 0, len(unsuccessful))
	for _, id := range unsuccessful {
		items = append(items, unsuccessfulEndpointItem{ID: id})
	}

	return &deleteVpcEndpointsResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		Unsuccessful: unsuccessfulEndpointSet{Items: items},
	}, nil
}

// ---- XML response types ----

type deleteVpcEndpointsResponse struct {
	XMLName      xml.Name                `xml:"DeleteVpcEndpointsResponse"`
	Xmlns        string                  `xml:"xmlns,attr"`
	RequestID    string                  `xml:"requestId"`
	Unsuccessful unsuccessfulEndpointSet `xml:"unsuccessful"`
}

type describeVpcEndpointServicesResponse struct {
	XMLName      xml.Name `xml:"DescribeVpcEndpointServicesResponse"`
	RequestID    string   `xml:"requestId"`
	ServiceNames struct {
		Items []serviceNameItem `xml:"item"`
	} `xml:"serviceNameSet"`
}

type serviceNameItem struct {
	ServiceName string `xml:"serviceName"`
}

func (h *Handler) handleDescribeVpcEndpointServices(_ url.Values, reqID string) (any, error) {
	names := h.Backend.DescribeVpcEndpointServices()
	resp := &describeVpcEndpointServicesResponse{RequestID: reqID}

	for _, n := range names {
		resp.ServiceNames.Items = append(resp.ServiceNames.Items, serviceNameItem{ServiceName: n})
	}

	return resp, nil
}

type rejectVpcEndpointConnectionsResponse struct {
	XMLName xml.Name `xml:"RejectVpcEndpointConnectionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	// RequestID is the AWS request identifier.
	RequestID string `xml:"requestId"`
	// Unsuccessful holds items that FAILED to be rejected (no matching
	// connection). On full success this list is empty.
	Unsuccessful vpcEndpointConnectionSet `xml:"unsuccessful"`
}

func (h *Handler) handleRejectVpcEndpointConnections(vals url.Values, reqID string) (any, error) {
	serviceID := vals.Get("ServiceId")
	endpointIDs := parseMemberList(vals, "VpcEndpointId")

	unsuccessful, err := h.Backend.RejectVpcEndpointConnections(serviceID, endpointIDs)
	if err != nil {
		return nil, err
	}

	resp := &rejectVpcEndpointConnectionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, epID := range unsuccessful {
		resp.Unsuccessful.Items = append(resp.Unsuccessful.Items, vpcEndpointConnectionItem{
			ServiceID:     serviceID,
			VpcEndpointID: epID,
		})
	}

	return resp, nil
}

// registerVpcEndpointsOps registers the VpcEndpoints operation handlers.
func registerVpcEndpointsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateVpcEndpointConnectionNotification"] = h.handleCreateVpcEndpointConnectionNotification
	ops["DeleteVpcEndpointConnectionNotifications"] = h.handleDeleteVpcEndpointConnectionNotifications
	ops["DescribeVpcEndpointConnectionNotifications"] = h.handleDescribeVpcEndpointConnectionNotifications
	ops["ModifyVpcEndpointConnectionNotification"] = h.handleModifyVpcEndpointConnectionNotification
	ops["DescribeVpcEndpointConnections"] = h.handleDescribeVpcEndpointConnections
	ops["DescribeVpcEndpointAssociations"] = h.handleDescribeVpcEndpointAssociations
	ops["ModifyVpcEndpointServicePayerResponsibility"] = h.handleModifyVpcEndpointServicePayerResponsibility
	ops["DescribeVpcEndpointServicePermissions"] = h.handleDescribeVpcEndpointServicePermissions
	ops["ModifyVpcEndpointServicePermissions"] = h.handleModifyVpcEndpointServicePermissions
	ops["ModifyVpcEndpoint"] = h.handleModifyVpcEndpoint
	ops["DeleteVpcEndpoints"] = h.handleDeleteVpcEndpoints
	ops["DescribeVpcEndpointServices"] = h.handleDescribeVpcEndpointServices
	ops["RejectVpcEndpointConnections"] = h.handleRejectVpcEndpointConnections
}

// vpcEndpointsSupportedOperations lists the operation names registered by
// registerVpcEndpointsOps, for GetSupportedOperations().
func vpcEndpointsSupportedOperations() []string {
	return []string{
		"CreateVpcEndpointConnectionNotification",
		"DeleteVpcEndpointConnectionNotifications",
		"DescribeVpcEndpointConnectionNotifications",
		"ModifyVpcEndpointConnectionNotification",
		"DescribeVpcEndpointConnections",
		"DescribeVpcEndpointAssociations",
		"ModifyVpcEndpointServicePayerResponsibility",
		"DescribeVpcEndpointServicePermissions",
		"ModifyVpcEndpointServicePermissions",
		"ModifyVpcEndpoint",
		"DeleteVpcEndpoints",
		"DescribeVpcEndpointServices",
		"RejectVpcEndpointConnections",
	}
}
