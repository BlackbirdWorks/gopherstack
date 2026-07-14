package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

type niAttributeResponse struct {
	XMLName            xml.Name `xml:"DescribeNetworkInterfaceAttributeResponse"`
	RequestID          string   `xml:"requestId"`
	NetworkInterfaceID string   `xml:"networkInterfaceId"`
	Description        struct {
		Value string `xml:"value"`
	} `xml:"description"`
	SourceDestCheck struct {
		Value bool `xml:"value"`
	} `xml:"sourceDestCheck"`
}

type niPermissionStateItem struct {
	State string `xml:"state"`
}

type niPermissionItem struct {
	NetworkInterfacePermissionID string                `xml:"networkInterfacePermissionId"`
	NetworkInterfaceID           string                `xml:"networkInterfaceId"`
	AwsAccountID                 string                `xml:"awsAccountId,omitempty"`
	AwsService                   string                `xml:"awsService,omitempty"`
	Permission                   string                `xml:"permission"`
	PermissionState              niPermissionStateItem `xml:"permissionState"`
}

type describeNIPermissionsResponse struct {
	XMLName                     xml.Name `xml:"DescribeNetworkInterfacePermissionsResponse"`
	RequestID                   string   `xml:"requestId"`
	NetworkInterfacePermissions struct {
		Items []niPermissionItem `xml:"item"`
	} `xml:"networkInterfacePermissions"`
}

type createNIPermissionResponse struct {
	XMLName             xml.Name         `xml:"CreateNetworkInterfacePermissionResponse"`
	RequestID           string           `xml:"requestId"`
	InterfacePermission niPermissionItem `xml:"interfacePermission"`
}

type assignIpv6Response struct {
	XMLName               xml.Name `xml:"AssignIpv6AddressesResponse"`
	RequestID             string   `xml:"requestId"`
	NetworkInterfaceID    string   `xml:"networkInterfaceId"`
	AssignedIpv6Addresses struct {
		Items []struct {
			Ipv6Address string `xml:"item"`
		} `xml:"item"`
	} `xml:"assignedIpv6Addresses"`
}

type unassignIpv6Response struct {
	XMLName                 xml.Name `xml:"UnassignIpv6AddressesResponse"`
	RequestID               string   `xml:"requestId"`
	NetworkInterfaceID      string   `xml:"networkInterfaceId"`
	UnassignedIpv6Addresses struct {
		Items []struct {
			Ipv6Address string `xml:"item"`
		} `xml:"item"`
	} `xml:"unassignedIpv6Addresses"`
}

type accountAttributeValueItem struct {
	AttributeValue string `xml:"attributeValue"`
}

type accountAttributeItem struct {
	AttributeName   string                      `xml:"attributeName"`
	AttributeValues []accountAttributeValueItem `xml:"attributeValueSet>item"`
}

func (h *Handler) handleDescribeNetworkInterfaceAttribute(
	vals url.Values,
	reqID string,
) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	attribute := vals.Get("Attribute")
	result, err := h.Backend.DescribeNetworkInterfaceAttribute(niID, attribute)
	if err != nil {
		return nil, err
	}
	resp := &niAttributeResponse{
		RequestID:          reqID,
		NetworkInterfaceID: result.NetworkInterfaceID,
	}
	resp.Description.Value = result.Description
	resp.SourceDestCheck.Value = result.SourceDestCheck

	return resp, nil
}

func (h *Handler) handleResetNetworkInterfaceAttribute(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	if err := h.Backend.ResetNetworkInterfaceAttribute(niID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetNetworkInterfaceAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeNetworkInterfacePermissions(
	vals url.Values,
	reqID string,
) (any, error) {
	niIDs := parseMemberList(vals, "NetworkInterfaceId")
	perms := h.Backend.DescribeNetworkInterfacePermissions(niIDs)

	resp := &describeNIPermissionsResponse{RequestID: reqID}
	for _, p := range perms {
		item := niPermissionItem{
			NetworkInterfacePermissionID: p.PermissionID,
			NetworkInterfaceID:           p.NetworkInterfaceID,
			AwsAccountID:                 p.AwsAccountID,
			AwsService:                   p.AwsService,
			Permission:                   p.Permission,
		}
		item.PermissionState.State = p.State
		resp.NetworkInterfacePermissions.Items = append(
			resp.NetworkInterfacePermissions.Items,
			item,
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateNetworkInterfacePermission(
	vals url.Values,
	reqID string,
) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	awsAccountID := vals.Get("AwsAccountId")
	awsService := vals.Get("AwsService")
	permission := vals.Get("Permission")

	perm, err := h.Backend.CreateNetworkInterfacePermission(
		niID,
		awsAccountID,
		awsService,
		permission,
	)
	if err != nil {
		return nil, err
	}
	resp := &createNIPermissionResponse{RequestID: reqID}
	resp.InterfacePermission.NetworkInterfacePermissionID = perm.PermissionID
	resp.InterfacePermission.NetworkInterfaceID = perm.NetworkInterfaceID
	resp.InterfacePermission.AwsAccountID = perm.AwsAccountID
	resp.InterfacePermission.AwsService = perm.AwsService
	resp.InterfacePermission.Permission = perm.Permission
	resp.InterfacePermission.PermissionState.State = perm.State

	return resp, nil
}

func (h *Handler) handleDeleteNetworkInterfacePermission(
	vals url.Values,
	reqID string,
) (any, error) {
	permID := vals.Get("NetworkInterfacePermissionId")
	if err := h.Backend.DeleteNetworkInterfacePermission(permID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInterfacePermissionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssignIpv6Addresses(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	count, _ := strconv.Atoi(vals.Get("Ipv6AddressCount"))
	if count == 0 {
		count = 1
	}

	assigned, err := h.Backend.AssignIpv6Addresses(niID, count)
	if err != nil {
		return nil, err
	}

	resp := &assignIpv6Response{RequestID: reqID, NetworkInterfaceID: niID}
	for _, addr := range assigned {
		resp.AssignedIpv6Addresses.Items = append(resp.AssignedIpv6Addresses.Items, struct {
			Ipv6Address string `xml:"item"`
		}{Ipv6Address: addr})
	}

	return resp, nil
}

func (h *Handler) handleUnassignIpv6Addresses(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	addrs := parseMemberList(vals, "Ipv6Addresses")

	if err := h.Backend.UnassignIpv6Addresses(niID, addrs); err != nil {
		return nil, err
	}

	resp := &unassignIpv6Response{RequestID: reqID, NetworkInterfaceID: niID}
	for _, addr := range addrs {
		resp.UnassignedIpv6Addresses.Items = append(resp.UnassignedIpv6Addresses.Items, struct {
			Ipv6Address string `xml:"item"`
		}{Ipv6Address: addr})
	}

	return resp, nil
}

// registerNetworkInterfacesOps registers the NetworkInterfaces operation handlers.
func registerNetworkInterfacesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DescribeNetworkInterfaceAttribute"] = h.handleDescribeNetworkInterfaceAttribute
	ops["ResetNetworkInterfaceAttribute"] = h.handleResetNetworkInterfaceAttribute
	ops["DescribeNetworkInterfacePermissions"] = h.handleDescribeNetworkInterfacePermissions
	ops["CreateNetworkInterfacePermission"] = h.handleCreateNetworkInterfacePermission
	ops["DeleteNetworkInterfacePermission"] = h.handleDeleteNetworkInterfacePermission
	ops["AssignIpv6Addresses"] = h.handleAssignIpv6Addresses
	ops["UnassignIpv6Addresses"] = h.handleUnassignIpv6Addresses
}

// networkInterfacesSupportedOperations lists the operation names registered by
// registerNetworkInterfacesOps, for GetSupportedOperations().
func networkInterfacesSupportedOperations() []string {
	return []string{
		"DescribeNetworkInterfaceAttribute",
		"ResetNetworkInterfaceAttribute",
		"DescribeNetworkInterfacePermissions",
		"CreateNetworkInterfacePermission",
		"DeleteNetworkInterfacePermission",
		"AssignIpv6Addresses",
		"UnassignIpv6Addresses",
	}
}
