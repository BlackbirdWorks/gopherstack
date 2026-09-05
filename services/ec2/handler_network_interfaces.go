package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

type niAttachmentAttr struct {
	AttachmentID        string `xml:"attachmentId,omitempty"`
	InstanceID          string `xml:"instanceId,omitempty"`
	Status              string `xml:"status,omitempty"`
	DeviceIndex         int    `xml:"deviceIndex"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
}

type niAttributeResponse struct {
	Description *struct {
		Value string `xml:"value"`
	} `xml:"description,omitempty"`
	SourceDestCheck *struct {
		Value bool `xml:"value"`
	} `xml:"sourceDestCheck,omitempty"`
	Attachment         *niAttachmentAttr `xml:"attachment,omitempty"`
	XMLName            xml.Name          `xml:"DescribeNetworkInterfaceAttributeResponse"`
	RequestID          string            `xml:"requestId"`
	NetworkInterfaceID string            `xml:"networkInterfaceId"`
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
	NextToken                   string   `xml:"nextToken,omitempty"`
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
		Items []string `xml:"item"`
	} `xml:"assignedIpv6Addresses"`
}

type unassignIpv6Response struct {
	XMLName                 xml.Name `xml:"UnassignIpv6AddressesResponse"`
	RequestID               string   `xml:"requestId"`
	NetworkInterfaceID      string   `xml:"networkInterfaceId"`
	UnassignedIpv6Addresses struct {
		Items []string `xml:"item"`
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

	// Real AWS returns only the block matching the requested Attribute
	// (ec2@v1.319.1 deserializers.go,
	// awsEc2query_deserializeOpDocumentDescribeNetworkInterfaceAttributeOutput):
	// description/groupSet/sourceDestCheck/attachment/
	// associatePublicIpAddress are mutually exclusive per call, not all
	// echoed together.
	switch attribute {
	case "attachment":
		if result.HasAttachment {
			resp.Attachment = &niAttachmentAttr{
				AttachmentID:        result.AttachmentID,
				InstanceID:          result.AttachInstanceID,
				DeviceIndex:         result.AttachDeviceIndex,
				Status:              result.AttachStatus,
				DeleteOnTermination: result.AttachDeleteOnTerm,
			}
		}
	case "sourceDestCheck":
		resp.SourceDestCheck = &struct {
			Value bool `xml:"value"`
		}{Value: result.SourceDestCheck}
	case "groupSet", "associatePublicIpAddress":
		// Not modeled by this backend: security groups and the launch-time
		// public-IP-association flag are not tracked per network interface.
	default:
		resp.Description = &struct {
			Value string `xml:"value"`
		}{Value: result.Description}
	}

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

	maxResults, offset, err := parseEC2Pagination(
		vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageDefaultNIPermissions,
	)
	if err != nil {
		return nil, err
	}

	var nextToken string
	perms, nextToken = pageSlice(perms, offset, maxResults)

	resp := &describeNIPermissionsResponse{RequestID: reqID, NextToken: nextToken}
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
	resp.AssignedIpv6Addresses.Items = assigned

	return resp, nil
}

func (h *Handler) handleUnassignIpv6Addresses(vals url.Values, reqID string) (any, error) {
	niID := vals.Get("NetworkInterfaceId")
	addrs := parseMemberList(vals, "Ipv6Addresses")

	if err := h.Backend.UnassignIpv6Addresses(niID, addrs); err != nil {
		return nil, err
	}

	resp := &unassignIpv6Response{RequestID: reqID, NetworkInterfaceID: niID}
	resp.UnassignedIpv6Addresses.Items = addrs

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

type networkInterfacePrivateIPItem struct {
	PrivateIPAddress string `xml:"privateIpAddress"`
	Primary          bool   `xml:"primary"`
}

type networkInterfacePrivateIPSet struct {
	Items []networkInterfacePrivateIPItem `xml:"item"`
}

type networkInterfaceAttachment struct {
	AttachmentID        string `xml:"attachmentId,omitempty"`
	InstanceID          string `xml:"instanceId,omitempty"`
	Status              string `xml:"status,omitempty"`
	DeviceIndex         int    `xml:"deviceIndex,omitempty"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
}

type networkInterfaceItem struct {
	Attachment             *networkInterfaceAttachment  `xml:"attachment,omitempty"`
	PublicIPDNSNameOptions *publicIPDNSNameOptionsItem  `xml:"publicIpDnsNameOptions,omitempty"`
	NetworkInterfaceID     string                       `xml:"networkInterfaceId"`
	SubnetID               string                       `xml:"subnetId"`
	VPCID                  string                       `xml:"vpcId"`
	PrivateIPAddress       string                       `xml:"privateIpAddress"`
	Description            string                       `xml:"description"`
	Status                 string                       `xml:"status"`
	OwnerID                string                       `xml:"ownerId,omitempty"`
	PrivateIPAddressesSet  networkInterfacePrivateIPSet `xml:"privateIpAddressesSet"`
	TagSet                 []simpleTagItem              `xml:"tagSet>item"`
	SourceDestCheck        bool                         `xml:"sourceDestCheck"`
}

type publicIPDNSNameOptionsItem struct {
	DNSHostnameType string `xml:"dnsHostnameType,omitempty"`
}

type networkInterfaceItemSet struct {
	Items []networkInterfaceItem `xml:"item"`
}

type describeNetworkInterfacesResponse struct {
	XMLName             xml.Name                `xml:"DescribeNetworkInterfacesResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	NetworkInterfaceSet networkInterfaceItemSet `xml:"networkInterfaceSet"`
}

func (h *Handler) handleDescribeNetworkInterfaces(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "NetworkInterfaceId")
	enis := h.Backend.DescribeNetworkInterfaces(ids)

	filters := parseEC2Filters(vals)
	enis = applyENIFilters(enis, filters, h.Backend)

	items := make([]networkInterfaceItem, 0, len(enis))
	for _, eni := range enis {
		items = append(items, toNetworkInterfaceItem(eni, h.Backend.TagsForResource(eni.ID)))
	}

	return &describeNetworkInterfacesResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		NetworkInterfaceSet: networkInterfaceItemSet{Items: items},
	}, nil
}

// parseIPPermissions parses EC2 IpPermissions from [url.Values].
// Handles: IpPermissions.N.IpProtocol, .FromPort, .ToPort, .IpRanges.M.CidrIp,
// and .Groups.M.GroupId (security-group source references, gap 6).

// ---- network interface helpers ----

func toNetworkInterfaceItem(eni *NetworkInterface, tags map[string]string) networkInterfaceItem {
	privateIPs := make([]networkInterfacePrivateIPItem, 0, 1+len(eni.SecondaryPrivateIPs))
	privateIPs = append(privateIPs, networkInterfacePrivateIPItem{
		PrivateIPAddress: eni.PrivateIP,
		Primary:          true,
	})

	for _, ip := range eni.SecondaryPrivateIPs {
		privateIPs = append(privateIPs, networkInterfacePrivateIPItem{
			PrivateIPAddress: ip,
			Primary:          false,
		})
	}

	item := networkInterfaceItem{
		NetworkInterfaceID:    eni.ID,
		SubnetID:              eni.SubnetID,
		VPCID:                 eni.VPCID,
		PrivateIPAddress:      eni.PrivateIP,
		Description:           eni.Description,
		Status:                eni.Status,
		OwnerID:               eni.OwnerID,
		SourceDestCheck:       eni.SourceDestCheck,
		PrivateIPAddressesSet: networkInterfacePrivateIPSet{Items: privateIPs},
		TagSet:                tagItemsFromMap(tags),
	}

	if eni.PublicDNSHostnameType != "" {
		item.PublicIPDNSNameOptions = &publicIPDNSNameOptionsItem{DNSHostnameType: eni.PublicDNSHostnameType}
	}

	if eni.AttachmentID != "" {
		item.Attachment = &networkInterfaceAttachment{
			AttachmentID:        eni.AttachmentID,
			InstanceID:          eni.InstanceID,
			DeviceIndex:         eni.DeviceIndex,
			Status:              attachmentStateAttached,
			DeleteOnTermination: eni.DeleteOnTermination,
		}
	}

	return item
}

// ---- network interface CRUD handlers ----

func (h *Handler) handleCreateNetworkInterface(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	if subnetID == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")

	eni, err := h.Backend.CreateNetworkInterface(subnetID, description)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, resourceTypeENI)
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{eni.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createNetworkInterfaceResponse{
		Xmlns:            ec2XMLNS,
		RequestID:        reqID,
		NetworkInterface: toNetworkInterfaceItem(eni, tags),
	}, nil
}

type createNetworkInterfaceResponse struct {
	XMLName          xml.Name             `xml:"CreateNetworkInterfaceResponse"`
	Xmlns            string               `xml:"xmlns,attr"`
	RequestID        string               `xml:"requestId"`
	NetworkInterface networkInterfaceItem `xml:"networkInterface"`
}

func (h *Handler) handleDeleteNetworkInterface(vals url.Values, reqID string) (any, error) {
	id := vals.Get("NetworkInterfaceId")
	if id == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteNetworkInterface(id); err != nil {
		return nil, err
	}

	return &deleteNetworkInterfaceResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

type deleteNetworkInterfaceResponse struct {
	XMLName   xml.Name `xml:"DeleteNetworkInterfaceResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleAttachNetworkInterface(vals url.Values, reqID string) (any, error) {
	eniID := vals.Get("NetworkInterfaceId")
	instanceID := vals.Get("InstanceId")

	if eniID == "" || instanceID == "" {
		return nil, fmt.Errorf(
			"%w: NetworkInterfaceId and InstanceId are required",
			ErrInvalidParameter,
		)
	}

	deviceIndex := 1
	if v := vals.Get("DeviceIndex"); v != "" {
		_, _ = fmt.Sscan(v, &deviceIndex) // parse best-effort; deviceIndex stays 1 on error
	}

	attachmentID, err := h.Backend.AttachNetworkInterface(eniID, instanceID, deviceIndex)
	if err != nil {
		return nil, err
	}

	return &attachNetworkInterfaceResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		AttachmentID: attachmentID,
		// This backend only ever attaches to network card 0 (no multi-card
		// instance types modeled), matching AWS's documented default.
		NetworkCardIndex: 0,
	}, nil
}

type attachNetworkInterfaceResponse struct {
	XMLName          xml.Name `xml:"AttachNetworkInterfaceResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	RequestID        string   `xml:"requestId"`
	AttachmentID     string   `xml:"attachmentId"`
	NetworkCardIndex int      `xml:"networkCardIndex"`
}

func (h *Handler) handleDetachNetworkInterface(vals url.Values, reqID string) (any, error) {
	attachmentID := vals.Get("AttachmentId")
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: AttachmentId is required", ErrInvalidParameter)
	}

	force := vals.Get("Force") == ec2BooleanTrue

	if err := h.Backend.DetachNetworkInterface(attachmentID, force); err != nil {
		return nil, err
	}

	return &detachNetworkInterfaceResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

type detachNetworkInterfaceResponse struct {
	XMLName   xml.Name `xml:"DetachNetworkInterfaceResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleAssignPrivateIPAddresses(vals url.Values, reqID string) (any, error) {
	eniID := vals.Get("NetworkInterfaceId")
	if eniID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	count := 0
	if v := vals.Get("SecondaryPrivateIpAddressCount"); v != "" {
		_, _ = fmt.Sscan(v, &count) // parse best-effort; count stays 0 on error
	}

	ips := parseMemberList(vals, "PrivateIpAddress")

	assigned, err := h.Backend.AssignPrivateIPAddresses(eniID, count, ips)
	if err != nil {
		return nil, err
	}

	items := make([]assignedPrivateIPItem, 0, len(assigned))
	for _, ip := range assigned {
		items = append(items, assignedPrivateIPItem{PrivateIPAddress: ip})
	}

	return &assignPrivateIPAddressesResponse{
		Xmlns:                      ec2XMLNS,
		RequestID:                  reqID,
		NetworkInterfaceID:         eniID,
		AssignedPrivateIPAddresses: items,
	}, nil
}

// assignPrivateIPAddressesResponse matches AssignPrivateIpAddressesOutput
// (ec2@v1.319.1 api_op_AssignPrivateIpAddresses.go): there is no Return
// member at all -- the real deserializer has no case for it, only
// assignedIpv4PrefixSet/assignedPrivateIpAddressesSet/networkInterfaceId.
// AssignedIpv4Prefixes is omitted: this backend doesn't support the
// Ipv4Prefix request form, only individual addresses/count.
type assignPrivateIPAddressesResponse struct {
	XMLName                    xml.Name                `xml:"AssignPrivateIpAddressesResponse"`
	Xmlns                      string                  `xml:"xmlns,attr"`
	RequestID                  string                  `xml:"requestId"`
	NetworkInterfaceID         string                  `xml:"networkInterfaceId"`
	AssignedPrivateIPAddresses []assignedPrivateIPItem `xml:"assignedPrivateIpAddressesSet>item,omitempty"`
}

type assignedPrivateIPItem struct {
	PrivateIPAddress string `xml:"privateIpAddress"`
}

func (h *Handler) handleUnassignPrivateIPAddresses(vals url.Values, reqID string) (any, error) {
	eniID := vals.Get("NetworkInterfaceId")
	if eniID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	ips := parseMemberList(vals, "PrivateIpAddress")

	if err := h.Backend.UnassignPrivateIPAddresses(eniID, ips); err != nil {
		return nil, err
	}

	return &unassignPrivateIPAddressesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

type unassignPrivateIPAddressesResponse struct {
	XMLName   xml.Name `xml:"UnassignPrivateIpAddressesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleModifyNetworkInterfaceAttribute(
	vals url.Values,
	reqID string,
) (any, error) {
	eniID := vals.Get("NetworkInterfaceId")
	if eniID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	// Determine which attribute is being modified.
	// AWS sends the attribute as a nested element: e.g. Description.Value, SourceDestCheck.Value,
	// Attachment.AttachmentId + Attachment.DeleteOnTermination.
	// Use HasKey to allow clearing the description (empty string is valid).
	attr := ""
	value := ""

	_, hasDesc := vals["Description.Value"]
	_, hasSdc := vals["SourceDestCheck.Value"]
	_, hasAttachment := vals["Attachment.AttachmentId"]

	switch {
	case hasAttachment:
		attachmentID := vals.Get("Attachment.AttachmentId")
		del := vals.Get("Attachment.DeleteOnTermination") == ec2BooleanTrue
		if err := h.Backend.SetNetworkInterfaceDeleteOnTermination(attachmentID, del); err != nil {
			return nil, err
		}
	default:
		if hasDesc {
			attr = filterKeyDescription
			value = vals.Get("Description.Value")
		} else if hasSdc {
			attr = attrSourceDest
			value = vals.Get("SourceDestCheck.Value")
		}

		if err := h.Backend.ModifyNetworkInterfaceAttribute(eniID, attr, value); err != nil {
			return nil, err
		}
	}

	return &modifyNetworkInterfaceAttributeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

type modifyNetworkInterfaceAttributeResponse struct {
	XMLName   xml.Name `xml:"ModifyNetworkInterfaceAttributeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}
