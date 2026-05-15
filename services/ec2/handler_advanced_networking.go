package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// ---- Handler registration ----

func registerAdvancedNetworkingOps(h *Handler, ops map[string]ec2ActionFn) {
	// VPN Gateways
	ops["CreateVpnGateway"] = h.handleCreateVpnGateway
	ops["DescribeVpnGateways"] = h.handleDescribeVpnGateways
	ops["DeleteVpnGateway"] = h.handleDeleteVpnGateway
	ops["AttachVpnGateway"] = h.handleAttachVpnGateway
	ops["DetachVpnGateway"] = h.handleDetachVpnGateway

	// Customer Gateways
	ops["CreateCustomerGateway"] = h.handleCreateCustomerGateway
	ops["DescribeCustomerGateways"] = h.handleDescribeCustomerGateways
	ops["DeleteCustomerGateway"] = h.handleDeleteCustomerGateway

	// VPN Connections
	ops["CreateVpnConnection"] = h.handleCreateVpnConnection
	ops["DescribeVpnConnections"] = h.handleDescribeVpnConnections
	ops["DeleteVpnConnection"] = h.handleDeleteVpnConnection

	// VPC Peering extras
	ops["RejectVpcPeeringConnection"] = h.handleRejectVpcPeeringConnection

	// VPC Endpoint Service Configurations
	ops["CreateVpcEndpointServiceConfiguration"] = h.handleCreateVpcEndpointServiceConfiguration
	ops["DescribeVpcEndpointServiceConfigurations"] = h.handleDescribeVpcEndpointServiceConfigurations
	ops["DeleteVpcEndpointServiceConfigurations"] = h.handleDeleteVpcEndpointServiceConfigurations
	ops["ModifyVpcEndpointServiceConfiguration"] = h.handleModifyVpcEndpointServiceConfiguration

	// IPAM
	ops["CreateIpam"] = h.handleCreateIpam
	ops["DescribeIpams"] = h.handleDescribeIpams
	ops["DeleteIpam"] = h.handleDeleteIpam
	ops["CreateIpamPool"] = h.handleCreateIpamPool
	ops["DescribeIpamPools"] = h.handleDescribeIpamPools
	ops["DeleteIpamPool"] = h.handleDeleteIpamPool
	ops["AllocateIpamPoolCidr"] = h.handleAllocateIpamPoolCidr
	ops["GetIpamPoolCidrs"] = h.handleGetIpamPoolCidrs
	ops["ReleaseIpamPoolAllocation"] = h.handleReleaseIpamPoolAllocation
}

func advancedNetworkingSupportedOperations() []string {
	return []string{
		"CreateVpnGateway",
		"DescribeVpnGateways",
		"DeleteVpnGateway",
		"AttachVpnGateway",
		"DetachVpnGateway",
		"CreateCustomerGateway",
		"DescribeCustomerGateways",
		"DeleteCustomerGateway",
		"CreateVpnConnection",
		"DescribeVpnConnections",
		"DeleteVpnConnection",
		"RejectVpcPeeringConnection",
		"CreateVpcEndpointServiceConfiguration",
		"DescribeVpcEndpointServiceConfigurations",
		"DeleteVpcEndpointServiceConfigurations",
		"ModifyVpcEndpointServiceConfiguration",
		"CreateIpam",
		"DescribeIpams",
		"DeleteIpam",
		"CreateIpamPool",
		"DescribeIpamPools",
		"DeleteIpamPool",
		"AllocateIpamPoolCidr",
		"GetIpamPoolCidrs",
		"ReleaseIpamPoolAllocation",
	}
}

// ---- XML response types ----

type vpnGatewayItem struct {
	VpnGatewayID    string `xml:"vpnGatewayId"`
	State           string `xml:"state"`
	Type            string `xml:"type"`
	AttachedVPCID   string `xml:"attachments>item>vpcId,omitempty"`
	AttachmentState string `xml:"attachments>item>state,omitempty"`
}

type createVpnGatewayResponse struct {
	XMLName    xml.Name       `xml:"CreateVpnGatewayResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	RequestID  string         `xml:"requestId"`
	VpnGateway vpnGatewayItem `xml:"vpnGateway"`
}

type describeVpnGatewaysResponse struct {
	XMLName       xml.Name `xml:"DescribeVpnGatewaysResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	VpnGatewaySet struct {
		Items []vpnGatewayItem `xml:"item"`
	} `xml:"vpnGatewaySet"`
}

type deleteVpnGatewayResponse struct {
	XMLName   xml.Name `xml:"DeleteVpnGatewayResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type attachVpnGatewayResponse struct {
	XMLName         xml.Name `xml:"AttachVpnGatewayResponse"`
	RequestID       string   `xml:"requestId"`
	AttachmentState string   `xml:"attachment>state"`
	VpcID           string   `xml:"attachment>vpcId"`
}

type detachVpnGatewayResponse struct {
	XMLName   xml.Name `xml:"DetachVpnGatewayResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type customerGatewayItem struct {
	CustomerGatewayID string `xml:"customerGatewayId"`
	State             string `xml:"state"`
	Type              string `xml:"type"`
	BgpAsn            string `xml:"bgpAsn"`
	IPAddress         string `xml:"ipAddress"`
}

type createCustomerGatewayResponse struct {
	XMLName         xml.Name            `xml:"CreateCustomerGatewayResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	RequestID       string              `xml:"requestId"`
	CustomerGateway customerGatewayItem `xml:"customerGateway"`
}

type describeCustomerGatewaysResponse struct {
	XMLName            xml.Name `xml:"DescribeCustomerGatewaysResponse"`
	Xmlns              string   `xml:"xmlns,attr"`
	RequestID          string   `xml:"requestId"`
	CustomerGatewaySet struct {
		Items []customerGatewayItem `xml:"item"`
	} `xml:"customerGatewaySet"`
}

type deleteCustomerGatewayResponse struct {
	XMLName   xml.Name `xml:"DeleteCustomerGatewayResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type vpnConnectionItem struct {
	VpnConnectionID   string `xml:"vpnConnectionId"`
	State             string `xml:"state"`
	CustomerGatewayID string `xml:"customerGatewayId"`
	VpnGatewayID      string `xml:"vpnGatewayId"`
	Type              string `xml:"type"`
}

type createVpnConnectionResponse struct {
	XMLName       xml.Name          `xml:"CreateVpnConnectionResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	VpnConnection vpnConnectionItem `xml:"vpnConnection"`
}

type describeVpnConnectionsResponse struct {
	XMLName          xml.Name `xml:"DescribeVpnConnectionsResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	RequestID        string   `xml:"requestId"`
	VpnConnectionSet struct {
		Items []vpnConnectionItem `xml:"item"`
	} `xml:"vpnConnectionSet"`
}

type deleteVpnConnectionResponse struct {
	XMLName   xml.Name `xml:"DeleteVpnConnectionResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type rejectVpcPeeringConnectionResponse struct {
	XMLName   xml.Name `xml:"RejectVpcPeeringConnectionResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type vpcEndpointServiceConfigItem struct {
	ServiceID          string `xml:"serviceId"`
	ServiceName        string `xml:"serviceName"`
	ServiceType        string `xml:"serviceType>item>serviceType"`
	AcceptanceRequired bool   `xml:"acceptanceRequired"`
}

type createVpcEndpointServiceConfigurationResponse struct {
	XMLName       xml.Name                     `xml:"CreateVpcEndpointServiceConfigurationResponse"`
	Xmlns         string                       `xml:"xmlns,attr"`
	RequestID     string                       `xml:"requestId"`
	ServiceConfig vpcEndpointServiceConfigItem `xml:"serviceConfiguration"`
}

type describeVpcEndpointServiceConfigurationsResponse struct {
	XMLName          xml.Name `xml:"DescribeVpcEndpointServiceConfigurationsResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	RequestID        string   `xml:"requestId"`
	ServiceConfigSet struct {
		Items []vpcEndpointServiceConfigItem `xml:"item"`
	} `xml:"serviceConfigurationSet"`
}

type deleteVpcEndpointServiceConfigurationsResponse struct {
	XMLName   xml.Name `xml:"DeleteVpcEndpointServiceConfigurationsResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type modifyVpcEndpointServiceConfigurationResponse struct {
	XMLName   xml.Name `xml:"ModifyVpcEndpointServiceConfigurationResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type ipamItem struct {
	IpamID  string `xml:"ipamId"`
	IpamARN string `xml:"ipamArn"`
	State   string `xml:"state"`
	Region  string `xml:"operatingRegions>item>regionName"`
}

type createIpamResponse struct {
	XMLName   xml.Name `xml:"CreateIpamResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Ipam      ipamItem `xml:"ipam"`
}

type describeIpamsResponse struct {
	XMLName   xml.Name `xml:"DescribeIpamsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	IpamSet   struct {
		Items []ipamItem `xml:"item"`
	} `xml:"ipamSet"`
}

type deleteIpamResponse struct {
	XMLName   xml.Name `xml:"DeleteIpamResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type ipamPoolItem struct {
	IpamPoolID    string `xml:"ipamPoolId"`
	IpamPoolARN   string `xml:"ipamPoolArn"`
	IpamID        string `xml:"ipamId"`
	State         string `xml:"state"`
	Locale        string `xml:"locale,omitempty"`
	AddressFamily string `xml:"addressFamily"`
	Cidr          string `xml:"allocatedCidrs>item>cidr,omitempty"`
}

type createIpamPoolResponse struct {
	XMLName   xml.Name     `xml:"CreateIpamPoolResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"requestId"`
	IpamPool  ipamPoolItem `xml:"ipamPool"`
}

type describeIpamPoolsResponse struct {
	XMLName     xml.Name `xml:"DescribeIpamPoolsResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	IpamPoolSet struct {
		Items []ipamPoolItem `xml:"item"`
	} `xml:"ipamPoolSet"`
}

type deleteIpamPoolResponse struct {
	XMLName   xml.Name `xml:"DeleteIpamPoolResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type ipamPoolAllocationItem struct {
	IpamPoolAllocationID string `xml:"ipamPoolAllocationId"`
	Cidr                 string `xml:"cidr"`
	Description          string `xml:"description,omitempty"`
}

type allocateIpamPoolCidrResponse struct {
	XMLName            xml.Name               `xml:"AllocateIpamPoolCidrResponse"`
	Xmlns              string                 `xml:"xmlns,attr"`
	RequestID          string                 `xml:"requestId"`
	IpamPoolAllocation ipamPoolAllocationItem `xml:"ipamPoolAllocation"`
}

type getIpamPoolCidrsResponse struct {
	XMLName               xml.Name `xml:"GetIpamPoolCidrsResponse"`
	Xmlns                 string   `xml:"xmlns,attr"`
	RequestID             string   `xml:"requestId"`
	IpamPoolAllocationSet struct {
		Items []ipamPoolAllocationItem `xml:"item"`
	} `xml:"ipamPoolAllocationSet"`
}

type releaseIpamPoolAllocationResponse struct {
	XMLName   xml.Name `xml:"ReleaseIpamPoolAllocationResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

// ---- VPN Gateway handlers ----

func (h *Handler) handleCreateVpnGateway(vals url.Values, reqID string) (any, error) {
	vgw, err := h.Backend.CreateVpnGateway(vals.Get("Type"))
	if err != nil {
		return nil, err
	}

	item := vpnGatewayItem{
		VpnGatewayID:    vgw.VpnGatewayID,
		State:           vgw.State,
		Type:            vgw.Type,
		AttachedVPCID:   vgw.AttachedVPCID,
		AttachmentState: vgw.AttachmentState,
	}

	return &createVpnGatewayResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		VpnGateway: item,
	}, nil
}

func (h *Handler) handleDescribeVpnGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpnGatewayId")
	vgws := h.Backend.DescribeVpnGateways(ids)

	resp := &describeVpnGatewaysResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, vgw := range vgws {
		resp.VpnGatewaySet.Items = append(resp.VpnGatewaySet.Items, vpnGatewayItem{
			VpnGatewayID:    vgw.VpnGatewayID,
			State:           vgw.State,
			Type:            vgw.Type,
			AttachedVPCID:   vgw.AttachedVPCID,
			AttachmentState: vgw.AttachmentState,
		})
	}

	return resp, nil
}

func (h *Handler) handleDeleteVpnGateway(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteVpnGateway(vals.Get("VpnGatewayId")); err != nil {
		return nil, err
	}

	return &deleteVpnGatewayResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleAttachVpnGateway(vals url.Values, reqID string) (any, error) {
	vgwID := vals.Get("VpnGatewayId")
	vpcID := vals.Get("VpcId")

	if err := h.Backend.AttachVpnGateway(vgwID, vpcID); err != nil {
		return nil, err
	}

	return &attachVpnGatewayResponse{
		RequestID:       reqID,
		AttachmentState: attachmentStateAttached,
		VpcID:           vpcID,
	}, nil
}

func (h *Handler) handleDetachVpnGateway(vals url.Values, reqID string) (any, error) {
	vgwID := vals.Get("VpnGatewayId")
	vpcID := vals.Get("VpcId")

	if err := h.Backend.DetachVpnGateway(vgwID, vpcID); err != nil {
		return nil, err
	}

	return &detachVpnGatewayResponse{RequestID: reqID, Return: true}, nil
}

// ---- Customer Gateway handlers ----

func (h *Handler) handleCreateCustomerGateway(vals url.Values, reqID string) (any, error) {
	cgw, err := h.Backend.CreateCustomerGateway(
		vals.Get("Type"),
		vals.Get("IpAddress"),
		vals.Get("BgpAsn"),
	)
	if err != nil {
		return nil, err
	}

	return &createCustomerGatewayResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		CustomerGateway: customerGatewayItem{
			CustomerGatewayID: cgw.CustomerGatewayID,
			State:             cgw.State,
			Type:              cgw.Type,
			BgpAsn:            cgw.BgpAsn,
			IPAddress:         cgw.IPAddress,
		},
	}, nil
}

func (h *Handler) handleDescribeCustomerGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CustomerGatewayId")
	cgws := h.Backend.DescribeCustomerGateways(ids)

	resp := &describeCustomerGatewaysResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, cgw := range cgws {
		resp.CustomerGatewaySet.Items = append(resp.CustomerGatewaySet.Items, customerGatewayItem{
			CustomerGatewayID: cgw.CustomerGatewayID,
			State:             cgw.State,
			Type:              cgw.Type,
			BgpAsn:            cgw.BgpAsn,
			IPAddress:         cgw.IPAddress,
		})
	}

	return resp, nil
}

func (h *Handler) handleDeleteCustomerGateway(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteCustomerGateway(vals.Get("CustomerGatewayId")); err != nil {
		return nil, err
	}

	return &deleteCustomerGatewayResponse{RequestID: reqID, Return: true}, nil
}

// ---- VPN Connection handlers ----

func (h *Handler) handleCreateVpnConnection(vals url.Values, reqID string) (any, error) {
	conn, err := h.Backend.CreateVpnConnection(
		vals.Get("Type"),
		vals.Get("CustomerGatewayId"),
		vals.Get("VpnGatewayId"),
	)
	if err != nil {
		return nil, err
	}

	return &createVpnConnectionResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VpnConnection: vpnConnectionItem{
			VpnConnectionID:   conn.VpnConnectionID,
			State:             conn.State,
			CustomerGatewayID: conn.CustomerGatewayID,
			VpnGatewayID:      conn.VpnGatewayID,
			Type:              conn.Type,
		},
	}, nil
}

func (h *Handler) handleDescribeVpnConnections(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpnConnectionId")
	conns := h.Backend.DescribeVpnConnections(ids)

	resp := &describeVpnConnectionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, conn := range conns {
		resp.VpnConnectionSet.Items = append(resp.VpnConnectionSet.Items, vpnConnectionItem{
			VpnConnectionID:   conn.VpnConnectionID,
			State:             conn.State,
			CustomerGatewayID: conn.CustomerGatewayID,
			VpnGatewayID:      conn.VpnGatewayID,
			Type:              conn.Type,
		})
	}

	return resp, nil
}

func (h *Handler) handleDeleteVpnConnection(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteVpnConnection(vals.Get("VpnConnectionId")); err != nil {
		return nil, err
	}

	return &deleteVpnConnectionResponse{RequestID: reqID, Return: true}, nil
}

// ---- VPC Peering: Reject ----

func (h *Handler) handleRejectVpcPeeringConnection(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.RejectVpcPeeringConnection(vals.Get("VpcPeeringConnectionId")); err != nil {
		return nil, err
	}

	return &rejectVpcPeeringConnectionResponse{RequestID: reqID, Return: true}, nil
}

// ---- VPC Endpoint Service Configuration handlers ----

func (h *Handler) handleCreateVpcEndpointServiceConfiguration(
	vals url.Values,
	reqID string,
) (any, error) {
	acceptanceRequiredStr := vals.Get("AcceptanceRequired")
	acceptanceRequired := acceptanceRequiredStr == ec2BooleanTrue

	nlbARNs := parseMemberList(vals, "NetworkLoadBalancerArn")

	cfg, err := h.Backend.CreateVpcEndpointServiceConfiguration(acceptanceRequired, nlbARNs)
	if err != nil {
		return nil, err
	}

	return &createVpcEndpointServiceConfigurationResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ServiceConfig: vpcEndpointServiceConfigItem{
			ServiceID:          cfg.ServiceID,
			ServiceName:        cfg.ServiceName,
			ServiceType:        cfg.ServiceType,
			AcceptanceRequired: cfg.AcceptanceRequired,
		},
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointServiceConfigurations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ServiceId")
	cfgs := h.Backend.DescribeVpcEndpointServiceConfigurations(ids)

	resp := &describeVpcEndpointServiceConfigurationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, cfg := range cfgs {
		resp.ServiceConfigSet.Items = append(
			resp.ServiceConfigSet.Items,
			vpcEndpointServiceConfigItem{
				ServiceID:          cfg.ServiceID,
				ServiceName:        cfg.ServiceName,
				ServiceType:        cfg.ServiceType,
				AcceptanceRequired: cfg.AcceptanceRequired,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteVpcEndpointServiceConfigurations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ServiceId")

	if err := h.Backend.DeleteVpcEndpointServiceConfigurations(ids); err != nil {
		return nil, err
	}

	return &deleteVpcEndpointServiceConfigurationsResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleModifyVpcEndpointServiceConfiguration(
	vals url.Values,
	reqID string,
) (any, error) {
	svcID := vals.Get("ServiceId")
	acceptanceRequired := vals.Get("AcceptanceRequired") == ec2BooleanTrue

	if err := h.Backend.ModifyVpcEndpointServiceConfiguration(svcID, acceptanceRequired); err != nil {
		return nil, err
	}

	return &modifyVpcEndpointServiceConfigurationResponse{RequestID: reqID, Return: true}, nil
}

// ---- IPAM handlers ----

func (h *Handler) handleCreateIpam(_ url.Values, reqID string) (any, error) {
	ipam, err := h.Backend.CreateIpam()
	if err != nil {
		return nil, err
	}

	return &createIpamResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Ipam: ipamItem{
			IpamID:  ipam.IpamID,
			IpamARN: ipam.IpamARN,
			State:   ipam.State,
			Region:  ipam.Region,
		},
	}, nil
}

func (h *Handler) handleDescribeIpams(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamId")
	ipams := h.Backend.DescribeIpams(ids)

	resp := &describeIpamsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, ipam := range ipams {
		resp.IpamSet.Items = append(resp.IpamSet.Items, ipamItem{
			IpamID:  ipam.IpamID,
			IpamARN: ipam.IpamARN,
			State:   ipam.State,
			Region:  ipam.Region,
		})
	}

	return resp, nil
}

func (h *Handler) handleDeleteIpam(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteIpam(vals.Get("IpamId")); err != nil {
		return nil, err
	}

	return &deleteIpamResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleCreateIpamPool(vals url.Values, reqID string) (any, error) {
	// Accept either IpamId directly or fall back to the scope's parent IPAM.
	// For simplicity, prefer IpamId; if not present, use IpamScopeId as-is.
	ipamID := vals.Get("IpamId")
	if ipamID == "" {
		ipamID = vals.Get("IpamScopeId")
	}

	pool, err := h.Backend.CreateIpamPool(
		ipamID,
		vals.Get("AddressFamily"),
		vals.Get("Locale"),
		vals.Get("ProvisionedCidrs.item.1.Cidr"),
	)
	if err != nil {
		return nil, err
	}

	return &createIpamPoolResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamPool: ipamPoolItem{
			IpamPoolID:    pool.IpamPoolID,
			IpamPoolARN:   pool.IpamPoolARN,
			IpamID:        pool.IpamID,
			State:         pool.State,
			Locale:        pool.Locale,
			AddressFamily: pool.AddressFamily,
			Cidr:          pool.Cidr,
		},
	}, nil
}

func (h *Handler) handleDescribeIpamPools(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamPoolId")
	pools := h.Backend.DescribeIpamPools(ids)

	resp := &describeIpamPoolsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, pool := range pools {
		resp.IpamPoolSet.Items = append(resp.IpamPoolSet.Items, ipamPoolItem{
			IpamPoolID:    pool.IpamPoolID,
			IpamPoolARN:   pool.IpamPoolARN,
			IpamID:        pool.IpamID,
			State:         pool.State,
			Locale:        pool.Locale,
			AddressFamily: pool.AddressFamily,
			Cidr:          pool.Cidr,
		})
	}

	return resp, nil
}

func (h *Handler) handleDeleteIpamPool(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteIpamPool(vals.Get("IpamPoolId")); err != nil {
		return nil, err
	}

	return &deleteIpamPoolResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleAllocateIpamPoolCidr(vals url.Values, reqID string) (any, error) {
	netmaskLenStr := vals.Get("NetmaskLength")
	netmaskLen, _ := strconv.Atoi(netmaskLenStr)

	alloc, err := h.Backend.AllocateIpamPoolCidr(
		vals.Get("IpamPoolId"),
		vals.Get("Cidr"),
		netmaskLen,
	)
	if err != nil {
		return nil, err
	}

	return &allocateIpamPoolCidrResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamPoolAllocation: ipamPoolAllocationItem{
			IpamPoolAllocationID: alloc.IpamPoolAllocationID,
			Cidr:                 alloc.Cidr,
			Description:          alloc.Description,
		},
	}, nil
}

func (h *Handler) handleGetIpamPoolCidrs(vals url.Values, reqID string) (any, error) {
	poolID := vals.Get("IpamPoolId")
	allocs := h.Backend.GetIpamPoolCidrs(poolID)

	resp := &getIpamPoolCidrsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, alloc := range allocs {
		resp.IpamPoolAllocationSet.Items = append(
			resp.IpamPoolAllocationSet.Items,
			ipamPoolAllocationItem{
				IpamPoolAllocationID: alloc.IpamPoolAllocationID,
				Cidr:                 alloc.Cidr,
				Description:          alloc.Description,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleReleaseIpamPoolAllocation(vals url.Values, reqID string) (any, error) {
	poolID := vals.Get("IpamPoolId")
	allocID := vals.Get("IpamPoolAllocationId")

	if err := h.Backend.ReleaseIpamPoolAllocation(poolID, allocID); err != nil {
		return nil, err
	}

	return &releaseIpamPoolAllocationResponse{RequestID: reqID, Return: true}, nil
}
