package ec2

import (
	"encoding/xml"
	"fmt"
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
	ops["ModifyIpam"] = h.handleModifyIpam
	ops["DeleteIpam"] = h.handleDeleteIpam
	ops["CreateIpamScope"] = h.handleCreateIpamScope
	ops["DescribeIpamScopes"] = h.handleDescribeIpamScopes
	ops["ModifyIpamScope"] = h.handleModifyIpamScope
	ops["DeleteIpamScope"] = h.handleDeleteIpamScope
	ops["CreateIpamPool"] = h.handleCreateIpamPool
	ops["DescribeIpamPools"] = h.handleDescribeIpamPools
	ops["ModifyIpamPool"] = h.handleModifyIpamPool
	ops["DeleteIpamPool"] = h.handleDeleteIpamPool
	ops["ProvisionIpamPoolCidr"] = h.handleProvisionIpamPoolCidr
	ops["DeprovisionIpamPoolCidr"] = h.handleDeprovisionIpamPoolCidr
	ops["AllocateIpamPoolCidr"] = h.handleAllocateIpamPoolCidr
	ops["GetIpamPoolCidrs"] = h.handleGetIpamPoolCidrs
	ops["GetIpamPoolAllocations"] = h.handleGetIpamPoolAllocations
	ops["ReleaseIpamPoolAllocation"] = h.handleReleaseIpamPoolAllocation
	ops["DescribeIpamResourceDiscoveries"] = h.handleDescribeIpamResourceDiscoveries
	ops["DescribeIpamResourceDiscoveryAssociations"] = h.handleDescribeIpamResourceDiscoveryAssociations
	ops["GetIpamAddressHistory"] = h.handleGetIpamAddressHistory
	ops["GetIpamDiscoveredAccounts"] = h.handleGetIpamDiscoveredAccounts
	ops["GetIpamDiscoveredResourceCidrs"] = h.handleGetIpamDiscoveredResourceCidrs
	ops["GetIpamDiscoveredPublicAddresses"] = h.handleGetIpamDiscoveredPublicAddresses
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
		"ModifyIpam",
		"DeleteIpam",
		"CreateIpamScope",
		"DescribeIpamScopes",
		"ModifyIpamScope",
		"DeleteIpamScope",
		"CreateIpamPool",
		"DescribeIpamPools",
		"ModifyIpamPool",
		"DeleteIpamPool",
		"ProvisionIpamPoolCidr",
		"DeprovisionIpamPoolCidr",
		"AllocateIpamPoolCidr",
		"GetIpamPoolCidrs",
		"GetIpamPoolAllocations",
		"ReleaseIpamPoolAllocation",
		"DescribeIpamResourceDiscoveries",
		"DescribeIpamResourceDiscoveryAssociations",
		"GetIpamAddressHistory",
		"GetIpamDiscoveredAccounts",
		"GetIpamDiscoveredResourceCidrs",
		"GetIpamDiscoveredPublicAddresses",
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

type ipamOperatingRegionItem struct {
	RegionName string `xml:"regionName"`
}

type ipamItem struct {
	State                                 string `xml:"state"`
	DefaultResourceDiscoveryID            string `xml:"defaultResourceDiscoveryId,omitempty"`
	IpamARN                               string `xml:"ipamArn"`
	IpamRegion                            string `xml:"ipamRegion,omitempty"`
	PublicDefaultScopeID                  string `xml:"publicDefaultScopeId,omitempty"`
	PrivateDefaultScopeID                 string `xml:"privateDefaultScopeId,omitempty"`
	Tier                                  string `xml:"tier,omitempty"`
	Description                           string `xml:"description,omitempty"`
	OwnerID                               string `xml:"ownerId,omitempty"`
	IpamID                                string `xml:"ipamId"`
	DefaultResourceDiscoveryAssociationID string `xml:"defaultResourceDiscoveryAssociationId,omitempty"`
	OperatingRegionSet                    struct {
		Items []ipamOperatingRegionItem `xml:"item"`
	} `xml:"operatingRegions"`
	ScopeCount                        int32 `xml:"scopeCount,omitempty"`
	ResourceDiscoveryAssociationCount int32 `xml:"resourceDiscoveryAssociationCount,omitempty"`
}

func toIpamItem(ipam *Ipam) ipamItem {
	item := ipamItem{
		IpamID:                                ipam.IpamID,
		OwnerID:                               ipam.OwnerID,
		IpamARN:                               ipam.IpamARN,
		IpamRegion:                            ipam.Region,
		PublicDefaultScopeID:                  ipam.PublicDefaultScopeID,
		PrivateDefaultScopeID:                 ipam.PrivateDefaultScopeID,
		ScopeCount:                            ipam.ScopeCount,
		Description:                           ipam.Description,
		State:                                 ipam.State,
		DefaultResourceDiscoveryID:            ipam.DefaultResourceDiscoveryID,
		DefaultResourceDiscoveryAssociationID: ipam.DefaultResourceDiscoveryAssociationID,
		ResourceDiscoveryAssociationCount:     ipam.ResourceDiscoveryAssociationCount,
		Tier:                                  ipam.Tier,
	}

	for _, r := range ipam.OperatingRegions {
		item.OperatingRegionSet.Items = append(item.OperatingRegionSet.Items, ipamOperatingRegionItem{RegionName: r})
	}

	return item
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

type modifyIpamResponse struct {
	XMLName   xml.Name `xml:"ModifyIpamResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Ipam      ipamItem `xml:"ipam"`
}

type deleteIpamResponse struct {
	XMLName   xml.Name `xml:"DeleteIpamResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Ipam      ipamItem `xml:"ipam"`
}

type ipamScopeItem struct {
	IpamScopeID   string `xml:"ipamScopeId"`
	IpamScopeARN  string `xml:"ipamScopeArn"`
	IpamID        string `xml:"ipamId"`
	IpamScopeType string `xml:"ipamScopeType"`
	Description   string `xml:"description,omitempty"`
	State         string `xml:"state"`
	PoolCount     int32  `xml:"poolCount,omitempty"`
	IsDefault     bool   `xml:"isDefault"`
}

func toIpamScopeItem(scope *IpamScope) ipamScopeItem {
	return ipamScopeItem{
		IpamScopeID:   scope.IpamScopeID,
		IpamScopeARN:  scope.IpamScopeARN,
		IpamID:        scope.IpamID,
		IpamScopeType: scope.IpamScopeType,
		IsDefault:     scope.IsDefault,
		Description:   scope.Description,
		PoolCount:     scope.PoolCount,
		State:         scope.State,
	}
}

type createIpamScopeResponse struct {
	XMLName   xml.Name      `xml:"CreateIpamScopeResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	IpamScope ipamScopeItem `xml:"ipamScope"`
}

type describeIpamScopesResponse struct {
	XMLName      xml.Name `xml:"DescribeIpamScopesResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	IpamScopeSet struct {
		Items []ipamScopeItem `xml:"item"`
	} `xml:"ipamScopeSet"`
}

type modifyIpamScopeResponse struct {
	XMLName   xml.Name      `xml:"ModifyIpamScopeResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	IpamScope ipamScopeItem `xml:"ipamScope"`
}

type deleteIpamScopeResponse struct {
	XMLName   xml.Name      `xml:"DeleteIpamScopeResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	IpamScope ipamScopeItem `xml:"ipamScope"`
}

type ipamPoolItem struct {
	IpamPoolID                     string `xml:"ipamPoolId"`
	IpamPoolARN                    string `xml:"ipamPoolArn"`
	IpamID                         string `xml:"ipamId"`
	IpamScopeID                    string `xml:"ipamScopeId,omitempty"`
	State                          string `xml:"state"`
	Locale                         string `xml:"locale,omitempty"`
	AddressFamily                  string `xml:"addressFamily"`
	Description                    string `xml:"description,omitempty"`
	AutoImport                     bool   `xml:"autoImport,omitempty"`
	PubliclyAdvertisable           bool   `xml:"publiclyAdvertisable,omitempty"`
	AllocationMinNetmaskLength     int32  `xml:"allocationMinNetmaskLength,omitempty"`
	AllocationMaxNetmaskLength     int32  `xml:"allocationMaxNetmaskLength,omitempty"`
	AllocationDefaultNetmaskLength int32  `xml:"allocationDefaultNetmaskLength,omitempty"`
}

func toIpamPoolItem(pool *IpamPool) ipamPoolItem {
	return ipamPoolItem{
		IpamPoolID:                     pool.IpamPoolID,
		IpamPoolARN:                    pool.IpamPoolARN,
		IpamID:                         pool.IpamID,
		IpamScopeID:                    pool.IpamScopeID,
		State:                          pool.State,
		Locale:                         pool.Locale,
		AddressFamily:                  pool.AddressFamily,
		Description:                    pool.Description,
		AutoImport:                     pool.AutoImport,
		PubliclyAdvertisable:           pool.PubliclyAdvertisable,
		AllocationMinNetmaskLength:     pool.AllocationMinNetmaskLength,
		AllocationMaxNetmaskLength:     pool.AllocationMaxNetmaskLength,
		AllocationDefaultNetmaskLength: pool.AllocationDefaultNetmaskLength,
	}
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

type modifyIpamPoolResponse struct {
	XMLName   xml.Name     `xml:"ModifyIpamPoolResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"requestId"`
	IpamPool  ipamPoolItem `xml:"ipamPool"`
}

type deleteIpamPoolResponse struct {
	XMLName   xml.Name     `xml:"DeleteIpamPoolResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"requestId"`
	IpamPool  ipamPoolItem `xml:"ipamPool"`
}

type ipamPoolCidrItem struct {
	Cidr  string `xml:"cidr"`
	State string `xml:"state"`
}

func toIpamPoolCidrItem(c *IpamPoolCidr) ipamPoolCidrItem {
	return ipamPoolCidrItem{Cidr: c.Cidr, State: c.State}
}

type provisionIpamPoolCidrResponse struct {
	XMLName      xml.Name         `xml:"ProvisionIpamPoolCidrResponse"`
	Xmlns        string           `xml:"xmlns,attr"`
	RequestID    string           `xml:"requestId"`
	IpamPoolCidr ipamPoolCidrItem `xml:"ipamPoolCidr"`
}

type deprovisionIpamPoolCidrResponse struct {
	XMLName      xml.Name         `xml:"DeprovisionIpamPoolCidrResponse"`
	Xmlns        string           `xml:"xmlns,attr"`
	RequestID    string           `xml:"requestId"`
	IpamPoolCidr ipamPoolCidrItem `xml:"ipamPoolCidr"`
}

type getIpamPoolCidrsResponse struct {
	XMLName         xml.Name `xml:"GetIpamPoolCidrsResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	RequestID       string   `xml:"requestId"`
	IpamPoolCidrSet struct {
		Items []ipamPoolCidrItem `xml:"item"`
	} `xml:"ipamPoolCidrSet"`
}

type ipamPoolAllocationItem struct {
	IpamPoolAllocationID string `xml:"ipamPoolAllocationId"`
	Cidr                 string `xml:"cidr"`
	Description          string `xml:"description,omitempty"`
	ResourceType         string `xml:"resourceType,omitempty"`
	ResourceID           string `xml:"resourceId,omitempty"`
	ResourceOwner        string `xml:"resourceOwner,omitempty"`
}

func toIpamPoolAllocationItem(a *IpamPoolAllocation) ipamPoolAllocationItem {
	return ipamPoolAllocationItem{
		IpamPoolAllocationID: a.IpamPoolAllocationID,
		Cidr:                 a.Cidr,
		Description:          a.Description,
		ResourceType:         a.ResourceType,
		ResourceID:           a.ResourceID,
		ResourceOwner:        a.ResourceOwner,
	}
}

type allocateIpamPoolCidrResponse struct {
	XMLName            xml.Name               `xml:"AllocateIpamPoolCidrResponse"`
	Xmlns              string                 `xml:"xmlns,attr"`
	RequestID          string                 `xml:"requestId"`
	IpamPoolAllocation ipamPoolAllocationItem `xml:"ipamPoolAllocation"`
}

type getIpamPoolAllocationsResponse struct {
	XMLName               xml.Name `xml:"GetIpamPoolAllocationsResponse"`
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

type ipamResourceDiscoveryItem struct {
	IpamResourceDiscoveryID  string `xml:"ipamResourceDiscoveryId"`
	OwnerID                  string `xml:"ownerId,omitempty"`
	IpamResourceDiscoveryARN string `xml:"ipamResourceDiscoveryArn"`
	State                    string `xml:"state"`
	Description              string `xml:"description,omitempty"`
	IsDefault                bool   `xml:"isDefault"`
}

type describeIpamResourceDiscoveriesResponse struct {
	XMLName                  xml.Name `xml:"DescribeIpamResourceDiscoveriesResponse"`
	Xmlns                    string   `xml:"xmlns,attr"`
	RequestID                string   `xml:"requestId"`
	IpamResourceDiscoverySet struct {
		Items []ipamResourceDiscoveryItem `xml:"item"`
	} `xml:"ipamResourceDiscoverySet"`
}

type ipamResourceDiscoveryAssociationItem struct {
	IpamResourceDiscoveryAssociationID string `xml:"ipamResourceDiscoveryAssociationId"`
	IpamID                             string `xml:"ipamId"`
	IpamRegion                         string `xml:"ipamRegion,omitempty"`
	IpamResourceDiscoveryID            string `xml:"ipamResourceDiscoveryId"`
	ResourceDiscoveryStatus            string `xml:"resourceDiscoveryStatus,omitempty"`
	State                              string `xml:"state"`
	IsDefault                          bool   `xml:"isDefault"`
}

type describeIpamResourceDiscoveryAssociationsResponse struct {
	XMLName                             xml.Name `xml:"DescribeIpamResourceDiscoveryAssociationsResponse"`
	Xmlns                               string   `xml:"xmlns,attr"`
	RequestID                           string   `xml:"requestId"`
	IpamResourceDiscoveryAssociationSet struct {
		Items []ipamResourceDiscoveryAssociationItem `xml:"item"`
	} `xml:"ipamResourceDiscoveryAssociationSet"`
}

// getIpamAddressHistoryResponse models GetIpamAddressHistory. Address history tracking
// requires a real IPAM discovery pipeline that this mock does not implement, so it always
// returns a correctly-shaped, empty history record set.
type getIpamAddressHistoryResponse struct {
	XMLName          xml.Name `xml:"GetIpamAddressHistoryResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	RequestID        string   `xml:"requestId"`
	HistoryRecordSet struct {
		Items []struct{} `xml:"item"`
	} `xml:"historyRecordSet"`
}

// getIpamDiscoveredAccountsResponse models GetIpamDiscoveredAccounts; always empty (see
// getIpamAddressHistoryResponse for rationale).
type getIpamDiscoveredAccountsResponse struct {
	XMLName                  xml.Name `xml:"GetIpamDiscoveredAccountsResponse"`
	Xmlns                    string   `xml:"xmlns,attr"`
	RequestID                string   `xml:"requestId"`
	IpamDiscoveredAccountSet struct {
		Items []struct{} `xml:"item"`
	} `xml:"ipamDiscoveredAccountSet"`
}

// getIpamDiscoveredResourceCidrsResponse models GetIpamDiscoveredResourceCidrs; always empty.
type getIpamDiscoveredResourceCidrsResponse struct {
	XMLName                       xml.Name `xml:"GetIpamDiscoveredResourceCidrsResponse"`
	Xmlns                         string   `xml:"xmlns,attr"`
	RequestID                     string   `xml:"requestId"`
	IpamDiscoveredResourceCidrSet struct {
		Items []struct{} `xml:"item"`
	} `xml:"ipamDiscoveredResourceCidrSet"`
}

// getIpamDiscoveredPublicAddressesResponse models GetIpamDiscoveredPublicAddresses; always empty.
type getIpamDiscoveredPublicAddressesResponse struct {
	XMLName                        xml.Name `xml:"GetIpamDiscoveredPublicAddressesResponse"`
	Xmlns                          string   `xml:"xmlns,attr"`
	RequestID                      string   `xml:"requestId"`
	OldestSampleTime               string   `xml:"oldestSampleTime,omitempty"`
	IpamDiscoveredPublicAddressSet struct {
		Items []struct{} `xml:"item"`
	} `xml:"ipamDiscoveredPublicAddressSet"`
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

// parseIpamOperatingRegions extracts OperatingRegion.N.RegionName parameters.
func parseIpamOperatingRegions(vals url.Values) []string {
	var regions []string

	for i := 1; ; i++ {
		r := vals.Get(fmt.Sprintf("OperatingRegion.%d.RegionName", i))
		if r == "" {
			return regions
		}

		regions = append(regions, r)
	}
}

func (h *Handler) handleCreateIpam(vals url.Values, reqID string) (any, error) {
	ipam, err := h.Backend.CreateIpam(IpamOptions{
		Description:      vals.Get("Description"),
		OperatingRegions: parseIpamOperatingRegions(vals),
		Tier:             vals.Get("Tier"),
	})
	if err != nil {
		return nil, err
	}

	return &createIpamResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Ipam:      toIpamItem(ipam),
	}, nil
}

func (h *Handler) handleDescribeIpams(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamId")
	ipams := h.Backend.DescribeIpams(ids)

	resp := &describeIpamsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, ipam := range ipams {
		resp.IpamSet.Items = append(resp.IpamSet.Items, toIpamItem(ipam))
	}

	return resp, nil
}

func (h *Handler) handleModifyIpam(vals url.Values, reqID string) (any, error) {
	ipam, err := h.Backend.ModifyIpam(vals.Get("IpamId"), IpamOptions{
		Description:      vals.Get("Description"),
		OperatingRegions: parseIpamOperatingRegions(vals),
		Tier:             vals.Get("Tier"),
	})
	if err != nil {
		return nil, err
	}

	return &modifyIpamResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Ipam:      toIpamItem(ipam),
	}, nil
}

func (h *Handler) handleDeleteIpam(vals url.Values, reqID string) (any, error) {
	id := vals.Get("IpamId")

	ipams := h.Backend.DescribeIpams([]string{id})
	if len(ipams) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, id)
	}

	if err := h.Backend.DeleteIpam(id); err != nil {
		return nil, err
	}

	item := toIpamItem(ipams[0])
	item.State = ipamStateDeleteComplete

	return &deleteIpamResponse{Xmlns: ec2XMLNS, RequestID: reqID, Ipam: item}, nil
}

func (h *Handler) handleCreateIpamScope(vals url.Values, reqID string) (any, error) {
	scope, err := h.Backend.CreateIpamScope(vals.Get("IpamId"), vals.Get("Description"))
	if err != nil {
		return nil, err
	}

	return &createIpamScopeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamScope: toIpamScopeItem(scope),
	}, nil
}

func (h *Handler) handleDescribeIpamScopes(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamScopeId")
	scopes := h.Backend.DescribeIpamScopes(ids)

	resp := &describeIpamScopesResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, scope := range scopes {
		resp.IpamScopeSet.Items = append(resp.IpamScopeSet.Items, toIpamScopeItem(scope))
	}

	return resp, nil
}

func (h *Handler) handleModifyIpamScope(vals url.Values, reqID string) (any, error) {
	scope, err := h.Backend.ModifyIpamScope(vals.Get("IpamScopeId"), vals.Get("Description"))
	if err != nil {
		return nil, err
	}

	return &modifyIpamScopeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamScope: toIpamScopeItem(scope),
	}, nil
}

func (h *Handler) handleDeleteIpamScope(vals url.Values, reqID string) (any, error) {
	id := vals.Get("IpamScopeId")

	scopes := h.Backend.DescribeIpamScopes([]string{id})
	if len(scopes) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrIpamScopeNotFound, id)
	}

	if err := h.Backend.DeleteIpamScope(id); err != nil {
		return nil, err
	}

	item := toIpamScopeItem(scopes[0])
	item.State = ipamStateDeleteComplete

	return &deleteIpamScopeResponse{Xmlns: ec2XMLNS, RequestID: reqID, IpamScope: item}, nil
}

func (h *Handler) handleCreateIpamPool(vals url.Values, reqID string) (any, error) {
	// Accept either IpamId directly or fall back to the scope's parent IPAM.
	// For simplicity, prefer IpamId; if not present, use IpamScopeId as-is.
	ipamID := vals.Get("IpamId")
	if ipamID == "" {
		ipamID = vals.Get("IpamScopeId")
	}

	minNetmask, _ := strconv.Atoi(vals.Get("AllocationMinNetmaskLength"))
	maxNetmask, _ := strconv.Atoi(vals.Get("AllocationMaxNetmaskLength"))
	defaultNetmask, _ := strconv.Atoi(vals.Get("AllocationDefaultNetmaskLength"))

	pool, err := h.Backend.CreateIpamPool(
		ipamID,
		vals.Get("AddressFamily"),
		vals.Get("Locale"),
		vals.Get("ProvisionedCidrs.item.1.Cidr"),
		IpamPoolOptions{
			IpamScopeID:                    vals.Get("IpamScopeId"),
			Description:                    vals.Get("Description"),
			AutoImport:                     vals.Get("AutoImport") == ec2BooleanTrue,
			PubliclyAdvertisable:           vals.Get("PubliclyAdvertisable") == ec2BooleanTrue,
			AllocationMinNetmaskLength:     int32(minNetmask),     //nolint:gosec // bounded by netmask range
			AllocationMaxNetmaskLength:     int32(maxNetmask),     //nolint:gosec // bounded by netmask range
			AllocationDefaultNetmaskLength: int32(defaultNetmask), //nolint:gosec // bounded by netmask range
		},
	)
	if err != nil {
		return nil, err
	}

	return &createIpamPoolResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamPool:  toIpamPoolItem(pool),
	}, nil
}

func (h *Handler) handleDescribeIpamPools(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamPoolId")
	pools := h.Backend.DescribeIpamPools(ids)

	resp := &describeIpamPoolsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, pool := range pools {
		resp.IpamPoolSet.Items = append(resp.IpamPoolSet.Items, toIpamPoolItem(pool))
	}

	return resp, nil
}

func (h *Handler) handleModifyIpamPool(vals url.Values, reqID string) (any, error) {
	minNetmask, _ := strconv.Atoi(vals.Get("AllocationMinNetmaskLength"))
	maxNetmask, _ := strconv.Atoi(vals.Get("AllocationMaxNetmaskLength"))
	defaultNetmask, _ := strconv.Atoi(vals.Get("AllocationDefaultNetmaskLength"))

	pool, err := h.Backend.ModifyIpamPool(vals.Get("IpamPoolId"), IpamPoolOptions{
		Description:                    vals.Get("Description"),
		AutoImport:                     vals.Get("AutoImport") == ec2BooleanTrue,
		AllocationMinNetmaskLength:     int32(minNetmask),     //nolint:gosec // bounded by netmask range
		AllocationMaxNetmaskLength:     int32(maxNetmask),     //nolint:gosec // bounded by netmask range
		AllocationDefaultNetmaskLength: int32(defaultNetmask), //nolint:gosec // bounded by netmask range
	})
	if err != nil {
		return nil, err
	}

	return &modifyIpamPoolResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamPool:  toIpamPoolItem(pool),
	}, nil
}

func (h *Handler) handleDeleteIpamPool(vals url.Values, reqID string) (any, error) {
	id := vals.Get("IpamPoolId")

	pools := h.Backend.DescribeIpamPools([]string{id})
	if len(pools) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, id)
	}

	if err := h.Backend.DeleteIpamPool(id); err != nil {
		return nil, err
	}

	item := toIpamPoolItem(pools[0])
	item.State = ipamStateDeleteComplete

	return &deleteIpamPoolResponse{Xmlns: ec2XMLNS, RequestID: reqID, IpamPool: item}, nil
}

func (h *Handler) handleProvisionIpamPoolCidr(vals url.Values, reqID string) (any, error) {
	cidr, err := h.Backend.ProvisionIpamPoolCidr(vals.Get("IpamPoolId"), vals.Get("Cidr"))
	if err != nil {
		return nil, err
	}

	return &provisionIpamPoolCidrResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		IpamPoolCidr: toIpamPoolCidrItem(cidr),
	}, nil
}

func (h *Handler) handleDeprovisionIpamPoolCidr(vals url.Values, reqID string) (any, error) {
	cidr, err := h.Backend.DeprovisionIpamPoolCidr(vals.Get("IpamPoolId"), vals.Get("Cidr"))
	if err != nil {
		return nil, err
	}

	return &deprovisionIpamPoolCidrResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		IpamPoolCidr: toIpamPoolCidrItem(cidr),
	}, nil
}

func (h *Handler) handleGetIpamPoolCidrs(vals url.Values, reqID string) (any, error) {
	poolID := vals.Get("IpamPoolId")
	cidrs := h.Backend.GetIpamPoolCidrs(poolID)

	resp := &getIpamPoolCidrsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, c := range cidrs {
		resp.IpamPoolCidrSet.Items = append(resp.IpamPoolCidrSet.Items, toIpamPoolCidrItem(c))
	}

	return resp, nil
}

func (h *Handler) handleAllocateIpamPoolCidr(vals url.Values, reqID string) (any, error) {
	netmaskLenStr := vals.Get("NetmaskLength")
	netmaskLen, _ := strconv.Atoi(netmaskLenStr)

	alloc, err := h.Backend.AllocateIpamPoolCidr(
		vals.Get("IpamPoolId"),
		vals.Get("Cidr"),
		netmaskLen,
		IpamAllocationOptions{Description: vals.Get("Description")},
	)
	if err != nil {
		return nil, err
	}

	return &allocateIpamPoolCidrResponse{
		Xmlns:              ec2XMLNS,
		RequestID:          reqID,
		IpamPoolAllocation: toIpamPoolAllocationItem(alloc),
	}, nil
}

func (h *Handler) handleGetIpamPoolAllocations(vals url.Values, reqID string) (any, error) {
	poolID := vals.Get("IpamPoolId")
	allocationID := vals.Get("IpamPoolAllocationId")

	allocs, err := h.Backend.GetIpamPoolAllocations(poolID, allocationID)
	if err != nil {
		return nil, err
	}

	resp := &getIpamPoolAllocationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, alloc := range allocs {
		resp.IpamPoolAllocationSet.Items = append(
			resp.IpamPoolAllocationSet.Items,
			toIpamPoolAllocationItem(alloc),
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

func (h *Handler) handleDescribeIpamResourceDiscoveries(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamResourceDiscoveryId")
	discoveries := h.Backend.DescribeIpamResourceDiscoveries(ids)

	resp := &describeIpamResourceDiscoveriesResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, d := range discoveries {
		resp.IpamResourceDiscoverySet.Items = append(resp.IpamResourceDiscoverySet.Items, ipamResourceDiscoveryItem{
			IpamResourceDiscoveryID:  d.IpamResourceDiscoveryID,
			OwnerID:                  d.OwnerID,
			IpamResourceDiscoveryARN: d.IpamResourceDiscoveryARN,
			State:                    d.State,
			IsDefault:                d.IsDefault,
			Description:              d.Description,
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeIpamResourceDiscoveryAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "IpamResourceDiscoveryAssociationId")
	assocs := h.Backend.DescribeIpamResourceDiscoveryAssociations(ids)

	resp := &describeIpamResourceDiscoveryAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, a := range assocs {
		resp.IpamResourceDiscoveryAssociationSet.Items = append(
			resp.IpamResourceDiscoveryAssociationSet.Items,
			ipamResourceDiscoveryAssociationItem{
				IpamResourceDiscoveryAssociationID: a.IpamResourceDiscoveryAssociationID,
				IpamID:                             a.IpamID,
				IpamRegion:                         a.IpamRegion,
				IpamResourceDiscoveryID:            a.IpamResourceDiscoveryID,
				IsDefault:                          a.IsDefault,
				ResourceDiscoveryStatus:            a.ResourceDiscoveryStatus,
				State:                              a.State,
			},
		)
	}

	return resp, nil
}

// handleGetIpamAddressHistory always returns an empty (but correctly shaped) history record
// set: modeling real IPAM address-usage history requires a live discovery pipeline this mock
// does not implement.
func (h *Handler) handleGetIpamAddressHistory(_ url.Values, reqID string) (any, error) {
	return &getIpamAddressHistoryResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

// handleGetIpamDiscoveredAccounts always returns an empty (but correctly shaped) account set.
func (h *Handler) handleGetIpamDiscoveredAccounts(_ url.Values, reqID string) (any, error) {
	return &getIpamDiscoveredAccountsResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

// handleGetIpamDiscoveredResourceCidrs always returns an empty (but correctly shaped) CIDR set.
func (h *Handler) handleGetIpamDiscoveredResourceCidrs(_ url.Values, reqID string) (any, error) {
	return &getIpamDiscoveredResourceCidrsResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

// handleGetIpamDiscoveredPublicAddresses always returns an empty (but correctly shaped) address set.
func (h *Handler) handleGetIpamDiscoveredPublicAddresses(_ url.Values, reqID string) (any, error) {
	return &getIpamDiscoveredPublicAddressesResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}
