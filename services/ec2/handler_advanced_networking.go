package ec2

import (
	"encoding/xml"
)

// note: handler_advanced_networking.go holds the shared operation-registration
// table and XML response types for the "advanced networking" op families:
// VPN/customer gateways, VPN connections, VPC endpoint service configurations,
// and IPAM. The handler implementations themselves live in
// handler_vpn_gateways.go, handler_vpn_connections.go,
// handler_vpc_endpoint_services.go, and handler_ipam.go respectively.

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
	ops["ModifyVpnConnectionOptions"] = h.handleModifyVpnConnectionOptions
	ops["ModifyVpnTunnelOptions"] = h.handleModifyVpnTunnelOptions
	ops["ModifyVpnTunnelCertificate"] = h.handleModifyVpnTunnelCertificate
	ops["GetVpnConnectionDeviceTypes"] = h.handleGetVpnConnectionDeviceTypes
	ops["GetVpnConnectionDeviceSampleConfiguration"] = h.handleGetVpnConnectionDeviceSampleConfiguration
	ops["GetVpnTunnelReplacementStatus"] = h.handleGetVpnTunnelReplacementStatus

	// VPC Peering extras
	ops["RejectVpcPeeringConnection"] = h.handleRejectVpcPeeringConnection

	// VPC Endpoint Service Configurations
	ops["CreateVpcEndpointServiceConfiguration"] = h.handleCreateVpcEndpointServiceConfiguration
	ops["DescribeVpcEndpointServiceConfigurations"] = h.handleDescribeVpcEndpointServiceConfigurations
	ops["DeleteVpcEndpointServiceConfigurations"] = h.handleDeleteVpcEndpointServiceConfigurations
	ops["ModifyVpcEndpointServiceConfiguration"] = h.handleModifyVpcEndpointServiceConfiguration
	ops["StartVpcEndpointServicePrivateDnsVerification"] = h.handleStartVpcEndpointServicePrivateDNSVerification

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
	ops["DescribeIpamPoolAllocations"] = h.handleDescribeIpamPoolAllocations
	ops["ModifyIpamPoolAllocation"] = h.handleModifyIpamPoolAllocation
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
		"ModifyVpnConnectionOptions",
		"ModifyVpnTunnelOptions",
		"ModifyVpnTunnelCertificate",
		"GetVpnConnectionDeviceTypes",
		"GetVpnConnectionDeviceSampleConfiguration",
		"GetVpnTunnelReplacementStatus",
		"RejectVpcPeeringConnection",
		"CreateVpcEndpointServiceConfiguration",
		"DescribeVpcEndpointServiceConfigurations",
		"DeleteVpcEndpointServiceConfigurations",
		"ModifyVpcEndpointServiceConfiguration",
		"StartVpcEndpointServicePrivateDnsVerification",
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
		"DescribeIpamPoolAllocations",
		"ModifyIpamPoolAllocation",
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

type ikeVersionItem struct {
	Value string `xml:"value"`
}

type vpnTunnelOptionItem struct {
	OutsideIPAddress string `xml:"outsideIpAddress,omitempty"`
	TunnelInsideCidr string `xml:"tunnelInsideCidr,omitempty"`
	PreSharedKey     string `xml:"preSharedKey,omitempty"`
	DPDTimeoutAction string `xml:"dpdTimeoutAction,omitempty"`
	StartupAction    string `xml:"startupAction,omitempty"`
	CertificateArn   string `xml:"certificateArn,omitempty"`
	// Real field name is "ikeVersionSet", not "ikeVersions"
	// (ec2@v1.319.1 deserializers.go: awsEc2query_deserializeDocumentTunnelOption).
	IKEVersionSet struct {
		Items []ikeVersionItem `xml:"item"`
	} `xml:"ikeVersionSet"`
	Phase1LifetimeSeconds  int32 `xml:"phase1LifetimeSeconds,omitempty"`
	Phase2LifetimeSeconds  int32 `xml:"phase2LifetimeSeconds,omitempty"`
	RekeyMarginTimeSeconds int32 `xml:"rekeyMarginTimeSeconds,omitempty"`
	DPDTimeoutSeconds      int32 `xml:"dpdTimeoutSeconds,omitempty"`
}

type vpnConnectionOptionsItem struct {
	LocalIpv4NetworkCidr  string `xml:"localIpv4NetworkCidr,omitempty"`
	RemoteIpv4NetworkCidr string `xml:"remoteIpv4NetworkCidr,omitempty"`
	// Real field name is "tunnelOptionSet", not "tunnelOptions"
	// (ec2@v1.319.1 deserializers.go: awsEc2query_deserializeDocumentVpnConnectionOptions).
	TunnelOptionsSet struct {
		Items []vpnTunnelOptionItem `xml:"item"`
	} `xml:"tunnelOptionSet"`
	StaticRoutesOnly bool `xml:"staticRoutesOnly"`
}

type vgwTelemetryItem struct {
	OutsideIPAddress   string `xml:"outsideIpAddress"`
	Status             string `xml:"status"`
	StatusMessage      string `xml:"statusMessage,omitempty"`
	LastStatusChange   string `xml:"lastStatusChange,omitempty"`
	CertificateArn     string `xml:"certificateArn,omitempty"`
	AcceptedRouteCount int32  `xml:"acceptedRouteCount"`
}

type vpnStaticRouteItem struct {
	DestinationCidrBlock string `xml:"destinationCidrBlock"`
	State                string `xml:"state"`
	Source               string `xml:"source"`
}

type vpnConnectionItem struct {
	VpnConnectionID              string `xml:"vpnConnectionId"`
	State                        string `xml:"state"`
	CustomerGatewayConfiguration string `xml:"customerGatewayConfiguration,omitempty"`
	Type                         string `xml:"type"`
	CustomerGatewayID            string `xml:"customerGatewayId"`
	VpnGatewayID                 string `xml:"vpnGatewayId,omitempty"`
	TransitGatewayID             string `xml:"transitGatewayId,omitempty"`
	Category                     string `xml:"category,omitempty"`
	RoutesSet                    struct {
		Items []vpnStaticRouteItem `xml:"item"`
	} `xml:"routes"`
	VgwTelemetrySet struct {
		Items []vgwTelemetryItem `xml:"item"`
	} `xml:"vgwTelemetry"`
	Options vpnConnectionOptionsItem `xml:"options"`
}

// toVpnConnectionItem builds the full wire representation of a VPN connection, including its
// negotiated tunnel options, VGW telemetry, and any static routes registered against it.
func (h *Handler) toVpnConnectionItem(conn *VpnConnection) vpnConnectionItem {
	item := vpnConnectionItem{
		VpnConnectionID:              conn.VpnConnectionID,
		State:                        conn.State,
		CustomerGatewayConfiguration: conn.CustomerGatewayConfiguration,
		Type:                         conn.Type,
		CustomerGatewayID:            conn.CustomerGatewayID,
		VpnGatewayID:                 conn.VpnGatewayID,
		TransitGatewayID:             conn.TransitGatewayID,
		Category:                     conn.Category,
	}

	item.Options.StaticRoutesOnly = conn.Options.StaticRoutesOnly
	item.Options.LocalIpv4NetworkCidr = conn.Options.LocalIPv4NetworkCIDR
	item.Options.RemoteIpv4NetworkCidr = conn.Options.RemoteIPv4NetworkCIDR

	for _, t := range conn.Options.TunnelOptions {
		tItem := vpnTunnelOptionItem{
			OutsideIPAddress:       t.OutsideIPAddress,
			TunnelInsideCidr:       t.TunnelInsideCIDR,
			PreSharedKey:           t.PreSharedKey,
			Phase1LifetimeSeconds:  t.Phase1LifetimeSeconds,
			Phase2LifetimeSeconds:  t.Phase2LifetimeSeconds,
			RekeyMarginTimeSeconds: t.RekeyMarginTimeSeconds,
			DPDTimeoutSeconds:      t.DPDTimeoutSeconds,
			DPDTimeoutAction:       t.DPDTimeoutAction,
			StartupAction:          t.StartupAction,
			CertificateArn:         t.CertificateARN,
		}

		for _, v := range t.IKEVersions {
			tItem.IKEVersionSet.Items = append(tItem.IKEVersionSet.Items, ikeVersionItem{Value: v})
		}

		item.Options.TunnelOptionsSet.Items = append(item.Options.TunnelOptionsSet.Items, tItem)
	}

	for _, t := range conn.VgwTelemetry {
		item.VgwTelemetrySet.Items = append(item.VgwTelemetrySet.Items, vgwTelemetryItem{
			OutsideIPAddress:   t.OutsideIPAddress,
			Status:             t.Status,
			StatusMessage:      t.StatusMessage,
			AcceptedRouteCount: t.AcceptedRouteCount,
			LastStatusChange:   t.LastStatusChange,
			CertificateArn:     t.CertificateARN,
		})
	}

	for _, r := range h.Backend.GetVpnConnectionRoutes(conn.VpnConnectionID) {
		item.RoutesSet.Items = append(item.RoutesSet.Items, vpnStaticRouteItem{
			DestinationCidrBlock: r.DestinationCIDR,
			State:                r.State,
			Source:               "Static",
		})
	}

	return item
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

type modifyVpnConnectionResponse struct {
	XMLName       xml.Name          `xml:"ModifyVpnConnectionResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	VpnConnection vpnConnectionItem `xml:"vpnConnection"`
}

type modifyVpnConnectionOptionsResponse struct {
	XMLName       xml.Name          `xml:"ModifyVpnConnectionOptionsResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	VpnConnection vpnConnectionItem `xml:"vpnConnection"`
}

type modifyVpnTunnelOptionsResponse struct {
	XMLName       xml.Name          `xml:"ModifyVpnTunnelOptionsResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	VpnConnection vpnConnectionItem `xml:"vpnConnection"`
}

type modifyVpnTunnelCertificateResponse struct {
	XMLName       xml.Name          `xml:"ModifyVpnTunnelCertificateResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	VpnConnection vpnConnectionItem `xml:"vpnConnection"`
}

type vpnConnectionDeviceTypeItem struct {
	VpnConnectionDeviceTypeID string `xml:"vpnConnectionDeviceTypeId"`
	Vendor                    string `xml:"vendor"`
	Platform                  string `xml:"platform"`
	Software                  string `xml:"software"`
}

type getVpnConnectionDeviceTypesResponse struct {
	XMLName                    xml.Name `xml:"GetVpnConnectionDeviceTypesResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	RequestID                  string   `xml:"requestId"`
	VpnConnectionDeviceTypeSet struct {
		Items []vpnConnectionDeviceTypeItem `xml:"item"`
	} `xml:"vpnConnectionDeviceTypeSet"`
}

type getVpnConnectionDeviceSampleConfigurationResponse struct {
	XMLName                                xml.Name `xml:"GetVpnConnectionDeviceSampleConfigurationResponse"`
	Xmlns                                  string   `xml:"xmlns,attr"`
	RequestID                              string   `xml:"requestId"`
	VpnConnectionDeviceSampleConfiguration string   `xml:"vpnConnectionDeviceSampleConfiguration"`
}

type vpnTunnelMaintenanceDetailsItem struct {
	PendingMaintenance string `xml:"pendingMaintenance"`
}

type getVpnTunnelReplacementStatusResponse struct {
	XMLName                   xml.Name                        `xml:"GetVpnTunnelReplacementStatusResponse"`
	Xmlns                     string                          `xml:"xmlns,attr"`
	RequestID                 string                          `xml:"requestId"`
	VpnConnectionID           string                          `xml:"vpnConnectionId"`
	TransitGatewayID          string                          `xml:"transitGatewayId,omitempty"`
	VpnGatewayID              string                          `xml:"vpnGatewayId,omitempty"`
	CustomerGatewayID         string                          `xml:"customerGatewayId,omitempty"`
	VpnTunnelOutsideIPAddress string                          `xml:"vpnTunnelOutsideIpAddress"`
	MaintenanceDetails        vpnTunnelMaintenanceDetailsItem `xml:"maintenanceDetails"`
}

type rejectVpcPeeringConnectionResponse struct {
	XMLName   xml.Name `xml:"RejectVpcPeeringConnectionResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type privateDNSNameConfigurationItem struct {
	State string `xml:"state,omitempty"`
}

type vpcEndpointServiceConfigItem struct {
	ServiceID                   string                          `xml:"serviceId"`
	ServiceName                 string                          `xml:"serviceName"`
	ServiceType                 string                          `xml:"serviceType>item>serviceType"`
	PayerResponsibility         string                          `xml:"payerResponsibility,omitempty"`
	PrivateDNSNameConfiguration privateDNSNameConfigurationItem `xml:"privateDnsNameConfiguration"`
	NetworkLoadBalancerArnSet   []string                        `xml:"networkLoadBalancerArnSet>item"`
	AcceptanceRequired          bool                            `xml:"acceptanceRequired"`
}

func toVpcEndpointServiceConfigItem(cfg *VpcEndpointServiceConfig) vpcEndpointServiceConfigItem {
	return vpcEndpointServiceConfigItem{
		ServiceID:                 cfg.ServiceID,
		ServiceName:               cfg.ServiceName,
		ServiceType:               cfg.ServiceType,
		PayerResponsibility:       cfg.PayerResponsibility,
		AcceptanceRequired:        cfg.AcceptanceRequired,
		NetworkLoadBalancerArnSet: cfg.NetworkLoadBalancerARNs,
		PrivateDNSNameConfiguration: privateDNSNameConfigurationItem{
			State: cfg.PrivateDNSNameState,
		},
	}
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
	XMLName      xml.Name              `xml:"DeleteVpcEndpointServiceConfigurationsResponse"`
	RequestID    string                `xml:"requestId"`
	Unsuccessful []unsuccessfulItemXML `xml:"unsuccessful>item"`
}

type modifyVpcEndpointServiceConfigurationResponse struct {
	XMLName   xml.Name `xml:"ModifyVpcEndpointServiceConfigurationResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type startVpcEndpointServicePrivateDNSVerificationResponse struct {
	XMLName   xml.Name `xml:"StartVpcEndpointServicePrivateDnsVerificationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
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
	} `xml:"operatingRegionSet"`
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
	ResourceRegion       string `xml:"resourceRegion,omitempty"`
}

func toIpamPoolAllocationItem(a *IpamPoolAllocation) ipamPoolAllocationItem {
	return ipamPoolAllocationItem{
		IpamPoolAllocationID: a.IpamPoolAllocationID,
		Cidr:                 a.Cidr,
		Description:          a.Description,
		ResourceType:         a.ResourceType,
		ResourceID:           a.ResourceID,
		ResourceOwner:        a.ResourceOwner,
		ResourceRegion:       a.ResourceRegion,
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

// releaseIpamPoolAllocationResponse mirrors the real
// ReleaseIpamPoolAllocationOutput: the wire field is "success", not "return"
// (deserializers.go, awsEc2query_deserializeOpDocumentReleaseIpamPoolAllocationOutput).
type releaseIpamPoolAllocationResponse struct {
	XMLName   xml.Name `xml:"ReleaseIpamPoolAllocationResponse"`
	RequestID string   `xml:"requestId"`
	Success   bool     `xml:"success"`
}

type ipamResourceDiscoveryItem struct {
	IpamResourceDiscoveryID     string `xml:"ipamResourceDiscoveryId"`
	OwnerID                     string `xml:"ownerId,omitempty"`
	IpamResourceDiscoveryARN    string `xml:"ipamResourceDiscoveryArn"`
	IpamResourceDiscoveryRegion string `xml:"ipamResourceDiscoveryRegion,omitempty"`
	State                       string `xml:"state"`
	Description                 string `xml:"description,omitempty"`
	OperatingRegionSet          struct {
		Items []ipamOperatingRegionItem `xml:"item"`
	} `xml:"operatingRegionSet"`
	IsDefault bool `xml:"isDefault"`
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
	IpamResourceDiscoveryAssociationID  string `xml:"ipamResourceDiscoveryAssociationId"`
	IpamResourceDiscoveryAssociationARN string `xml:"ipamResourceDiscoveryAssociationArn,omitempty"`
	IpamID                              string `xml:"ipamId"`
	IpamARN                             string `xml:"ipamArn,omitempty"`
	IpamRegion                          string `xml:"ipamRegion,omitempty"`
	IpamResourceDiscoveryID             string `xml:"ipamResourceDiscoveryId"`
	OwnerID                             string `xml:"ownerId,omitempty"`
	ResourceDiscoveryStatus             string `xml:"resourceDiscoveryStatus,omitempty"`
	State                               string `xml:"state"`
	IsDefault                           bool   `xml:"isDefault"`
}

// toIpamResourceDiscoveryAssociationItem builds the full wire representation of a resource
// discovery association. Shared by the Associate/Disassociate/Describe handlers.
func toIpamResourceDiscoveryAssociationItem(a *IpamResourceDiscoveryAssociation) ipamResourceDiscoveryAssociationItem {
	return ipamResourceDiscoveryAssociationItem{
		IpamResourceDiscoveryAssociationID:  a.IpamResourceDiscoveryAssociationID,
		IpamResourceDiscoveryAssociationARN: a.IpamResourceDiscoveryAssociationARN,
		IpamID:                              a.IpamID,
		IpamARN:                             a.IpamARN,
		IpamRegion:                          a.IpamRegion,
		IpamResourceDiscoveryID:             a.IpamResourceDiscoveryID,
		OwnerID:                             a.OwnerID,
		IsDefault:                           a.IsDefault,
		ResourceDiscoveryStatus:             a.ResourceDiscoveryStatus,
		State:                               a.State,
	}
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
