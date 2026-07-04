package ec2

import (
	"encoding/xml"
	"net/url"
)

// stubResponse is a minimal success response for stub EC2 operations. XMLName deliberately
// carries no xml tag: encoding/xml gives a tagged XMLName field priority over the runtime
// value, which would otherwise force every caller's root element to the literal name
// "StubResponse" instead of the action-specific name (e.g. "CreateFooResponse") each call site
// assigns via a struct literal.
type stubResponse struct {
	XMLName   xml.Name
	RequestID string `xml:"requestId"`
	Return    bool   `xml:"return"`
}

//nolint:funlen
func registerStubOps(h *Handler, ops map[string]ec2ActionFn) {
	// ops["AllocateIpamPoolCidr"] — real handler in handler_advanced_networking.go, covered there
	// ApplySecurityGroupsToClientVpnTargetNetwork — moved to handler_batch4.go
	// AssociateCapacityReservationBillingOwner — moved to handler_capacity_family.go
	// AssociateClientVpnTargetNetwork — moved to handler_batch4.go
	// AssociateEnclaveCertificateIamRole — moved to trunkEnclaveSupportedOperations
	// AssociateIamInstanceProfile — moved to handler_ec2core.go
	ops["AssociateInstanceEventWindow"] = h.handleStubAssociateInstanceEventWindow
	// ops["AssociateIpamByoasn"] — moved to ipamDiscoverySupportedOperations
	// ops["AssociateIpamResourceDiscovery"] — moved to ipamDiscoverySupportedOperations
	// AssociateRouteServer — real handler in handler_route_server.go, covered there
	// AssociateTransitGatewayMulticastDomain — moved to handler_tgw_multicast.go
	// AssociateTransitGatewayPolicyTable — moved to handler_tgw_peripherals.go
	// AssociateTransitGatewayRouteTable — moved to handler_ec2core.go
	// AssociateTrunkInterface — moved to trunkEnclaveSupportedOperations
	// AssociateVpcCidrBlock — moved to handler_ec2core.go
	// AttachClassicLinkVpc — moved to handler_vpc_config.go
	// AttachVerifiedAccessTrustProvider — moved to handler_batch4.go
	// AttachVpnGateway — moved to advancedNetworkingSupportedOperations
	// AuthorizeClientVpnIngress — moved to handler_batch4.go
	// BundleInstance — moved to vmImportExportSupportedOperations
	// CancelBundleTask — moved to vmImportExportSupportedOperations
	// CancelCapacityReservationFleets — moved to handler_capacity_family.go
	// CancelConversionTask — moved to vmImportExportSupportedOperations
	ops["CancelDeclarativePoliciesReport"] = h.handleStubCancelDeclarativePoliciesReport
	// CancelExportTask — moved to vmImportExportSupportedOperations
	ops["CancelImageLaunchPermission"] = h.handleStubCancelImageLaunchPermission
	// CancelImportTask — moved to vmImportExportSupportedOperations
	// CancelReservedInstancesListing — real handler in handler_batch5.go, covered there
	// CancelSpotFleetRequests — spot fleet handler in handler_spot_fleet.go, covered there
	// ConfirmProductInstance — moved to imageOpsSupportedOperations()
	// CopyFpgaImage — moved to handler_fpga_image.go
	// CreateCapacityManagerDataExport — moved to handler_capacity_family.go
	// CreateCapacityReservationBySplitting — moved to handler_capacity_family.go
	// CreateCapacityReservationFleet — moved to handler_capacity_family.go
	// CreateCarrierGateway — real handler in handler_batch5.go, covered there
	// CreateClientVpnEndpoint — moved to handler_batch4.go
	// CreateClientVpnRoute — moved to handler_batch4.go
	// CreateCoipCidr — moved to ipPoolSupportedOperations()
	// CreateCoipPool — moved to ipPoolSupportedOperations()
	// CreateCustomerGateway — moved to advancedNetworkingSupportedOperations
	ops["CreateDelegateMacVolumeOwnershipTask"] = h.handleStubCreateDelegateMacVolumeOwnershipTask
	// CreateEgressOnlyInternetGateway — moved to handler_ec2core.go
	// CreateFleet — real handler in handler_batch5.go, covered there
	// CreateFpgaImage — moved to handler_fpga_image.go
	// CreateImageUsageReport — moved to imageOpsSupportedOperations()
	// CreateInstanceExportTask — moved to vmImportExportSupportedOperations
	ops["CreateInterruptibleCapacityReservationAllocation"] = h.handleStubCreateInterruptibleCapacityReservationAllocation
	// ops["CreateIpam"] — real handler in handler_advanced_networking.go, covered there
	// ops["CreateIpamExternalResourceVerificationToken"] — moved to ipamDiscoverySupportedOperations
	// ops["CreateIpamPolicy"] — moved to ipamPolicySupportedOperations
	// ops["CreateIpamPool"] — real handler in handler_advanced_networking.go, covered there
	// ops["CreateIpamPrefixListResolver"] — moved to ipamDiscoverySupportedOperations
	// ops["CreateIpamPrefixListResolverTarget"] — moved to ipamDiscoverySupportedOperations
	// ops["CreateIpamResourceDiscovery"] — moved to ipamDiscoverySupportedOperations
	// ops["CreateIpamScope"] — moved to advancedNetworkingSupportedOperations
	// CreateLocalGatewayRoute — real handler in handler_local_gateway.go
	// CreateLocalGatewayRouteTable — real handler in handler_local_gateway.go
	// CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation — real handler in handler_local_gateway.go
	// CreateLocalGatewayRouteTableVpcAssociation — real handler in handler_local_gateway.go
	ops["CreateLocalGatewayVirtualInterface"] = h.handleStubCreateLocalGatewayVirtualInterface
	ops["CreateLocalGatewayVirtualInterfaceGroup"] = h.handleStubCreateLocalGatewayVirtualInterfaceGroup
	ops["CreateMacSystemIntegrityProtectionModificationTask"] = h.handleStubCreateMacSystemIntegrityProtectionModificationTask //nolint:lll // existing issue.
	// CreateManagedPrefixList — moved to handler_batch4.go
	// CreateNetworkInsightsAccessScope — real handler in handler_batch5.go, covered there
	// CreateNetworkInsightsPath — real handler in handler_batch5.go, covered there
	// CreateReservedInstancesListing — real handler in handler_batch5.go, covered there
	// CreateRestoreImageTask — moved to imageOpsSupportedOperations()
	// CreateRouteServer — real handler in handler_route_server.go, covered there
	// CreateRouteServerEndpoint — real handler in handler_route_server.go, covered there
	// CreateRouteServerPeer — real handler in handler_route_server.go, covered there
	ops["CreateSecondaryNetwork"] = h.handleStubCreateSecondaryNetwork
	ops["CreateSecondarySubnet"] = h.handleStubCreateSecondarySubnet
	// CreateStoreImageTask — moved to imageOpsSupportedOperations()
	// CreateTrafficMirrorFilter — moved to handler_batch5.go
	// CreateTrafficMirrorFilterRule — moved to handler_batch5.go
	// CreateTrafficMirrorSession — moved to handler_batch5.go
	// CreateTrafficMirrorTarget — moved to handler_batch5.go
	// CreateTransitGatewayConnect — moved to handler_batch4.go
	// CreateTransitGatewayConnectPeer — moved to handler_batch4.go
	// CreateTransitGatewayMeteringPolicy — moved to handler_tgw_multicast.go
	// CreateTransitGatewayMeteringPolicyEntry — moved to handler_tgw_multicast.go
	// CreateTransitGatewayMulticastDomain — moved to handler_tgw_multicast.go
	// CreateTransitGatewayPeeringAttachment — moved to handler_batch4.go
	// CreateTransitGatewayPolicyTable — moved to handler_tgw_peripherals.go
	// CreateTransitGatewayPrefixListReference — moved to handler_batch4.go
	// CreateTransitGatewayRoute — moved to handler_ec2core.go
	// CreateTransitGatewayRouteTable — moved to handler_ec2core.go
	// CreateTransitGatewayRouteTableAnnouncement — moved to handler_tgw_peripherals.go
	// CreateVerifiedAccessEndpoint — moved to handler_batch4.go
	// CreateVerifiedAccessGroup — moved to handler_batch4.go
	// CreateVerifiedAccessInstance — moved to handler_batch4.go
	// CreateVerifiedAccessTrustProvider — moved to handler_batch4.go
	// CreateVpcBlockPublicAccessExclusion — moved to handler_vpc_config.go
	ops["CreateVpcEncryptionControl"] = h.handleStubCreateVpcEncryptionControl
	// CreateVpcEndpointServiceConfiguration — real handler in handler_advanced_networking.go, covered there
	ops["CreateVpnConcentrator"] = h.handleStubCreateVpnConcentrator
	// CreateVpnConnection — moved to advancedNetworkingSupportedOperations
	// CreateVpnGateway — moved to advancedNetworkingSupportedOperations
	// DeleteCapacityManagerDataExport — moved to handler_capacity_family.go
	// DeleteCarrierGateway — real handler in handler_batch5.go, covered there
	// DeleteClientVpnEndpoint — moved to handler_batch4.go
	// DeleteClientVpnRoute — moved to handler_batch4.go
	// DeleteCoipCidr — moved to ipPoolSupportedOperations()
	// DeleteCoipPool — moved to ipPoolSupportedOperations()
	// DeleteCustomerGateway — moved to advancedNetworkingSupportedOperations
	// DeleteEgressOnlyInternetGateway — moved to handler_ec2core.go
	// DeleteFleets — real handler in handler_batch5.go, covered there
	// DeleteFpgaImage — moved to handler_fpga_image.go
	// DeleteImageUsageReport — moved to imageOpsSupportedOperations()
	// ops["DeleteIpam"] — real handler in handler_advanced_networking.go, covered there
	// ops["DeleteIpamExternalResourceVerificationToken"] — moved to ipamDiscoverySupportedOperations
	// ops["DeleteIpamPolicy"] — moved to ipamPolicySupportedOperations
	// ops["DeleteIpamPool"] — real handler in handler_advanced_networking.go, covered there
	// ops["DeleteIpamPrefixListResolver"] — moved to ipamDiscoverySupportedOperations
	// ops["DeleteIpamPrefixListResolverTarget"] — moved to ipamDiscoverySupportedOperations
	// ops["DeleteIpamResourceDiscovery"] — moved to ipamDiscoverySupportedOperations
	// ops["DeleteIpamScope"] — moved to advancedNetworkingSupportedOperations
	// DeleteLocalGatewayRoute — real handler in handler_local_gateway.go
	// DeleteLocalGatewayRouteTable — real handler in handler_local_gateway.go
	// DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation — real handler in handler_local_gateway.go
	// DeleteLocalGatewayRouteTableVpcAssociation — real handler in handler_local_gateway.go
	ops["DeleteLocalGatewayVirtualInterface"] = h.handleStubDeleteLocalGatewayVirtualInterface
	ops["DeleteLocalGatewayVirtualInterfaceGroup"] = h.handleStubDeleteLocalGatewayVirtualInterfaceGroup
	// DeleteManagedPrefixList — moved to handler_batch4.go
	// DeleteNetworkInsightsAccessScope — real handler in handler_batch5.go, covered there
	// DeleteNetworkInsightsAccessScopeAnalysis — real handler in handler_batch5.go, covered there
	// DeleteNetworkInsightsAnalysis — real handler in handler_batch5.go, covered there
	// DeleteNetworkInsightsPath — real handler in handler_batch5.go, covered there
	// DeleteQueuedReservedInstances — real handler in handler_batch5.go, covered there
	// DeleteRouteServer — real handler in handler_route_server.go, covered there
	// DeleteRouteServerEndpoint — real handler in handler_route_server.go, covered there
	// DeleteRouteServerPeer — real handler in handler_route_server.go, covered there
	ops["DeleteSecondaryNetwork"] = h.handleStubDeleteSecondaryNetwork
	ops["DeleteSecondarySubnet"] = h.handleStubDeleteSecondarySubnet
	// DeleteTrafficMirrorFilter — moved to handler_batch5.go
	// DeleteTrafficMirrorFilterRule — moved to handler_batch5.go
	// DeleteTrafficMirrorSession — moved to handler_batch5.go
	// DeleteTrafficMirrorTarget — moved to handler_batch5.go
	// DeleteTransitGatewayConnect — moved to handler_batch4.go
	// DeleteTransitGatewayConnectPeer — moved to handler_batch4.go
	// DeleteTransitGatewayMeteringPolicy — moved to handler_tgw_multicast.go
	// DeleteTransitGatewayMeteringPolicyEntry — moved to handler_tgw_multicast.go
	// DeleteTransitGatewayMulticastDomain — moved to handler_tgw_multicast.go
	// DeleteTransitGatewayPeeringAttachment — moved to handler_batch4.go
	// DeleteTransitGatewayPolicyTable — moved to handler_tgw_peripherals.go
	// DeleteTransitGatewayPrefixListReference — moved to handler_batch4.go
	// DeleteTransitGatewayRoute — moved to handler_ec2core.go
	// DeleteTransitGatewayRouteTable — moved to handler_ec2core.go
	// DeleteTransitGatewayRouteTableAnnouncement — moved to handler_tgw_peripherals.go
	// DeleteVerifiedAccessEndpoint — moved to handler_batch4.go
	// DeleteVerifiedAccessGroup — moved to handler_batch4.go
	// DeleteVerifiedAccessInstance — moved to handler_batch4.go
	// DeleteVerifiedAccessTrustProvider — moved to handler_batch4.go
	// DeleteVpcBlockPublicAccessExclusion — moved to handler_vpc_config.go
	ops["DeleteVpcEncryptionControl"] = h.handleStubDeleteVpcEncryptionControl
	// DeleteVpcEndpointServiceConfigurations — real handler in handler_advanced_networking.go, covered there
	ops["DeleteVpnConcentrator"] = h.handleStubDeleteVpnConcentrator
	// DeleteVpnConnection — moved to advancedNetworkingSupportedOperations
	// DeleteVpnGateway — moved to advancedNetworkingSupportedOperations
	// DeprovisionByoipCidr — real handler in handler_batch5.go, covered there
	// ops["DeprovisionIpamByoasn"] — moved to ipamDiscoverySupportedOperations
	// ops["DeprovisionIpamPoolCidr"] — moved to advancedNetworkingSupportedOperations
	// DeregisterTransitGatewayMulticastGroupMembers — moved to handler_tgw_multicast.go
	// DeregisterTransitGatewayMulticastGroupSources — moved to handler_tgw_multicast.go
	ops["DescribeAwsNetworkPerformanceMetricSubscriptions"] = h.handleStubDescribeAwsNetworkPerformanceMetricSubscriptions
	// DescribeBundleTasks — moved to vmImportExportSupportedOperations
	// DescribeCapacityBlockExtensionHistory — moved to handler_capacity_family.go
	// DescribeCapacityBlockExtensionOfferings — moved to handler_capacity_family.go
	// DescribeCapacityBlockOfferings — moved to handler_capacity_family.go
	// DescribeCapacityBlockStatus — moved to handler_capacity_family.go
	// DescribeCapacityBlocks — moved to handler_capacity_family.go
	// DescribeCapacityManagerDataExports — moved to handler_capacity_family.go
	// DescribeCapacityReservationBillingRequests — moved to handler_capacity_family.go
	// DescribeCapacityReservationFleets — moved to handler_capacity_family.go
	ops["DescribeCapacityReservationTopology"] = h.handleStubDescribeCapacityReservationTopology
	// DescribeCarrierGateways — real handler in handler_batch5.go, covered there
	// DescribeClassicLinkInstances — moved to handler_vpc_config.go
	// DescribeClientVpnAuthorizationRules — moved to handler_batch4.go
	// DescribeClientVpnConnections — moved to handler_batch4.go
	// DescribeClientVpnEndpoints — moved to handler_batch4.go
	// DescribeClientVpnRoutes — moved to handler_batch4.go
	// DescribeClientVpnTargetNetworks — moved to handler_batch4.go
	// DescribeCoipPools — moved to ipPoolSupportedOperations()
	// DescribeConversionTasks — moved to vmImportExportSupportedOperations
	// DescribeCustomerGateways — moved to advancedNetworkingSupportedOperations
	ops["DescribeDeclarativePoliciesReports"] = h.handleStubDescribeDeclarativePoliciesReports
	// DescribeEgressOnlyInternetGateways — moved to handler_ec2core.go
	ops["DescribeElasticGpus"] = h.handleStubDescribeElasticGpus
	// DescribeExportImageTasks — moved to batch3SupportedOperations
	// DescribeExportTasks — moved to vmImportExportSupportedOperations
	// DescribeFleetHistory — real handler in handler_batch5.go, covered there
	// DescribeFleetInstances — real handler in handler_batch5.go, covered there
	// DescribeFleets — real handler in handler_batch5.go, covered there
	// DescribeFpgaImageAttribute — moved to handler_fpga_image.go
	// DescribeFpgaImages — moved to handler_fpga_image.go
	ops["DescribeHostReservationOfferings"] = h.handleStubDescribeHostReservationOfferings
	ops["DescribeHostReservations"] = h.handleStubDescribeHostReservations
	// DescribeIamInstanceProfileAssociations — moved to handler_ec2core.go
	ops["DescribeImageReferences"] = h.handleStubDescribeImageReferences
	// DescribeImageUsageReportEntries — moved to imageOpsSupportedOperations()
	ops["DescribeInstanceSqlHaHistoryStates"] = h.handleStubDescribeInstanceSQLHaHistoryStates
	ops["DescribeInstanceSqlHaStates"] = h.handleStubDescribeInstanceSQLHaStates
	// ops["DescribeIpamByoasn"] — moved to ipamDiscoverySupportedOperations
	// ops["DescribeIpamExternalResourceVerificationTokens"] — moved to ipamDiscoverySupportedOperations
	// ops["DescribeIpamPolicies"] — moved to ipamPolicySupportedOperations
	// ops["DescribeIpamPools"] — real handler in handler_advanced_networking.go, covered there
	// ops["DescribeIpamPrefixListResolverTargets"] — moved to ipamDiscoverySupportedOperations
	// ops["DescribeIpamPrefixListResolvers"] — moved to ipamDiscoverySupportedOperations
	// ops["DescribeIpamResourceDiscoveries"] — moved to advancedNetworkingSupportedOperations
	// ops["DescribeIpamResourceDiscoveryAssociations"] — moved to advancedNetworkingSupportedOperations
	// ops["DescribeIpamScopes"] — moved to advancedNetworkingSupportedOperations
	// ops["DescribeIpams"] — real handler in handler_advanced_networking.go, covered there
	// DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations — real handler in handler_local_gateway.go
	// DescribeLocalGatewayRouteTableVpcAssociations — real handler in handler_local_gateway.go
	// DescribeLocalGatewayRouteTables — real handler in handler_local_gateway.go
	// DescribeLocalGatewayVirtualInterfaceGroups — real handler in handler_local_gateway.go
	// DescribeLocalGatewayVirtualInterfaces — real handler in handler_local_gateway.go
	// DescribeLocalGateways — real handler in handler_local_gateway.go
	ops["DescribeMacHosts"] = h.handleStubDescribeMacHosts
	ops["DescribeMacModificationTasks"] = h.handleStubDescribeMacModificationTasks
	// DescribeManagedPrefixLists — moved to handler_batch4.go
	ops["DescribeMovingAddresses"] = h.handleStubDescribeMovingAddresses
	// DescribeNetworkInsightsAccessScopeAnalyses — real handler in handler_batch5.go, covered there
	// DescribeNetworkInsightsAccessScopes — real handler in handler_batch5.go, covered there
	// DescribeNetworkInsightsAnalyses — real handler in handler_batch5.go, covered there
	// DescribeNetworkInsightsPaths — real handler in handler_batch5.go, covered there
	ops["DescribeOutpostLags"] = h.handleStubDescribeOutpostLags
	// DescribeReservedInstances — real handler in handler_batch5.go, covered there
	// DescribeReservedInstancesListings — real handler in handler_batch5.go, covered there
	// DescribeReservedInstancesModifications — real handler in handler_batch5.go, covered there
	// DescribeReservedInstancesOfferings — real handler in handler_batch5.go, covered there
	// DescribeRouteServerEndpoints — real handler in handler_route_server.go, covered there
	// DescribeRouteServerPeers — real handler in handler_route_server.go, covered there
	// DescribeRouteServers — real handler in handler_route_server.go, covered there
	// DescribeScheduledInstanceAvailability — moved to scheduledInstanceSupportedOperations()
	// DescribeScheduledInstances — moved to scheduledInstanceSupportedOperations()
	ops["DescribeSecondaryInterfaces"] = h.handleStubDescribeSecondaryInterfaces
	ops["DescribeSecondaryNetworks"] = h.handleStubDescribeSecondaryNetworks
	ops["DescribeSecondarySubnets"] = h.handleStubDescribeSecondarySubnets
	ops["DescribeServiceLinkVirtualInterfaces"] = h.handleStubDescribeServiceLinkVirtualInterfaces
	// DescribeSpotFleetInstances — spot fleet handler in handler_spot_fleet.go, covered there
	// DescribeSpotFleetRequestHistory — spot fleet handler in handler_spot_fleet.go, covered there
	// DescribeSpotFleetRequests — spot fleet handler in handler_spot_fleet.go, covered there
	// DescribeStoreImageTasks — moved to imageOpsSupportedOperations()
	// DescribeTrafficMirrorFilterRules — moved to handler_batch5.go
	// DescribeTrafficMirrorFilters — moved to handler_batch5.go
	// DescribeTrafficMirrorSessions — moved to handler_batch5.go
	// DescribeTrafficMirrorTargets — moved to handler_batch5.go
	ops["DescribeTransitGatewayAttachments"] = h.handleStubDescribeTransitGatewayAttachments
	// DescribeTransitGatewayConnectPeers — moved to handler_batch4.go
	// DescribeTransitGatewayConnects — moved to handler_batch4.go
	// DescribeTransitGatewayMeteringPolicies — moved to handler_tgw_multicast.go
	// DescribeTransitGatewayMulticastDomains — moved to handler_tgw_multicast.go
	// DescribeTransitGatewayPeeringAttachments — moved to handler_batch4.go
	// DescribeTransitGatewayPolicyTables — moved to handler_tgw_peripherals.go
	// DescribeTransitGatewayRouteTableAnnouncements — moved to handler_tgw_peripherals.go
	// DescribeTransitGatewayRouteTables — moved to handler_ec2core.go
	// DescribeTrunkInterfaceAssociations — moved to trunkEnclaveSupportedOperations
	// DescribeVerifiedAccessEndpoints — moved to handler_batch4.go
	// DescribeVerifiedAccessGroups — moved to handler_batch4.go
	// DescribeVerifiedAccessInstanceLoggingConfigurations — moved to handler_verifiedaccess_ext.go
	// DescribeVerifiedAccessInstances — moved to handler_batch4.go
	// DescribeVerifiedAccessTrustProviders — moved to handler_batch4.go
	// DescribeVpcBlockPublicAccessExclusions — moved to handler_vpc_config.go
	// DescribeVpcBlockPublicAccessOptions — moved to handler_vpc_config.go
	// DescribeVpcClassicLink — moved to handler_vpc_config.go
	// DescribeVpcClassicLinkDnsSupport — moved to handler_vpc_config.go
	ops["DescribeVpcEncryptionControls"] = h.handleStubDescribeVpcEncryptionControls
	// DescribeVpcEndpointServiceConfigurations — real handler in handler_advanced_networking.go, covered there
	ops["DescribeVpnConcentrators"] = h.handleStubDescribeVpnConcentrators
	// DescribeVpnConnections — moved to advancedNetworkingSupportedOperations
	// DescribeVpnGateways — moved to advancedNetworkingSupportedOperations
	// DetachClassicLinkVpc — moved to handler_vpc_config.go
	// DetachVerifiedAccessTrustProvider — moved to handler_batch4.go
	// DisableAllowedImagesSettings — moved to imageOpsSupportedOperations()
	ops["DisableAwsNetworkPerformanceMetricSubscription"] = h.handleStubDisableAwsNetworkPerformanceMetricSubscription
	// DisableCapacityManager — moved to handler_capacity_family.go
	ops["DisableInstanceSqlHaStandbyDetections"] = h.handleStubDisableInstanceSQLHaStandbyDetections
	// ops["DisableIpamOrganizationAdminAccount"] — moved to ipamPolicySupportedOperations
	// ops["DisableIpamPolicy"] — moved to ipamPolicySupportedOperations
	// DisableRouteServerPropagation — real handler in handler_route_server.go, covered there
	ops["DisableTransitGatewayRouteTablePropagation"] = h.handleStubDisableTransitGatewayRouteTablePropagation
	// DisableVpcClassicLink — moved to handler_vpc_config.go
	// DisableVpcClassicLinkDnsSupport — moved to handler_vpc_config.go
	// DisassociateCapacityReservationBillingOwner — moved to handler_capacity_family.go
	// DisassociateClientVpnTargetNetwork — moved to handler_batch4.go
	// DisassociateEnclaveCertificateIamRole — moved to trunkEnclaveSupportedOperations
	// DisassociateIamInstanceProfile — moved to handler_ec2core.go
	ops["DisassociateInstanceEventWindow"] = h.handleStubDisassociateInstanceEventWindow
	// ops["DisassociateIpamByoasn"] — moved to ipamDiscoverySupportedOperations
	// ops["DisassociateIpamResourceDiscovery"] — moved to ipamDiscoverySupportedOperations
	// DisassociateRouteServer — real handler in handler_route_server.go, covered there
	// DisassociateTransitGatewayMulticastDomain — moved to handler_tgw_multicast.go
	// DisassociateTransitGatewayPolicyTable — moved to handler_tgw_peripherals.go
	// DisassociateTransitGatewayRouteTable — moved to handler_ec2core.go
	// DisassociateTrunkInterface — moved to trunkEnclaveSupportedOperations
	// EnableAllowedImagesSettings — moved to imageOpsSupportedOperations()
	ops["EnableAwsNetworkPerformanceMetricSubscription"] = h.handleStubEnableAwsNetworkPerformanceMetricSubscription
	// EnableCapacityManager — moved to handler_capacity_family.go
	ops["EnableInstanceSqlHaStandbyDetections"] = h.handleStubEnableInstanceSQLHaStandbyDetections
	// ops["EnableIpamOrganizationAdminAccount"] — moved to ipamPolicySupportedOperations
	// ops["EnableIpamPolicy"] — moved to ipamPolicySupportedOperations
	ops["EnableReachabilityAnalyzerOrganizationSharing"] = h.handleStubEnableReachabilityAnalyzerOrganizationSharing
	// EnableRouteServerPropagation — real handler in handler_route_server.go, covered there
	ops["EnableTransitGatewayRouteTablePropagation"] = h.handleStubEnableTransitGatewayRouteTablePropagation
	// EnableVpcClassicLink — moved to handler_vpc_config.go
	// EnableVpcClassicLinkDnsSupport — moved to handler_vpc_config.go
	// ExportClientVpnClientCertificateRevocationList — moved to handler_batch4.go
	// ExportClientVpnClientConfiguration — moved to handler_batch4.go
	// ExportTransitGatewayRoutes — moved to handler_tgw_peripherals.go
	// ExportVerifiedAccessInstanceClientConfiguration — moved to handler_verifiedaccess_ext.go
	ops["GetActiveVpnTunnelStatus"] = h.handleStubGetActiveVpnTunnelStatus
	// GetAllowedImagesSettings — moved to imageOpsSupportedOperations()
	// GetAssociatedEnclaveCertificateIamRoles — moved to trunkEnclaveSupportedOperations
	ops["GetAwsNetworkPerformanceData"] = h.handleStubGetAwsNetworkPerformanceData
	// GetCapacityManagerAttributes — moved to handler_capacity_family.go
	// GetCapacityManagerMetricData — moved to handler_capacity_family.go
	// GetCapacityManagerMetricDimensions — moved to handler_capacity_family.go
	ops["GetCapacityReservationUsage"] = h.handleStubGetCapacityReservationUsage
	// GetCoipPoolUsage — moved to ipPoolSupportedOperations()
	ops["GetDeclarativePoliciesReportSummary"] = h.handleStubGetDeclarativePoliciesReportSummary
	// ops["GetEnabledIpamPolicy"] — moved to ipamPolicySupportedOperations
	ops["GetFlowLogsIntegrationTemplate"] = h.handleStubGetFlowLogsIntegrationTemplate
	ops["GetHostReservationPurchasePreview"] = h.handleStubGetHostReservationPurchasePreview
	ops["GetImageAncestry"] = h.handleStubGetImageAncestry
	ops["GetInstanceTpmEkPub"] = h.handleStubGetInstanceTpmEkPub
	ops["GetInstanceUefiData"] = h.handleStubGetInstanceUefiData
	// ops["GetIpamAddressHistory"] — moved to advancedNetworkingSupportedOperations
	// ops["GetIpamDiscoveredAccounts"] — moved to advancedNetworkingSupportedOperations
	// ops["GetIpamDiscoveredPublicAddresses"] — moved to advancedNetworkingSupportedOperations
	// ops["GetIpamDiscoveredResourceCidrs"] — moved to advancedNetworkingSupportedOperations
	// ops["GetIpamPolicyAllocationRules"] — moved to ipamPolicySupportedOperations
	// ops["GetIpamPolicyOrganizationTargets"] — moved to ipamPolicySupportedOperations
	// ops["GetIpamPoolAllocations"] — moved to advancedNetworkingSupportedOperations
	// ops["GetIpamPoolCidrs"] — real handler in handler_advanced_networking.go, covered there
	// ops["GetIpamPrefixListResolverRules"] — moved to ipamDiscoverySupportedOperations
	// ops["GetIpamPrefixListResolverVersionEntries"] — moved to ipamDiscoverySupportedOperations
	// ops["GetIpamPrefixListResolverVersions"] — moved to ipamDiscoverySupportedOperations
	// ops["GetIpamResourceCidrs"] — moved to ipamDiscoverySupportedOperations
	// GetManagedPrefixListAssociations — moved to handler_batch4.go
	// GetManagedPrefixListEntries — moved to handler_batch4.go
	// GetNetworkInsightsAccessScopeAnalysisFindings — real handler in handler_batch5.go, covered there
	// GetNetworkInsightsAccessScopeContent — real handler in handler_batch5.go, covered there
	// GetReservedInstancesExchangeQuote — real handler in handler_batch5.go, covered there
	// GetRouteServerAssociations — real handler in handler_route_server.go, covered there
	// GetRouteServerPropagations — real handler in handler_route_server.go, covered there
	// GetRouteServerRoutingDatabase — real handler in handler_route_server.go, covered there
	ops["GetSpotPlacementScores"] = h.handleStubGetSpotPlacementScores
	// GetTransitGatewayAttachmentPropagations — moved to handler_tgw_peripherals.go
	// GetTransitGatewayMeteringPolicyEntries — moved to handler_tgw_peripherals.go
	// GetTransitGatewayMulticastDomainAssociations — moved to handler_tgw_multicast.go
	// GetTransitGatewayPolicyTableAssociations — moved to handler_tgw_peripherals.go
	// GetTransitGatewayPolicyTableEntries — moved to handler_tgw_peripherals.go
	// GetTransitGatewayPrefixListReferences — moved to handler_batch4.go
	// GetTransitGatewayRouteTableAssociations — moved to handler_tgw_peripherals.go
	// GetTransitGatewayRouteTablePropagations — moved to handler_tgw_peripherals.go
	// GetVerifiedAccessEndpointPolicy — moved to handler_verifiedaccess_ext.go
	// GetVerifiedAccessEndpointTargets — moved to handler_verifiedaccess_ext.go
	// GetVerifiedAccessGroupPolicy — moved to handler_verifiedaccess_ext.go
	ops["GetVpcResourcesBlockingEncryptionEnforcement"] = h.handleStubGetVpcResourcesBlockingEncryptionEnforcement
	// GetVpnConnectionDeviceSampleConfiguration — moved to advancedNetworkingSupportedOperations
	// GetVpnConnectionDeviceTypes — moved to advancedNetworkingSupportedOperations
	// GetVpnTunnelReplacementStatus — moved to advancedNetworkingSupportedOperations
	// ImportClientVpnClientCertificateRevocationList — moved to handler_batch4.go
	// ImportInstance — moved to vmImportExportSupportedOperations
	// ImportVolume — moved to vmImportExportSupportedOperations
	ops["ModifyAvailabilityZoneGroup"] = h.handleStubModifyAvailabilityZoneGroup
	// ModifyCapacityReservationFleet — moved to handler_capacity_family.go
	// ModifyClientVpnEndpoint — moved to handler_batch4.go
	// ModifyFleet — real handler in handler_batch5.go, covered there
	// ModifyFpgaImageAttribute — moved to handler_fpga_image.go
	ops["ModifyHosts"] = h.handleStubModifyHosts
	ops["ModifyInstanceCapacityReservationAttributes"] = h.handleStubModifyInstanceCapacityReservationAttributes
	ops["ModifyInstanceCpuOptions"] = h.handleStubModifyInstanceCPUOptions
	ops["ModifyInstanceEventStartTime"] = h.handleStubModifyInstanceEventStartTime
	ops["ModifyInstanceMaintenanceOptions"] = h.handleStubModifyInstanceMaintenanceOptions
	ops["ModifyInstanceNetworkPerformanceOptions"] = h.handleStubModifyInstanceNetworkPerformanceOptions
	ops["ModifyInstancePlacement"] = h.handleStubModifyInstancePlacement
	// ops["ModifyIpam"] — moved to advancedNetworkingSupportedOperations
	// ops["ModifyIpamPolicyAllocationRules"] — moved to ipamPolicySupportedOperations
	// ops["ModifyIpamPool"] — moved to advancedNetworkingSupportedOperations
	// ops["ModifyIpamPrefixListResolver"] — moved to ipamDiscoverySupportedOperations
	// ops["ModifyIpamPrefixListResolverTarget"] — moved to ipamDiscoverySupportedOperations
	// ops["ModifyIpamResourceCidr"] — moved to ipamDiscoverySupportedOperations
	// ops["ModifyIpamResourceDiscovery"] — moved to ipamDiscoverySupportedOperations
	// ops["ModifyIpamScope"] — moved to advancedNetworkingSupportedOperations
	// ModifyLocalGatewayRoute — real handler in handler_local_gateway.go
	// ModifyManagedPrefixList — moved to handler_batch4.go
	ops["ModifyPrivateDnsNameOptions"] = h.handleStubModifyPrivateDNSNameOptions
	ops["ModifyPublicIpDnsNameOptions"] = h.handleStubModifyPublicIPDNSNameOptions
	// ModifyReservedInstances — real handler in handler_batch5.go, covered there
	// ModifyRouteServer — real handler in handler_route_server.go, covered there
	// ModifySpotFleetRequest — spot fleet handler in handler_spot_fleet.go, covered there
	// ModifyTrafficMirrorFilterNetworkServices — moved to handler_batch5.go
	// ModifyTrafficMirrorFilterRule — moved to handler_batch5.go
	// ModifyTrafficMirrorSession — moved to handler_batch5.go
	// ModifyTransitGatewayMeteringPolicy — moved to handler_tgw_peripherals.go
	// ModifyTransitGatewayPrefixListReference — moved to handler_tgw_peripherals.go
	// ModifyTransitGatewayVpcAttachment — moved to handler_tgw_peripherals.go
	// ModifyVerifiedAccessEndpoint — moved to handler_batch4.go
	// ModifyVerifiedAccessEndpointPolicy — moved to handler_verifiedaccess_ext.go
	ops["ModifyVerifiedAccessGroup"] = h.handleStubModifyVerifiedAccessGroup
	// ModifyVerifiedAccessGroupPolicy — moved to handler_verifiedaccess_ext.go
	ops["ModifyVerifiedAccessInstance"] = h.handleStubModifyVerifiedAccessInstance
	// ModifyVerifiedAccessInstanceLoggingConfiguration — moved to handler_verifiedaccess_ext.go
	ops["ModifyVerifiedAccessTrustProvider"] = h.handleStubModifyVerifiedAccessTrustProvider
	// ModifyVpcBlockPublicAccessExclusion — moved to handler_vpc_config.go
	// ModifyVpcBlockPublicAccessOptions — moved to handler_vpc_config.go
	ops["ModifyVpcEncryptionControl"] = h.handleStubModifyVpcEncryptionControl
	// ModifyVpnConnectionOptions — moved to advancedNetworkingSupportedOperations
	// ModifyVpnTunnelCertificate — moved to advancedNetworkingSupportedOperations
	// ModifyVpnTunnelOptions — moved to advancedNetworkingSupportedOperations
	ops["MoveAddressToVpc"] = h.handleStubMoveAddressToVpc
	// ops["MoveByoipCidrToIpam"] — moved to ipamPolicySupportedOperations
	// MoveCapacityReservationInstances — moved to handler_capacity_family.go
	// ProvisionByoipCidr — real handler in handler_batch5.go, covered there
	// ops["ProvisionIpamByoasn"] — moved to ipamDiscoverySupportedOperations
	// ops["ProvisionIpamPoolCidr"] — moved to advancedNetworkingSupportedOperations
	// PurchaseCapacityBlock — moved to handler_capacity_family.go
	// PurchaseCapacityBlockExtension — moved to handler_capacity_family.go
	ops["PurchaseHostReservation"] = h.handleStubPurchaseHostReservation
	// PurchaseReservedInstancesOffering — real handler in handler_batch5.go, covered there
	// PurchaseScheduledInstances — moved to scheduledInstanceSupportedOperations()
	// RegisterTransitGatewayMulticastGroupMembers — moved to handler_tgw_multicast.go
	// RegisterTransitGatewayMulticastGroupSources — moved to handler_tgw_multicast.go
	// RejectCapacityReservationBillingOwnership — moved to handler_capacity_family.go
	// RejectTransitGatewayMulticastDomainAssociations — moved to handler_tgw_peripherals.go
	// RejectTransitGatewayPeeringAttachment — moved to handler_tgw_peripherals.go
	// RejectTransitGatewayVpcAttachment — moved to handler_tgw_peripherals.go
	ops["RejectVpcEndpointConnections"] = h.handleStubRejectVpcEndpointConnections
	// RejectVpcPeeringConnection — real handler in handler_advanced_networking.go, covered there
	ops["ReleaseHosts"] = h.handleStubReleaseHosts
	// ops["ReleaseIpamPoolAllocation"] — real handler in handler_advanced_networking.go, covered there
	// ReplaceIamInstanceProfileAssociation — moved to handler_ec2core.go
	// ReplaceImageCriteriaInAllowedImagesSettings — moved to imageOpsSupportedOperations()
	// ReplaceRouteTableAssociation — moved to handler_ec2core.go
	// ReplaceTransitGatewayRoute — moved to handler_ec2core.go
	ops["ReplaceVpnTunnel"] = h.handleStubReplaceVpnTunnel
	// RequestSpotFleet — spot fleet handler in handler_spot_fleet.go, covered there
	// ResetFpgaImageAttribute — moved to handler_fpga_image.go
	// RestoreManagedPrefixListVersion — moved to handler_batch4.go
	// RevokeClientVpnIngress — moved to handler_batch4.go
	// RunScheduledInstances — moved to scheduledInstanceSupportedOperations()
	// SearchLocalGatewayRoutes — real handler in handler_local_gateway.go
	// SearchTransitGatewayMulticastGroups — moved to handler_tgw_multicast.go
	// SearchTransitGatewayRoutes — moved to handler_tgw_peripherals.go
	ops["SendDiagnosticInterrupt"] = h.handleStubSendDiagnosticInterrupt
	ops["StartDeclarativePoliciesReport"] = h.handleStubStartDeclarativePoliciesReport
	// StartNetworkInsightsAccessScopeAnalysis — real handler in handler_batch5.go, covered there
	// StartNetworkInsightsAnalysis — real handler in handler_batch5.go, covered there
	// StartVpcEndpointServicePrivateDnsVerification — moved to handler_advanced_networking.go
	// TerminateClientVpnConnections — moved to handler_batch4.go
	ops["UnassignPrivateNatGatewayAddress"] = h.handleStubUnassignPrivateNatGatewayAddress
	// UpdateCapacityManagerOrganizationsAccess — moved to handler_capacity_family.go
	ops["UpdateInterruptibleCapacityReservationAllocation"] = h.handleStubUpdateInterruptibleCapacityReservationAllocation
	// WithdrawByoipCidr — real handler in handler_batch5.go, covered there
	// CreatePublicIpv4Pool — moved to ipPoolSupportedOperations()
	// DeletePublicIpv4Pool — moved to ipPoolSupportedOperations()
	// DeprovisionPublicIpv4PoolCidr — moved to ipPoolSupportedOperations()
	// DescribeIpv6Pools — moved to ipPoolSupportedOperations()
	// DescribePublicIpv4Pools — moved to ipPoolSupportedOperations()
	// GetAssociatedIpv6PoolCidrs — moved to ipPoolSupportedOperations()
	// ProvisionPublicIpv4PoolCidr — moved to ipPoolSupportedOperations()
}

//nolint:funlen
func stubSupportedOperations() []string {
	return []string{
		// "AllocateIpamPoolCidr", — moved to advancedNetworkingSupportedOperations
		// "ApplySecurityGroupsToClientVpnTargetNetwork", — moved to batch4SupportedOperations
		"AssociateCapacityReservationBillingOwner",
		// "AssociateClientVpnTargetNetwork", — moved to batch4SupportedOperations
		// "AssociateEnclaveCertificateIamRole", — moved to trunkEnclaveSupportedOperations
		// AssociateIamInstanceProfile — now in ec2CoreSupportedOperations
		"AssociateInstanceEventWindow",
		// "AssociateIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "AssociateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		"AssociateRouteServer",
		// "AssociateTransitGatewayMulticastDomain", — moved to handler_tgw_multicast.go
		// "AssociateTransitGatewayPolicyTable", — moved to tgwPeripheralsSupportedOperations
		// AssociateTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		// "AssociateTrunkInterface", — moved to trunkEnclaveSupportedOperations
		// AssociateVpcCidrBlock — moved to ec2CoreSupportedOperations
		// "AttachClassicLinkVpc", — moved to vpcConfigSupportedOperations
		// "AttachVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		// "AttachVpnGateway", — moved to advancedNetworkingSupportedOperations
		// "AuthorizeClientVpnIngress", — moved to batch4SupportedOperations
		// "BundleInstance", — moved to vmImportExportSupportedOperations
		// "CancelBundleTask", — moved to vmImportExportSupportedOperations
		"CancelCapacityReservationFleets",
		// "CancelConversionTask", — moved to vmImportExportSupportedOperations
		"CancelDeclarativePoliciesReport",
		// "CancelExportTask", — moved to vmImportExportSupportedOperations
		"CancelImageLaunchPermission",
		// "CancelImportTask", — moved to vmImportExportSupportedOperations
		"CancelReservedInstancesListing",
		// "ConfirmProductInstance", — moved to imageOpsSupportedOperations()
		// "CopyFpgaImage", — moved to handler_fpga_image.go
		"CreateCapacityManagerDataExport",
		"CreateCapacityReservationBySplitting",
		"CreateCapacityReservationFleet",
		"CreateCarrierGateway",
		// "CreateClientVpnEndpoint", — moved to batch4SupportedOperations
		// "CreateClientVpnRoute", — moved to batch4SupportedOperations
		// "CreateCoipCidr", — moved to ipPoolSupportedOperations()
		// "CreateCoipPool", — moved to ipPoolSupportedOperations()
		// "CreateCustomerGateway", — moved to advancedNetworkingSupportedOperations
		"CreateDelegateMacVolumeOwnershipTask",
		// CreateEgressOnlyInternetGateway — moved to ec2CoreSupportedOperations
		"CreateFleet",
		// "CreateFpgaImage", — moved to handler_fpga_image.go
		// "CreateImageUsageReport", — moved to imageOpsSupportedOperations()
		// "CreateInstanceExportTask", — moved to vmImportExportSupportedOperations
		"CreateInterruptibleCapacityReservationAllocation",
		// "CreateIpam", — moved to advancedNetworkingSupportedOperations
		// "CreateIpamExternalResourceVerificationToken", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamPolicy", — moved to ipamPolicySupportedOperations
		// "CreateIpamPool", — moved to advancedNetworkingSupportedOperations
		// "CreateIpamPrefixListResolver", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamPrefixListResolverTarget", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamScope", — moved to advancedNetworkingSupportedOperations
		// "CreateLocalGatewayRoute", — moved to localGatewaySupportedOperations
		// "CreateLocalGatewayRouteTable", — moved to localGatewaySupportedOperations
		// "CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation", — moved to localGatewaySupportedOperations
		// "CreateLocalGatewayRouteTableVpcAssociation", — moved to localGatewaySupportedOperations
		"CreateLocalGatewayVirtualInterface",
		"CreateLocalGatewayVirtualInterfaceGroup",
		"CreateMacSystemIntegrityProtectionModificationTask",
		// "CreateManagedPrefixList", — moved to batch4SupportedOperations
		"CreateNetworkInsightsAccessScope",
		"CreateNetworkInsightsPath",
		"CreateReservedInstancesListing",
		// "CreateRestoreImageTask", — moved to imageOpsSupportedOperations()
		"CreateRouteServer",
		"CreateRouteServerEndpoint",
		"CreateRouteServerPeer",
		"CreateSecondaryNetwork",
		"CreateSecondarySubnet",
		// "CreateStoreImageTask", — moved to imageOpsSupportedOperations()
		"CreateTrafficMirrorFilter",
		"CreateTrafficMirrorFilterRule",
		"CreateTrafficMirrorSession",
		"CreateTrafficMirrorTarget",
		// "CreateTransitGatewayConnect", — moved to batch4SupportedOperations
		// "CreateTransitGatewayConnectPeer", — moved to batch4SupportedOperations
		// "CreateTransitGatewayMeteringPolicy", — moved to handler_tgw_multicast.go
		// "CreateTransitGatewayMeteringPolicyEntry", — moved to handler_tgw_multicast.go
		// "CreateTransitGatewayMulticastDomain", — moved to handler_tgw_multicast.go
		// "CreateTransitGatewayPeeringAttachment", — moved to batch4SupportedOperations
		// "CreateTransitGatewayPolicyTable", — moved to tgwPeripheralsSupportedOperations
		// "CreateTransitGatewayPrefixListReference", — moved to batch4SupportedOperations
		// CreateTransitGatewayRoute — moved to ec2CoreSupportedOperations
		// CreateTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		// "CreateTransitGatewayRouteTableAnnouncement", — moved to tgwPeripheralsSupportedOperations
		// "CreateVerifiedAccessEndpoint", — moved to batch4SupportedOperations
		// "CreateVerifiedAccessGroup", — moved to batch4SupportedOperations
		// "CreateVerifiedAccessInstance", — moved to batch4SupportedOperations
		// "CreateVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		// "CreateVpcBlockPublicAccessExclusion", — moved to vpcConfigSupportedOperations
		"CreateVpcEncryptionControl",
		// "CreateVpcEndpointServiceConfiguration", — moved to advancedNetworkingSupportedOperations
		"CreateVpnConcentrator",
		// "CreateVpnConnection", — moved to advancedNetworkingSupportedOperations
		// "CreateVpnGateway", — moved to advancedNetworkingSupportedOperations
		"DeleteCapacityManagerDataExport",
		"DeleteCarrierGateway",
		// "DeleteClientVpnEndpoint", — moved to batch4SupportedOperations
		// "DeleteClientVpnRoute", — moved to batch4SupportedOperations
		// "DeleteCoipCidr", — moved to ipPoolSupportedOperations()
		// "DeleteCoipPool", — moved to ipPoolSupportedOperations()
		// "DeleteCustomerGateway", — moved to advancedNetworkingSupportedOperations
		// DeleteEgressOnlyInternetGateway — moved to ec2CoreSupportedOperations
		"DeleteFleets",
		// "DeleteFpgaImage", — moved to handler_fpga_image.go
		// "DeleteImageUsageReport", — moved to imageOpsSupportedOperations()
		// "DeleteIpam", — moved to advancedNetworkingSupportedOperations
		// "DeleteIpamExternalResourceVerificationToken", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamPolicy", — moved to ipamPolicySupportedOperations
		// "DeleteIpamPool", — moved to advancedNetworkingSupportedOperations
		// "DeleteIpamPrefixListResolver", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamPrefixListResolverTarget", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamScope", — moved to advancedNetworkingSupportedOperations
		// "DeleteLocalGatewayRoute", — moved to localGatewaySupportedOperations
		// "DeleteLocalGatewayRouteTable", — moved to localGatewaySupportedOperations
		// "DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation", — moved to localGatewaySupportedOperations
		// "DeleteLocalGatewayRouteTableVpcAssociation", — moved to localGatewaySupportedOperations
		"DeleteLocalGatewayVirtualInterface",
		"DeleteLocalGatewayVirtualInterfaceGroup",
		// "DeleteManagedPrefixList", — moved to batch4SupportedOperations
		"DeleteNetworkInsightsAccessScope",
		"DeleteNetworkInsightsAccessScopeAnalysis",
		"DeleteNetworkInsightsAnalysis",
		"DeleteNetworkInsightsPath",
		"DeleteQueuedReservedInstances",
		"DeleteRouteServer",
		"DeleteRouteServerEndpoint",
		"DeleteRouteServerPeer",
		"DeleteSecondaryNetwork",
		"DeleteSecondarySubnet",
		"DeleteTrafficMirrorFilter",
		"DeleteTrafficMirrorFilterRule",
		"DeleteTrafficMirrorSession",
		"DeleteTrafficMirrorTarget",
		// "DeleteTransitGatewayConnect", — moved to batch4SupportedOperations
		// "DeleteTransitGatewayConnectPeer", — moved to batch4SupportedOperations
		// "DeleteTransitGatewayMeteringPolicy", — moved to handler_tgw_multicast.go
		// "DeleteTransitGatewayMeteringPolicyEntry", — moved to handler_tgw_multicast.go
		// "DeleteTransitGatewayMulticastDomain", — moved to handler_tgw_multicast.go
		// "DeleteTransitGatewayPeeringAttachment", — moved to batch4SupportedOperations
		// "DeleteTransitGatewayPolicyTable", — moved to tgwPeripheralsSupportedOperations
		// "DeleteTransitGatewayPrefixListReference", — moved to batch4SupportedOperations
		// DeleteTransitGatewayRoute — moved to ec2CoreSupportedOperations
		// DeleteTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		// "DeleteTransitGatewayRouteTableAnnouncement", — moved to tgwPeripheralsSupportedOperations
		// "DeleteVerifiedAccessEndpoint", — moved to batch4SupportedOperations
		// "DeleteVerifiedAccessGroup", — moved to batch4SupportedOperations
		// "DeleteVerifiedAccessInstance", — moved to batch4SupportedOperations
		// "DeleteVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		// "DeleteVpcBlockPublicAccessExclusion", — moved to vpcConfigSupportedOperations
		"DeleteVpcEncryptionControl",
		// "DeleteVpcEndpointServiceConfigurations", — moved to advancedNetworkingSupportedOperations
		"DeleteVpnConcentrator",
		// "DeleteVpnConnection", — moved to advancedNetworkingSupportedOperations
		// "DeleteVpnGateway", — moved to advancedNetworkingSupportedOperations
		"DeprovisionByoipCidr",
		// "DeprovisionIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DeprovisionIpamPoolCidr", — moved to advancedNetworkingSupportedOperations
		// "DeregisterTransitGatewayMulticastGroupMembers", — moved to handler_tgw_multicast.go
		// "DeregisterTransitGatewayMulticastGroupSources", — moved to handler_tgw_multicast.go
		"DescribeAwsNetworkPerformanceMetricSubscriptions",
		// "DescribeBundleTasks", — moved to vmImportExportSupportedOperations
		"DescribeCapacityBlockExtensionHistory",
		"DescribeCapacityBlockExtensionOfferings",
		"DescribeCapacityBlockOfferings",
		"DescribeCapacityBlockStatus",
		"DescribeCapacityBlocks",
		"DescribeCapacityManagerDataExports",
		"DescribeCapacityReservationBillingRequests",
		"DescribeCapacityReservationFleets",
		"DescribeCapacityReservationTopology",
		"DescribeCarrierGateways",
		// "DescribeClassicLinkInstances", — moved to vpcConfigSupportedOperations
		// "DescribeClientVpnAuthorizationRules", — moved to batch4SupportedOperations
		// "DescribeClientVpnConnections", — moved to batch4SupportedOperations
		// "DescribeClientVpnEndpoints", — moved to batch4SupportedOperations
		// "DescribeClientVpnRoutes", — moved to batch4SupportedOperations
		// "DescribeClientVpnTargetNetworks", — moved to batch4SupportedOperations
		// "DescribeCoipPools", — moved to ipPoolSupportedOperations()
		// "DescribeConversionTasks", — moved to vmImportExportSupportedOperations
		// "DescribeCustomerGateways", — moved to advancedNetworkingSupportedOperations
		"DescribeDeclarativePoliciesReports",
		// DescribeEgressOnlyInternetGateways — moved to ec2CoreSupportedOperations
		"DescribeElasticGpus",
		// "DescribeExportImageTasks", — moved to batch3SupportedOperations
		// "DescribeExportTasks", — moved to vmImportExportSupportedOperations
		"DescribeFleetHistory",
		"DescribeFleetInstances",
		"DescribeFleets",
		// "DescribeFpgaImageAttribute", — moved to handler_fpga_image.go
		// "DescribeFpgaImages", — moved to handler_fpga_image.go
		"DescribeHostReservationOfferings",
		"DescribeHostReservations",
		// DescribeIamInstanceProfileAssociations — moved to ec2CoreSupportedOperations
		"DescribeImageReferences",
		// "DescribeImageUsageReportEntries", — moved to imageOpsSupportedOperations()
		"DescribeInstanceSqlHaHistoryStates",
		"DescribeInstanceSqlHaStates",
		// "DescribeIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DescribeIpamExternalResourceVerificationTokens", — moved to ipamDiscoverySupportedOperations
		// "DescribeIpamPolicies", — moved to ipamPolicySupportedOperations
		// "DescribeIpamPools", — moved to advancedNetworkingSupportedOperations
		// "DescribeIpamPrefixListResolverTargets", — moved to ipamDiscoverySupportedOperations
		// "DescribeIpamPrefixListResolvers", — moved to ipamDiscoverySupportedOperations
		// "DescribeIpamResourceDiscoveries", — moved to advancedNetworkingSupportedOperations
		// "DescribeIpamResourceDiscoveryAssociations", — moved to advancedNetworkingSupportedOperations
		// "DescribeIpamScopes", — moved to advancedNetworkingSupportedOperations
		// "DescribeIpams", — moved to advancedNetworkingSupportedOperations
		// "DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations", — moved to localGatewaySupportedOperations
		// "DescribeLocalGatewayRouteTableVpcAssociations", — moved to localGatewaySupportedOperations
		// "DescribeLocalGatewayRouteTables", — moved to localGatewaySupportedOperations
		// "DescribeLocalGatewayVirtualInterfaceGroups", — moved to localGatewaySupportedOperations
		// "DescribeLocalGatewayVirtualInterfaces", — moved to localGatewaySupportedOperations
		// "DescribeLocalGateways", — moved to localGatewaySupportedOperations
		"DescribeMacHosts",
		"DescribeMacModificationTasks",
		// "DescribeManagedPrefixLists", — moved to batch4SupportedOperations
		"DescribeMovingAddresses",
		"DescribeNetworkInsightsAccessScopeAnalyses",
		"DescribeNetworkInsightsAccessScopes",
		"DescribeNetworkInsightsAnalyses",
		"DescribeNetworkInsightsPaths",
		"DescribeOutpostLags",
		"DescribeReservedInstances",
		"DescribeReservedInstancesListings",
		"DescribeReservedInstancesModifications",
		"DescribeReservedInstancesOfferings",
		"DescribeRouteServerEndpoints",
		"DescribeRouteServerPeers",
		"DescribeRouteServers",
		// "DescribeScheduledInstanceAvailability", — moved to scheduledInstanceSupportedOperations()
		// "DescribeScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		"DescribeSecondaryInterfaces",
		"DescribeSecondaryNetworks",
		"DescribeSecondarySubnets",
		"DescribeServiceLinkVirtualInterfaces",
		// "DescribeStoreImageTasks", — moved to imageOpsSupportedOperations()
		"DescribeTrafficMirrorFilterRules",
		"DescribeTrafficMirrorFilters",
		"DescribeTrafficMirrorSessions",
		"DescribeTrafficMirrorTargets",
		"DescribeTransitGatewayAttachments",
		// "DescribeTransitGatewayConnectPeers", — moved to batch4SupportedOperations
		// "DescribeTransitGatewayConnects", — moved to batch4SupportedOperations
		// "DescribeTransitGatewayMeteringPolicies", — moved to handler_tgw_multicast.go
		// "DescribeTransitGatewayMulticastDomains", — moved to handler_tgw_multicast.go
		// "DescribeTransitGatewayPeeringAttachments", — moved to batch4SupportedOperations
		// "DescribeTransitGatewayPolicyTables", — moved to tgwPeripheralsSupportedOperations
		// "DescribeTransitGatewayRouteTableAnnouncements", — moved to tgwPeripheralsSupportedOperations
		// DescribeTransitGatewayRouteTables — moved to ec2CoreSupportedOperations
		// "DescribeTrunkInterfaceAssociations", — moved to trunkEnclaveSupportedOperations
		// "DescribeVerifiedAccessEndpoints", — moved to batch4SupportedOperations
		// "DescribeVerifiedAccessGroups", — moved to batch4SupportedOperations
		// "DescribeVerifiedAccessInstanceLoggingConfigurations", — moved to handler_verifiedaccess_ext.go
		// "DescribeVerifiedAccessInstances", — moved to batch4SupportedOperations
		// "DescribeVerifiedAccessTrustProviders", — moved to batch4SupportedOperations
		// "DescribeVpcBlockPublicAccessExclusions", — moved to vpcConfigSupportedOperations
		// "DescribeVpcBlockPublicAccessOptions", — moved to vpcConfigSupportedOperations
		// "DescribeVpcClassicLink", — moved to vpcConfigSupportedOperations
		// "DescribeVpcClassicLinkDnsSupport", — moved to vpcConfigSupportedOperations
		"DescribeVpcEncryptionControls",
		// "DescribeVpcEndpointServiceConfigurations", — moved to advancedNetworkingSupportedOperations
		"DescribeVpnConcentrators",
		// "DescribeVpnConnections", — moved to advancedNetworkingSupportedOperations
		// "DescribeVpnGateways", — moved to advancedNetworkingSupportedOperations
		// "DetachClassicLinkVpc", — moved to vpcConfigSupportedOperations
		// "DetachVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		// "DetachVpnGateway", — moved to advancedNetworkingSupportedOperations
		// "DisableAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		"DisableAwsNetworkPerformanceMetricSubscription",
		"DisableCapacityManager",
		"DisableInstanceSqlHaStandbyDetections",
		// "DisableIpamOrganizationAdminAccount", — moved to ipamPolicySupportedOperations
		// "DisableIpamPolicy", — moved to ipamPolicySupportedOperations
		"DisableRouteServerPropagation",
		"DisableTransitGatewayRouteTablePropagation",
		// "DisableVpcClassicLink", — moved to vpcConfigSupportedOperations
		// "DisableVpcClassicLinkDnsSupport", — moved to vpcConfigSupportedOperations
		"DisassociateCapacityReservationBillingOwner",
		// "DisassociateClientVpnTargetNetwork", — moved to batch4SupportedOperations
		// "DisassociateEnclaveCertificateIamRole", — moved to trunkEnclaveSupportedOperations
		// DisassociateIamInstanceProfile — moved to ec2CoreSupportedOperations
		"DisassociateInstanceEventWindow",
		// "DisassociateIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DisassociateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		"DisassociateRouteServer",
		// "DisassociateTransitGatewayMulticastDomain", — moved to handler_tgw_multicast.go
		// "DisassociateTransitGatewayPolicyTable", — moved to tgwPeripheralsSupportedOperations
		// DisassociateTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		// "DisassociateTrunkInterface", — moved to trunkEnclaveSupportedOperations
		// "EnableAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		"EnableAwsNetworkPerformanceMetricSubscription",
		"EnableCapacityManager",
		"EnableInstanceSqlHaStandbyDetections",
		// "EnableIpamOrganizationAdminAccount", — moved to ipamPolicySupportedOperations
		// "EnableIpamPolicy", — moved to ipamPolicySupportedOperations
		"EnableReachabilityAnalyzerOrganizationSharing",
		"EnableRouteServerPropagation",
		"EnableTransitGatewayRouteTablePropagation",
		// "EnableVpcClassicLink", — moved to vpcConfigSupportedOperations
		// "EnableVpcClassicLinkDnsSupport", — moved to vpcConfigSupportedOperations
		// ExportClientVpnClientCertificateRevocationList — moved to batch4SupportedOperations
		// ExportClientVpnClientConfiguration — moved to batch4SupportedOperations
		// "ExportTransitGatewayRoutes", — moved to tgwPeripheralsSupportedOperations
		// "ExportVerifiedAccessInstanceClientConfiguration", — moved to handler_verifiedaccess_ext.go
		"GetActiveVpnTunnelStatus",
		// "GetAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// "GetAssociatedEnclaveCertificateIamRoles", — moved to trunkEnclaveSupportedOperations
		"GetAwsNetworkPerformanceData",
		"GetCapacityManagerAttributes",
		"GetCapacityManagerMetricData",
		"GetCapacityManagerMetricDimensions",
		"GetCapacityReservationUsage",
		// "GetCoipPoolUsage", — moved to ipPoolSupportedOperations()
		"GetDeclarativePoliciesReportSummary",
		// "GetEnabledIpamPolicy", — moved to ipamPolicySupportedOperations
		"GetFlowLogsIntegrationTemplate",
		"GetHostReservationPurchasePreview",
		"GetImageAncestry",
		"GetInstanceTpmEkPub",
		"GetInstanceUefiData",
		// "GetIpamAddressHistory", — moved to advancedNetworkingSupportedOperations
		// "GetIpamDiscoveredAccounts", — moved to advancedNetworkingSupportedOperations
		// "GetIpamDiscoveredPublicAddresses", — moved to advancedNetworkingSupportedOperations
		// "GetIpamDiscoveredResourceCidrs", — moved to advancedNetworkingSupportedOperations
		// "GetIpamPolicyAllocationRules", — moved to ipamPolicySupportedOperations
		// "GetIpamPolicyOrganizationTargets", — moved to ipamPolicySupportedOperations
		// "GetIpamPoolAllocations", — moved to advancedNetworkingSupportedOperations
		// "GetIpamPoolCidrs", — moved to advancedNetworkingSupportedOperations
		// "GetIpamPrefixListResolverRules", — moved to ipamDiscoverySupportedOperations
		// "GetIpamPrefixListResolverVersionEntries", — moved to ipamDiscoverySupportedOperations
		// "GetIpamPrefixListResolverVersions", — moved to ipamDiscoverySupportedOperations
		// "GetIpamResourceCidrs", — moved to ipamDiscoverySupportedOperations
		// "GetManagedPrefixListAssociations", — moved to batch4SupportedOperations
		// "GetManagedPrefixListEntries", — moved to batch4SupportedOperations
		"GetNetworkInsightsAccessScopeAnalysisFindings",
		"GetNetworkInsightsAccessScopeContent",
		"GetReservedInstancesExchangeQuote",
		"GetRouteServerAssociations",
		"GetRouteServerPropagations",
		"GetRouteServerRoutingDatabase",
		"GetSpotPlacementScores",
		// "GetTransitGatewayAttachmentPropagations", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayMeteringPolicyEntries", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayMulticastDomainAssociations", — moved to handler_tgw_multicast.go
		// "GetTransitGatewayPolicyTableAssociations", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayPolicyTableEntries", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayPrefixListReferences", — moved to batch4SupportedOperations
		// "GetTransitGatewayRouteTableAssociations", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayRouteTablePropagations", — moved to tgwPeripheralsSupportedOperations
		// "GetVerifiedAccessEndpointPolicy", — moved to handler_verifiedaccess_ext.go
		// "GetVerifiedAccessEndpointTargets", — moved to handler_verifiedaccess_ext.go
		// "GetVerifiedAccessGroupPolicy", — moved to handler_verifiedaccess_ext.go
		"GetVpcResourcesBlockingEncryptionEnforcement",
		// "GetVpnConnectionDeviceSampleConfiguration", — moved to advancedNetworkingSupportedOperations
		// "GetVpnConnectionDeviceTypes", — moved to advancedNetworkingSupportedOperations
		// "GetVpnTunnelReplacementStatus", — moved to advancedNetworkingSupportedOperations
		// ImportClientVpnClientCertificateRevocationList — moved to batch4SupportedOperations
		// "ImportInstance", — moved to vmImportExportSupportedOperations
		// "ImportVolume", — moved to vmImportExportSupportedOperations
		"ModifyAvailabilityZoneGroup",
		"ModifyCapacityReservationFleet",
		// "ModifyClientVpnEndpoint", — moved to batch4SupportedOperations
		"ModifyFleet",
		// "ModifyFpgaImageAttribute", — moved to handler_fpga_image.go
		"ModifyHosts",
		"ModifyInstanceCapacityReservationAttributes",
		"ModifyInstanceCpuOptions",
		"ModifyInstanceEventStartTime",
		"ModifyInstanceMaintenanceOptions",
		"ModifyInstanceNetworkPerformanceOptions",
		"ModifyInstancePlacement",
		// "ModifyIpam", — moved to advancedNetworkingSupportedOperations
		// "ModifyIpamPolicyAllocationRules", — moved to ipamPolicySupportedOperations
		// "ModifyIpamPool", — moved to advancedNetworkingSupportedOperations
		// "ModifyIpamPrefixListResolver", — moved to ipamDiscoverySupportedOperations
		// "ModifyIpamPrefixListResolverTarget", — moved to ipamDiscoverySupportedOperations
		// "ModifyIpamResourceCidr", — moved to ipamDiscoverySupportedOperations
		// "ModifyIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "ModifyIpamScope", — moved to advancedNetworkingSupportedOperations
		// "ModifyLocalGatewayRoute", — moved to localGatewaySupportedOperations
		// "ModifyManagedPrefixList", — moved to batch4SupportedOperations
		"ModifyPrivateDnsNameOptions",
		"ModifyPublicIpDnsNameOptions",
		"ModifyReservedInstances",
		"ModifyRouteServer",
		"ModifyTrafficMirrorFilterNetworkServices",
		"ModifyTrafficMirrorFilterRule",
		"ModifyTrafficMirrorSession",
		// "ModifyTransitGatewayMeteringPolicy", — moved to tgwPeripheralsSupportedOperations
		// "ModifyTransitGatewayPrefixListReference", — moved to tgwPeripheralsSupportedOperations
		// "ModifyTransitGatewayVpcAttachment", — moved to tgwPeripheralsSupportedOperations
		// "ModifyVerifiedAccessEndpoint", — moved to batch4SupportedOperations
		// "ModifyVerifiedAccessEndpointPolicy", — moved to handler_verifiedaccess_ext.go
		"ModifyVerifiedAccessGroup",
		// "ModifyVerifiedAccessGroupPolicy", — moved to handler_verifiedaccess_ext.go
		"ModifyVerifiedAccessInstance",
		// "ModifyVerifiedAccessInstanceLoggingConfiguration", — moved to handler_verifiedaccess_ext.go
		"ModifyVerifiedAccessTrustProvider",
		// "ModifyVpcBlockPublicAccessExclusion", — moved to vpcConfigSupportedOperations
		// "ModifyVpcBlockPublicAccessOptions", — moved to vpcConfigSupportedOperations
		"ModifyVpcEncryptionControl",
		// "ModifyVpcEndpointServiceConfiguration", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnConnectionOptions", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnTunnelCertificate", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnTunnelOptions", — moved to advancedNetworkingSupportedOperations
		"MoveAddressToVpc",
		// "MoveByoipCidrToIpam", — moved to ipamPolicySupportedOperations
		"MoveCapacityReservationInstances",
		"ProvisionByoipCidr",
		// "ProvisionIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "ProvisionIpamPoolCidr", — moved to advancedNetworkingSupportedOperations
		"PurchaseCapacityBlock",
		"PurchaseCapacityBlockExtension",
		"PurchaseHostReservation",
		"PurchaseReservedInstancesOffering",
		// "PurchaseScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		// "RegisterTransitGatewayMulticastGroupMembers", — moved to handler_tgw_multicast.go
		// "RegisterTransitGatewayMulticastGroupSources", — moved to handler_tgw_multicast.go
		"RejectCapacityReservationBillingOwnership",
		// "RejectTransitGatewayMulticastDomainAssociations", — moved to tgwPeripheralsSupportedOperations
		// "RejectTransitGatewayPeeringAttachment", — moved to tgwPeripheralsSupportedOperations
		// "RejectTransitGatewayVpcAttachment", — moved to tgwPeripheralsSupportedOperations
		"RejectVpcEndpointConnections",
		// "RejectVpcPeeringConnection", — moved to advancedNetworkingSupportedOperations
		"ReleaseHosts",
		// "ReleaseIpamPoolAllocation", — moved to advancedNetworkingSupportedOperations
		// ReplaceIamInstanceProfileAssociation — moved to ec2CoreSupportedOperations
		// "ReplaceImageCriteriaInAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// ReplaceRouteTableAssociation — moved to ec2CoreSupportedOperations
		// ReplaceTransitGatewayRoute — moved to ec2CoreSupportedOperations
		"ReplaceVpnTunnel",
		// "ResetFpgaImageAttribute", — moved to handler_fpga_image.go
		// "RestoreManagedPrefixListVersion", — moved to batch4SupportedOperations
		// "RevokeClientVpnIngress", — moved to batch4SupportedOperations
		// "RunScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		// "SearchLocalGatewayRoutes", — moved to localGatewaySupportedOperations
		// "SearchTransitGatewayMulticastGroups", — moved to handler_tgw_multicast.go
		// "SearchTransitGatewayRoutes", — moved to tgwPeripheralsSupportedOperations
		"SendDiagnosticInterrupt",
		"StartDeclarativePoliciesReport",
		"StartNetworkInsightsAccessScopeAnalysis",
		"StartNetworkInsightsAnalysis",
		// "StartVpcEndpointServicePrivateDnsVerification", — moved to advancedNetworkingSupportedOperations
		// "TerminateClientVpnConnections", — moved to batch4SupportedOperations
		"UnassignPrivateNatGatewayAddress",
		"UpdateCapacityManagerOrganizationsAccess",
		"UpdateInterruptibleCapacityReservationAllocation",
		"WithdrawByoipCidr",
		// "CreatePublicIpv4Pool", — moved to ipPoolSupportedOperations()
		// "DeletePublicIpv4Pool", — moved to ipPoolSupportedOperations()
		// "DeprovisionPublicIpv4PoolCidr", — moved to ipPoolSupportedOperations()
		// "DescribeIpv6Pools", — moved to ipPoolSupportedOperations()
		// "DescribePublicIpv4Pools", — moved to ipPoolSupportedOperations()
		// "GetAssociatedIpv6PoolCidrs", — moved to ipPoolSupportedOperations()
		// "ProvisionPublicIpv4PoolCidr", — moved to ipPoolSupportedOperations()
	}
}

func (h *Handler) handleStubAssociateInstanceEventWindow(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateInstanceEventWindowResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCancelDeclarativePoliciesReport(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelDeclarativePoliciesReportResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCancelImageLaunchPermission(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelImageLaunchPermissionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateDelegateMacVolumeOwnershipTask(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateDelegateMacVolumeOwnershipTaskResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateInterruptibleCapacityReservationAllocation(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateInterruptibleCapacityReservationAllocationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateLocalGatewayVirtualInterface(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateLocalGatewayVirtualInterfaceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateLocalGatewayVirtualInterfaceGroup(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateLocalGatewayVirtualInterfaceGroupResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateMacSystemIntegrityProtectionModificationTask(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateMacSystemIntegrityProtectionModificationTaskResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateSecondaryNetwork(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateSecondaryNetworkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateSecondarySubnet(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateSecondarySubnetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateVpcEncryptionControl(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVpcEncryptionControlResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateVpnConcentrator(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVpnConcentratorResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteLocalGatewayVirtualInterface(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteLocalGatewayVirtualInterfaceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteLocalGatewayVirtualInterfaceGroup(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteLocalGatewayVirtualInterfaceGroupResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteSecondaryNetwork(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteSecondaryNetworkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteSecondarySubnet(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteSecondarySubnetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteVpcEncryptionControl(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpcEncryptionControlResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteVpnConcentrator(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpnConcentratorResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeAwsNetworkPerformanceMetricSubscriptions(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeAwsNetworkPerformanceMetricSubscriptionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityReservationTopology(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityReservationTopologyResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeDeclarativePoliciesReports(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeDeclarativePoliciesReportsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeElasticGpus(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeElasticGpusResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeHostReservationOfferings(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeHostReservationOfferingsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeHostReservations(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeHostReservationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeImageReferences(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeImageReferencesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeInstanceSQLHaHistoryStates(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeInstanceSqlHaHistoryStatesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeInstanceSQLHaStates(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeInstanceSqlHaStatesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeMacHosts(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeMacHostsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeMacModificationTasks(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeMacModificationTasksResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeMovingAddresses(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeMovingAddressesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeOutpostLags(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeOutpostLagsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeSecondaryInterfaces(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeSecondaryInterfacesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeSecondaryNetworks(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeSecondaryNetworksResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeSecondarySubnets(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeSecondarySubnetsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeServiceLinkVirtualInterfaces(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeServiceLinkVirtualInterfacesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTransitGatewayAttachments(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTransitGatewayAttachmentsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVpcEncryptionControls(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpcEncryptionControlsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVpnConcentrators(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpnConcentratorsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisableAwsNetworkPerformanceMetricSubscription(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableAwsNetworkPerformanceMetricSubscriptionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisableInstanceSQLHaStandbyDetections(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableInstanceSqlHaStandbyDetectionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisableTransitGatewayRouteTablePropagation(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableTransitGatewayRouteTablePropagationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisassociateInstanceEventWindow(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateInstanceEventWindowResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubEnableAwsNetworkPerformanceMetricSubscription(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableAwsNetworkPerformanceMetricSubscriptionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubEnableInstanceSQLHaStandbyDetections(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableInstanceSqlHaStandbyDetectionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubEnableReachabilityAnalyzerOrganizationSharing(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableReachabilityAnalyzerOrganizationSharingResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubEnableTransitGatewayRouteTablePropagation(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableTransitGatewayRouteTablePropagationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetActiveVpnTunnelStatus(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetActiveVpnTunnelStatusResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetAwsNetworkPerformanceData(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetAwsNetworkPerformanceDataResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetCapacityReservationUsage(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetCapacityReservationUsageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetDeclarativePoliciesReportSummary(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetDeclarativePoliciesReportSummaryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetFlowLogsIntegrationTemplate(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetFlowLogsIntegrationTemplateResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetHostReservationPurchasePreview(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetHostReservationPurchasePreviewResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetImageAncestry(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetImageAncestryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetInstanceTpmEkPub(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetInstanceTpmEkPubResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetInstanceUefiData(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetInstanceUefiDataResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetSpotPlacementScores(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetSpotPlacementScoresResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetVpcResourcesBlockingEncryptionEnforcement(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetVpcResourcesBlockingEncryptionEnforcementResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyAvailabilityZoneGroup(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyAvailabilityZoneGroupResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyHosts(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyHostsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyInstanceCapacityReservationAttributes(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstanceCapacityReservationAttributesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyInstanceCPUOptions(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstanceCpuOptionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyInstanceEventStartTime(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstanceEventStartTimeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyInstanceMaintenanceOptions(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstanceMaintenanceOptionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyInstanceNetworkPerformanceOptions(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstanceNetworkPerformanceOptionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyInstancePlacement(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyInstancePlacementResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyPrivateDNSNameOptions(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyPrivateDnsNameOptionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyPublicIPDNSNameOptions(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyPublicIpDnsNameOptionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVerifiedAccessGroup(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVerifiedAccessGroupResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVerifiedAccessInstance(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVerifiedAccessInstanceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVerifiedAccessTrustProvider(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVerifiedAccessTrustProviderResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVpcEncryptionControl(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcEncryptionControlResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubMoveAddressToVpc(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "MoveAddressToVpcResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubPurchaseHostReservation(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "PurchaseHostReservationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRejectVpcEndpointConnections(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RejectVpcEndpointConnectionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubReleaseHosts(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ReleaseHostsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubReplaceVpnTunnel(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ReplaceVpnTunnelResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubSendDiagnosticInterrupt(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "SendDiagnosticInterruptResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubStartDeclarativePoliciesReport(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "StartDeclarativePoliciesReportResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubUnassignPrivateNatGatewayAddress(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "UnassignPrivateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubUpdateInterruptibleCapacityReservationAllocation(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "UpdateInterruptibleCapacityReservationAllocationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// ---- Additional IPv4/IPv6 stub handlers (SDK naming uses Ipv4/Ipv6) ----
