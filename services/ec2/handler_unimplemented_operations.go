package ec2

import (
	"encoding/xml"
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

func stubSupportedOperations() []string {
	return []string{
		// "AllocateIpamPoolCidr", — moved to advancedNetworkingSupportedOperations
		// "ApplySecurityGroupsToClientVpnTargetNetwork", — moved to batch4SupportedOperations
		"AssociateCapacityReservationBillingOwner",
		// "AssociateClientVpnTargetNetwork", — moved to batch4SupportedOperations
		// "AssociateEnclaveCertificateIamRole", — moved to trunkEnclaveSupportedOperations
		// AssociateIamInstanceProfile — now in ec2CoreSupportedOperations
		// "AssociateInstanceEventWindow", — moved to instanceAttrSupportedOperations
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
		// "CancelDeclarativePoliciesReport", — real handler in handler_declarative_policies.go, covered there
		// "CancelExportTask", — moved to vmImportExportSupportedOperations
		// "CancelImageLaunchPermission", — moved to parityFinalSupportedOperations
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
		// "CreateDelegateMacVolumeOwnershipTask", — moved to macHostSupportedOperations
		// CreateEgressOnlyInternetGateway — moved to ec2CoreSupportedOperations
		// "CreateFleet", — already listed in the core GetSupportedOperations list;
		// real implementation in handler_batch5.go
		// "CreateFpgaImage", — moved to handler_fpga_image.go
		// "CreateImageUsageReport", — moved to imageOpsSupportedOperations()
		// "CreateInstanceExportTask", — moved to vmImportExportSupportedOperations
		// "CreateInterruptibleCapacityReservationAllocation", — moved to parityFinalSupportedOperations
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
		// "CreateLocalGatewayVirtualInterface", — real handler in handler_local_gateway.go, covered there
		// "CreateLocalGatewayVirtualInterfaceGroup", — real handler in handler_local_gateway.go, covered there
		// "CreateMacSystemIntegrityProtectionModificationTask", — moved to macHostSupportedOperations
		// "CreateManagedPrefixList", — moved to batch4SupportedOperations
		"CreateNetworkInsightsAccessScope",
		"CreateNetworkInsightsPath",
		"CreateReservedInstancesListing",
		// "CreateRestoreImageTask", — moved to imageOpsSupportedOperations()
		"CreateRouteServer",
		"CreateRouteServerEndpoint",
		"CreateRouteServerPeer",
		// "CreateSecondaryNetwork", — moved to secondaryNetSupportedOperations
		// "CreateSecondarySubnet", — moved to secondaryNetSupportedOperations
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
		// "CreateVpcEncryptionControl", — real handler in handler_vpc_encryption_control.go, covered there
		// "CreateVpcEndpointServiceConfiguration", — moved to advancedNetworkingSupportedOperations
		// "CreateVpnConcentrator", — real handler in handler_vpn_concentrator.go, covered there
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
		// "DeleteLocalGatewayVirtualInterface", — real handler in handler_local_gateway.go, covered there
		// "DeleteLocalGatewayVirtualInterfaceGroup", — real handler in handler_local_gateway.go, covered there
		// "DeleteManagedPrefixList", — moved to batch4SupportedOperations
		"DeleteNetworkInsightsAccessScope",
		"DeleteNetworkInsightsAccessScopeAnalysis",
		"DeleteNetworkInsightsAnalysis",
		"DeleteNetworkInsightsPath",
		"DeleteQueuedReservedInstances",
		"DeleteRouteServer",
		"DeleteRouteServerEndpoint",
		"DeleteRouteServerPeer",
		// "DeleteSecondaryNetwork", — moved to secondaryNetSupportedOperations
		// "DeleteSecondarySubnet", — moved to secondaryNetSupportedOperations
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
		// "DeleteVpcEncryptionControl", — real handler in handler_vpc_encryption_control.go, covered there
		// "DeleteVpcEndpointServiceConfigurations", — moved to advancedNetworkingSupportedOperations
		// "DeleteVpnConcentrator", — real handler in handler_vpn_concentrator.go, covered there
		// "DeleteVpnConnection", — moved to advancedNetworkingSupportedOperations
		// "DeleteVpnGateway", — moved to advancedNetworkingSupportedOperations
		"DeprovisionByoipCidr",
		// "DeprovisionIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DeprovisionIpamPoolCidr", — moved to advancedNetworkingSupportedOperations
		// "DeregisterTransitGatewayMulticastGroupMembers", — moved to handler_tgw_multicast.go
		// "DeregisterTransitGatewayMulticastGroupSources", — moved to handler_tgw_multicast.go
		// "DescribeAwsNetworkPerformanceMetricSubscriptions", — real handler in handler_network_performance.go, covered there
		// "DescribeBundleTasks", — moved to vmImportExportSupportedOperations
		"DescribeCapacityBlockExtensionHistory",
		"DescribeCapacityBlockExtensionOfferings",
		"DescribeCapacityBlockOfferings",
		"DescribeCapacityBlockStatus",
		"DescribeCapacityBlocks",
		"DescribeCapacityManagerDataExports",
		"DescribeCapacityReservationBillingRequests",
		"DescribeCapacityReservationFleets",
		// "DescribeCapacityReservationTopology", — moved to parityFinalSupportedOperations
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
		// "DescribeDeclarativePoliciesReports", — real handler in handler_declarative_policies.go, covered there
		// DescribeEgressOnlyInternetGateways — moved to ec2CoreSupportedOperations
		// "DescribeElasticGpus", — moved to parityFinalSupportedOperations
		// "DescribeExportImageTasks", — moved to batch3SupportedOperations
		// "DescribeExportTasks", — moved to vmImportExportSupportedOperations
		"DescribeFleetHistory",
		"DescribeFleetInstances",
		"DescribeFleets",
		// "DescribeFpgaImageAttribute", — moved to handler_fpga_image.go
		// "DescribeFpgaImages", — moved to handler_fpga_image.go
		// "DescribeHostReservationOfferings", — real handler in handler_host_reservations.go, covered there
		// "DescribeHostReservations", — real handler in handler_host_reservations.go, covered there
		// DescribeIamInstanceProfileAssociations — moved to ec2CoreSupportedOperations
		// "DescribeImageReferences", — moved to parityFinalSupportedOperations
		// "DescribeImageUsageReportEntries", — moved to imageOpsSupportedOperations()
		// "DescribeInstanceSqlHaHistoryStates", — moved to sqlHaSupportedOperations
		// "DescribeInstanceSqlHaStates", — moved to sqlHaSupportedOperations
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
		// "DescribeMacHosts", — moved to macHostSupportedOperations
		// "DescribeMacModificationTasks", — moved to macHostSupportedOperations
		// "DescribeManagedPrefixLists", — moved to batch4SupportedOperations
		// "DescribeMovingAddresses", — moved to parityFinalSupportedOperations
		"DescribeNetworkInsightsAccessScopeAnalyses",
		"DescribeNetworkInsightsAccessScopes",
		"DescribeNetworkInsightsAnalyses",
		"DescribeNetworkInsightsPaths",
		// "DescribeOutpostLags", — moved to secondaryNetSupportedOperations
		"DescribeReservedInstances",
		"DescribeReservedInstancesListings",
		"DescribeReservedInstancesModifications",
		"DescribeReservedInstancesOfferings",
		"DescribeRouteServerEndpoints",
		"DescribeRouteServerPeers",
		"DescribeRouteServers",
		// "DescribeScheduledInstanceAvailability", — moved to scheduledInstanceSupportedOperations()
		// "DescribeScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		// "DescribeSecondaryInterfaces", — moved to secondaryNetSupportedOperations
		// "DescribeSecondaryNetworks", — moved to secondaryNetSupportedOperations
		// "DescribeSecondarySubnets", — moved to secondaryNetSupportedOperations
		// "DescribeServiceLinkVirtualInterfaces", — moved to secondaryNetSupportedOperations
		// "DescribeStoreImageTasks", — moved to imageOpsSupportedOperations()
		"DescribeTrafficMirrorFilterRules",
		"DescribeTrafficMirrorFilters",
		"DescribeTrafficMirrorSessions",
		"DescribeTrafficMirrorTargets",
		// "DescribeTransitGatewayAttachments", — moved to parityFinalSupportedOperations
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
		// "DescribeVerifiedAccessInstanceLoggingConfigurations", — moved to handler_verified_access_policy.go
		// "DescribeVerifiedAccessInstances", — moved to batch4SupportedOperations
		// "DescribeVerifiedAccessTrustProviders", — moved to batch4SupportedOperations
		// "DescribeVpcBlockPublicAccessExclusions", — moved to vpcConfigSupportedOperations
		// "DescribeVpcBlockPublicAccessOptions", — moved to vpcConfigSupportedOperations
		// "DescribeVpcClassicLink", — moved to vpcConfigSupportedOperations
		// "DescribeVpcClassicLinkDnsSupport", — moved to vpcConfigSupportedOperations
		// "DescribeVpcEncryptionControls", — real handler in handler_vpc_encryption_control.go, covered there
		// "DescribeVpcEndpointServiceConfigurations", — moved to advancedNetworkingSupportedOperations
		// "DescribeVpnConcentrators", — real handler in handler_vpn_concentrator.go, covered there
		// "DescribeVpnConnections", — moved to advancedNetworkingSupportedOperations
		// "DescribeVpnGateways", — moved to advancedNetworkingSupportedOperations
		// "DetachClassicLinkVpc", — moved to vpcConfigSupportedOperations
		// "DetachVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		// "DetachVpnGateway", — moved to advancedNetworkingSupportedOperations
		// "DisableAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// "DisableAwsNetworkPerformanceMetricSubscription", — real handler in handler_network_performance.go, covered there
		"DisableCapacityManager",
		// "DisableInstanceSqlHaStandbyDetections", — moved to sqlHaSupportedOperations
		// "DisableIpamOrganizationAdminAccount", — moved to ipamPolicySupportedOperations
		// "DisableIpamPolicy", — moved to ipamPolicySupportedOperations
		"DisableRouteServerPropagation",
		// "DisableTransitGatewayRouteTablePropagation", — moved to parityFinalSupportedOperations
		// "DisableVpcClassicLink", — moved to vpcConfigSupportedOperations
		// "DisableVpcClassicLinkDnsSupport", — moved to vpcConfigSupportedOperations
		"DisassociateCapacityReservationBillingOwner",
		// "DisassociateClientVpnTargetNetwork", — moved to batch4SupportedOperations
		// "DisassociateEnclaveCertificateIamRole", — moved to trunkEnclaveSupportedOperations
		// DisassociateIamInstanceProfile — moved to ec2CoreSupportedOperations
		// "DisassociateInstanceEventWindow", — moved to instanceAttrSupportedOperations
		// "DisassociateIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DisassociateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		"DisassociateRouteServer",
		// "DisassociateTransitGatewayMulticastDomain", — moved to handler_tgw_multicast.go
		// "DisassociateTransitGatewayPolicyTable", — moved to tgwPeripheralsSupportedOperations
		// DisassociateTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		// "DisassociateTrunkInterface", — moved to trunkEnclaveSupportedOperations
		// "EnableAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// "EnableAwsNetworkPerformanceMetricSubscription", — real handler in handler_network_performance.go, covered there
		"EnableCapacityManager",
		// "EnableInstanceSqlHaStandbyDetections", — moved to sqlHaSupportedOperations
		// "EnableIpamOrganizationAdminAccount", — moved to ipamPolicySupportedOperations
		// "EnableIpamPolicy", — moved to ipamPolicySupportedOperations
		// "EnableReachabilityAnalyzerOrganizationSharing", — moved to parityFinalSupportedOperations
		"EnableRouteServerPropagation",
		// "EnableTransitGatewayRouteTablePropagation", — moved to parityFinalSupportedOperations
		// "EnableVpcClassicLink", — moved to vpcConfigSupportedOperations
		// "EnableVpcClassicLinkDnsSupport", — moved to vpcConfigSupportedOperations
		// ExportClientVpnClientCertificateRevocationList — moved to batch4SupportedOperations
		// ExportClientVpnClientConfiguration — moved to batch4SupportedOperations
		// "ExportTransitGatewayRoutes", — moved to tgwPeripheralsSupportedOperations
		// "ExportVerifiedAccessInstanceClientConfiguration", — moved to handler_verified_access_policy.go
		// "GetActiveVpnTunnelStatus", — real handler in handler_vpn_concentrator.go, covered there
		// "GetAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// "GetAssociatedEnclaveCertificateIamRoles", — moved to trunkEnclaveSupportedOperations
		// "GetAwsNetworkPerformanceData", — real handler in handler_network_performance.go, covered there
		"GetCapacityManagerAttributes",
		"GetCapacityManagerMetricData",
		"GetCapacityManagerMetricDimensions",
		// "GetCapacityReservationUsage", — moved to parityFinalSupportedOperations
		// "GetCoipPoolUsage", — moved to ipPoolSupportedOperations()
		// "GetDeclarativePoliciesReportSummary", — real handler in handler_declarative_policies.go, covered there
		// "GetEnabledIpamPolicy", — moved to ipamPolicySupportedOperations
		// "GetFlowLogsIntegrationTemplate", — moved to parityFinalSupportedOperations
		// "GetHostReservationPurchasePreview", — real handler in handler_host_reservations.go, covered there
		// "GetImageAncestry", — moved to parityFinalSupportedOperations
		// "GetInstanceTpmEkPub", — moved to instanceAttrSupportedOperations
		// "GetInstanceUefiData", — moved to instanceAttrSupportedOperations
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
		// "GetSpotPlacementScores", — moved to parityFinalSupportedOperations
		// "GetTransitGatewayAttachmentPropagations", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayMeteringPolicyEntries", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayMulticastDomainAssociations", — moved to handler_tgw_multicast.go
		// "GetTransitGatewayPolicyTableAssociations", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayPolicyTableEntries", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayPrefixListReferences", — moved to batch4SupportedOperations
		// "GetTransitGatewayRouteTableAssociations", — moved to tgwPeripheralsSupportedOperations
		// "GetTransitGatewayRouteTablePropagations", — moved to tgwPeripheralsSupportedOperations
		// "GetVerifiedAccessEndpointPolicy", — moved to handler_verified_access_policy.go
		// "GetVerifiedAccessEndpointTargets", — moved to handler_verified_access_policy.go
		// "GetVerifiedAccessGroupPolicy", — moved to handler_verified_access_policy.go
		// "GetVpcResourcesBlockingEncryptionEnforcement", — real handler in handler_vpc_encryption_control.go, covered there
		// "GetVpnConnectionDeviceSampleConfiguration", — moved to advancedNetworkingSupportedOperations
		// "GetVpnConnectionDeviceTypes", — moved to advancedNetworkingSupportedOperations
		// "GetVpnTunnelReplacementStatus", — moved to advancedNetworkingSupportedOperations
		// ImportClientVpnClientCertificateRevocationList — moved to batch4SupportedOperations
		// "ImportInstance", — moved to vmImportExportSupportedOperations
		// "ImportVolume", — moved to vmImportExportSupportedOperations
		// "ModifyAvailabilityZoneGroup", — moved to instanceAttrSupportedOperations
		"ModifyCapacityReservationFleet",
		// "ModifyClientVpnEndpoint", — moved to batch4SupportedOperations
		"ModifyFleet",
		// "ModifyFpgaImageAttribute", — moved to handler_fpga_image.go
		// "ModifyHosts", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceCapacityReservationAttributes", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceCpuOptions", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceEventStartTime", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceMaintenanceOptions", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceNetworkPerformanceOptions", — moved to instanceAttrSupportedOperations
		// "ModifyInstancePlacement", — moved to instanceAttrSupportedOperations
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
		// "ModifyPrivateDnsNameOptions", — moved to instanceAttrSupportedOperations
		// "ModifyPublicIpDnsNameOptions", — moved to instanceAttrSupportedOperations
		"ModifyReservedInstances",
		"ModifyRouteServer",
		"ModifyTrafficMirrorFilterNetworkServices",
		"ModifyTrafficMirrorFilterRule",
		"ModifyTrafficMirrorSession",
		// "ModifyTransitGatewayMeteringPolicy", — moved to tgwPeripheralsSupportedOperations
		// "ModifyTransitGatewayPrefixListReference", — moved to tgwPeripheralsSupportedOperations
		// "ModifyTransitGatewayVpcAttachment", — moved to tgwPeripheralsSupportedOperations
		// "ModifyVerifiedAccessEndpoint", — moved to batch4SupportedOperations
		// "ModifyVerifiedAccessEndpointPolicy", — moved to handler_verified_access_policy.go
		// "ModifyVerifiedAccessGroup", — moved to parityFinalSupportedOperations
		// "ModifyVerifiedAccessGroupPolicy", — moved to handler_verified_access_policy.go
		// "ModifyVerifiedAccessInstance", — moved to parityFinalSupportedOperations
		// "ModifyVerifiedAccessInstanceLoggingConfiguration", — moved to handler_verified_access_policy.go
		// "ModifyVerifiedAccessTrustProvider", — moved to parityFinalSupportedOperations
		// "ModifyVpcBlockPublicAccessExclusion", — moved to vpcConfigSupportedOperations
		// "ModifyVpcBlockPublicAccessOptions", — moved to vpcConfigSupportedOperations
		// "ModifyVpcEncryptionControl", — real handler in handler_vpc_encryption_control.go, covered there
		// "ModifyVpcEndpointServiceConfiguration", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnConnectionOptions", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnTunnelCertificate", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnTunnelOptions", — moved to advancedNetworkingSupportedOperations
		// "MoveAddressToVpc", — moved to parityFinalSupportedOperations
		// "MoveByoipCidrToIpam", — moved to ipamPolicySupportedOperations
		"MoveCapacityReservationInstances",
		"ProvisionByoipCidr",
		// "ProvisionIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "ProvisionIpamPoolCidr", — moved to advancedNetworkingSupportedOperations
		"PurchaseCapacityBlock",
		"PurchaseCapacityBlockExtension",
		// "PurchaseHostReservation", — real handler in handler_host_reservations.go, covered there
		"PurchaseReservedInstancesOffering",
		// "PurchaseScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		// "RegisterTransitGatewayMulticastGroupMembers", — moved to handler_tgw_multicast.go
		// "RegisterTransitGatewayMulticastGroupSources", — moved to handler_tgw_multicast.go
		"RejectCapacityReservationBillingOwnership",
		// "RejectTransitGatewayMulticastDomainAssociations", — moved to tgwPeripheralsSupportedOperations
		// "RejectTransitGatewayPeeringAttachment", — moved to tgwPeripheralsSupportedOperations
		// "RejectTransitGatewayVpcAttachment", — moved to tgwPeripheralsSupportedOperations
		// "RejectVpcEndpointConnections", — moved to parityFinalSupportedOperations
		// "RejectVpcPeeringConnection", — moved to advancedNetworkingSupportedOperations
		// "ReleaseHosts", — real handler in handler_host_reservations.go, covered there
		// "ReleaseIpamPoolAllocation", — moved to advancedNetworkingSupportedOperations
		// ReplaceIamInstanceProfileAssociation — moved to ec2CoreSupportedOperations
		// "ReplaceImageCriteriaInAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// ReplaceRouteTableAssociation — moved to ec2CoreSupportedOperations
		// ReplaceTransitGatewayRoute — moved to ec2CoreSupportedOperations
		// "ReplaceVpnTunnel", — real handler in handler_vpn_concentrator.go, covered there
		// "ResetFpgaImageAttribute", — moved to handler_fpga_image.go
		// "RestoreManagedPrefixListVersion", — moved to batch4SupportedOperations
		// "RevokeClientVpnIngress", — moved to batch4SupportedOperations
		// "RunScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		// "SearchLocalGatewayRoutes", — moved to localGatewaySupportedOperations
		// "SearchTransitGatewayMulticastGroups", — moved to handler_tgw_multicast.go
		// "SearchTransitGatewayRoutes", — moved to tgwPeripheralsSupportedOperations
		// "SendDiagnosticInterrupt", — moved to parityFinalSupportedOperations
		// "StartDeclarativePoliciesReport", — real handler in handler_declarative_policies.go, covered there
		"StartNetworkInsightsAccessScopeAnalysis",
		"StartNetworkInsightsAnalysis",
		// "StartVpcEndpointServicePrivateDnsVerification", — moved to advancedNetworkingSupportedOperations
		// "TerminateClientVpnConnections", — moved to batch4SupportedOperations
		// "UnassignPrivateNatGatewayAddress", — moved to parityFinalSupportedOperations
		"UpdateCapacityManagerOrganizationsAccess",
		// "UpdateInterruptibleCapacityReservationAllocation", — moved to parityFinalSupportedOperations
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
