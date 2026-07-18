package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStubOperations is a smoke test verifying the listed EC2 operations
// return 200 OK through the HTTP handler dispatch table. registerStubOps was
// retired once the final EC2 parity sweep (gopherstack-5o9) de-stubbed the
// last operations it registered (see backend_parity_final.go /
// handler_parity_final.go) — the names below are real handlers registered
// elsewhere, kept here as a broad dispatch smoke test.
func TestStubOperations(t *testing.T) {
	t.Parallel()

	stubOps := []string{
		// "ApplySecurityGroupsToClientVpnTargetNetwork", — moved to batch4
		// "AssociateCapacityReservationBillingOwner", — moved to handler_capacity_family.go
		// "AssociateClientVpnTargetNetwork", — moved to batch4
		// "AssociateEnclaveCertificateIamRole", — moved to trunkEnclaveSupportedOperations
		// "AssociateInstanceEventWindow", — moved to instanceAttrSupportedOperations
		// "AssociateIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "AssociateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "AssociateRouteServer", — real handler in handler_route_server.go, covered there
		// "AssociateTransitGatewayMulticastDomain", — real handler in handler_tgw_multicast.go, covered there
		// "AssociateTransitGatewayPolicyTable", — moved to handler_tgw_peripherals.go
		// "AssociateTrunkInterface", — moved to trunkEnclaveSupportedOperations
		// "AttachVerifiedAccessTrustProvider", — moved to batch4
		// "AuthorizeClientVpnIngress", — moved to batch4
		// "BundleInstance", — moved to vmImportExportSupportedOperations
		// "CancelBundleTask", — moved to vmImportExportSupportedOperations
		// "CancelCapacityReservationFleets", — moved to handler_capacity_family.go
		// "CancelConversionTask", — moved to vmImportExportSupportedOperations
		// "CancelDeclarativePoliciesReport", — real handler in handler_declarative_policies.go, covered there
		// "CancelExportTask", — moved to vmImportExportSupportedOperations
		// "CancelImportTask", — moved to vmImportExportSupportedOperations
		// "CancelReservedInstancesListing", — real handler in handler_batch5.go, covered there
		// "ConfirmProductInstance", — moved to imageOpsSupportedOperations()
		// "CopyFpgaImage", — moved to handler_fpga_image.go
		// "CreateCapacityManagerDataExport", — moved to handler_capacity_family.go
		// "CreateCapacityReservationBySplitting", — moved to handler_capacity_family.go
		// "CreateCapacityReservationFleet", — moved to handler_capacity_family.go
		// "CreateCarrierGateway", — real handler in handler_batch5.go, covered there
		// "CreateClientVpnEndpoint", — moved to batch4
		// "CreateClientVpnRoute", — moved to batch4
		// "CreateCoipCidr", — moved to ipPoolSupportedOperations()
		// "CreateCoipPool", — moved to ipPoolSupportedOperations()
		// "CreateDelegateMacVolumeOwnershipTask", — moved to macHostSupportedOperations
		// "CreateFleet", — real handler in handler_batch5.go, covered there
		// "CreateFpgaImage", — moved to handler_fpga_image.go
		// "CreateImageUsageReport", — moved to imageOpsSupportedOperations()
		// "CreateInstanceExportTask", — moved to vmImportExportSupportedOperations
		"CreateIpam",
		// "CreateIpamExternalResourceVerificationToken", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamPolicy", — moved to ipamPolicySupportedOperations (requires params)
		// "CreateIpamPrefixListResolver", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamPrefixListResolverTarget", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "CreateLocalGatewayVirtualInterface", — real handler in handler_local_gateway.go, covered there
		// "CreateLocalGatewayVirtualInterfaceGroup", — real handler in handler_local_gateway.go, covered there
		// "CreateMacSystemIntegrityProtectionModificationTask", — moved to macHostSupportedOperations
		// "CreateManagedPrefixList", — moved to batch4
		// "CreateNetworkInsightsAccessScope", — real handler in handler_batch5.go, covered there
		// "CreateNetworkInsightsPath", — real handler in handler_batch5.go, covered there
		// "CreateReservedInstancesListing", — real handler in handler_batch5.go, covered there
		// "CreateRestoreImageTask", — moved to imageOpsSupportedOperations()
		// "CreateRouteServer", — real handler in handler_route_server.go, covered there
		// "CreateRouteServerEndpoint", — real handler in handler_route_server.go, covered there
		// "CreateRouteServerPeer", — real handler in handler_route_server.go, covered there
		// "CreateSecondaryNetwork", — moved to secondaryNetSupportedOperations
		// "CreateSecondarySubnet", — moved to secondaryNetSupportedOperations
		// "CreateStoreImageTask", — moved to imageOpsSupportedOperations()
		// "CreateTrafficMirrorFilter", — real handler in handler_batch5.go, covered there
		// "CreateTrafficMirrorFilterRule", — real handler now requires an existing filter ID
		// "CreateTrafficMirrorSession", — real handler in handler_batch5.go, covered there
		// "CreateTrafficMirrorTarget", — real handler in handler_batch5.go, covered there
		// "CreateTransitGatewayConnect", — moved to batch4
		// "CreateTransitGatewayConnectPeer", — moved to batch4
		// "CreateTransitGatewayMeteringPolicy", — real handler in handler_tgw_multicast.go, covered there
		// "CreateTransitGatewayMeteringPolicyEntry", — real handler in handler_tgw_multicast.go, covered there
		// "CreateTransitGatewayMulticastDomain", — real handler in handler_tgw_multicast.go, covered there
		// "CreateTransitGatewayPeeringAttachment", — moved to batch4
		// "CreateTransitGatewayPolicyTable", — moved to handler_tgw_peripherals.go
		// "CreateTransitGatewayPrefixListReference", — moved to batch4
		// "CreateTransitGatewayRouteTableAnnouncement", — moved to handler_tgw_peripherals.go
		// "CreateVerifiedAccessEndpoint", — moved to batch4
		// "CreateVerifiedAccessGroup", — moved to batch4
		// "CreateVerifiedAccessInstance", — moved to batch4
		// "CreateVerifiedAccessTrustProvider", — moved to batch4
		// "CreateVpcEncryptionControl", — real handler in handler_vpc_encryption_control.go, covered there
		// "CreateVpcEndpointServiceConfiguration", — real handler in handler_advanced_networking.go, covered there
		// "CreateVpnConcentrator", — real handler in handler_vpn_concentrator.go, covered there
		// "CreateVpnGateway", — moved to advancedNetworkingSupportedOperations
		// "DeleteCapacityManagerDataExport", — moved to handler_capacity_family.go
		// "DeleteCarrierGateway", — real handler in handler_batch5.go, covered there
		// "DeleteClientVpnEndpoint", — moved to batch4
		// "DeleteClientVpnRoute", — moved to batch4
		// "DeleteCoipCidr", — moved to ipPoolSupportedOperations()
		// "DeleteCoipPool", — moved to ipPoolSupportedOperations()
		// "DeleteFleets", — real handler in handler_batch5.go, covered there
		// "DeleteFpgaImage", — moved to handler_fpga_image.go
		// "DeleteImageUsageReport", — moved to imageOpsSupportedOperations()
		// "DeleteIpamExternalResourceVerificationToken", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamPolicy", — moved to ipamPolicySupportedOperations (requires params)
		// "DeleteIpamPrefixListResolver", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamPrefixListResolverTarget", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "DeleteLocalGatewayVirtualInterface", — real handler in handler_local_gateway.go, covered there
		// "DeleteLocalGatewayVirtualInterfaceGroup", — real handler in handler_local_gateway.go, covered there
		// "DeleteManagedPrefixList", — moved to batch4
		// "DeleteNetworkInsightsAccessScope", — real handler in handler_batch5.go, covered there
		// "DeleteNetworkInsightsAccessScopeAnalysis", — real handler in handler_batch5.go, covered there
		// "DeleteNetworkInsightsAnalysis", — real handler in handler_batch5.go, covered there
		// "DeleteNetworkInsightsPath", — real handler in handler_batch5.go, covered there
		// "DeleteQueuedReservedInstances", — real handler in handler_batch5.go, covered there
		// "DeleteRouteServer", — real handler in handler_route_server.go, covered there
		// "DeleteRouteServerEndpoint", — real handler in handler_route_server.go, covered there
		// "DeleteRouteServerPeer", — real handler in handler_route_server.go, covered there
		// "DeleteSecondaryNetwork", — moved to secondaryNetSupportedOperations
		// "DeleteSecondarySubnet", — moved to secondaryNetSupportedOperations
		// "DeleteTrafficMirrorFilter", — real handler now requires an existing filter ID
		// "DeleteTrafficMirrorFilterRule", — real handler now requires an existing rule ID
		// "DeleteTrafficMirrorSession", — real handler now requires an existing session ID
		// "DeleteTrafficMirrorTarget", — real handler now requires an existing target ID
		// "DeleteTransitGatewayConnect", — moved to batch4
		// "DeleteTransitGatewayConnectPeer", — moved to batch4
		// "DeleteTransitGatewayMeteringPolicy", — real handler in handler_tgw_multicast.go, covered there
		// "DeleteTransitGatewayMeteringPolicyEntry", — real handler in handler_tgw_multicast.go, covered there
		// "DeleteTransitGatewayMulticastDomain", — real handler in handler_tgw_multicast.go, covered there
		// "DeleteTransitGatewayPeeringAttachment", — moved to batch4
		// "DeleteTransitGatewayPolicyTable", — moved to handler_tgw_peripherals.go
		// "DeleteTransitGatewayPrefixListReference", — moved to batch4
		// "DeleteTransitGatewayRouteTableAnnouncement", — moved to handler_tgw_peripherals.go
		// "DeleteVerifiedAccessEndpoint", — moved to batch4
		// "DeleteVerifiedAccessGroup", — moved to batch4
		// "DeleteVerifiedAccessInstance", — moved to batch4
		// "DeleteVerifiedAccessTrustProvider", — moved to batch4
		// "DeleteVpcEncryptionControl", — real handler in handler_vpc_encryption_control.go, covered there
		// "DeleteVpcEndpointServiceConfigurations", — real handler in handler_advanced_networking.go, covered there
		// "DeleteVpnConcentrator", — real handler in handler_vpn_concentrator.go, covered there
		// "DeprovisionByoipCidr", — real handler in handler_batch5.go, covered there
		// "DeprovisionIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DeregisterTransitGatewayMulticastGroupMembers", — real handler in handler_tgw_multicast.go, covered there
		// "DeregisterTransitGatewayMulticastGroupSources", — real handler in handler_tgw_multicast.go, covered there
		// "DescribeAwsNetworkPerformanceMetricSubscriptions", — real handler in handler_network_performance.go, covered there
		// "DescribeBundleTasks", — moved to vmImportExportSupportedOperations
		// "DescribeCapacityBlockExtensionHistory", — moved to handler_capacity_family.go
		// "DescribeCapacityBlockExtensionOfferings", — moved to handler_capacity_family.go
		// "DescribeCapacityBlockOfferings", — moved to handler_capacity_family.go
		// "DescribeCapacityBlockStatus", — moved to handler_capacity_family.go
		// "DescribeCapacityBlocks", — moved to handler_capacity_family.go
		// "DescribeCapacityManagerDataExports", — moved to handler_capacity_family.go
		// "DescribeCapacityReservationBillingRequests", — moved to handler_capacity_family.go
		// "DescribeCapacityReservationFleets", — moved to handler_capacity_family.go
		// "DescribeCarrierGateways", — real handler in handler_batch5.go, covered there
		// "DescribeClientVpnAuthorizationRules", — moved to batch4
		// "DescribeClientVpnConnections", — moved to batch4
		// "DescribeClientVpnEndpoints", — moved to batch4
		// "DescribeClientVpnRoutes", — moved to batch4
		// "DescribeClientVpnTargetNetworks", — moved to batch4
		// "DescribeCoipPools", — moved to ipPoolSupportedOperations()
		// "DescribeConversionTasks", — moved to vmImportExportSupportedOperations
		// "DescribeCustomerGateways", — moved to advancedNetworkingSupportedOperations
		// "DescribeDeclarativePoliciesReports", — real handler in handler_declarative_policies.go, covered there
		// "DescribeExportImageTasks", — moved to batch3SupportedOperations
		// "DescribeExportTasks", — moved to vmImportExportSupportedOperations
		// "DescribeFleetHistory", — real handler in handler_batch5.go, covered there
		// "DescribeFleetInstances", — real handler in handler_batch5.go, covered there
		// "DescribeFleets", — real handler in handler_batch5.go, covered there
		// "DescribeFpgaImageAttribute", — moved to handler_fpga_image.go
		// "DescribeFpgaImages", — moved to handler_fpga_image.go
		// "DescribeHostReservationOfferings", — real handler in handler_host_reservations.go, covered there
		// "DescribeHostReservations", — real handler in handler_host_reservations.go, covered there
		// "DescribeImageUsageReportEntries", — moved to imageOpsSupportedOperations()
		// "DescribeIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DescribeIpamExternalResourceVerificationTokens", — moved to ipamDiscoverySupportedOperations
		"DescribeIpamPolicies",
		"DescribeIpamPools",
		// "DescribeIpamPrefixListResolverTargets", — moved to ipamDiscoverySupportedOperations
		// "DescribeIpamPrefixListResolvers", — moved to ipamDiscoverySupportedOperations
		"DescribeIpamResourceDiscoveries",
		"DescribeIpamResourceDiscoveryAssociations",
		"DescribeIpamScopes",
		"DescribeIpams",
		// "DescribeMacHosts", — moved to macHostSupportedOperations
		// "DescribeMacModificationTasks", — moved to macHostSupportedOperations
		// "DescribeManagedPrefixLists", — moved to batch4
		// "DescribeNetworkInsightsAccessScopeAnalyses", — real handler in handler_batch5.go, covered there
		// "DescribeNetworkInsightsAccessScopes", — real handler in handler_batch5.go, covered there
		// "DescribeNetworkInsightsAnalyses", — real handler in handler_batch5.go, covered there
		// "DescribeNetworkInsightsPaths", — real handler in handler_batch5.go, covered there
		// "DescribeOutpostLags", — moved to secondaryNetSupportedOperations
		// "DescribeReservedInstances", — real handler in handler_batch5.go, covered there
		// "DescribeReservedInstancesListings", — real handler in handler_batch5.go, covered there
		// "DescribeReservedInstancesModifications", — real handler in handler_batch5.go, covered there
		// "DescribeReservedInstancesOfferings", — real handler in handler_batch5.go, covered there
		// "DescribeRouteServerEndpoints", — real handler in handler_route_server.go, covered there
		// "DescribeRouteServerPeers", — real handler in handler_route_server.go, covered there
		// "DescribeRouteServers", — real handler in handler_route_server.go, covered there
		// "DescribeScheduledInstanceAvailability", — moved to scheduledInstanceSupportedOperations()
		// "DescribeScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		// "DescribeSecondaryInterfaces", — moved to secondaryNetSupportedOperations
		// "DescribeSecondaryNetworks", — moved to secondaryNetSupportedOperations
		// "DescribeSecondarySubnets", — moved to secondaryNetSupportedOperations
		// "DescribeServiceLinkVirtualInterfaces", — moved to secondaryNetSupportedOperations
		// "DescribeSpotFleetRequests", — spot fleet handler in handler_spot_fleet.go, covered there
		// "DescribeStoreImageTasks", — moved to imageOpsSupportedOperations()
		// "DescribeTrafficMirrorFilterRules", — real handler now requires an existing filter ID
		// "DescribeTrafficMirrorFilters", — real handler in handler_batch5.go, covered there
		// "DescribeTrafficMirrorSessions", — real handler in handler_batch5.go, covered there
		// "DescribeTrafficMirrorTargets", — real handler in handler_batch5.go, covered there
		// "DescribeTransitGatewayConnectPeers", — moved to batch4
		// "DescribeTransitGatewayConnects", — moved to batch4
		// "DescribeTransitGatewayMeteringPolicies", — real handler in handler_tgw_multicast.go, covered there
		// "DescribeTransitGatewayMulticastDomains", — real handler in handler_tgw_multicast.go, covered there
		// "DescribeTransitGatewayPeeringAttachments", — moved to batch4
		// "DescribeTransitGatewayPolicyTables", — moved to handler_tgw_peripherals.go
		// "DescribeTransitGatewayRouteTableAnnouncements", — moved to handler_tgw_peripherals.go
		// "DescribeTrunkInterfaceAssociations", — moved to trunkEnclaveSupportedOperations
		// "DescribeVerifiedAccessEndpoints", — moved to batch4
		// "DescribeVerifiedAccessGroups", — moved to batch4
		// "DescribeVerifiedAccessInstanceLoggingConfigurations", — moved to handler_verifiedaccess_ext.go
		// "DescribeVerifiedAccessInstances", — moved to batch4
		// "DescribeVerifiedAccessTrustProviders", — moved to batch4
		// "DescribeVpcEncryptionControls", — real handler in handler_vpc_encryption_control.go, covered there
		// "DescribeVpcEndpointServiceConfigurations", — real handler in handler_advanced_networking.go, covered there
		// "DescribeVpnConcentrators", — real handler in handler_vpn_concentrator.go, covered there
		// "DescribeVpnConnections", — moved to advancedNetworkingSupportedOperations
		// "DescribeVpnGateways", — moved to advancedNetworkingSupportedOperations
		// "DetachVerifiedAccessTrustProvider", — moved to batch4
		// "DisableAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// "DisableAwsNetworkPerformanceMetricSubscription", — real handler in handler_network_performance.go, covered there
		// "DisableCapacityManager", — moved to handler_capacity_family.go
		// "DisableIpamOrganizationAdminAccount", — moved to ipamPolicySupportedOperations (requires params)
		// "DisableIpamPolicy", — moved to ipamPolicySupportedOperations (requires params)
		// "DisableRouteServerPropagation", — real handler in handler_route_server.go, covered there
		// "DisassociateCapacityReservationBillingOwner", — moved to handler_capacity_family.go
		// "DisassociateClientVpnTargetNetwork", — moved to batch4
		// "DisassociateEnclaveCertificateIamRole", — moved to trunkEnclaveSupportedOperations
		// "DisassociateInstanceEventWindow", — moved to instanceAttrSupportedOperations
		// "DisassociateIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DisassociateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "DisassociateRouteServer", — real handler in handler_route_server.go, covered there
		// "DisassociateTransitGatewayMulticastDomain", — real handler in handler_tgw_multicast.go, covered there
		// "DisassociateTransitGatewayPolicyTable", — moved to handler_tgw_peripherals.go
		// "DisassociateTrunkInterface", — moved to trunkEnclaveSupportedOperations
		// "EnableAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// "EnableAwsNetworkPerformanceMetricSubscription", — real handler in handler_network_performance.go, covered there
		// "EnableCapacityManager", — moved to handler_capacity_family.go
		// "EnableIpamOrganizationAdminAccount", — moved to ipamPolicySupportedOperations (requires params)
		// "EnableIpamPolicy", — moved to ipamPolicySupportedOperations (requires params)
		// "EnableRouteServerPropagation", — real handler in handler_route_server.go, covered there
		// "ExportClientVpnClientCertificateRevocationList", — moved to batch4 (requires ClientVpnEndpointId)
		// "ExportClientVpnClientConfiguration", — moved to batch4 (requires ClientVpnEndpointId)
		// "ExportTransitGatewayRoutes", — moved to handler_tgw_peripherals.go
		// "ExportVerifiedAccessInstanceClientConfiguration", — moved to handler_verifiedaccess_ext.go
		// "GetActiveVpnTunnelStatus", — real handler in handler_vpn_concentrator.go, covered there
		// "GetAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// "GetAssociatedEnclaveCertificateIamRoles", — moved to trunkEnclaveSupportedOperations
		// "GetAwsNetworkPerformanceData", — real handler in handler_network_performance.go, covered there
		// "GetCapacityManagerAttributes", — moved to handler_capacity_family.go
		// "GetCapacityManagerMetricData", — moved to handler_capacity_family.go
		// "GetCapacityManagerMetricDimensions", — moved to handler_capacity_family.go
		// "GetCoipPoolUsage", — moved to ipPoolSupportedOperations()
		// "GetDeclarativePoliciesReportSummary", — real handler in handler_declarative_policies.go, covered there
		"GetEnabledIpamPolicy",
		// "GetHostReservationPurchasePreview", — real handler in handler_host_reservations.go, covered there
		// "GetInstanceTpmEkPub", — moved to instanceAttrSupportedOperations
		// "GetInstanceUefiData", — moved to instanceAttrSupportedOperations
		"GetIpamAddressHistory",
		"GetIpamDiscoveredAccounts",
		"GetIpamDiscoveredPublicAddresses",
		"GetIpamDiscoveredResourceCidrs",
		// "GetIpamPolicyAllocationRules", — moved to ipamPolicySupportedOperations (requires params)
		// "GetIpamPolicyOrganizationTargets", — moved to ipamPolicySupportedOperations (requires params)
		"GetIpamPoolCidrs",
		// "GetIpamPrefixListResolverRules", — moved to ipamDiscoverySupportedOperations
		// "GetIpamPrefixListResolverVersionEntries", — moved to ipamDiscoverySupportedOperations
		// "GetIpamPrefixListResolverVersions", — moved to ipamDiscoverySupportedOperations
		// "GetIpamResourceCidrs", — moved to ipamDiscoverySupportedOperations
		// "GetManagedPrefixListAssociations", — moved to batch4
		// "GetManagedPrefixListEntries", — moved to batch4
		// "GetNetworkInsightsAccessScopeAnalysisFindings", — real handler in handler_batch5.go, covered there
		// "GetNetworkInsightsAccessScopeContent", — real handler in handler_batch5.go, covered there
		// "GetReservedInstancesExchangeQuote", — real handler in handler_batch5.go, covered there
		// "GetRouteServerAssociations", — real handler in handler_route_server.go, covered there
		// "GetRouteServerPropagations", — real handler in handler_route_server.go, covered there
		// "GetRouteServerRoutingDatabase", — real handler in handler_route_server.go, covered there
		// "GetTransitGatewayAttachmentPropagations", — moved to handler_tgw_peripherals.go
		// "GetTransitGatewayMeteringPolicyEntries", — moved to handler_tgw_peripherals.go
		// "GetTransitGatewayMulticastDomainAssociations", — real handler in handler_tgw_multicast.go, covered there
		// "GetTransitGatewayPolicyTableAssociations", — moved to handler_tgw_peripherals.go
		// "GetTransitGatewayPolicyTableEntries", — moved to handler_tgw_peripherals.go
		// "GetTransitGatewayPrefixListReferences", — moved to batch4
		// "GetTransitGatewayRouteTableAssociations", — moved to handler_tgw_peripherals.go
		// "GetTransitGatewayRouteTablePropagations", — moved to handler_tgw_peripherals.go
		// "GetVerifiedAccessEndpointPolicy", — moved to handler_verifiedaccess_ext.go
		// "GetVerifiedAccessEndpointTargets", — moved to handler_verifiedaccess_ext.go
		// "GetVerifiedAccessGroupPolicy", — moved to handler_verifiedaccess_ext.go
		// "GetVpcResourcesBlockingEncryptionEnforcement", — real handler in handler_vpc_encryption_control.go, covered there
		// "GetVpnConnectionDeviceSampleConfiguration", — moved to advancedNetworkingSupportedOperations
		// "GetVpnConnectionDeviceTypes", — moved to advancedNetworkingSupportedOperations
		// "GetVpnTunnelReplacementStatus", — moved to advancedNetworkingSupportedOperations
		// "ImportClientVpnClientCertificateRevocationList", — moved to batch4 (requires ClientVpnEndpointId)
		// "ImportInstance", — moved to vmImportExportSupportedOperations
		// "ImportVolume", — moved to vmImportExportSupportedOperations
		// "ModifyAvailabilityZoneGroup", — moved to instanceAttrSupportedOperations
		// "ModifyCapacityReservationFleet", — moved to handler_capacity_family.go
		// "ModifyClientVpnEndpoint", — moved to batch4
		// "ModifyFleet", — real handler in handler_batch5.go, covered there
		// "ModifyFpgaImageAttribute", — moved to handler_fpga_image.go
		// "ModifyHosts", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceCapacityReservationAttributes", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceEventStartTime", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceMaintenanceOptions", — moved to instanceAttrSupportedOperations
		// "ModifyInstanceNetworkPerformanceOptions", — moved to instanceAttrSupportedOperations
		// "ModifyInstancePlacement", — moved to instanceAttrSupportedOperations
		// "ModifyIpamPolicyAllocationRules", — moved to ipamPolicySupportedOperations (requires params)
		// "ModifyIpamPrefixListResolver", — moved to ipamDiscoverySupportedOperations
		// "ModifyIpamPrefixListResolverTarget", — moved to ipamDiscoverySupportedOperations
		// "ModifyIpamResourceCidr", — moved to ipamDiscoverySupportedOperations
		// "ModifyIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "ModifyManagedPrefixList", — moved to batch4
		// "ModifyReservedInstances", — real handler in handler_batch5.go, covered there
		// "ModifyRouteServer", — real handler in handler_route_server.go, covered there
		// "ModifyTrafficMirrorFilterNetworkServices", — real handler now requires an existing filter ID
		// "ModifyTrafficMirrorFilterRule", — real handler now requires an existing rule ID
		// "ModifyTrafficMirrorSession", — real handler now requires an existing session ID
		// "ModifyTransitGatewayMeteringPolicy", — moved to handler_tgw_peripherals.go
		// "ModifyTransitGatewayPrefixListReference", — moved to handler_tgw_peripherals.go
		// "ModifyTransitGatewayVpcAttachment", — moved to handler_tgw_peripherals.go
		// "ModifyVerifiedAccessEndpoint", — moved to batch4
		// "ModifyVerifiedAccessEndpointPolicy", — moved to handler_verifiedaccess_ext.go
		// "ModifyVerifiedAccessGroupPolicy", — moved to handler_verifiedaccess_ext.go
		// "ModifyVerifiedAccessInstanceLoggingConfiguration", — moved to handler_verifiedaccess_ext.go
		// "ModifyVpcEncryptionControl", — real handler in handler_vpc_encryption_control.go, covered there
		// "ModifyVpnConnectionOptions", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnTunnelCertificate", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnTunnelOptions", — moved to advancedNetworkingSupportedOperations
		// "MoveByoipCidrToIpam", — moved to ipamPolicySupportedOperations (requires params)
		// "MoveCapacityReservationInstances", — moved to handler_capacity_family.go
		// "ProvisionByoipCidr", — real handler in handler_batch5.go, covered there
		// "ProvisionIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "PurchaseCapacityBlock", — moved to handler_capacity_family.go
		// "PurchaseCapacityBlockExtension", — moved to handler_capacity_family.go
		// "PurchaseHostReservation", — real handler in handler_host_reservations.go, covered there
		// "PurchaseReservedInstancesOffering", — real handler in handler_batch5.go, covered there
		// "PurchaseScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		// "RegisterTransitGatewayMulticastGroupMembers", — real handler in handler_tgw_multicast.go, covered there
		// "RegisterTransitGatewayMulticastGroupSources", — real handler in handler_tgw_multicast.go, covered there
		// "RejectCapacityReservationBillingOwnership", — moved to handler_capacity_family.go
		// "RejectTransitGatewayMulticastDomainAssociations", — moved to handler_tgw_peripherals.go
		// "RejectTransitGatewayPeeringAttachment", — moved to handler_tgw_peripherals.go
		// "RejectTransitGatewayVpcAttachment", — moved to handler_tgw_peripherals.go
		// "ReleaseHosts", — real handler in handler_host_reservations.go, covered there
		// "ReplaceImageCriteriaInAllowedImagesSettings", — moved to imageOpsSupportedOperations()
		// "ReplaceVpnTunnel", — real handler in handler_vpn_concentrator.go, covered there
		// "ResetFpgaImageAttribute", — moved to handler_fpga_image.go
		// "RestoreManagedPrefixListVersion", — moved to batch4
		// "RevokeClientVpnIngress", — moved to batch4
		// "RunScheduledInstances", — moved to scheduledInstanceSupportedOperations()
		// "SearchTransitGatewayMulticastGroups", — real handler in handler_tgw_multicast.go, covered there
		// "SearchTransitGatewayRoutes", — moved to handler_tgw_peripherals.go
		// "StartDeclarativePoliciesReport", — real handler in handler_declarative_policies.go, covered there
		// "StartNetworkInsightsAccessScopeAnalysis", — real handler in handler_batch5.go, covered there
		// "StartNetworkInsightsAnalysis", — real handler in handler_batch5.go, covered there
		// "TerminateClientVpnConnections", — moved to batch4
		// "UpdateCapacityManagerOrganizationsAccess", — moved to handler_capacity_family.go
		// "WithdrawByoipCidr", — real handler in handler_batch5.go, covered there
		// "CreatePublicIpv4Pool", — moved to ipPoolSupportedOperations()
		// "DeletePublicIpv4Pool", — moved to ipPoolSupportedOperations()
		// "DeprovisionPublicIpv4PoolCidr", — moved to ipPoolSupportedOperations()
		// "DescribeIpv6Pools", — moved to ipPoolSupportedOperations()
		// "DescribePublicIpv4Pools", — moved to ipPoolSupportedOperations()
		// "GetAssociatedIpv6PoolCidrs", — moved to ipPoolSupportedOperations()
		// "ProvisionPublicIpv4PoolCidr", — moved to ipPoolSupportedOperations()
	}

	h := newHandler()

	for _, action := range stubOps {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			rec := postForm(t, h, "Action="+action+"&Version=2016-11-15")
			assert.Equal(t, http.StatusOK, rec.Code, "action %s should return 200", action)
		})
	}
}
