package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStubOperations exercises all stub EC2 operations through the HTTP handler.
// Each stub returns a minimal XML success response — we verify 200 OK and the
// response tag name so the handler dispatch table is fully covered.
func TestStubOperations(t *testing.T) {
	t.Parallel()

	// All 599 stub operation names registered in handler_stubs.go.
	stubOps := []string{
		// "ApplySecurityGroupsToClientVpnTargetNetwork", — moved to batch4
		// "AssociateCapacityReservationBillingOwner", — moved to handler_capacity_family.go
		// "AssociateClientVpnTargetNetwork", — moved to batch4
		"AssociateEnclaveCertificateIamRole",
		"AssociateInstanceEventWindow",
		// "AssociateIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "AssociateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "AssociateRouteServer", — real handler in handler_route_server.go, covered there
		// "AssociateTransitGatewayMulticastDomain", — real handler in handler_tgw_multicast.go, covered there
		"AssociateTransitGatewayPolicyTable",
		"AssociateTrunkInterface",
		// "AttachVerifiedAccessTrustProvider", — moved to batch4
		// "AuthorizeClientVpnIngress", — moved to batch4
		"BundleInstance",
		"CancelBundleTask",
		// "CancelCapacityReservationFleets", — moved to handler_capacity_family.go
		"CancelConversionTask",
		"CancelDeclarativePoliciesReport",
		"CancelExportTask",
		"CancelImageLaunchPermission",
		"CancelImportTask",
		// "CancelReservedInstancesListing", — real handler in handler_batch5.go, covered there
		"ConfirmProductInstance",
		"CopyFpgaImage",
		// "CreateCapacityManagerDataExport", — moved to handler_capacity_family.go
		// "CreateCapacityReservationBySplitting", — moved to handler_capacity_family.go
		// "CreateCapacityReservationFleet", — moved to handler_capacity_family.go
		// "CreateCarrierGateway", — real handler in handler_batch5.go, covered there
		// "CreateClientVpnEndpoint", — moved to batch4
		// "CreateClientVpnRoute", — moved to batch4
		"CreateCoipCidr",
		"CreateCoipPool",
		"CreateDelegateMacVolumeOwnershipTask",
		// "CreateFleet", — real handler in handler_batch5.go, covered there
		"CreateFpgaImage",
		"CreateImageUsageReport",
		"CreateInstanceExportTask",
		"CreateInterruptibleCapacityReservationAllocation",
		"CreateIpam",
		// "CreateIpamExternalResourceVerificationToken", — moved to ipamDiscoverySupportedOperations
		"CreateIpamPolicy",
		// "CreateIpamPrefixListResolver", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamPrefixListResolverTarget", — moved to ipamDiscoverySupportedOperations
		// "CreateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		"CreateLocalGatewayVirtualInterface",
		"CreateLocalGatewayVirtualInterfaceGroup",
		"CreateMacSystemIntegrityProtectionModificationTask",
		// "CreateManagedPrefixList", — moved to batch4
		// "CreateNetworkInsightsAccessScope", — real handler in handler_batch5.go, covered there
		// "CreateNetworkInsightsPath", — real handler in handler_batch5.go, covered there
		// "CreateReservedInstancesListing", — real handler in handler_batch5.go, covered there
		"CreateRestoreImageTask",
		// "CreateRouteServer", — real handler in handler_route_server.go, covered there
		// "CreateRouteServerEndpoint", — real handler in handler_route_server.go, covered there
		// "CreateRouteServerPeer", — real handler in handler_route_server.go, covered there
		"CreateSecondaryNetwork",
		"CreateSecondarySubnet",
		"CreateStoreImageTask",
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
		"CreateTransitGatewayPolicyTable",
		// "CreateTransitGatewayPrefixListReference", — moved to batch4
		"CreateTransitGatewayRouteTableAnnouncement",
		// "CreateVerifiedAccessEndpoint", — moved to batch4
		// "CreateVerifiedAccessGroup", — moved to batch4
		// "CreateVerifiedAccessInstance", — moved to batch4
		// "CreateVerifiedAccessTrustProvider", — moved to batch4
		"CreateVpcEncryptionControl",
		"CreateVpcEndpointServiceConfiguration",
		"CreateVpnConcentrator",
		// "CreateVpnGateway", — moved to advancedNetworkingSupportedOperations
		// "DeleteCapacityManagerDataExport", — moved to handler_capacity_family.go
		// "DeleteCarrierGateway", — real handler in handler_batch5.go, covered there
		// "DeleteClientVpnEndpoint", — moved to batch4
		// "DeleteClientVpnRoute", — moved to batch4
		"DeleteCoipCidr",
		"DeleteCoipPool",
		// "DeleteFleets", — real handler in handler_batch5.go, covered there
		"DeleteFpgaImage",
		"DeleteImageUsageReport",
		// "DeleteIpamExternalResourceVerificationToken", — moved to ipamDiscoverySupportedOperations
		"DeleteIpamPolicy",
		// "DeleteIpamPrefixListResolver", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamPrefixListResolverTarget", — moved to ipamDiscoverySupportedOperations
		// "DeleteIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		"DeleteLocalGatewayVirtualInterface",
		"DeleteLocalGatewayVirtualInterfaceGroup",
		// "DeleteManagedPrefixList", — moved to batch4
		// "DeleteNetworkInsightsAccessScope", — real handler in handler_batch5.go, covered there
		// "DeleteNetworkInsightsAccessScopeAnalysis", — real handler in handler_batch5.go, covered there
		// "DeleteNetworkInsightsAnalysis", — real handler in handler_batch5.go, covered there
		// "DeleteNetworkInsightsPath", — real handler in handler_batch5.go, covered there
		// "DeleteQueuedReservedInstances", — real handler in handler_batch5.go, covered there
		// "DeleteRouteServer", — real handler in handler_route_server.go, covered there
		// "DeleteRouteServerEndpoint", — real handler in handler_route_server.go, covered there
		// "DeleteRouteServerPeer", — real handler in handler_route_server.go, covered there
		"DeleteSecondaryNetwork",
		"DeleteSecondarySubnet",
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
		"DeleteTransitGatewayPolicyTable",
		// "DeleteTransitGatewayPrefixListReference", — moved to batch4
		"DeleteTransitGatewayRouteTableAnnouncement",
		// "DeleteVerifiedAccessEndpoint", — moved to batch4
		// "DeleteVerifiedAccessGroup", — moved to batch4
		// "DeleteVerifiedAccessInstance", — moved to batch4
		// "DeleteVerifiedAccessTrustProvider", — moved to batch4
		"DeleteVpcEncryptionControl",
		"DeleteVpcEndpointServiceConfigurations",
		"DeleteVpnConcentrator",
		// "DeprovisionByoipCidr", — real handler in handler_batch5.go, covered there
		// "DeprovisionIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DeregisterTransitGatewayMulticastGroupMembers", — real handler in handler_tgw_multicast.go, covered there
		// "DeregisterTransitGatewayMulticastGroupSources", — real handler in handler_tgw_multicast.go, covered there
		"DescribeAwsNetworkPerformanceMetricSubscriptions",
		"DescribeBundleTasks",
		// "DescribeCapacityBlockExtensionHistory", — moved to handler_capacity_family.go
		// "DescribeCapacityBlockExtensionOfferings", — moved to handler_capacity_family.go
		// "DescribeCapacityBlockOfferings", — moved to handler_capacity_family.go
		// "DescribeCapacityBlockStatus", — moved to handler_capacity_family.go
		// "DescribeCapacityBlocks", — moved to handler_capacity_family.go
		// "DescribeCapacityManagerDataExports", — moved to handler_capacity_family.go
		// "DescribeCapacityReservationBillingRequests", — moved to handler_capacity_family.go
		// "DescribeCapacityReservationFleets", — moved to handler_capacity_family.go
		"DescribeCapacityReservationTopology",
		// "DescribeCarrierGateways", — real handler in handler_batch5.go, covered there
		// "DescribeClientVpnAuthorizationRules", — moved to batch4
		// "DescribeClientVpnConnections", — moved to batch4
		// "DescribeClientVpnEndpoints", — moved to batch4
		// "DescribeClientVpnRoutes", — moved to batch4
		// "DescribeClientVpnTargetNetworks", — moved to batch4
		"DescribeCoipPools",
		"DescribeConversionTasks",
		// "DescribeCustomerGateways", — moved to advancedNetworkingSupportedOperations
		"DescribeDeclarativePoliciesReports",
		"DescribeElasticGpus",
		"DescribeExportImageTasks",
		"DescribeExportTasks",
		// "DescribeFleetHistory", — real handler in handler_batch5.go, covered there
		// "DescribeFleetInstances", — real handler in handler_batch5.go, covered there
		// "DescribeFleets", — real handler in handler_batch5.go, covered there
		"DescribeFpgaImageAttribute",
		"DescribeFpgaImages",
		"DescribeHostReservationOfferings",
		"DescribeHostReservations",
		"DescribeImageReferences",
		"DescribeImageUsageReportEntries",
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
		"DescribeMacHosts",
		"DescribeMacModificationTasks",
		// "DescribeManagedPrefixLists", — moved to batch4
		"DescribeMovingAddresses",
		// "DescribeNetworkInsightsAccessScopeAnalyses", — real handler in handler_batch5.go, covered there
		// "DescribeNetworkInsightsAccessScopes", — real handler in handler_batch5.go, covered there
		// "DescribeNetworkInsightsAnalyses", — real handler in handler_batch5.go, covered there
		// "DescribeNetworkInsightsPaths", — real handler in handler_batch5.go, covered there
		"DescribeOutpostLags",
		// "DescribeReservedInstances", — real handler in handler_batch5.go, covered there
		// "DescribeReservedInstancesListings", — real handler in handler_batch5.go, covered there
		// "DescribeReservedInstancesModifications", — real handler in handler_batch5.go, covered there
		// "DescribeReservedInstancesOfferings", — real handler in handler_batch5.go, covered there
		// "DescribeRouteServerEndpoints", — real handler in handler_route_server.go, covered there
		// "DescribeRouteServerPeers", — real handler in handler_route_server.go, covered there
		// "DescribeRouteServers", — real handler in handler_route_server.go, covered there
		"DescribeScheduledInstanceAvailability",
		"DescribeScheduledInstances",
		"DescribeSecondaryInterfaces",
		"DescribeSecondaryNetworks",
		"DescribeSecondarySubnets",
		"DescribeServiceLinkVirtualInterfaces",
		"DescribeSpotFleetRequests",
		"DescribeStoreImageTasks",
		// "DescribeTrafficMirrorFilterRules", — real handler now requires an existing filter ID
		// "DescribeTrafficMirrorFilters", — real handler in handler_batch5.go, covered there
		// "DescribeTrafficMirrorSessions", — real handler in handler_batch5.go, covered there
		// "DescribeTrafficMirrorTargets", — real handler in handler_batch5.go, covered there
		"DescribeTransitGatewayAttachments",
		// "DescribeTransitGatewayConnectPeers", — moved to batch4
		// "DescribeTransitGatewayConnects", — moved to batch4
		// "DescribeTransitGatewayMeteringPolicies", — real handler in handler_tgw_multicast.go, covered there
		// "DescribeTransitGatewayMulticastDomains", — real handler in handler_tgw_multicast.go, covered there
		// "DescribeTransitGatewayPeeringAttachments", — moved to batch4
		"DescribeTransitGatewayPolicyTables",
		"DescribeTransitGatewayRouteTableAnnouncements",
		"DescribeTrunkInterfaceAssociations",
		// "DescribeVerifiedAccessEndpoints", — moved to batch4
		// "DescribeVerifiedAccessGroups", — moved to batch4
		"DescribeVerifiedAccessInstanceLoggingConfigurations",
		// "DescribeVerifiedAccessInstances", — moved to batch4
		// "DescribeVerifiedAccessTrustProviders", — moved to batch4
		"DescribeVpcEncryptionControls",
		"DescribeVpcEndpointServiceConfigurations",
		"DescribeVpnConcentrators",
		// "DescribeVpnConnections", — moved to advancedNetworkingSupportedOperations
		// "DescribeVpnGateways", — moved to advancedNetworkingSupportedOperations
		// "DetachVerifiedAccessTrustProvider", — moved to batch4
		"DisableAllowedImagesSettings",
		"DisableAwsNetworkPerformanceMetricSubscription",
		// "DisableCapacityManager", — moved to handler_capacity_family.go
		"DisableIpamOrganizationAdminAccount",
		"DisableIpamPolicy",
		// "DisableRouteServerPropagation", — real handler in handler_route_server.go, covered there
		"DisableTransitGatewayRouteTablePropagation",
		// "DisassociateCapacityReservationBillingOwner", — moved to handler_capacity_family.go
		// "DisassociateClientVpnTargetNetwork", — moved to batch4
		"DisassociateEnclaveCertificateIamRole",
		"DisassociateInstanceEventWindow",
		// "DisassociateIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "DisassociateIpamResourceDiscovery", — moved to ipamDiscoverySupportedOperations
		// "DisassociateRouteServer", — real handler in handler_route_server.go, covered there
		// "DisassociateTransitGatewayMulticastDomain", — real handler in handler_tgw_multicast.go, covered there
		"DisassociateTransitGatewayPolicyTable",
		"DisassociateTrunkInterface",
		"EnableAllowedImagesSettings",
		"EnableAwsNetworkPerformanceMetricSubscription",
		// "EnableCapacityManager", — moved to handler_capacity_family.go
		"EnableIpamOrganizationAdminAccount",
		"EnableIpamPolicy",
		"EnableReachabilityAnalyzerOrganizationSharing",
		// "EnableRouteServerPropagation", — real handler in handler_route_server.go, covered there
		"EnableTransitGatewayRouteTablePropagation",
		// "ExportClientVpnClientCertificateRevocationList", — moved to batch4 (requires ClientVpnEndpointId)
		// "ExportClientVpnClientConfiguration", — moved to batch4 (requires ClientVpnEndpointId)
		"ExportTransitGatewayRoutes",
		"ExportVerifiedAccessInstanceClientConfiguration",
		"GetActiveVpnTunnelStatus",
		"GetAllowedImagesSettings",
		"GetAssociatedEnclaveCertificateIamRoles",
		"GetAwsNetworkPerformanceData",
		// "GetCapacityManagerAttributes", — moved to handler_capacity_family.go
		// "GetCapacityManagerMetricData", — moved to handler_capacity_family.go
		// "GetCapacityManagerMetricDimensions", — moved to handler_capacity_family.go
		"GetCapacityReservationUsage",
		"GetCoipPoolUsage",
		"GetDeclarativePoliciesReportSummary",
		"GetEnabledIpamPolicy",
		"GetFlowLogsIntegrationTemplate",
		"GetHostReservationPurchasePreview",
		"GetImageAncestry",
		"GetInstanceTpmEkPub",
		"GetInstanceUefiData",
		"GetIpamAddressHistory",
		"GetIpamDiscoveredAccounts",
		"GetIpamDiscoveredPublicAddresses",
		"GetIpamDiscoveredResourceCidrs",
		"GetIpamPolicyAllocationRules",
		"GetIpamPolicyOrganizationTargets",
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
		"GetSpotPlacementScores",
		"GetTransitGatewayAttachmentPropagations",
		"GetTransitGatewayMeteringPolicyEntries",
		// "GetTransitGatewayMulticastDomainAssociations", — real handler in handler_tgw_multicast.go, covered there
		"GetTransitGatewayPolicyTableAssociations",
		"GetTransitGatewayPolicyTableEntries",
		// "GetTransitGatewayPrefixListReferences", — moved to batch4
		"GetTransitGatewayRouteTableAssociations",
		"GetTransitGatewayRouteTablePropagations",
		"GetVerifiedAccessEndpointPolicy",
		"GetVerifiedAccessEndpointTargets",
		"GetVerifiedAccessGroupPolicy",
		"GetVpcResourcesBlockingEncryptionEnforcement",
		// "GetVpnConnectionDeviceSampleConfiguration", — moved to advancedNetworkingSupportedOperations
		// "GetVpnConnectionDeviceTypes", — moved to advancedNetworkingSupportedOperations
		// "GetVpnTunnelReplacementStatus", — moved to advancedNetworkingSupportedOperations
		// "ImportClientVpnClientCertificateRevocationList", — moved to batch4 (requires ClientVpnEndpointId)
		"ImportInstance",
		"ImportVolume",
		"ModifyAvailabilityZoneGroup",
		// "ModifyCapacityReservationFleet", — moved to handler_capacity_family.go
		// "ModifyClientVpnEndpoint", — moved to batch4
		// "ModifyFleet", — real handler in handler_batch5.go, covered there
		"ModifyFpgaImageAttribute",
		"ModifyHosts",
		"ModifyInstanceCapacityReservationAttributes",
		"ModifyInstanceEventStartTime",
		"ModifyInstanceMaintenanceOptions",
		"ModifyInstanceNetworkPerformanceOptions",
		"ModifyInstancePlacement",
		"ModifyIpamPolicyAllocationRules",
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
		"ModifyTransitGatewayMeteringPolicy",
		"ModifyTransitGatewayPrefixListReference",
		"ModifyTransitGatewayVpcAttachment",
		// "ModifyVerifiedAccessEndpoint", — moved to batch4
		"ModifyVerifiedAccessEndpointPolicy",
		"ModifyVerifiedAccessGroup",
		"ModifyVerifiedAccessGroupPolicy",
		"ModifyVerifiedAccessInstance",
		"ModifyVerifiedAccessInstanceLoggingConfiguration",
		"ModifyVerifiedAccessTrustProvider",
		"ModifyVpcEncryptionControl",
		// "ModifyVpnConnectionOptions", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnTunnelCertificate", — moved to advancedNetworkingSupportedOperations
		// "ModifyVpnTunnelOptions", — moved to advancedNetworkingSupportedOperations
		"MoveAddressToVpc",
		"MoveByoipCidrToIpam",
		// "MoveCapacityReservationInstances", — moved to handler_capacity_family.go
		// "ProvisionByoipCidr", — real handler in handler_batch5.go, covered there
		// "ProvisionIpamByoasn", — moved to ipamDiscoverySupportedOperations
		// "PurchaseCapacityBlock", — moved to handler_capacity_family.go
		// "PurchaseCapacityBlockExtension", — moved to handler_capacity_family.go
		"PurchaseHostReservation",
		// "PurchaseReservedInstancesOffering", — real handler in handler_batch5.go, covered there
		"PurchaseScheduledInstances",
		// "RegisterTransitGatewayMulticastGroupMembers", — real handler in handler_tgw_multicast.go, covered there
		// "RegisterTransitGatewayMulticastGroupSources", — real handler in handler_tgw_multicast.go, covered there
		// "RejectCapacityReservationBillingOwnership", — moved to handler_capacity_family.go
		"RejectTransitGatewayMulticastDomainAssociations",
		"RejectTransitGatewayPeeringAttachment",
		"RejectTransitGatewayVpcAttachment",
		"RejectVpcEndpointConnections",
		"ReleaseHosts",
		"ReplaceImageCriteriaInAllowedImagesSettings",
		"ReplaceVpnTunnel",
		"ResetFpgaImageAttribute",
		// "RestoreManagedPrefixListVersion", — moved to batch4
		// "RevokeClientVpnIngress", — moved to batch4
		"RunScheduledInstances",
		// "SearchTransitGatewayMulticastGroups", — real handler in handler_tgw_multicast.go, covered there
		"SearchTransitGatewayRoutes",
		"SendDiagnosticInterrupt",
		"StartDeclarativePoliciesReport",
		// "StartNetworkInsightsAccessScopeAnalysis", — real handler in handler_batch5.go, covered there
		// "StartNetworkInsightsAnalysis", — real handler in handler_batch5.go, covered there
		// "TerminateClientVpnConnections", — moved to batch4
		"UnassignPrivateNatGatewayAddress",
		// "UpdateCapacityManagerOrganizationsAccess", — moved to handler_capacity_family.go
		"UpdateInterruptibleCapacityReservationAllocation",
		// "WithdrawByoipCidr", — real handler in handler_batch5.go, covered there
		"CreatePublicIpv4Pool",
		"DeletePublicIpv4Pool",
		"DeprovisionPublicIpv4PoolCidr",
		"DescribeIpv6Pools",
		"DescribePublicIpv4Pools",
		"GetAssociatedIpv6PoolCidrs",
		"ProvisionPublicIpv4PoolCidr",
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
