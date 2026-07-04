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
		"AssociateCapacityReservationBillingOwner",
		// "AssociateClientVpnTargetNetwork", — moved to batch4
		"AssociateEnclaveCertificateIamRole",
		"AssociateInstanceEventWindow",
		"AssociateIpamByoasn",
		"AssociateIpamResourceDiscovery",
		"AssociateRouteServer",
		"AssociateTransitGatewayMulticastDomain",
		"AssociateTransitGatewayPolicyTable",
		"AssociateTrunkInterface",
		"AttachClassicLinkVpc",
		// "AttachVerifiedAccessTrustProvider", — moved to batch4
		// "AuthorizeClientVpnIngress", — moved to batch4
		"BundleInstance",
		"CancelBundleTask",
		"CancelCapacityReservationFleets",
		"CancelConversionTask",
		"CancelDeclarativePoliciesReport",
		"CancelExportTask",
		"CancelImageLaunchPermission",
		"CancelImportTask",
		"CancelReservedInstancesListing",
		"ConfirmProductInstance",
		"CopyFpgaImage",
		"CreateCapacityManagerDataExport",
		"CreateCapacityReservationBySplitting",
		"CreateCapacityReservationFleet",
		"CreateCarrierGateway",
		// "CreateClientVpnEndpoint", — moved to batch4
		// "CreateClientVpnRoute", — moved to batch4
		"CreateCoipCidr",
		"CreateCoipPool",
		"CreateDelegateMacVolumeOwnershipTask",
		"CreateFleet",
		"CreateFpgaImage",
		"CreateImageUsageReport",
		"CreateInstanceExportTask",
		"CreateInterruptibleCapacityReservationAllocation",
		"CreateIpam",
		"CreateIpamExternalResourceVerificationToken",
		"CreateIpamPolicy",
		"CreateIpamPrefixListResolver",
		"CreateIpamPrefixListResolverTarget",
		"CreateIpamResourceDiscovery",
		"CreateLocalGatewayRoute",
		"CreateLocalGatewayRouteTable",
		"CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
		"CreateLocalGatewayRouteTableVpcAssociation",
		"CreateLocalGatewayVirtualInterface",
		"CreateLocalGatewayVirtualInterfaceGroup",
		"CreateMacSystemIntegrityProtectionModificationTask",
		// "CreateManagedPrefixList", — moved to batch4
		"CreateNetworkInsightsAccessScope",
		"CreateNetworkInsightsPath",
		"CreateReservedInstancesListing",
		"CreateRestoreImageTask",
		"CreateRouteServer",
		"CreateRouteServerEndpoint",
		"CreateRouteServerPeer",
		"CreateSecondaryNetwork",
		"CreateSecondarySubnet",
		"CreateStoreImageTask",
		// "CreateTrafficMirrorFilter", — real handler in handler_batch5.go, covered there
		// "CreateTrafficMirrorFilterRule", — real handler now requires an existing filter ID
		// "CreateTrafficMirrorSession", — real handler in handler_batch5.go, covered there
		// "CreateTrafficMirrorTarget", — real handler in handler_batch5.go, covered there
		// "CreateTransitGatewayConnect", — moved to batch4
		// "CreateTransitGatewayConnectPeer", — moved to batch4
		"CreateTransitGatewayMeteringPolicy",
		"CreateTransitGatewayMeteringPolicyEntry",
		"CreateTransitGatewayMulticastDomain",
		// "CreateTransitGatewayPeeringAttachment", — moved to batch4
		"CreateTransitGatewayPolicyTable",
		// "CreateTransitGatewayPrefixListReference", — moved to batch4
		"CreateTransitGatewayRouteTableAnnouncement",
		// "CreateVerifiedAccessEndpoint", — moved to batch4
		// "CreateVerifiedAccessGroup", — moved to batch4
		// "CreateVerifiedAccessInstance", — moved to batch4
		// "CreateVerifiedAccessTrustProvider", — moved to batch4
		"CreateVpcBlockPublicAccessExclusion",
		"CreateVpcEncryptionControl",
		"CreateVpcEndpointServiceConfiguration",
		"CreateVpnConcentrator",
		"CreateVpnGateway",
		"DeleteCapacityManagerDataExport",
		"DeleteCarrierGateway",
		// "DeleteClientVpnEndpoint", — moved to batch4
		// "DeleteClientVpnRoute", — moved to batch4
		"DeleteCoipCidr",
		"DeleteCoipPool",
		"DeleteFleets",
		"DeleteFpgaImage",
		"DeleteImageUsageReport",
		"DeleteIpamExternalResourceVerificationToken",
		"DeleteIpamPolicy",
		"DeleteIpamPrefixListResolver",
		"DeleteIpamPrefixListResolverTarget",
		"DeleteIpamResourceDiscovery",
		"DeleteLocalGatewayRoute",
		"DeleteLocalGatewayRouteTable",
		"DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
		"DeleteLocalGatewayRouteTableVpcAssociation",
		"DeleteLocalGatewayVirtualInterface",
		"DeleteLocalGatewayVirtualInterfaceGroup",
		// "DeleteManagedPrefixList", — moved to batch4
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
		// "DeleteTrafficMirrorFilter", — real handler now requires an existing filter ID
		// "DeleteTrafficMirrorFilterRule", — real handler now requires an existing rule ID
		// "DeleteTrafficMirrorSession", — real handler now requires an existing session ID
		// "DeleteTrafficMirrorTarget", — real handler now requires an existing target ID
		// "DeleteTransitGatewayConnect", — moved to batch4
		// "DeleteTransitGatewayConnectPeer", — moved to batch4
		"DeleteTransitGatewayMeteringPolicy",
		"DeleteTransitGatewayMeteringPolicyEntry",
		"DeleteTransitGatewayMulticastDomain",
		// "DeleteTransitGatewayPeeringAttachment", — moved to batch4
		"DeleteTransitGatewayPolicyTable",
		// "DeleteTransitGatewayPrefixListReference", — moved to batch4
		"DeleteTransitGatewayRouteTableAnnouncement",
		// "DeleteVerifiedAccessEndpoint", — moved to batch4
		// "DeleteVerifiedAccessGroup", — moved to batch4
		// "DeleteVerifiedAccessInstance", — moved to batch4
		// "DeleteVerifiedAccessTrustProvider", — moved to batch4
		"DeleteVpcBlockPublicAccessExclusion",
		"DeleteVpcEncryptionControl",
		"DeleteVpcEndpointServiceConfigurations",
		"DeleteVpnConcentrator",
		"DeprovisionByoipCidr",
		"DeprovisionIpamByoasn",
		"DeregisterTransitGatewayMulticastGroupMembers",
		"DeregisterTransitGatewayMulticastGroupSources",
		"DescribeAwsNetworkPerformanceMetricSubscriptions",
		"DescribeBundleTasks",
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
		"DescribeClassicLinkInstances",
		// "DescribeClientVpnAuthorizationRules", — moved to batch4
		// "DescribeClientVpnConnections", — moved to batch4
		// "DescribeClientVpnEndpoints", — moved to batch4
		// "DescribeClientVpnRoutes", — moved to batch4
		// "DescribeClientVpnTargetNetworks", — moved to batch4
		"DescribeCoipPools",
		"DescribeConversionTasks",
		"DescribeCustomerGateways",
		"DescribeDeclarativePoliciesReports",
		"DescribeElasticGpus",
		"DescribeExportImageTasks",
		"DescribeExportTasks",
		"DescribeFleetHistory",
		"DescribeFleetInstances",
		"DescribeFleets",
		"DescribeFpgaImageAttribute",
		"DescribeFpgaImages",
		"DescribeHostReservationOfferings",
		"DescribeHostReservations",
		"DescribeImageReferences",
		"DescribeImageUsageReportEntries",
		"DescribeIpamByoasn",
		"DescribeIpamExternalResourceVerificationTokens",
		"DescribeIpamPolicies",
		"DescribeIpamPools",
		"DescribeIpamPrefixListResolverTargets",
		"DescribeIpamPrefixListResolvers",
		"DescribeIpamResourceDiscoveries",
		"DescribeIpamResourceDiscoveryAssociations",
		"DescribeIpamScopes",
		"DescribeIpams",
		"DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations",
		"DescribeLocalGatewayRouteTableVpcAssociations",
		"DescribeLocalGatewayRouteTables",
		"DescribeLocalGatewayVirtualInterfaceGroups",
		"DescribeLocalGatewayVirtualInterfaces",
		"DescribeLocalGateways",
		"DescribeMacHosts",
		"DescribeMacModificationTasks",
		// "DescribeManagedPrefixLists", — moved to batch4
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
		"DescribeTransitGatewayMeteringPolicies",
		"DescribeTransitGatewayMulticastDomains",
		// "DescribeTransitGatewayPeeringAttachments", — moved to batch4
		"DescribeTransitGatewayPolicyTables",
		"DescribeTransitGatewayRouteTableAnnouncements",
		"DescribeTrunkInterfaceAssociations",
		// "DescribeVerifiedAccessEndpoints", — moved to batch4
		// "DescribeVerifiedAccessGroups", — moved to batch4
		"DescribeVerifiedAccessInstanceLoggingConfigurations",
		// "DescribeVerifiedAccessInstances", — moved to batch4
		// "DescribeVerifiedAccessTrustProviders", — moved to batch4
		"DescribeVpcBlockPublicAccessExclusions",
		"DescribeVpcBlockPublicAccessOptions",
		"DescribeVpcClassicLink",
		"DescribeVpcEncryptionControls",
		"DescribeVpcEndpointServiceConfigurations",
		"DescribeVpnConcentrators",
		"DescribeVpnConnections",
		"DescribeVpnGateways",
		"DetachClassicLinkVpc",
		// "DetachVerifiedAccessTrustProvider", — moved to batch4
		"DisableAllowedImagesSettings",
		"DisableAwsNetworkPerformanceMetricSubscription",
		"DisableCapacityManager",
		"DisableIpamOrganizationAdminAccount",
		"DisableIpamPolicy",
		"DisableRouteServerPropagation",
		"DisableTransitGatewayRouteTablePropagation",
		"DisableVpcClassicLink",
		"DisassociateCapacityReservationBillingOwner",
		// "DisassociateClientVpnTargetNetwork", — moved to batch4
		"DisassociateEnclaveCertificateIamRole",
		"DisassociateInstanceEventWindow",
		"DisassociateIpamByoasn",
		"DisassociateIpamResourceDiscovery",
		"DisassociateRouteServer",
		"DisassociateTransitGatewayMulticastDomain",
		"DisassociateTransitGatewayPolicyTable",
		"DisassociateTrunkInterface",
		"EnableAllowedImagesSettings",
		"EnableAwsNetworkPerformanceMetricSubscription",
		"EnableCapacityManager",
		"EnableIpamOrganizationAdminAccount",
		"EnableIpamPolicy",
		"EnableReachabilityAnalyzerOrganizationSharing",
		"EnableRouteServerPropagation",
		"EnableTransitGatewayRouteTablePropagation",
		"EnableVpcClassicLink",
		// "ExportClientVpnClientCertificateRevocationList", — moved to batch4 (requires ClientVpnEndpointId)
		// "ExportClientVpnClientConfiguration", — moved to batch4 (requires ClientVpnEndpointId)
		"ExportTransitGatewayRoutes",
		"ExportVerifiedAccessInstanceClientConfiguration",
		"GetActiveVpnTunnelStatus",
		"GetAllowedImagesSettings",
		"GetAssociatedEnclaveCertificateIamRoles",
		"GetAwsNetworkPerformanceData",
		"GetCapacityManagerAttributes",
		"GetCapacityManagerMetricData",
		"GetCapacityManagerMetricDimensions",
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
		"GetIpamPrefixListResolverRules",
		"GetIpamPrefixListResolverVersionEntries",
		"GetIpamPrefixListResolverVersions",
		"GetIpamResourceCidrs",
		// "GetManagedPrefixListAssociations", — moved to batch4
		// "GetManagedPrefixListEntries", — moved to batch4
		"GetNetworkInsightsAccessScopeAnalysisFindings",
		"GetNetworkInsightsAccessScopeContent",
		"GetReservedInstancesExchangeQuote",
		"GetRouteServerAssociations",
		"GetRouteServerPropagations",
		"GetRouteServerRoutingDatabase",
		"GetSpotPlacementScores",
		"GetTransitGatewayAttachmentPropagations",
		"GetTransitGatewayMeteringPolicyEntries",
		"GetTransitGatewayMulticastDomainAssociations",
		"GetTransitGatewayPolicyTableAssociations",
		"GetTransitGatewayPolicyTableEntries",
		// "GetTransitGatewayPrefixListReferences", — moved to batch4
		"GetTransitGatewayRouteTableAssociations",
		"GetTransitGatewayRouteTablePropagations",
		"GetVerifiedAccessEndpointPolicy",
		"GetVerifiedAccessEndpointTargets",
		"GetVerifiedAccessGroupPolicy",
		"GetVpcResourcesBlockingEncryptionEnforcement",
		"GetVpnConnectionDeviceSampleConfiguration",
		"GetVpnConnectionDeviceTypes",
		"GetVpnTunnelReplacementStatus",
		// "ImportClientVpnClientCertificateRevocationList", — moved to batch4 (requires ClientVpnEndpointId)
		"ImportInstance",
		"ImportVolume",
		"ModifyAvailabilityZoneGroup",
		"ModifyCapacityReservationFleet",
		// "ModifyClientVpnEndpoint", — moved to batch4
		"ModifyFleet",
		"ModifyFpgaImageAttribute",
		"ModifyHosts",
		"ModifyInstanceCapacityReservationAttributes",
		"ModifyInstanceEventStartTime",
		"ModifyInstanceMaintenanceOptions",
		"ModifyInstanceNetworkPerformanceOptions",
		"ModifyInstancePlacement",
		"ModifyIpamPolicyAllocationRules",
		"ModifyIpamPrefixListResolver",
		"ModifyIpamPrefixListResolverTarget",
		"ModifyIpamResourceCidr",
		"ModifyIpamResourceDiscovery",
		"ModifyLocalGatewayRoute",
		// "ModifyManagedPrefixList", — moved to batch4
		"ModifyReservedInstances",
		"ModifyRouteServer",
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
		"ModifyVpcBlockPublicAccessExclusion",
		"ModifyVpcBlockPublicAccessOptions",
		"ModifyVpcEncryptionControl",
		"ModifyVpnConnectionOptions",
		"ModifyVpnTunnelCertificate",
		"ModifyVpnTunnelOptions",
		"MoveAddressToVpc",
		"MoveByoipCidrToIpam",
		"MoveCapacityReservationInstances",
		"ProvisionByoipCidr",
		"ProvisionIpamByoasn",
		"PurchaseCapacityBlock",
		"PurchaseCapacityBlockExtension",
		"PurchaseHostReservation",
		"PurchaseReservedInstancesOffering",
		"PurchaseScheduledInstances",
		"RegisterTransitGatewayMulticastGroupMembers",
		"RegisterTransitGatewayMulticastGroupSources",
		"RejectCapacityReservationBillingOwnership",
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
		"SearchLocalGatewayRoutes",
		"SearchTransitGatewayMulticastGroups",
		"SearchTransitGatewayRoutes",
		"SendDiagnosticInterrupt",
		"StartDeclarativePoliciesReport",
		"StartNetworkInsightsAccessScopeAnalysis",
		"StartNetworkInsightsAnalysis",
		// "TerminateClientVpnConnections", — moved to batch4
		"UnassignPrivateNatGatewayAddress",
		"UpdateCapacityManagerOrganizationsAccess",
		"UpdateInterruptibleCapacityReservationAllocation",
		"WithdrawByoipCidr",
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
