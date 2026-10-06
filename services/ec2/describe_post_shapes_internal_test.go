package ec2

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagedOpResponseShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resp any
		op   string
	}{
		{op: "DescribeApplicationStatus", resp: &describeApplicationStatusResponse{}},
		{op: "DescribeApplicationStatusCheckAssociations", resp: &describeApplicationStatusCheckAssociationsResponse{}},
		{op: "DescribeApplicationStatusChecks", resp: &describeApplicationStatusChecksResponse{}},
		{
			op:   "DescribeAwsNetworkPerformanceMetricSubscriptions",
			resp: &describeAwsNetworkPerformanceMetricSubscriptionsResponse{},
		},
		{op: "DescribeByoipCidrs", resp: &describeByoipCidrsResponse{}},
		{op: "DescribeCapacityBlockExtensionOfferings", resp: &describeCapacityBlockExtensionOfferingsResponse{}},
		{op: "DescribeCarrierGateways", resp: &describeCarrierGatewaysResponse{}},
		{op: "DescribeClassicLinkInstances", resp: &describeClassicLinkInstancesResponse{}},
		{op: "DescribeClientVpnConnections", resp: &describeClientVpnConnectionsResponse{}},
		{op: "DescribeCoipPools", resp: &describeCoipPoolsResponse{}},
		{op: "DescribeDeclarativePoliciesReports", resp: &describeDeclarativePoliciesReportsResponse{}},
		{op: "DescribeDhcpOptions", resp: &describeDhcpOptionsResponse{}},
		{op: "DescribeEgressOnlyInternetGateways", resp: &describeEgressOnlyInternetGatewaysResponse{}},
		{op: "DescribeFleets", resp: &describeFleetsResponse{}},
		{op: "DescribeFlowLogs", resp: &describeFlowLogsResponse{}},
		{op: "DescribeFpgaImages", resp: &describeFpgaImagesResponse{}},
		{op: "DescribeHostReservationOfferings", resp: &describeHostReservationOfferingsResponse{}},
		{op: "DescribeHostReservations", resp: &describeHostReservationsResponse{}},
		{op: "DescribeHosts", resp: &describeHostsResponse{}},
		{op: "DescribeIamInstanceProfileAssociations", resp: &describeIamInstanceProfileAssociationsResponse{}},
		{op: "DescribeImageUsageReportEntries", resp: &describeImageUsageReportEntriesResponse{}},
		{op: "DescribeImageUsageReports", resp: &describeImageUsageReportsResponse{}},
		{op: "DescribeInstanceSqlHaHistoryStates", resp: &describeInstanceSQLHaHistoryStatesResponse{}},
		{op: "DescribeInstanceSqlHaStates", resp: &describeInstanceSQLHaStatesResponse{}},
		{op: "DescribeInstanceStatus", resp: &describeInstanceStatusResponse{}},
		{op: "DescribeInstanceTypeOfferings", resp: &describeInstanceTypeOfferingsResponse{}},
		{op: "DescribeInternetGateways", resp: &describeInternetGatewaysResponse{}},
		{op: "DescribeIpamByoasn", resp: &describeIpamByoasnResponse{}},
		{
			op:   "DescribeIpamExternalResourceVerificationTokens",
			resp: &describeIpamExternalResourceVerificationTokensResponse{},
		},
		{op: "DescribeIpamPolicies", resp: &describeIpamPoliciesResponse{}},
		{op: "DescribeIpamPoolAllocations", resp: &describeIpamPoolAllocationsResponse{}},
		{op: "DescribeIpamPools", resp: &describeIpamPoolsResponse{}},
		{op: "DescribeIpamPrefixListResolverTargets", resp: &describeIpamPrefixListResolverTargetsResponse{}},
		{op: "DescribeIpamPrefixListResolvers", resp: &describeIpamPrefixListResolversResponse{}},
		{op: "DescribeIpamResourceDiscoveries", resp: &describeIpamResourceDiscoveriesResponse{}},
		{op: "DescribeIpamResourceDiscoveryAssociations", resp: &describeIpamResourceDiscoveryAssociationsResponse{}},
		{op: "DescribeIpamScopes", resp: &describeIpamScopesResponse{}},
		{op: "DescribeIpams", resp: &describeIpamsResponse{}},
		{op: "DescribeIpv6Pools", resp: &describeIpv6PoolsResponse{}},
		{op: "DescribeLaunchTemplateVersions", resp: &describeLaunchTemplateVersionsResponse{}},
		{op: "DescribeLaunchTemplates", resp: &describeLaunchTemplatesResponse{}},
		{
			op:   "DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations",
			resp: &describeLGWVifGroupAssocsResponse{},
		},
		{
			op:   "DescribeLocalGatewayRouteTableVpcAssociations",
			resp: &describeLocalGatewayRouteTableVpcAssociationsResponse{},
		},
		{op: "DescribeLocalGatewayRouteTables", resp: &describeLocalGatewayRouteTablesResponse{}},
		{op: "DescribeLocalGatewayVirtualInterfaceGroups", resp: &describeLocalGatewayVirtualInterfaceGroupsResponse{}},
		{op: "DescribeLocalGatewayVirtualInterfaces", resp: &describeLocalGatewayVirtualInterfacesResponse{}},
		{op: "DescribeLocalGateways", resp: &describeLocalGatewaysResponse{}},
		{op: "DescribeMacHosts", resp: &describeMacHostsResponse{}},
		{op: "DescribeMacModificationTasks", resp: &describeMacModificationTasksResponse{}},
		{op: "DescribeManagedPrefixLists", resp: &describeManagedPrefixListsResponse{}},
		{op: "DescribeNatGateways", resp: &describeNatGatewaysResponse{}},
		{op: "DescribeNetworkInsightsAccessScopeAnalyses", resp: &describeNetworkInsightsAccessScopeAnalysesResponse{}},
		{op: "DescribeNetworkInsightsAccessScopes", resp: &describeNetworkInsightsAccessScopesResponse{}},
		{op: "DescribeNetworkInsightsAnalyses", resp: &describeNetworkInsightsAnalysesResponse{}},
		{op: "DescribeNetworkInsightsPaths", resp: &describeNetworkInsightsPathsResponse{}},
		{op: "DescribeNetworkInterfaces", resp: &describeNetworkInterfacesResponse{}},
		{op: "DescribeOutpostLags", resp: &describeOutpostLagsResponse{}},
		{op: "DescribePrefixLists", resp: &describePrefixListsResponse{}},
		{op: "DescribePrincipalIdFormat", resp: &describePrincipalIDFormatResponse{}},
		{op: "DescribePublicIpv4Pools", resp: &describePublicIpv4PoolsResponse{}},
		{op: "DescribeReservedInstancesModifications", resp: &describeReservedInstancesModificationsResponse{}},
		{op: "DescribeRouteServerEndpoints", resp: &describeRouteServerEndpointsResponse{}},
		{op: "DescribeRouteServerPeers", resp: &describeRouteServerPeersResponse{}},
		{op: "DescribeRouteServers", resp: &describeRouteServersResponse{}},
		{op: "DescribeRouteTables", resp: &describeRouteTablesResponse{}},
		{op: "DescribeSecondaryInterfaces", resp: &describeSecondaryInterfacesResponse{}},
		{op: "DescribeSecondaryNetworks", resp: &describeSecondaryNetworksResponse{}},
		{op: "DescribeSecondarySubnets", resp: &describeSecondarySubnetsResponse{}},
		{op: "DescribeServiceLinkVirtualInterfaces", resp: &describeServiceLinkVirtualInterfacesResponse{}},
		{op: "DescribeSpotInstanceRequests", resp: &describeSpotInstanceRequestsResponse{}},
		{op: "DescribeSpotPriceHistory", resp: &describeSpotPriceHistoryResponse{}},
		{op: "DescribeStoreImageTasks", resp: &describeStoreImageTasksResponse{}},
		{op: "DescribeSubnets", resp: &describeSubnetsResponse{}},
		{op: "DescribeTags", resp: &describeTagsResponse{}},
		{op: "DescribeTrafficMirrorFilterRules", resp: &describeTrafficMirrorFilterRulesResponse{}},
		{op: "DescribeTrafficMirrorFilters", resp: &describeTrafficMirrorFiltersResponse{}},
		{op: "DescribeTrafficMirrorSessions", resp: &describeTrafficMirrorSessionsResponse{}},
		{op: "DescribeTrafficMirrorTargets", resp: &describeTrafficMirrorTargetsResponse{}},
		{op: "DescribeTransitGatewayAttachments", resp: &describeTransitGatewayAttachmentsResponse{}},
		{op: "DescribeTransitGatewayConnectPeers", resp: &describeTransitGatewayConnectPeersResponse{}},
		{op: "DescribeTransitGatewayConnects", resp: &describeTransitGatewayConnectsResponse{}},
		{op: "DescribeTransitGatewayPeeringAttachments", resp: &describeTransitGatewayPeeringAttachmentsResponse{}},
		{op: "DescribeTransitGatewayPolicyTables", resp: &describeTransitGatewayPolicyTablesResponse{}},
		{
			op:   "DescribeTransitGatewayRouteTableAnnouncements",
			resp: &describeTransitGatewayRouteTableAnnouncementsResponse{},
		},
		{op: "DescribeTransitGatewayRouteTables", resp: &describeTransitGatewayRouteTablesResponse{}},
		{op: "DescribeTransitGatewayVpcAttachments", resp: &describeTransitGatewayVpcAttachmentsResponse{}},
		{op: "DescribeTransitGateways", resp: &describeTransitGatewaysResponse{}},
		{op: "DescribeVerifiedAccessEndpoints", resp: &describeVerifiedAccessEndpointsResponse{}},
		{op: "DescribeVerifiedAccessGroups", resp: &describeVerifiedAccessGroupsResponse{}},
		{
			op:   "DescribeVerifiedAccessInstanceLoggingConfigurations",
			resp: &describeVerifiedAccessInstanceLoggingConfigurationsResponse{},
		},
		{op: "DescribeVerifiedAccessInstances", resp: &describeVerifiedAccessInstancesResponse{}},
		{op: "DescribeVerifiedAccessTrustProviders", resp: &describeVerifiedAccessTrustProvidersResponse{}},
		{op: "DescribeVolumes", resp: &describeVolumesResponse{}},
		{op: "DescribeVpcBlockPublicAccessExclusions", resp: &describeVpcBlockPublicAccessExclusionsResponse{}},
		{op: "DescribeVpcClassicLinkDnsSupport", resp: &describeVpcClassicLinkDNSSupportResponse{}},
		{op: "DescribeVpcEncryptionControls", resp: &describeVpcEncryptionControlsResponse{}},
		{op: "DescribeVpcEndpointAssociations", resp: &describeVpcEndpointAssociationsResponse{}},
		{op: "DescribeVpcEndpointConnectionNotifications", resp: &describeVpcEndpointConnectionNotificationsResponse{}},
		{op: "DescribeVpcEndpointConnections", resp: &describeVpcEndpointConnectionsResponse{}},
		{op: "DescribeVpcEndpointServiceConfigurations", resp: &describeVpcEndpointServiceConfigurationsResponse{}},
		{op: "DescribeVpcEndpointServicePermissions", resp: &describeVpcEndpointServicePermissionsResponse{}},
		{op: "DescribeVpcEndpointServices", resp: &describeVpcEndpointServicesResponse{}},
		{op: "DescribeVpcEndpoints", resp: &describeVpcEndpointsResponse{}},
		{op: "DescribeVpcPeeringConnections", resp: &describeVpcPeeringConnectionsResponse{}},
		{op: "DescribeVpcs", resp: &describeVpcsResponse{}},
		{op: "DescribeVpnConcentrators", resp: &describeVpnConcentratorsResponse{}},
		{op: "GetAssociatedIpv6PoolCidrs", resp: &getAssociatedIpv6PoolCidrsResponse{}},
		{op: "GetAwsNetworkPerformanceData", resp: &getAwsNetworkPerformanceDataResponse{}},
		{op: "GetCapacityManagerMetricData", resp: &getCapacityManagerMetricDataResponse{}},
		{op: "GetCapacityManagerMetricDimensions", resp: &getCapacityManagerMetricDimensionsResponse{}},
		{op: "GetCapacityReservationUsage", resp: &getCapacityReservationUsageResponse{}},
		{op: "GetCoipPoolUsage", resp: &getCoipPoolUsageResponse{}},
		{op: "GetGroupsForCapacityReservation", resp: &groupsForCapacityReservationResponse{}},
		{op: "GetIpamAddressHistory", resp: &getIpamAddressHistoryResponse{}},
		{op: "GetIpamDiscoveredAccounts", resp: &getIpamDiscoveredAccountsResponse{}},
		{op: "GetIpamDiscoveredPublicAddresses", resp: &getIpamDiscoveredPublicAddressesResponse{}},
		{op: "GetIpamDiscoveredResourceCidrs", resp: &getIpamDiscoveredResourceCidrsResponse{}},
		{op: "GetIpamDiscoveredRoutes", resp: &getIpamDiscoveredRoutesResponse{}},
		{op: "GetIpamPolicyAllocationRules", resp: &getIpamPolicyAllocationRulesResponse{}},
		{op: "GetIpamPolicyOrganizationTargets", resp: &getIpamPolicyOrganizationTargetsResponse{}},
		{op: "GetIpamPoolAllocations", resp: &getIpamPoolAllocationsResponse{}},
		{op: "GetIpamPoolCidrs", resp: &getIpamPoolCidrsResponse{}},
		{op: "GetIpamPrefixListResolverRules", resp: &getIpamPrefixListResolverRulesResponse{}},
		{op: "GetIpamPrefixListResolverVersionEntries", resp: &getIpamPrefixListResolverVersionEntriesResponse{}},
		{op: "GetIpamPrefixListResolverVersions", resp: &getIpamPrefixListResolverVersionsResponse{}},
		{op: "GetIpamResourceCidrs", resp: &getIpamResourceCidrsResponse{}},
		{op: "GetIpamRouteProtectionFindings", resp: &getIpamRouteProtectionFindingsResponse{}},
		{op: "GetManagedPrefixListAssociations", resp: &getManagedPrefixListAssociationsResponse{}},
		{op: "GetManagedPrefixListEntries", resp: &getManagedPrefixListEntriesResponse{}},
		{
			op:   "GetNetworkInsightsAccessScopeAnalysisFindings",
			resp: &getNetworkInsightsAccessScopeAnalysisFindingsResponse{},
		},
		{op: "GetRouteServerRoutingDatabase", resp: &getRouteServerRoutingDatabaseResponse{}},
		{op: "GetSubnetCidrReservations", resp: &getSubnetCidrReservationsResponse{}},
		{op: "GetTransitGatewayAttachmentPropagations", resp: &getTransitGatewayAttachmentPropagationsResponse{}},
		{op: "GetTransitGatewayMeteringPolicyEntries", resp: &getTransitGatewayMeteringPolicyEntriesResponse{}},
		{op: "GetTransitGatewayPolicyTableAssociations", resp: &getTransitGatewayPolicyTableAssociationsResponse{}},
		{op: "GetTransitGatewayPolicyTableEntries", resp: &getTransitGatewayPolicyTableEntriesResponse{}},
		{op: "GetTransitGatewayPrefixListReferences", resp: &getTransitGatewayPrefixListReferencesResponse{}},
		{op: "GetTransitGatewayRouteTableAssociations", resp: &getTransitGatewayRouteTableAssociationsResponse{}},
		{op: "GetTransitGatewayRouteTablePropagations", resp: &getTransitGatewayRouteTablePropagationsResponse{}},
		{op: "GetVerifiedAccessEndpointTargets", resp: &getVerifiedAccessEndpointTargetsResponse{}},
		{op: "GetVpcResourcesBlockingEncryptionEnforcement", resp: &getVpcEncBlockingResourcesResponse{}},
		{op: "SearchLocalGatewayRoutes", resp: &searchLocalGatewayRoutesResponse{}},
		{op: "DescribeCapacityBlockOfferings", resp: &describeCapacityBlockOfferingsResponse{}},
		{op: "DescribeScheduledInstances", resp: &describeScheduledInstancesResponse{}},
		{op: "DescribeExportTasks", resp: &describeExportTasksResponse{}},
		{op: "DescribeElasticGpus", resp: &describeElasticGpusResponse{}},
		{op: "DescribeImportSnapshotTasks", resp: &describeImportSnapshotTasksResponse{}},
		{op: "DescribeCapacityManagerDataExports", resp: &describeCapacityManagerDataExportsResponse{}},
		{
			op:   "DescribeCapacityReservationCancellationQuotes",
			resp: &describeCapacityReservationCancellationQuotesResponse{},
		},
		{op: "DescribeTransitGatewayMeteringPolicies", resp: &describeTransitGatewayMeteringPoliciesResponse{}},
		{op: "GetIpamInternetRegistryAssociationAsns", resp: &getIpamInternetRegistryAssociationAsnsResponse{}},
		{op: "GetIpamInternetRegistryAssociationCidrs", resp: &getIpamInternetRegistryAssociationCidrsResponse{}},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()

			rv := reflect.ValueOf(tt.resp).Elem()
			require.NotEmpty(t, findItemSets(rv), "repeated item set")
			assert.Len(t, findItemSets(rv), wantSets(tt.op), "item set count")

			if tt.op != "DescribeExportTasks" {
				nt := rv.FieldByName("NextToken")
				require.True(t, nt.IsValid(), "NextToken field")
				assert.Equal(t, reflect.String, nt.Kind())
			}
		})
	}
}

const twoItemSets = 2

func wantSets(op string) int {
	switch op {
	case "GetSubnetCidrReservations", "DescribeVpcEndpointServices":
		return twoItemSets
	}

	return 1
}
