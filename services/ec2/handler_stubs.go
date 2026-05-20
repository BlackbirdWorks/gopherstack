package ec2

import (
	"encoding/xml"
	"net/url"
)

// stubResponse is a minimal success response for stub EC2 operations.
type stubResponse struct {
	XMLName   xml.Name `xml:"StubResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

//nolint:funlen
func registerStubOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["AllocateIpamPoolCidr"] = h.handleStubAllocateIpamPoolCidr
	// ApplySecurityGroupsToClientVpnTargetNetwork — moved to handler_batch4.go
	ops["AssociateCapacityReservationBillingOwner"] = h.handleStubAssociateCapacityReservationBillingOwner
	// AssociateClientVpnTargetNetwork — moved to handler_batch4.go
	ops["AssociateEnclaveCertificateIamRole"] = h.handleStubAssociateEnclaveCertificateIamRole
	// AssociateIamInstanceProfile — moved to handler_ec2core.go
	ops["AssociateInstanceEventWindow"] = h.handleStubAssociateInstanceEventWindow
	ops["AssociateIpamByoasn"] = h.handleStubAssociateIpamByoasn
	ops["AssociateIpamResourceDiscovery"] = h.handleStubAssociateIpamResourceDiscovery
	ops["AssociateRouteServer"] = h.handleStubAssociateRouteServer
	ops["AssociateTransitGatewayMulticastDomain"] = h.handleStubAssociateTransitGatewayMulticastDomain
	ops["AssociateTransitGatewayPolicyTable"] = h.handleStubAssociateTransitGatewayPolicyTable
	// AssociateTransitGatewayRouteTable — moved to handler_ec2core.go
	ops["AssociateTrunkInterface"] = h.handleStubAssociateTrunkInterface
	// AssociateVpcCidrBlock — moved to handler_ec2core.go
	ops["AttachClassicLinkVpc"] = h.handleStubAttachClassicLinkVpc
	// AttachVerifiedAccessTrustProvider — moved to handler_batch4.go
	ops["AttachVpnGateway"] = h.handleStubAttachVpnGateway
	// AuthorizeClientVpnIngress — moved to handler_batch4.go
	ops["BundleInstance"] = h.handleStubBundleInstance
	ops["CancelBundleTask"] = h.handleStubCancelBundleTask
	ops["CancelCapacityReservationFleets"] = h.handleStubCancelCapacityReservationFleets
	ops["CancelConversionTask"] = h.handleStubCancelConversionTask
	ops["CancelDeclarativePoliciesReport"] = h.handleStubCancelDeclarativePoliciesReport
	ops["CancelExportTask"] = h.handleStubCancelExportTask
	ops["CancelImageLaunchPermission"] = h.handleStubCancelImageLaunchPermission
	ops["CancelImportTask"] = h.handleStubCancelImportTask
	ops["CancelReservedInstancesListing"] = h.handleStubCancelReservedInstancesListing
	ops["CancelSpotFleetRequests"] = h.handleStubCancelSpotFleetRequests
	ops["ConfirmProductInstance"] = h.handleStubConfirmProductInstance
	ops["CopyFpgaImage"] = h.handleStubCopyFpgaImage
	ops["CreateCapacityManagerDataExport"] = h.handleStubCreateCapacityManagerDataExport
	ops["CreateCapacityReservationBySplitting"] = h.handleStubCreateCapacityReservationBySplitting
	ops["CreateCapacityReservationFleet"] = h.handleStubCreateCapacityReservationFleet
	ops["CreateCarrierGateway"] = h.handleStubCreateCarrierGateway
	// CreateClientVpnEndpoint — moved to handler_batch4.go
	// CreateClientVpnRoute — moved to handler_batch4.go
	ops["CreateCoipCidr"] = h.handleStubCreateCoipCidr
	ops["CreateCoipPool"] = h.handleStubCreateCoipPool
	ops["CreateCustomerGateway"] = h.handleStubCreateCustomerGateway
	ops["CreateDelegateMacVolumeOwnershipTask"] = h.handleStubCreateDelegateMacVolumeOwnershipTask
	// CreateEgressOnlyInternetGateway — moved to handler_ec2core.go
	ops["CreateFleet"] = h.handleStubCreateFleet
	ops["CreateFpgaImage"] = h.handleStubCreateFpgaImage
	ops["CreateImageUsageReport"] = h.handleStubCreateImageUsageReport
	ops["CreateInstanceExportTask"] = h.handleStubCreateInstanceExportTask
	ops["CreateInterruptibleCapacityReservationAllocation"] = h.handleStubCreateInterruptibleCapacityReservationAllocation
	ops["CreateIpam"] = h.handleStubCreateIpam
	ops["CreateIpamExternalResourceVerificationToken"] = h.handleStubCreateIpamExternalResourceVerificationToken
	ops["CreateIpamPolicy"] = h.handleStubCreateIpamPolicy
	ops["CreateIpamPool"] = h.handleStubCreateIpamPool
	ops["CreateIpamPrefixListResolver"] = h.handleStubCreateIpamPrefixListResolver
	ops["CreateIpamPrefixListResolverTarget"] = h.handleStubCreateIpamPrefixListResolverTarget
	ops["CreateIpamResourceDiscovery"] = h.handleStubCreateIpamResourceDiscovery
	ops["CreateIpamScope"] = h.handleStubCreateIpamScope
	ops["CreateLocalGatewayRoute"] = h.handleStubCreateLocalGatewayRoute
	ops["CreateLocalGatewayRouteTable"] = h.handleStubCreateLocalGatewayRouteTable
	ops["CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation"] =
		h.handleStubCreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation
	ops["CreateLocalGatewayRouteTableVpcAssociation"] = h.handleStubCreateLocalGatewayRouteTableVpcAssociation
	ops["CreateLocalGatewayVirtualInterface"] = h.handleStubCreateLocalGatewayVirtualInterface
	ops["CreateLocalGatewayVirtualInterfaceGroup"] = h.handleStubCreateLocalGatewayVirtualInterfaceGroup
	ops["CreateMacSystemIntegrityProtectionModificationTask"] =
		h.handleStubCreateMacSystemIntegrityProtectionModificationTask
	// CreateManagedPrefixList — moved to handler_batch4.go
	ops["CreateNetworkInsightsAccessScope"] = h.handleStubCreateNetworkInsightsAccessScope
	ops["CreateNetworkInsightsPath"] = h.handleStubCreateNetworkInsightsPath
	ops["CreateReservedInstancesListing"] = h.handleStubCreateReservedInstancesListing
	ops["CreateRestoreImageTask"] = h.handleStubCreateRestoreImageTask
	ops["CreateRouteServer"] = h.handleStubCreateRouteServer
	ops["CreateRouteServerEndpoint"] = h.handleStubCreateRouteServerEndpoint
	ops["CreateRouteServerPeer"] = h.handleStubCreateRouteServerPeer
	ops["CreateSecondaryNetwork"] = h.handleStubCreateSecondaryNetwork
	ops["CreateSecondarySubnet"] = h.handleStubCreateSecondarySubnet
	ops["CreateStoreImageTask"] = h.handleStubCreateStoreImageTask
	ops["CreateTrafficMirrorFilter"] = h.handleStubCreateTrafficMirrorFilter
	ops["CreateTrafficMirrorFilterRule"] = h.handleStubCreateTrafficMirrorFilterRule
	ops["CreateTrafficMirrorSession"] = h.handleStubCreateTrafficMirrorSession
	ops["CreateTrafficMirrorTarget"] = h.handleStubCreateTrafficMirrorTarget
	// CreateTransitGatewayConnect — moved to handler_batch4.go
	// CreateTransitGatewayConnectPeer — moved to handler_batch4.go
	ops["CreateTransitGatewayMeteringPolicy"] = h.handleStubCreateTransitGatewayMeteringPolicy
	ops["CreateTransitGatewayMeteringPolicyEntry"] = h.handleStubCreateTransitGatewayMeteringPolicyEntry
	ops["CreateTransitGatewayMulticastDomain"] = h.handleStubCreateTransitGatewayMulticastDomain
	// CreateTransitGatewayPeeringAttachment — moved to handler_batch4.go
	ops["CreateTransitGatewayPolicyTable"] = h.handleStubCreateTransitGatewayPolicyTable
	// CreateTransitGatewayPrefixListReference — moved to handler_batch4.go
	// CreateTransitGatewayRoute — moved to handler_ec2core.go
	// CreateTransitGatewayRouteTable — moved to handler_ec2core.go
	ops["CreateTransitGatewayRouteTableAnnouncement"] = h.handleStubCreateTransitGatewayRouteTableAnnouncement
	// CreateVerifiedAccessEndpoint — moved to handler_batch4.go
	// CreateVerifiedAccessGroup — moved to handler_batch4.go
	// CreateVerifiedAccessInstance — moved to handler_batch4.go
	// CreateVerifiedAccessTrustProvider — moved to handler_batch4.go
	ops["CreateVpcBlockPublicAccessExclusion"] = h.handleStubCreateVpcBlockPublicAccessExclusion
	ops["CreateVpcEncryptionControl"] = h.handleStubCreateVpcEncryptionControl
	ops["CreateVpcEndpointServiceConfiguration"] = h.handleStubCreateVpcEndpointServiceConfiguration
	ops["CreateVpnConcentrator"] = h.handleStubCreateVpnConcentrator
	ops["CreateVpnConnection"] = h.handleStubCreateVpnConnection
	ops["CreateVpnGateway"] = h.handleStubCreateVpnGateway
	ops["DeleteCapacityManagerDataExport"] = h.handleStubDeleteCapacityManagerDataExport
	ops["DeleteCarrierGateway"] = h.handleStubDeleteCarrierGateway
	// DeleteClientVpnEndpoint — moved to handler_batch4.go
	// DeleteClientVpnRoute — moved to handler_batch4.go
	ops["DeleteCoipCidr"] = h.handleStubDeleteCoipCidr
	ops["DeleteCoipPool"] = h.handleStubDeleteCoipPool
	ops["DeleteCustomerGateway"] = h.handleStubDeleteCustomerGateway
	// DeleteEgressOnlyInternetGateway — moved to handler_ec2core.go
	ops["DeleteFleets"] = h.handleStubDeleteFleets
	ops["DeleteFpgaImage"] = h.handleStubDeleteFpgaImage
	ops["DeleteImageUsageReport"] = h.handleStubDeleteImageUsageReport
	ops["DeleteIpam"] = h.handleStubDeleteIpam
	ops["DeleteIpamExternalResourceVerificationToken"] = h.handleStubDeleteIpamExternalResourceVerificationToken
	ops["DeleteIpamPolicy"] = h.handleStubDeleteIpamPolicy
	ops["DeleteIpamPool"] = h.handleStubDeleteIpamPool
	ops["DeleteIpamPrefixListResolver"] = h.handleStubDeleteIpamPrefixListResolver
	ops["DeleteIpamPrefixListResolverTarget"] = h.handleStubDeleteIpamPrefixListResolverTarget
	ops["DeleteIpamResourceDiscovery"] = h.handleStubDeleteIpamResourceDiscovery
	ops["DeleteIpamScope"] = h.handleStubDeleteIpamScope
	ops["DeleteLocalGatewayRoute"] = h.handleStubDeleteLocalGatewayRoute
	ops["DeleteLocalGatewayRouteTable"] = h.handleStubDeleteLocalGatewayRouteTable
	ops["DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation"] =
		h.handleStubDeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation
	ops["DeleteLocalGatewayRouteTableVpcAssociation"] = h.handleStubDeleteLocalGatewayRouteTableVpcAssociation
	ops["DeleteLocalGatewayVirtualInterface"] = h.handleStubDeleteLocalGatewayVirtualInterface
	ops["DeleteLocalGatewayVirtualInterfaceGroup"] = h.handleStubDeleteLocalGatewayVirtualInterfaceGroup
	// DeleteManagedPrefixList — moved to handler_batch4.go
	ops["DeleteNetworkInsightsAccessScope"] = h.handleStubDeleteNetworkInsightsAccessScope
	ops["DeleteNetworkInsightsAccessScopeAnalysis"] = h.handleStubDeleteNetworkInsightsAccessScopeAnalysis
	ops["DeleteNetworkInsightsAnalysis"] = h.handleStubDeleteNetworkInsightsAnalysis
	ops["DeleteNetworkInsightsPath"] = h.handleStubDeleteNetworkInsightsPath
	ops["DeleteQueuedReservedInstances"] = h.handleStubDeleteQueuedReservedInstances
	ops["DeleteRouteServer"] = h.handleStubDeleteRouteServer
	ops["DeleteRouteServerEndpoint"] = h.handleStubDeleteRouteServerEndpoint
	ops["DeleteRouteServerPeer"] = h.handleStubDeleteRouteServerPeer
	ops["DeleteSecondaryNetwork"] = h.handleStubDeleteSecondaryNetwork
	ops["DeleteSecondarySubnet"] = h.handleStubDeleteSecondarySubnet
	ops["DeleteTrafficMirrorFilter"] = h.handleStubDeleteTrafficMirrorFilter
	ops["DeleteTrafficMirrorFilterRule"] = h.handleStubDeleteTrafficMirrorFilterRule
	ops["DeleteTrafficMirrorSession"] = h.handleStubDeleteTrafficMirrorSession
	ops["DeleteTrafficMirrorTarget"] = h.handleStubDeleteTrafficMirrorTarget
	// DeleteTransitGatewayConnect — moved to handler_batch4.go
	// DeleteTransitGatewayConnectPeer — moved to handler_batch4.go
	ops["DeleteTransitGatewayMeteringPolicy"] = h.handleStubDeleteTransitGatewayMeteringPolicy
	ops["DeleteTransitGatewayMeteringPolicyEntry"] = h.handleStubDeleteTransitGatewayMeteringPolicyEntry
	ops["DeleteTransitGatewayMulticastDomain"] = h.handleStubDeleteTransitGatewayMulticastDomain
	// DeleteTransitGatewayPeeringAttachment — moved to handler_batch4.go
	ops["DeleteTransitGatewayPolicyTable"] = h.handleStubDeleteTransitGatewayPolicyTable
	// DeleteTransitGatewayPrefixListReference — moved to handler_batch4.go
	// DeleteTransitGatewayRoute — moved to handler_ec2core.go
	// DeleteTransitGatewayRouteTable — moved to handler_ec2core.go
	ops["DeleteTransitGatewayRouteTableAnnouncement"] = h.handleStubDeleteTransitGatewayRouteTableAnnouncement
	// DeleteVerifiedAccessEndpoint — moved to handler_batch4.go
	// DeleteVerifiedAccessGroup — moved to handler_batch4.go
	// DeleteVerifiedAccessInstance — moved to handler_batch4.go
	// DeleteVerifiedAccessTrustProvider — moved to handler_batch4.go
	ops["DeleteVpcBlockPublicAccessExclusion"] = h.handleStubDeleteVpcBlockPublicAccessExclusion
	ops["DeleteVpcEncryptionControl"] = h.handleStubDeleteVpcEncryptionControl
	ops["DeleteVpcEndpointServiceConfigurations"] = h.handleStubDeleteVpcEndpointServiceConfigurations
	ops["DeleteVpnConcentrator"] = h.handleStubDeleteVpnConcentrator
	ops["DeleteVpnConnection"] = h.handleStubDeleteVpnConnection
	ops["DeleteVpnGateway"] = h.handleStubDeleteVpnGateway
	ops["DeprovisionByoipCidr"] = h.handleStubDeprovisionByoipCidr
	ops["DeprovisionIpamByoasn"] = h.handleStubDeprovisionIpamByoasn
	ops["DeprovisionIpamPoolCidr"] = h.handleStubDeprovisionIpamPoolCidr
	ops["DeregisterTransitGatewayMulticastGroupMembers"] = h.handleStubDeregisterTransitGatewayMulticastGroupMembers
	ops["DeregisterTransitGatewayMulticastGroupSources"] = h.handleStubDeregisterTransitGatewayMulticastGroupSources
	ops["DescribeAwsNetworkPerformanceMetricSubscriptions"] = h.handleStubDescribeAwsNetworkPerformanceMetricSubscriptions
	ops["DescribeBundleTasks"] = h.handleStubDescribeBundleTasks
	ops["DescribeCapacityBlockExtensionHistory"] = h.handleStubDescribeCapacityBlockExtensionHistory
	ops["DescribeCapacityBlockExtensionOfferings"] = h.handleStubDescribeCapacityBlockExtensionOfferings
	ops["DescribeCapacityBlockOfferings"] = h.handleStubDescribeCapacityBlockOfferings
	ops["DescribeCapacityBlockStatus"] = h.handleStubDescribeCapacityBlockStatus
	ops["DescribeCapacityBlocks"] = h.handleStubDescribeCapacityBlocks
	ops["DescribeCapacityManagerDataExports"] = h.handleStubDescribeCapacityManagerDataExports
	ops["DescribeCapacityReservationBillingRequests"] = h.handleStubDescribeCapacityReservationBillingRequests
	ops["DescribeCapacityReservationFleets"] = h.handleStubDescribeCapacityReservationFleets
	ops["DescribeCapacityReservationTopology"] = h.handleStubDescribeCapacityReservationTopology
	ops["DescribeCarrierGateways"] = h.handleStubDescribeCarrierGateways
	ops["DescribeClassicLinkInstances"] = h.handleStubDescribeClassicLinkInstances
	// DescribeClientVpnAuthorizationRules — moved to handler_batch4.go
	// DescribeClientVpnConnections — moved to handler_batch4.go
	// DescribeClientVpnEndpoints — moved to handler_batch4.go
	// DescribeClientVpnRoutes — moved to handler_batch4.go
	// DescribeClientVpnTargetNetworks — moved to handler_batch4.go
	ops["DescribeCoipPools"] = h.handleStubDescribeCoipPools
	ops["DescribeConversionTasks"] = h.handleStubDescribeConversionTasks
	ops["DescribeCustomerGateways"] = h.handleStubDescribeCustomerGateways
	ops["DescribeDeclarativePoliciesReports"] = h.handleStubDescribeDeclarativePoliciesReports
	// DescribeEgressOnlyInternetGateways — moved to handler_ec2core.go
	ops["DescribeElasticGpus"] = h.handleStubDescribeElasticGpus
	ops["DescribeExportImageTasks"] = h.handleStubDescribeExportImageTasks
	ops["DescribeExportTasks"] = h.handleStubDescribeExportTasks
	ops["DescribeFleetHistory"] = h.handleStubDescribeFleetHistory
	ops["DescribeFleetInstances"] = h.handleStubDescribeFleetInstances
	ops["DescribeFleets"] = h.handleStubDescribeFleets
	ops["DescribeFpgaImageAttribute"] = h.handleStubDescribeFpgaImageAttribute
	ops["DescribeFpgaImages"] = h.handleStubDescribeFpgaImages
	ops["DescribeHostReservationOfferings"] = h.handleStubDescribeHostReservationOfferings
	ops["DescribeHostReservations"] = h.handleStubDescribeHostReservations
	// DescribeIamInstanceProfileAssociations — moved to handler_ec2core.go
	ops["DescribeImageReferences"] = h.handleStubDescribeImageReferences
	ops["DescribeImageUsageReportEntries"] = h.handleStubDescribeImageUsageReportEntries
	ops["DescribeInstanceSqlHaHistoryStates"] = h.handleStubDescribeInstanceSQLHaHistoryStates
	ops["DescribeInstanceSqlHaStates"] = h.handleStubDescribeInstanceSQLHaStates
	ops["DescribeIpamByoasn"] = h.handleStubDescribeIpamByoasn
	ops["DescribeIpamExternalResourceVerificationTokens"] = h.handleStubDescribeIpamExternalResourceVerificationTokens
	ops["DescribeIpamPolicies"] = h.handleStubDescribeIpamPolicies
	ops["DescribeIpamPools"] = h.handleStubDescribeIpamPools
	ops["DescribeIpamPrefixListResolverTargets"] = h.handleStubDescribeIpamPrefixListResolverTargets
	ops["DescribeIpamPrefixListResolvers"] = h.handleStubDescribeIpamPrefixListResolvers
	ops["DescribeIpamResourceDiscoveries"] = h.handleStubDescribeIpamResourceDiscoveries
	ops["DescribeIpamResourceDiscoveryAssociations"] = h.handleStubDescribeIpamResourceDiscoveryAssociations
	ops["DescribeIpamScopes"] = h.handleStubDescribeIpamScopes
	ops["DescribeIpams"] = h.handleStubDescribeIpams
	ops["DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations"] =
		h.handleStubDescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations
	ops["DescribeLocalGatewayRouteTableVpcAssociations"] = h.handleStubDescribeLocalGatewayRouteTableVpcAssociations
	ops["DescribeLocalGatewayRouteTables"] = h.handleStubDescribeLocalGatewayRouteTables
	ops["DescribeLocalGatewayVirtualInterfaceGroups"] = h.handleStubDescribeLocalGatewayVirtualInterfaceGroups
	ops["DescribeLocalGatewayVirtualInterfaces"] = h.handleStubDescribeLocalGatewayVirtualInterfaces
	ops["DescribeLocalGateways"] = h.handleStubDescribeLocalGateways
	ops["DescribeMacHosts"] = h.handleStubDescribeMacHosts
	ops["DescribeMacModificationTasks"] = h.handleStubDescribeMacModificationTasks
	// DescribeManagedPrefixLists — moved to handler_batch4.go
	ops["DescribeMovingAddresses"] = h.handleStubDescribeMovingAddresses
	ops["DescribeNetworkInsightsAccessScopeAnalyses"] = h.handleStubDescribeNetworkInsightsAccessScopeAnalyses
	ops["DescribeNetworkInsightsAccessScopes"] = h.handleStubDescribeNetworkInsightsAccessScopes
	ops["DescribeNetworkInsightsAnalyses"] = h.handleStubDescribeNetworkInsightsAnalyses
	ops["DescribeNetworkInsightsPaths"] = h.handleStubDescribeNetworkInsightsPaths
	ops["DescribeOutpostLags"] = h.handleStubDescribeOutpostLags
	ops["DescribeReservedInstances"] = h.handleStubDescribeReservedInstances
	ops["DescribeReservedInstancesListings"] = h.handleStubDescribeReservedInstancesListings
	ops["DescribeReservedInstancesModifications"] = h.handleStubDescribeReservedInstancesModifications
	ops["DescribeReservedInstancesOfferings"] = h.handleStubDescribeReservedInstancesOfferings
	ops["DescribeRouteServerEndpoints"] = h.handleStubDescribeRouteServerEndpoints
	ops["DescribeRouteServerPeers"] = h.handleStubDescribeRouteServerPeers
	ops["DescribeRouteServers"] = h.handleStubDescribeRouteServers
	ops["DescribeScheduledInstanceAvailability"] = h.handleStubDescribeScheduledInstanceAvailability
	ops["DescribeScheduledInstances"] = h.handleStubDescribeScheduledInstances
	ops["DescribeSecondaryInterfaces"] = h.handleStubDescribeSecondaryInterfaces
	ops["DescribeSecondaryNetworks"] = h.handleStubDescribeSecondaryNetworks
	ops["DescribeSecondarySubnets"] = h.handleStubDescribeSecondarySubnets
	ops["DescribeServiceLinkVirtualInterfaces"] = h.handleStubDescribeServiceLinkVirtualInterfaces
	ops["DescribeSpotFleetInstances"] = h.handleStubDescribeSpotFleetInstances
	ops["DescribeSpotFleetRequestHistory"] = h.handleStubDescribeSpotFleetRequestHistory
	ops["DescribeSpotFleetRequests"] = h.handleStubDescribeSpotFleetRequests
	ops["DescribeStoreImageTasks"] = h.handleStubDescribeStoreImageTasks
	ops["DescribeTrafficMirrorFilterRules"] = h.handleStubDescribeTrafficMirrorFilterRules
	ops["DescribeTrafficMirrorFilters"] = h.handleStubDescribeTrafficMirrorFilters
	ops["DescribeTrafficMirrorSessions"] = h.handleStubDescribeTrafficMirrorSessions
	ops["DescribeTrafficMirrorTargets"] = h.handleStubDescribeTrafficMirrorTargets
	ops["DescribeTransitGatewayAttachments"] = h.handleStubDescribeTransitGatewayAttachments
	// DescribeTransitGatewayConnectPeers — moved to handler_batch4.go
	// DescribeTransitGatewayConnects — moved to handler_batch4.go
	ops["DescribeTransitGatewayMeteringPolicies"] = h.handleStubDescribeTransitGatewayMeteringPolicies
	ops["DescribeTransitGatewayMulticastDomains"] = h.handleStubDescribeTransitGatewayMulticastDomains
	// DescribeTransitGatewayPeeringAttachments — moved to handler_batch4.go
	ops["DescribeTransitGatewayPolicyTables"] = h.handleStubDescribeTransitGatewayPolicyTables
	ops["DescribeTransitGatewayRouteTableAnnouncements"] = h.handleStubDescribeTransitGatewayRouteTableAnnouncements
	// DescribeTransitGatewayRouteTables — moved to handler_ec2core.go
	ops["DescribeTrunkInterfaceAssociations"] = h.handleStubDescribeTrunkInterfaceAssociations
	// DescribeVerifiedAccessEndpoints — moved to handler_batch4.go
	// DescribeVerifiedAccessGroups — moved to handler_batch4.go
	ops["DescribeVerifiedAccessInstanceLoggingConfigurations"] =
		h.handleStubDescribeVerifiedAccessInstanceLoggingConfigurations
	// DescribeVerifiedAccessInstances — moved to handler_batch4.go
	// DescribeVerifiedAccessTrustProviders — moved to handler_batch4.go
	ops["DescribeVpcBlockPublicAccessExclusions"] = h.handleStubDescribeVpcBlockPublicAccessExclusions
	ops["DescribeVpcBlockPublicAccessOptions"] = h.handleStubDescribeVpcBlockPublicAccessOptions
	ops["DescribeVpcClassicLink"] = h.handleStubDescribeVpcClassicLink
	ops["DescribeVpcClassicLinkDnsSupport"] = h.handleStubDescribeVpcClassicLinkDNSSupport
	ops["DescribeVpcEncryptionControls"] = h.handleStubDescribeVpcEncryptionControls
	ops["DescribeVpcEndpointServiceConfigurations"] = h.handleStubDescribeVpcEndpointServiceConfigurations
	ops["DescribeVpnConcentrators"] = h.handleStubDescribeVpnConcentrators
	ops["DescribeVpnConnections"] = h.handleStubDescribeVpnConnections
	ops["DescribeVpnGateways"] = h.handleStubDescribeVpnGateways
	ops["DetachClassicLinkVpc"] = h.handleStubDetachClassicLinkVpc
	// DetachVerifiedAccessTrustProvider — moved to handler_batch4.go
	ops["DisableAllowedImagesSettings"] = h.handleStubDisableAllowedImagesSettings
	ops["DisableAwsNetworkPerformanceMetricSubscription"] = h.handleStubDisableAwsNetworkPerformanceMetricSubscription
	ops["DisableCapacityManager"] = h.handleStubDisableCapacityManager
	ops["DisableInstanceSqlHaStandbyDetections"] = h.handleStubDisableInstanceSQLHaStandbyDetections
	ops["DisableIpamOrganizationAdminAccount"] = h.handleStubDisableIpamOrganizationAdminAccount
	ops["DisableIpamPolicy"] = h.handleStubDisableIpamPolicy
	ops["DisableRouteServerPropagation"] = h.handleStubDisableRouteServerPropagation
	ops["DisableTransitGatewayRouteTablePropagation"] = h.handleStubDisableTransitGatewayRouteTablePropagation
	ops["DisableVpcClassicLink"] = h.handleStubDisableVpcClassicLink
	ops["DisableVpcClassicLinkDnsSupport"] = h.handleStubDisableVpcClassicLinkDNSSupport
	ops["DisassociateCapacityReservationBillingOwner"] = h.handleStubDisassociateCapacityReservationBillingOwner
	// DisassociateClientVpnTargetNetwork — moved to handler_batch4.go
	ops["DisassociateEnclaveCertificateIamRole"] = h.handleStubDisassociateEnclaveCertificateIamRole
	// DisassociateIamInstanceProfile — moved to handler_ec2core.go
	ops["DisassociateInstanceEventWindow"] = h.handleStubDisassociateInstanceEventWindow
	ops["DisassociateIpamByoasn"] = h.handleStubDisassociateIpamByoasn
	ops["DisassociateIpamResourceDiscovery"] = h.handleStubDisassociateIpamResourceDiscovery
	ops["DisassociateRouteServer"] = h.handleStubDisassociateRouteServer
	ops["DisassociateTransitGatewayMulticastDomain"] = h.handleStubDisassociateTransitGatewayMulticastDomain
	ops["DisassociateTransitGatewayPolicyTable"] = h.handleStubDisassociateTransitGatewayPolicyTable
	// DisassociateTransitGatewayRouteTable — moved to handler_ec2core.go
	ops["DisassociateTrunkInterface"] = h.handleStubDisassociateTrunkInterface
	ops["EnableAllowedImagesSettings"] = h.handleStubEnableAllowedImagesSettings
	ops["EnableAwsNetworkPerformanceMetricSubscription"] = h.handleStubEnableAwsNetworkPerformanceMetricSubscription
	ops["EnableCapacityManager"] = h.handleStubEnableCapacityManager
	ops["EnableInstanceSqlHaStandbyDetections"] = h.handleStubEnableInstanceSQLHaStandbyDetections
	ops["EnableIpamOrganizationAdminAccount"] = h.handleStubEnableIpamOrganizationAdminAccount
	ops["EnableIpamPolicy"] = h.handleStubEnableIpamPolicy
	ops["EnableReachabilityAnalyzerOrganizationSharing"] = h.handleStubEnableReachabilityAnalyzerOrganizationSharing
	ops["EnableRouteServerPropagation"] = h.handleStubEnableRouteServerPropagation
	ops["EnableTransitGatewayRouteTablePropagation"] = h.handleStubEnableTransitGatewayRouteTablePropagation
	ops["EnableVpcClassicLink"] = h.handleStubEnableVpcClassicLink
	ops["EnableVpcClassicLinkDnsSupport"] = h.handleStubEnableVpcClassicLinkDNSSupport
	ops["ExportClientVpnClientCertificateRevocationList"] = h.handleStubExportClientVpnClientCertificateRevocationList
	ops["ExportClientVpnClientConfiguration"] = h.handleStubExportClientVpnClientConfiguration
	ops["ExportTransitGatewayRoutes"] = h.handleStubExportTransitGatewayRoutes
	ops["ExportVerifiedAccessInstanceClientConfiguration"] = h.handleStubExportVerifiedAccessInstanceClientConfiguration
	ops["GetActiveVpnTunnelStatus"] = h.handleStubGetActiveVpnTunnelStatus
	ops["GetAllowedImagesSettings"] = h.handleStubGetAllowedImagesSettings
	ops["GetAssociatedEnclaveCertificateIamRoles"] = h.handleStubGetAssociatedEnclaveCertificateIamRoles
	ops["GetAwsNetworkPerformanceData"] = h.handleStubGetAwsNetworkPerformanceData
	ops["GetCapacityManagerAttributes"] = h.handleStubGetCapacityManagerAttributes
	ops["GetCapacityManagerMetricData"] = h.handleStubGetCapacityManagerMetricData
	ops["GetCapacityManagerMetricDimensions"] = h.handleStubGetCapacityManagerMetricDimensions
	ops["GetCapacityReservationUsage"] = h.handleStubGetCapacityReservationUsage
	ops["GetCoipPoolUsage"] = h.handleStubGetCoipPoolUsage
	ops["GetDeclarativePoliciesReportSummary"] = h.handleStubGetDeclarativePoliciesReportSummary
	ops["GetEnabledIpamPolicy"] = h.handleStubGetEnabledIpamPolicy
	ops["GetFlowLogsIntegrationTemplate"] = h.handleStubGetFlowLogsIntegrationTemplate
	ops["GetHostReservationPurchasePreview"] = h.handleStubGetHostReservationPurchasePreview
	ops["GetImageAncestry"] = h.handleStubGetImageAncestry
	ops["GetInstanceTpmEkPub"] = h.handleStubGetInstanceTpmEkPub
	ops["GetInstanceUefiData"] = h.handleStubGetInstanceUefiData
	ops["GetIpamAddressHistory"] = h.handleStubGetIpamAddressHistory
	ops["GetIpamDiscoveredAccounts"] = h.handleStubGetIpamDiscoveredAccounts
	ops["GetIpamDiscoveredPublicAddresses"] = h.handleStubGetIpamDiscoveredPublicAddresses
	ops["GetIpamDiscoveredResourceCidrs"] = h.handleStubGetIpamDiscoveredResourceCidrs
	ops["GetIpamPolicyAllocationRules"] = h.handleStubGetIpamPolicyAllocationRules
	ops["GetIpamPolicyOrganizationTargets"] = h.handleStubGetIpamPolicyOrganizationTargets
	ops["GetIpamPoolAllocations"] = h.handleStubGetIpamPoolAllocations
	ops["GetIpamPoolCidrs"] = h.handleStubGetIpamPoolCidrs
	ops["GetIpamPrefixListResolverRules"] = h.handleStubGetIpamPrefixListResolverRules
	ops["GetIpamPrefixListResolverVersionEntries"] = h.handleStubGetIpamPrefixListResolverVersionEntries
	ops["GetIpamPrefixListResolverVersions"] = h.handleStubGetIpamPrefixListResolverVersions
	ops["GetIpamResourceCidrs"] = h.handleStubGetIpamResourceCidrs
	// GetManagedPrefixListAssociations — moved to handler_batch4.go
	// GetManagedPrefixListEntries — moved to handler_batch4.go
	ops["GetNetworkInsightsAccessScopeAnalysisFindings"] = h.handleStubGetNetworkInsightsAccessScopeAnalysisFindings
	ops["GetNetworkInsightsAccessScopeContent"] = h.handleStubGetNetworkInsightsAccessScopeContent
	ops["GetReservedInstancesExchangeQuote"] = h.handleStubGetReservedInstancesExchangeQuote
	ops["GetRouteServerAssociations"] = h.handleStubGetRouteServerAssociations
	ops["GetRouteServerPropagations"] = h.handleStubGetRouteServerPropagations
	ops["GetRouteServerRoutingDatabase"] = h.handleStubGetRouteServerRoutingDatabase
	ops["GetSpotPlacementScores"] = h.handleStubGetSpotPlacementScores
	ops["GetTransitGatewayAttachmentPropagations"] = h.handleStubGetTransitGatewayAttachmentPropagations
	ops["GetTransitGatewayMeteringPolicyEntries"] = h.handleStubGetTransitGatewayMeteringPolicyEntries
	ops["GetTransitGatewayMulticastDomainAssociations"] = h.handleStubGetTransitGatewayMulticastDomainAssociations
	ops["GetTransitGatewayPolicyTableAssociations"] = h.handleStubGetTransitGatewayPolicyTableAssociations
	ops["GetTransitGatewayPolicyTableEntries"] = h.handleStubGetTransitGatewayPolicyTableEntries
	// GetTransitGatewayPrefixListReferences — moved to handler_batch4.go
	ops["GetTransitGatewayRouteTableAssociations"] = h.handleStubGetTransitGatewayRouteTableAssociations
	ops["GetTransitGatewayRouteTablePropagations"] = h.handleStubGetTransitGatewayRouteTablePropagations
	ops["GetVerifiedAccessEndpointPolicy"] = h.handleStubGetVerifiedAccessEndpointPolicy
	ops["GetVerifiedAccessEndpointTargets"] = h.handleStubGetVerifiedAccessEndpointTargets
	ops["GetVerifiedAccessGroupPolicy"] = h.handleStubGetVerifiedAccessGroupPolicy
	ops["GetVpcResourcesBlockingEncryptionEnforcement"] = h.handleStubGetVpcResourcesBlockingEncryptionEnforcement
	ops["GetVpnConnectionDeviceSampleConfiguration"] = h.handleStubGetVpnConnectionDeviceSampleConfiguration
	ops["GetVpnConnectionDeviceTypes"] = h.handleStubGetVpnConnectionDeviceTypes
	ops["GetVpnTunnelReplacementStatus"] = h.handleStubGetVpnTunnelReplacementStatus
	ops["ImportClientVpnClientCertificateRevocationList"] = h.handleStubImportClientVpnClientCertificateRevocationList
	ops["ImportInstance"] = h.handleStubImportInstance
	ops["ImportVolume"] = h.handleStubImportVolume
	ops["ModifyAvailabilityZoneGroup"] = h.handleStubModifyAvailabilityZoneGroup
	ops["ModifyCapacityReservationFleet"] = h.handleStubModifyCapacityReservationFleet
	// ModifyClientVpnEndpoint — moved to handler_batch4.go
	ops["ModifyFleet"] = h.handleStubModifyFleet
	ops["ModifyFpgaImageAttribute"] = h.handleStubModifyFpgaImageAttribute
	ops["ModifyHosts"] = h.handleStubModifyHosts
	ops["ModifyInstanceCapacityReservationAttributes"] = h.handleStubModifyInstanceCapacityReservationAttributes
	ops["ModifyInstanceCpuOptions"] = h.handleStubModifyInstanceCPUOptions
	ops["ModifyInstanceEventStartTime"] = h.handleStubModifyInstanceEventStartTime
	ops["ModifyInstanceMaintenanceOptions"] = h.handleStubModifyInstanceMaintenanceOptions
	ops["ModifyInstanceNetworkPerformanceOptions"] = h.handleStubModifyInstanceNetworkPerformanceOptions
	ops["ModifyInstancePlacement"] = h.handleStubModifyInstancePlacement
	ops["ModifyIpam"] = h.handleStubModifyIpam
	ops["ModifyIpamPolicyAllocationRules"] = h.handleStubModifyIpamPolicyAllocationRules
	ops["ModifyIpamPool"] = h.handleStubModifyIpamPool
	ops["ModifyIpamPrefixListResolver"] = h.handleStubModifyIpamPrefixListResolver
	ops["ModifyIpamPrefixListResolverTarget"] = h.handleStubModifyIpamPrefixListResolverTarget
	ops["ModifyIpamResourceCidr"] = h.handleStubModifyIpamResourceCidr
	ops["ModifyIpamResourceDiscovery"] = h.handleStubModifyIpamResourceDiscovery
	ops["ModifyIpamScope"] = h.handleStubModifyIpamScope
	ops["ModifyLocalGatewayRoute"] = h.handleStubModifyLocalGatewayRoute
	// ModifyManagedPrefixList — moved to handler_batch4.go
	ops["ModifyPrivateDnsNameOptions"] = h.handleStubModifyPrivateDNSNameOptions
	ops["ModifyPublicIpDnsNameOptions"] = h.handleStubModifyPublicIPDNSNameOptions
	ops["ModifyReservedInstances"] = h.handleStubModifyReservedInstances
	ops["ModifyRouteServer"] = h.handleStubModifyRouteServer
	ops["ModifySpotFleetRequest"] = h.handleStubModifySpotFleetRequest
	ops["ModifyTrafficMirrorFilterNetworkServices"] = h.handleStubModifyTrafficMirrorFilterNetworkServices
	ops["ModifyTrafficMirrorFilterRule"] = h.handleStubModifyTrafficMirrorFilterRule
	ops["ModifyTrafficMirrorSession"] = h.handleStubModifyTrafficMirrorSession
	ops["ModifyTransitGatewayMeteringPolicy"] = h.handleStubModifyTransitGatewayMeteringPolicy
	ops["ModifyTransitGatewayPrefixListReference"] = h.handleStubModifyTransitGatewayPrefixListReference
	ops["ModifyTransitGatewayVpcAttachment"] = h.handleStubModifyTransitGatewayVpcAttachment
	// ModifyVerifiedAccessEndpoint — moved to handler_batch4.go
	ops["ModifyVerifiedAccessEndpointPolicy"] = h.handleStubModifyVerifiedAccessEndpointPolicy
	ops["ModifyVerifiedAccessGroup"] = h.handleStubModifyVerifiedAccessGroup
	ops["ModifyVerifiedAccessGroupPolicy"] = h.handleStubModifyVerifiedAccessGroupPolicy
	ops["ModifyVerifiedAccessInstance"] = h.handleStubModifyVerifiedAccessInstance
	ops["ModifyVerifiedAccessInstanceLoggingConfiguration"] = h.handleStubModifyVerifiedAccessInstanceLoggingConfiguration
	ops["ModifyVerifiedAccessTrustProvider"] = h.handleStubModifyVerifiedAccessTrustProvider
	ops["ModifyVpcBlockPublicAccessExclusion"] = h.handleStubModifyVpcBlockPublicAccessExclusion
	ops["ModifyVpcBlockPublicAccessOptions"] = h.handleStubModifyVpcBlockPublicAccessOptions
	ops["ModifyVpcEncryptionControl"] = h.handleStubModifyVpcEncryptionControl
	ops["ModifyVpnConnectionOptions"] = h.handleStubModifyVpnConnectionOptions
	ops["ModifyVpnTunnelCertificate"] = h.handleStubModifyVpnTunnelCertificate
	ops["ModifyVpnTunnelOptions"] = h.handleStubModifyVpnTunnelOptions
	ops["MoveAddressToVpc"] = h.handleStubMoveAddressToVpc
	ops["MoveByoipCidrToIpam"] = h.handleStubMoveByoipCidrToIpam
	ops["MoveCapacityReservationInstances"] = h.handleStubMoveCapacityReservationInstances
	ops["ProvisionByoipCidr"] = h.handleStubProvisionByoipCidr
	ops["ProvisionIpamByoasn"] = h.handleStubProvisionIpamByoasn
	ops["ProvisionIpamPoolCidr"] = h.handleStubProvisionIpamPoolCidr
	ops["PurchaseCapacityBlock"] = h.handleStubPurchaseCapacityBlock
	ops["PurchaseCapacityBlockExtension"] = h.handleStubPurchaseCapacityBlockExtension
	ops["PurchaseHostReservation"] = h.handleStubPurchaseHostReservation
	ops["PurchaseReservedInstancesOffering"] = h.handleStubPurchaseReservedInstancesOffering
	ops["PurchaseScheduledInstances"] = h.handleStubPurchaseScheduledInstances
	ops["RegisterTransitGatewayMulticastGroupMembers"] = h.handleStubRegisterTransitGatewayMulticastGroupMembers
	ops["RegisterTransitGatewayMulticastGroupSources"] = h.handleStubRegisterTransitGatewayMulticastGroupSources
	ops["RejectCapacityReservationBillingOwnership"] = h.handleStubRejectCapacityReservationBillingOwnership
	ops["RejectTransitGatewayMulticastDomainAssociations"] = h.handleStubRejectTransitGatewayMulticastDomainAssociations
	ops["RejectTransitGatewayPeeringAttachment"] = h.handleStubRejectTransitGatewayPeeringAttachment
	ops["RejectTransitGatewayVpcAttachment"] = h.handleStubRejectTransitGatewayVpcAttachment
	ops["RejectVpcEndpointConnections"] = h.handleStubRejectVpcEndpointConnections
	ops["RejectVpcPeeringConnection"] = h.handleStubRejectVpcPeeringConnection
	ops["ReleaseHosts"] = h.handleStubReleaseHosts
	ops["ReleaseIpamPoolAllocation"] = h.handleStubReleaseIpamPoolAllocation
	// ReplaceIamInstanceProfileAssociation — moved to handler_ec2core.go
	ops["ReplaceImageCriteriaInAllowedImagesSettings"] = h.handleStubReplaceImageCriteriaInAllowedImagesSettings
	// ReplaceRouteTableAssociation — moved to handler_ec2core.go
	// ReplaceTransitGatewayRoute — moved to handler_ec2core.go
	ops["ReplaceVpnTunnel"] = h.handleStubReplaceVpnTunnel
	ops["RequestSpotFleet"] = h.handleStubRequestSpotFleet
	ops["ResetFpgaImageAttribute"] = h.handleStubResetFpgaImageAttribute
	// RestoreManagedPrefixListVersion — moved to handler_batch4.go
	// RevokeClientVpnIngress — moved to handler_batch4.go
	ops["RunScheduledInstances"] = h.handleStubRunScheduledInstances
	ops["SearchLocalGatewayRoutes"] = h.handleStubSearchLocalGatewayRoutes
	ops["SearchTransitGatewayMulticastGroups"] = h.handleStubSearchTransitGatewayMulticastGroups
	ops["SearchTransitGatewayRoutes"] = h.handleStubSearchTransitGatewayRoutes
	ops["SendDiagnosticInterrupt"] = h.handleStubSendDiagnosticInterrupt
	ops["StartDeclarativePoliciesReport"] = h.handleStubStartDeclarativePoliciesReport
	ops["StartNetworkInsightsAccessScopeAnalysis"] = h.handleStubStartNetworkInsightsAccessScopeAnalysis
	ops["StartNetworkInsightsAnalysis"] = h.handleStubStartNetworkInsightsAnalysis
	ops["StartVpcEndpointServicePrivateDnsVerification"] = h.handleStubStartVpcEndpointServicePrivateDNSVerification
	// TerminateClientVpnConnections — moved to handler_batch4.go
	ops["UnassignPrivateNatGatewayAddress"] = h.handleStubUnassignPrivateNatGatewayAddress
	ops["UpdateCapacityManagerOrganizationsAccess"] = h.handleStubUpdateCapacityManagerOrganizationsAccess
	ops["UpdateInterruptibleCapacityReservationAllocation"] = h.handleStubUpdateInterruptibleCapacityReservationAllocation
	ops["WithdrawByoipCidr"] = h.handleStubWithdrawByoipCidr
	ops["CreatePublicIpv4Pool"] = h.handleStubCreatePublicIpv4Pool
	ops["DeletePublicIpv4Pool"] = h.handleStubDeletePublicIpv4Pool
	ops["DeprovisionPublicIpv4PoolCidr"] = h.handleStubDeprovisionPublicIpv4PoolCidr
	ops["DescribeIpv6Pools"] = h.handleStubDescribeIpv6Pools
	ops["DescribePublicIpv4Pools"] = h.handleStubDescribePublicIpv4Pools
	ops["GetAssociatedIpv6PoolCidrs"] = h.handleStubGetAssociatedIpv6PoolCidrs
	ops["ProvisionPublicIpv4PoolCidr"] = h.handleStubProvisionPublicIpv4PoolCidr
}

//nolint:funlen
func stubSupportedOperations() []string {
	return []string{
		// "AllocateIpamPoolCidr", — moved to advancedNetworkingSupportedOperations
		// "ApplySecurityGroupsToClientVpnTargetNetwork", — moved to batch4SupportedOperations
		"AssociateCapacityReservationBillingOwner",
		// "AssociateClientVpnTargetNetwork", — moved to batch4SupportedOperations
		"AssociateEnclaveCertificateIamRole",
		// AssociateIamInstanceProfile — now in ec2CoreSupportedOperations
		"AssociateInstanceEventWindow",
		"AssociateIpamByoasn",
		"AssociateIpamResourceDiscovery",
		"AssociateRouteServer",
		"AssociateTransitGatewayMulticastDomain",
		"AssociateTransitGatewayPolicyTable",
		// AssociateTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		"AssociateTrunkInterface",
		// AssociateVpcCidrBlock — moved to ec2CoreSupportedOperations
		"AttachClassicLinkVpc",
		// "AttachVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		// "AttachVpnGateway", — moved to advancedNetworkingSupportedOperations
		// "AuthorizeClientVpnIngress", — moved to batch4SupportedOperations
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
		// "CreateClientVpnEndpoint", — moved to batch4SupportedOperations
		// "CreateClientVpnRoute", — moved to batch4SupportedOperations
		"CreateCoipCidr",
		"CreateCoipPool",
		// "CreateCustomerGateway", — moved to advancedNetworkingSupportedOperations
		"CreateDelegateMacVolumeOwnershipTask",
		// CreateEgressOnlyInternetGateway — moved to ec2CoreSupportedOperations
		"CreateFleet",
		"CreateFpgaImage",
		"CreateImageUsageReport",
		"CreateInstanceExportTask",
		"CreateInterruptibleCapacityReservationAllocation",
		// "CreateIpam", — moved to advancedNetworkingSupportedOperations
		"CreateIpamExternalResourceVerificationToken",
		"CreateIpamPolicy",
		// "CreateIpamPool", — moved to advancedNetworkingSupportedOperations
		"CreateIpamPrefixListResolver",
		"CreateIpamPrefixListResolverTarget",
		"CreateIpamResourceDiscovery",
		"CreateIpamScope",
		"CreateLocalGatewayRoute",
		"CreateLocalGatewayRouteTable",
		"CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
		"CreateLocalGatewayRouteTableVpcAssociation",
		"CreateLocalGatewayVirtualInterface",
		"CreateLocalGatewayVirtualInterfaceGroup",
		"CreateMacSystemIntegrityProtectionModificationTask",
		// "CreateManagedPrefixList", — moved to batch4SupportedOperations
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
		"CreateTrafficMirrorFilter",
		"CreateTrafficMirrorFilterRule",
		"CreateTrafficMirrorSession",
		"CreateTrafficMirrorTarget",
		// "CreateTransitGatewayConnect", — moved to batch4SupportedOperations
		// "CreateTransitGatewayConnectPeer", — moved to batch4SupportedOperations
		"CreateTransitGatewayMeteringPolicy",
		"CreateTransitGatewayMeteringPolicyEntry",
		"CreateTransitGatewayMulticastDomain",
		// "CreateTransitGatewayPeeringAttachment", — moved to batch4SupportedOperations
		"CreateTransitGatewayPolicyTable",
		// "CreateTransitGatewayPrefixListReference", — moved to batch4SupportedOperations
		// CreateTransitGatewayRoute — moved to ec2CoreSupportedOperations
		// CreateTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		"CreateTransitGatewayRouteTableAnnouncement",
		// "CreateVerifiedAccessEndpoint", — moved to batch4SupportedOperations
		// "CreateVerifiedAccessGroup", — moved to batch4SupportedOperations
		// "CreateVerifiedAccessInstance", — moved to batch4SupportedOperations
		// "CreateVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		"CreateVpcBlockPublicAccessExclusion",
		"CreateVpcEncryptionControl",
		// "CreateVpcEndpointServiceConfiguration", — moved to advancedNetworkingSupportedOperations
		"CreateVpnConcentrator",
		// "CreateVpnConnection", — moved to advancedNetworkingSupportedOperations
		// "CreateVpnGateway", — moved to advancedNetworkingSupportedOperations
		"DeleteCapacityManagerDataExport",
		"DeleteCarrierGateway",
		// "DeleteClientVpnEndpoint", — moved to batch4SupportedOperations
		// "DeleteClientVpnRoute", — moved to batch4SupportedOperations
		"DeleteCoipCidr",
		"DeleteCoipPool",
		// "DeleteCustomerGateway", — moved to advancedNetworkingSupportedOperations
		// DeleteEgressOnlyInternetGateway — moved to ec2CoreSupportedOperations
		"DeleteFleets",
		"DeleteFpgaImage",
		"DeleteImageUsageReport",
		// "DeleteIpam", — moved to advancedNetworkingSupportedOperations
		"DeleteIpamExternalResourceVerificationToken",
		"DeleteIpamPolicy",
		// "DeleteIpamPool", — moved to advancedNetworkingSupportedOperations
		"DeleteIpamPrefixListResolver",
		"DeleteIpamPrefixListResolverTarget",
		"DeleteIpamResourceDiscovery",
		"DeleteIpamScope",
		"DeleteLocalGatewayRoute",
		"DeleteLocalGatewayRouteTable",
		"DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
		"DeleteLocalGatewayRouteTableVpcAssociation",
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
		"DeleteTransitGatewayMeteringPolicy",
		"DeleteTransitGatewayMeteringPolicyEntry",
		"DeleteTransitGatewayMulticastDomain",
		// "DeleteTransitGatewayPeeringAttachment", — moved to batch4SupportedOperations
		"DeleteTransitGatewayPolicyTable",
		// "DeleteTransitGatewayPrefixListReference", — moved to batch4SupportedOperations
		// DeleteTransitGatewayRoute — moved to ec2CoreSupportedOperations
		// DeleteTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		"DeleteTransitGatewayRouteTableAnnouncement",
		// "DeleteVerifiedAccessEndpoint", — moved to batch4SupportedOperations
		// "DeleteVerifiedAccessGroup", — moved to batch4SupportedOperations
		// "DeleteVerifiedAccessInstance", — moved to batch4SupportedOperations
		// "DeleteVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		"DeleteVpcBlockPublicAccessExclusion",
		"DeleteVpcEncryptionControl",
		// "DeleteVpcEndpointServiceConfigurations", — moved to advancedNetworkingSupportedOperations
		"DeleteVpnConcentrator",
		// "DeleteVpnConnection", — moved to advancedNetworkingSupportedOperations
		// "DeleteVpnGateway", — moved to advancedNetworkingSupportedOperations
		"DeprovisionByoipCidr",
		"DeprovisionIpamByoasn",
		"DeprovisionIpamPoolCidr",
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
		// "DescribeClientVpnAuthorizationRules", — moved to batch4SupportedOperations
		// "DescribeClientVpnConnections", — moved to batch4SupportedOperations
		// "DescribeClientVpnEndpoints", — moved to batch4SupportedOperations
		// "DescribeClientVpnRoutes", — moved to batch4SupportedOperations
		// "DescribeClientVpnTargetNetworks", — moved to batch4SupportedOperations
		"DescribeCoipPools",
		"DescribeConversionTasks",
		// "DescribeCustomerGateways", — moved to advancedNetworkingSupportedOperations
		"DescribeDeclarativePoliciesReports",
		// DescribeEgressOnlyInternetGateways — moved to ec2CoreSupportedOperations
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
		// DescribeIamInstanceProfileAssociations — moved to ec2CoreSupportedOperations
		"DescribeImageReferences",
		"DescribeImageUsageReportEntries",
		"DescribeInstanceSqlHaHistoryStates",
		"DescribeInstanceSqlHaStates",
		"DescribeIpamByoasn",
		"DescribeIpamExternalResourceVerificationTokens",
		"DescribeIpamPolicies",
		// "DescribeIpamPools", — moved to advancedNetworkingSupportedOperations
		"DescribeIpamPrefixListResolverTargets",
		"DescribeIpamPrefixListResolvers",
		"DescribeIpamResourceDiscoveries",
		"DescribeIpamResourceDiscoveryAssociations",
		"DescribeIpamScopes",
		// "DescribeIpams", — moved to advancedNetworkingSupportedOperations
		"DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations",
		"DescribeLocalGatewayRouteTableVpcAssociations",
		"DescribeLocalGatewayRouteTables",
		"DescribeLocalGatewayVirtualInterfaceGroups",
		"DescribeLocalGatewayVirtualInterfaces",
		"DescribeLocalGateways",
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
		"DescribeScheduledInstanceAvailability",
		"DescribeScheduledInstances",
		"DescribeSecondaryInterfaces",
		"DescribeSecondaryNetworks",
		"DescribeSecondarySubnets",
		"DescribeServiceLinkVirtualInterfaces",
		"DescribeStoreImageTasks",
		"DescribeTrafficMirrorFilterRules",
		"DescribeTrafficMirrorFilters",
		"DescribeTrafficMirrorSessions",
		"DescribeTrafficMirrorTargets",
		"DescribeTransitGatewayAttachments",
		// "DescribeTransitGatewayConnectPeers", — moved to batch4SupportedOperations
		// "DescribeTransitGatewayConnects", — moved to batch4SupportedOperations
		"DescribeTransitGatewayMeteringPolicies",
		"DescribeTransitGatewayMulticastDomains",
		// "DescribeTransitGatewayPeeringAttachments", — moved to batch4SupportedOperations
		"DescribeTransitGatewayPolicyTables",
		"DescribeTransitGatewayRouteTableAnnouncements",
		// DescribeTransitGatewayRouteTables — moved to ec2CoreSupportedOperations
		"DescribeTrunkInterfaceAssociations",
		// "DescribeVerifiedAccessEndpoints", — moved to batch4SupportedOperations
		// "DescribeVerifiedAccessGroups", — moved to batch4SupportedOperations
		"DescribeVerifiedAccessInstanceLoggingConfigurations",
		// "DescribeVerifiedAccessInstances", — moved to batch4SupportedOperations
		// "DescribeVerifiedAccessTrustProviders", — moved to batch4SupportedOperations
		"DescribeVpcBlockPublicAccessExclusions",
		"DescribeVpcBlockPublicAccessOptions",
		"DescribeVpcClassicLink",
		"DescribeVpcClassicLinkDnsSupport",
		"DescribeVpcEncryptionControls",
		// "DescribeVpcEndpointServiceConfigurations", — moved to advancedNetworkingSupportedOperations
		"DescribeVpnConcentrators",
		// "DescribeVpnConnections", — moved to advancedNetworkingSupportedOperations
		// "DescribeVpnGateways", — moved to advancedNetworkingSupportedOperations
		"DetachClassicLinkVpc",
		// "DetachVerifiedAccessTrustProvider", — moved to batch4SupportedOperations
		// "DetachVpnGateway", — moved to advancedNetworkingSupportedOperations
		"DisableAllowedImagesSettings",
		"DisableAwsNetworkPerformanceMetricSubscription",
		"DisableCapacityManager",
		"DisableInstanceSqlHaStandbyDetections",
		"DisableIpamOrganizationAdminAccount",
		"DisableIpamPolicy",
		"DisableRouteServerPropagation",
		"DisableTransitGatewayRouteTablePropagation",
		"DisableVpcClassicLink",
		"DisableVpcClassicLinkDnsSupport",
		"DisassociateCapacityReservationBillingOwner",
		// "DisassociateClientVpnTargetNetwork", — moved to batch4SupportedOperations
		"DisassociateEnclaveCertificateIamRole",
		// DisassociateIamInstanceProfile — moved to ec2CoreSupportedOperations
		"DisassociateInstanceEventWindow",
		"DisassociateIpamByoasn",
		"DisassociateIpamResourceDiscovery",
		"DisassociateRouteServer",
		"DisassociateTransitGatewayMulticastDomain",
		"DisassociateTransitGatewayPolicyTable",
		// DisassociateTransitGatewayRouteTable — moved to ec2CoreSupportedOperations
		"DisassociateTrunkInterface",
		"EnableAllowedImagesSettings",
		"EnableAwsNetworkPerformanceMetricSubscription",
		"EnableCapacityManager",
		"EnableInstanceSqlHaStandbyDetections",
		"EnableIpamOrganizationAdminAccount",
		"EnableIpamPolicy",
		"EnableReachabilityAnalyzerOrganizationSharing",
		"EnableRouteServerPropagation",
		"EnableTransitGatewayRouteTablePropagation",
		"EnableVpcClassicLink",
		"EnableVpcClassicLinkDnsSupport",
		"ExportClientVpnClientCertificateRevocationList",
		"ExportClientVpnClientConfiguration",
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
		"GetIpamPoolAllocations",
		// "GetIpamPoolCidrs", — moved to advancedNetworkingSupportedOperations
		"GetIpamPrefixListResolverRules",
		"GetIpamPrefixListResolverVersionEntries",
		"GetIpamPrefixListResolverVersions",
		"GetIpamResourceCidrs",
		// "GetManagedPrefixListAssociations", — moved to batch4SupportedOperations
		// "GetManagedPrefixListEntries", — moved to batch4SupportedOperations
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
		// "GetTransitGatewayPrefixListReferences", — moved to batch4SupportedOperations
		"GetTransitGatewayRouteTableAssociations",
		"GetTransitGatewayRouteTablePropagations",
		"GetVerifiedAccessEndpointPolicy",
		"GetVerifiedAccessEndpointTargets",
		"GetVerifiedAccessGroupPolicy",
		"GetVpcResourcesBlockingEncryptionEnforcement",
		"GetVpnConnectionDeviceSampleConfiguration",
		"GetVpnConnectionDeviceTypes",
		"GetVpnTunnelReplacementStatus",
		"ImportClientVpnClientCertificateRevocationList",
		"ImportInstance",
		"ImportVolume",
		"ModifyAvailabilityZoneGroup",
		"ModifyCapacityReservationFleet",
		// "ModifyClientVpnEndpoint", — moved to batch4SupportedOperations
		"ModifyFleet",
		"ModifyFpgaImageAttribute",
		"ModifyHosts",
		"ModifyInstanceCapacityReservationAttributes",
		"ModifyInstanceCpuOptions",
		"ModifyInstanceEventStartTime",
		"ModifyInstanceMaintenanceOptions",
		"ModifyInstanceNetworkPerformanceOptions",
		"ModifyInstancePlacement",
		"ModifyIpam",
		"ModifyIpamPolicyAllocationRules",
		"ModifyIpamPool",
		"ModifyIpamPrefixListResolver",
		"ModifyIpamPrefixListResolverTarget",
		"ModifyIpamResourceCidr",
		"ModifyIpamResourceDiscovery",
		"ModifyIpamScope",
		"ModifyLocalGatewayRoute",
		// "ModifyManagedPrefixList", — moved to batch4SupportedOperations
		"ModifyPrivateDnsNameOptions",
		"ModifyPublicIpDnsNameOptions",
		"ModifyReservedInstances",
		"ModifyRouteServer",
		"ModifyTrafficMirrorFilterNetworkServices",
		"ModifyTrafficMirrorFilterRule",
		"ModifyTrafficMirrorSession",
		"ModifyTransitGatewayMeteringPolicy",
		"ModifyTransitGatewayPrefixListReference",
		"ModifyTransitGatewayVpcAttachment",
		// "ModifyVerifiedAccessEndpoint", — moved to batch4SupportedOperations
		"ModifyVerifiedAccessEndpointPolicy",
		"ModifyVerifiedAccessGroup",
		"ModifyVerifiedAccessGroupPolicy",
		"ModifyVerifiedAccessInstance",
		"ModifyVerifiedAccessInstanceLoggingConfiguration",
		"ModifyVerifiedAccessTrustProvider",
		"ModifyVpcBlockPublicAccessExclusion",
		"ModifyVpcBlockPublicAccessOptions",
		"ModifyVpcEncryptionControl",
		// "ModifyVpcEndpointServiceConfiguration", — moved to advancedNetworkingSupportedOperations
		"ModifyVpnConnectionOptions",
		"ModifyVpnTunnelCertificate",
		"ModifyVpnTunnelOptions",
		"MoveAddressToVpc",
		"MoveByoipCidrToIpam",
		"MoveCapacityReservationInstances",
		"ProvisionByoipCidr",
		"ProvisionIpamByoasn",
		"ProvisionIpamPoolCidr",
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
		// "RejectVpcPeeringConnection", — moved to advancedNetworkingSupportedOperations
		"ReleaseHosts",
		// "ReleaseIpamPoolAllocation", — moved to advancedNetworkingSupportedOperations
		// ReplaceIamInstanceProfileAssociation — moved to ec2CoreSupportedOperations
		"ReplaceImageCriteriaInAllowedImagesSettings",
		// ReplaceRouteTableAssociation — moved to ec2CoreSupportedOperations
		// ReplaceTransitGatewayRoute — moved to ec2CoreSupportedOperations
		"ReplaceVpnTunnel",
		"ResetFpgaImageAttribute",
		// "RestoreManagedPrefixListVersion", — moved to batch4SupportedOperations
		// "RevokeClientVpnIngress", — moved to batch4SupportedOperations
		"RunScheduledInstances",
		"SearchLocalGatewayRoutes",
		"SearchTransitGatewayMulticastGroups",
		"SearchTransitGatewayRoutes",
		"SendDiagnosticInterrupt",
		"StartDeclarativePoliciesReport",
		"StartNetworkInsightsAccessScopeAnalysis",
		"StartNetworkInsightsAnalysis",
		"StartVpcEndpointServicePrivateDnsVerification",
		// "TerminateClientVpnConnections", — moved to batch4SupportedOperations
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
}

func (h *Handler) handleStubAllocateIpamPoolCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AllocateIpamPoolCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubApplySecurityGroupsToClientVpnTargetNetwork(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ApplySecurityGroupsToClientVpnTargetNetworkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateCapacityReservationBillingOwner(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateCapacityReservationBillingOwnerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateClientVpnTargetNetwork(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateClientVpnTargetNetworkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateEnclaveCertificateIamRole(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateEnclaveCertificateIamRoleResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateInstanceEventWindow(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateInstanceEventWindowResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateIpamByoasn(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateIpamByoasnResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateIpamResourceDiscovery(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateIpamResourceDiscoveryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateRouteServer(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateRouteServerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateTransitGatewayMulticastDomain(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateTransitGatewayMulticastDomainResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateTransitGatewayPolicyTable(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateTransitGatewayPolicyTableResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAssociateTrunkInterface(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateTrunkInterfaceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAttachClassicLinkVpc(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AttachClassicLinkVpcResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAttachVpnGateway(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AttachVpnGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubAuthorizeClientVpnIngress(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "AuthorizeClientVpnIngressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubBundleInstance(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "BundleInstanceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCancelBundleTask(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelBundleTaskResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCancelCapacityReservationFleets(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelCapacityReservationFleetsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCancelConversionTask(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelConversionTaskResponse"},
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

func (h *Handler) handleStubCancelExportTask(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelExportTaskResponse"},
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

func (h *Handler) handleStubCancelImportTask(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelImportTaskResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCancelReservedInstancesListing(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelReservedInstancesListingResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCancelSpotFleetRequests(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelSpotFleetRequestsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubConfirmProductInstance(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ConfirmProductInstanceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCopyFpgaImage(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CopyFpgaImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateCapacityManagerDataExport(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateCapacityManagerDataExportResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateCapacityReservationBySplitting(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateCapacityReservationBySplittingResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateCapacityReservationFleet(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateCapacityReservationFleetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateCarrierGateway(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateCarrierGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateClientVpnEndpoint(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateClientVpnEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateClientVpnRoute(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateClientVpnRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateCoipCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateCoipCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateCoipPool(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateCoipPoolResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateCustomerGateway(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateCustomerGatewayResponse"},
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

func (h *Handler) handleStubCreateFleet(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateFleetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateFpgaImage(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateFpgaImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateImageUsageReport(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateImageUsageReportResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateInstanceExportTask(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateInstanceExportTaskResponse"},
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

func (h *Handler) handleStubCreateIpam(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateIpamResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateIpamExternalResourceVerificationToken(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateIpamExternalResourceVerificationTokenResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateIpamPolicy(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateIpamPolicyResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateIpamPool(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateIpamPoolResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateIpamPrefixListResolver(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateIpamPrefixListResolverResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateIpamPrefixListResolverTarget(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateIpamPrefixListResolverTargetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateIpamResourceDiscovery(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateIpamResourceDiscoveryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateIpamScope(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateIpamScopeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateLocalGatewayRoute(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateLocalGatewayRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateLocalGatewayRouteTable(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateLocalGatewayRouteTableResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName: xml.Name{
			Local: "CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociationResponse",
		},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateLocalGatewayRouteTableVpcAssociation(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateLocalGatewayRouteTableVpcAssociationResponse"},
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

func (h *Handler) handleStubCreateManagedPrefixList(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateManagedPrefixListResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateNetworkInsightsAccessScope(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateNetworkInsightsAccessScopeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateNetworkInsightsPath(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateNetworkInsightsPathResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateReservedInstancesListing(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateReservedInstancesListingResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateRestoreImageTask(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateRestoreImageTaskResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateRouteServer(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateRouteServerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateRouteServerEndpoint(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateRouteServerEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateRouteServerPeer(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateRouteServerPeerResponse"},
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

func (h *Handler) handleStubCreateStoreImageTask(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateStoreImageTaskResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTrafficMirrorFilter(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTrafficMirrorFilterResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTrafficMirrorFilterRule(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTrafficMirrorFilterRuleResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTrafficMirrorSession(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTrafficMirrorSessionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTrafficMirrorTarget(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTrafficMirrorTargetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTransitGatewayConnect(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTransitGatewayConnectResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTransitGatewayMeteringPolicy(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTransitGatewayMeteringPolicyResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTransitGatewayMeteringPolicyEntry(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTransitGatewayMeteringPolicyEntryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTransitGatewayMulticastDomain(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTransitGatewayMulticastDomainResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTransitGatewayPolicyTable(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTransitGatewayPolicyTableResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateTransitGatewayRouteTableAnnouncement(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateTransitGatewayRouteTableAnnouncementResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateVerifiedAccessEndpoint(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVerifiedAccessEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateVerifiedAccessGroup(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVerifiedAccessGroupResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateVerifiedAccessInstance(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVerifiedAccessInstanceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateVpcBlockPublicAccessExclusion(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVpcBlockPublicAccessExclusionResponse"},
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

func (h *Handler) handleStubCreateVpcEndpointServiceConfiguration(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVpcEndpointServiceConfigurationResponse"},
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

func (h *Handler) handleStubCreateVpnConnection(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVpnConnectionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubCreateVpnGateway(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVpnGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteCapacityManagerDataExport(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteCapacityManagerDataExportResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteCarrierGateway(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteCarrierGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteClientVpnEndpoint(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteClientVpnEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteClientVpnRoute(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteClientVpnRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteCoipCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteCoipCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteCoipPool(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteCoipPoolResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteCustomerGateway(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteCustomerGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteFleets(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteFleetsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteFpgaImage(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteFpgaImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteImageUsageReport(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteImageUsageReportResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteIpam(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteIpamResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteIpamExternalResourceVerificationToken(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteIpamExternalResourceVerificationTokenResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteIpamPolicy(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteIpamPolicyResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteIpamPool(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteIpamPoolResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteIpamPrefixListResolver(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteIpamPrefixListResolverResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteIpamPrefixListResolverTarget(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteIpamPrefixListResolverTargetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteIpamResourceDiscovery(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteIpamResourceDiscoveryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteIpamScope(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteIpamScopeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteLocalGatewayRoute(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteLocalGatewayRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteLocalGatewayRouteTable(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteLocalGatewayRouteTableResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName: xml.Name{
			Local: "DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociationResponse",
		},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteLocalGatewayRouteTableVpcAssociation(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteLocalGatewayRouteTableVpcAssociationResponse"},
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

func (h *Handler) handleStubDeleteManagedPrefixList(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteManagedPrefixListResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteNetworkInsightsAccessScope(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInsightsAccessScopeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteNetworkInsightsAccessScopeAnalysis(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInsightsAccessScopeAnalysisResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteNetworkInsightsAnalysis(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInsightsAnalysisResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteNetworkInsightsPath(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteNetworkInsightsPathResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteQueuedReservedInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteQueuedReservedInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteRouteServer(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteRouteServerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteRouteServerEndpoint(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteRouteServerEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteRouteServerPeer(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteRouteServerPeerResponse"},
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

func (h *Handler) handleStubDeleteTrafficMirrorFilter(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTrafficMirrorFilterResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTrafficMirrorFilterRule(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTrafficMirrorFilterRuleResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTrafficMirrorSession(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTrafficMirrorSessionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTrafficMirrorTarget(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTrafficMirrorTargetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTransitGatewayConnect(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayConnectResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTransitGatewayMeteringPolicy(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayMeteringPolicyResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTransitGatewayMeteringPolicyEntry(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayMeteringPolicyEntryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTransitGatewayMulticastDomain(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayMulticastDomainResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTransitGatewayPolicyTable(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayPolicyTableResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteTransitGatewayRouteTableAnnouncement(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteTransitGatewayRouteTableAnnouncementResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteVerifiedAccessEndpoint(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVerifiedAccessEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteVerifiedAccessGroup(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVerifiedAccessGroupResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteVerifiedAccessInstance(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVerifiedAccessInstanceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteVpcBlockPublicAccessExclusion(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpcBlockPublicAccessExclusionResponse"},
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

func (h *Handler) handleStubDeleteVpcEndpointServiceConfigurations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpcEndpointServiceConfigurationsResponse"},
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

func (h *Handler) handleStubDeleteVpnConnection(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpnConnectionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeleteVpnGateway(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpnGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeprovisionByoipCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeprovisionByoipCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeprovisionIpamByoasn(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeprovisionIpamByoasnResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeprovisionIpamPoolCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeprovisionIpamPoolCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeregisterTransitGatewayMulticastGroupMembers(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeregisterTransitGatewayMulticastGroupMembersResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeregisterTransitGatewayMulticastGroupSources(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeregisterTransitGatewayMulticastGroupSourcesResponse"},
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

func (h *Handler) handleStubDescribeBundleTasks(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeBundleTasksResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityBlockExtensionHistory(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityBlockExtensionHistoryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityBlockExtensionOfferings(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityBlockExtensionOfferingsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityBlockOfferings(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityBlockOfferingsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityBlockStatus(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityBlockStatusResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityBlocks(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityBlocksResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityManagerDataExports(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityManagerDataExportsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityReservationBillingRequests(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityReservationBillingRequestsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCapacityReservationFleets(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCapacityReservationFleetsResponse"},
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

func (h *Handler) handleStubDescribeCarrierGateways(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCarrierGatewaysResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeClassicLinkInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeClassicLinkInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeClientVpnConnections(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeClientVpnConnectionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeClientVpnEndpoints(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeClientVpnEndpointsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeClientVpnRoutes(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeClientVpnRoutesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCoipPools(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCoipPoolsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeConversionTasks(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeConversionTasksResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeCustomerGateways(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeCustomerGatewaysResponse"},
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

func (h *Handler) handleStubDescribeExportImageTasks(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeExportImageTasksResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeExportTasks(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeExportTasksResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeFleetHistory(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeFleetHistoryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeFleetInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeFleetInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeFleets(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeFleetsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeFpgaImageAttribute(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeFpgaImageAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeFpgaImages(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeFpgaImagesResponse"},
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

func (h *Handler) handleStubDescribeImageUsageReportEntries(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeImageUsageReportEntriesResponse"},
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

func (h *Handler) handleStubDescribeIpamByoasn(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamByoasnResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpamExternalResourceVerificationTokens(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamExternalResourceVerificationTokensResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpamPolicies(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamPoliciesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpamPools(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamPoolsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpamPrefixListResolverTargets(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamPrefixListResolverTargetsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpamPrefixListResolvers(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamPrefixListResolversResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpamResourceDiscoveries(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamResourceDiscoveriesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpamResourceDiscoveryAssociations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamResourceDiscoveryAssociationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpamScopes(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamScopesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpams(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpamsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName: xml.Name{
			Local: "DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociationsResponse",
		},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeLocalGatewayRouteTableVpcAssociations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeLocalGatewayRouteTableVpcAssociationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeLocalGatewayRouteTables(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeLocalGatewayRouteTablesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeLocalGatewayVirtualInterfaceGroups(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeLocalGatewayVirtualInterfaceGroupsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeLocalGatewayVirtualInterfaces(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeLocalGatewayVirtualInterfacesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeLocalGateways(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeLocalGatewaysResponse"},
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

func (h *Handler) handleStubDescribeManagedPrefixLists(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeManagedPrefixListsResponse"},
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

func (h *Handler) handleStubDescribeNetworkInsightsAccessScopeAnalyses(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeNetworkInsightsAccessScopeAnalysesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeNetworkInsightsAccessScopes(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeNetworkInsightsAccessScopesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeNetworkInsightsAnalyses(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeNetworkInsightsAnalysesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeNetworkInsightsPaths(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeNetworkInsightsPathsResponse"},
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

func (h *Handler) handleStubDescribeReservedInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeReservedInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeReservedInstancesListings(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeReservedInstancesListingsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeReservedInstancesModifications(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeReservedInstancesModificationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeReservedInstancesOfferings(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeReservedInstancesOfferingsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeRouteServerEndpoints(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeRouteServerEndpointsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeRouteServerPeers(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeRouteServerPeersResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeRouteServers(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeRouteServersResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeScheduledInstanceAvailability(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeScheduledInstanceAvailabilityResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeScheduledInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeScheduledInstancesResponse"},
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

func (h *Handler) handleStubDescribeSpotFleetInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeSpotFleetInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeSpotFleetRequestHistory(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeSpotFleetRequestHistoryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeSpotFleetRequests(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeSpotFleetRequestsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeStoreImageTasks(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeStoreImageTasksResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTrafficMirrorFilterRules(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTrafficMirrorFilterRulesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTrafficMirrorFilters(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTrafficMirrorFiltersResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTrafficMirrorSessions(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTrafficMirrorSessionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTrafficMirrorTargets(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTrafficMirrorTargetsResponse"},
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

func (h *Handler) handleStubDescribeTransitGatewayMeteringPolicies(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTransitGatewayMeteringPoliciesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTransitGatewayMulticastDomains(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTransitGatewayMulticastDomainsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTransitGatewayPolicyTables(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTransitGatewayPolicyTablesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTransitGatewayRouteTableAnnouncements(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTransitGatewayRouteTableAnnouncementsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeTrunkInterfaceAssociations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeTrunkInterfaceAssociationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVerifiedAccessGroups(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVerifiedAccessGroupsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVerifiedAccessInstanceLoggingConfigurations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVerifiedAccessInstanceLoggingConfigurationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVpcBlockPublicAccessExclusions(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpcBlockPublicAccessExclusionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVpcBlockPublicAccessOptions(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpcBlockPublicAccessOptionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVpcClassicLink(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpcClassicLinkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVpcClassicLinkDNSSupport(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpcClassicLinkDnsSupportResponse"},
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

func (h *Handler) handleStubDescribeVpcEndpointServiceConfigurations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpcEndpointServiceConfigurationsResponse"},
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

func (h *Handler) handleStubDescribeVpnConnections(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpnConnectionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeVpnGateways(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeVpnGatewaysResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDetachClassicLinkVpc(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DetachClassicLinkVpcResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisableAllowedImagesSettings(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableAllowedImagesSettingsResponse"},
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

func (h *Handler) handleStubDisableCapacityManager(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableCapacityManagerResponse"},
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

func (h *Handler) handleStubDisableIpamOrganizationAdminAccount(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableIpamOrganizationAdminAccountResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisableIpamPolicy(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableIpamPolicyResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisableRouteServerPropagation(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableRouteServerPropagationResponse"},
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

func (h *Handler) handleStubDisableVpcClassicLink(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableVpcClassicLinkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisableVpcClassicLinkDNSSupport(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableVpcClassicLinkDnsSupportResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisassociateCapacityReservationBillingOwner(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateCapacityReservationBillingOwnerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisassociateEnclaveCertificateIamRole(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateEnclaveCertificateIamRoleResponse"},
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

func (h *Handler) handleStubDisassociateIpamByoasn(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateIpamByoasnResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisassociateIpamResourceDiscovery(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateIpamResourceDiscoveryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisassociateRouteServer(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateRouteServerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisassociateTransitGatewayMulticastDomain(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateTransitGatewayMulticastDomainResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisassociateTransitGatewayPolicyTable(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateTransitGatewayPolicyTableResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDisassociateTrunkInterface(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateTrunkInterfaceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubEnableAllowedImagesSettings(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableAllowedImagesSettingsResponse"},
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

func (h *Handler) handleStubEnableCapacityManager(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableCapacityManagerResponse"},
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

func (h *Handler) handleStubEnableIpamOrganizationAdminAccount(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableIpamOrganizationAdminAccountResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubEnableIpamPolicy(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableIpamPolicyResponse"},
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

func (h *Handler) handleStubEnableRouteServerPropagation(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableRouteServerPropagationResponse"},
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

func (h *Handler) handleStubEnableVpcClassicLink(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableVpcClassicLinkResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubEnableVpcClassicLinkDNSSupport(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableVpcClassicLinkDnsSupportResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubExportClientVpnClientCertificateRevocationList(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ExportClientVpnClientCertificateRevocationListResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubExportClientVpnClientConfiguration(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ExportClientVpnClientConfigurationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubExportTransitGatewayRoutes(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ExportTransitGatewayRoutesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubExportVerifiedAccessInstanceClientConfiguration(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ExportVerifiedAccessInstanceClientConfigurationResponse"},
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

func (h *Handler) handleStubGetAllowedImagesSettings(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetAllowedImagesSettingsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetAssociatedEnclaveCertificateIamRoles(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetAssociatedEnclaveCertificateIamRolesResponse"},
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

func (h *Handler) handleStubGetCapacityManagerAttributes(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetCapacityManagerAttributesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetCapacityManagerMetricData(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetCapacityManagerMetricDataResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetCapacityManagerMetricDimensions(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetCapacityManagerMetricDimensionsResponse"},
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

func (h *Handler) handleStubGetCoipPoolUsage(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetCoipPoolUsageResponse"},
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

func (h *Handler) handleStubGetEnabledIpamPolicy(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetEnabledIpamPolicyResponse"},
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

func (h *Handler) handleStubGetIpamAddressHistory(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamAddressHistoryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamDiscoveredAccounts(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamDiscoveredAccountsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamDiscoveredPublicAddresses(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamDiscoveredPublicAddressesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamDiscoveredResourceCidrs(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamDiscoveredResourceCidrsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamPolicyAllocationRules(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamPolicyAllocationRulesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamPolicyOrganizationTargets(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamPolicyOrganizationTargetsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamPoolAllocations(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamPoolAllocationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamPoolCidrs(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamPoolCidrsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamPrefixListResolverRules(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamPrefixListResolverRulesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamPrefixListResolverVersionEntries(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamPrefixListResolverVersionEntriesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamPrefixListResolverVersions(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamPrefixListResolverVersionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetIpamResourceCidrs(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetIpamResourceCidrsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetManagedPrefixListEntries(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetManagedPrefixListEntriesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetNetworkInsightsAccessScopeAnalysisFindings(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetNetworkInsightsAccessScopeAnalysisFindingsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetNetworkInsightsAccessScopeContent(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetNetworkInsightsAccessScopeContentResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetReservedInstancesExchangeQuote(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetReservedInstancesExchangeQuoteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetRouteServerAssociations(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetRouteServerAssociationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetRouteServerPropagations(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetRouteServerPropagationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetRouteServerRoutingDatabase(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetRouteServerRoutingDatabaseResponse"},
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

func (h *Handler) handleStubGetTransitGatewayAttachmentPropagations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetTransitGatewayAttachmentPropagationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetTransitGatewayMeteringPolicyEntries(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetTransitGatewayMeteringPolicyEntriesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetTransitGatewayMulticastDomainAssociations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetTransitGatewayMulticastDomainAssociationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetTransitGatewayPolicyTableAssociations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetTransitGatewayPolicyTableAssociationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetTransitGatewayPolicyTableEntries(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetTransitGatewayPolicyTableEntriesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetTransitGatewayRouteTableAssociations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetTransitGatewayRouteTableAssociationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetTransitGatewayRouteTablePropagations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetTransitGatewayRouteTablePropagationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetVerifiedAccessEndpointPolicy(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetVerifiedAccessEndpointPolicyResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetVerifiedAccessEndpointTargets(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetVerifiedAccessEndpointTargetsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetVerifiedAccessGroupPolicy(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetVerifiedAccessGroupPolicyResponse"},
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

func (h *Handler) handleStubGetVpnConnectionDeviceSampleConfiguration(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetVpnConnectionDeviceSampleConfigurationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetVpnConnectionDeviceTypes(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetVpnConnectionDeviceTypesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetVpnTunnelReplacementStatus(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetVpnTunnelReplacementStatusResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubImportClientVpnClientCertificateRevocationList(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ImportClientVpnClientCertificateRevocationListResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubImportInstance(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ImportInstanceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubImportVolume(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ImportVolumeResponse"},
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

func (h *Handler) handleStubModifyCapacityReservationFleet(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyCapacityReservationFleetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyClientVpnEndpoint(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyClientVpnEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyFleet(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyFleetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyFpgaImageAttribute(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyFpgaImageAttributeResponse"},
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

func (h *Handler) handleStubModifyIpam(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIpamResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyIpamPolicyAllocationRules(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIpamPolicyAllocationRulesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyIpamPool(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIpamPoolResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyIpamPrefixListResolver(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIpamPrefixListResolverResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyIpamPrefixListResolverTarget(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIpamPrefixListResolverTargetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyIpamResourceCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIpamResourceCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyIpamResourceDiscovery(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIpamResourceDiscoveryResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyIpamScope(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIpamScopeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyLocalGatewayRoute(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyLocalGatewayRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyManagedPrefixList(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyManagedPrefixListResponse"},
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

func (h *Handler) handleStubModifyReservedInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyReservedInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyRouteServer(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyRouteServerResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifySpotFleetRequest(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifySpotFleetRequestResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyTrafficMirrorFilterNetworkServices(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTrafficMirrorFilterNetworkServicesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyTrafficMirrorFilterRule(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTrafficMirrorFilterRuleResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyTrafficMirrorSession(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTrafficMirrorSessionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyTransitGatewayMeteringPolicy(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTransitGatewayMeteringPolicyResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyTransitGatewayPrefixListReference(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTransitGatewayPrefixListReferenceResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyTransitGatewayVpcAttachment(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTransitGatewayVpcAttachmentResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVerifiedAccessEndpoint(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVerifiedAccessEndpointResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVerifiedAccessEndpointPolicy(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVerifiedAccessEndpointPolicyResponse"},
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

func (h *Handler) handleStubModifyVerifiedAccessGroupPolicy(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVerifiedAccessGroupPolicyResponse"},
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

func (h *Handler) handleStubModifyVerifiedAccessInstanceLoggingConfiguration(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVerifiedAccessInstanceLoggingConfigurationResponse"},
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

func (h *Handler) handleStubModifyVpcBlockPublicAccessExclusion(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcBlockPublicAccessExclusionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVpcBlockPublicAccessOptions(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpcBlockPublicAccessOptionsResponse"},
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

func (h *Handler) handleStubModifyVpnConnectionOptions(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpnConnectionOptionsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVpnTunnelCertificate(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpnTunnelCertificateResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubModifyVpnTunnelOptions(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyVpnTunnelOptionsResponse"},
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

func (h *Handler) handleStubMoveByoipCidrToIpam(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "MoveByoipCidrToIpamResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubMoveCapacityReservationInstances(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "MoveCapacityReservationInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubProvisionByoipCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ProvisionByoipCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubProvisionIpamByoasn(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ProvisionIpamByoasnResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubProvisionIpamPoolCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ProvisionIpamPoolCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubPurchaseCapacityBlock(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "PurchaseCapacityBlockResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubPurchaseCapacityBlockExtension(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "PurchaseCapacityBlockExtensionResponse"},
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

func (h *Handler) handleStubPurchaseReservedInstancesOffering(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "PurchaseReservedInstancesOfferingResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubPurchaseScheduledInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "PurchaseScheduledInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRegisterTransitGatewayMulticastGroupMembers(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RegisterTransitGatewayMulticastGroupMembersResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRegisterTransitGatewayMulticastGroupSources(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RegisterTransitGatewayMulticastGroupSourcesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRejectCapacityReservationBillingOwnership(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RejectCapacityReservationBillingOwnershipResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRejectTransitGatewayMulticastDomainAssociations(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RejectTransitGatewayMulticastDomainAssociationsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRejectTransitGatewayPeeringAttachment(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RejectTransitGatewayPeeringAttachmentResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRejectTransitGatewayVpcAttachment(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RejectTransitGatewayVpcAttachmentResponse"},
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

func (h *Handler) handleStubRejectVpcPeeringConnection(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RejectVpcPeeringConnectionResponse"},
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

func (h *Handler) handleStubReleaseIpamPoolAllocation(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ReleaseIpamPoolAllocationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubReplaceImageCriteriaInAllowedImagesSettings(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ReplaceImageCriteriaInAllowedImagesSettingsResponse"},
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

func (h *Handler) handleStubRequestSpotFleet(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RequestSpotFleetResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubResetFpgaImageAttribute(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetFpgaImageAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRevokeClientVpnIngress(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RevokeClientVpnIngressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubRunScheduledInstances(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "RunScheduledInstancesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubSearchLocalGatewayRoutes(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "SearchLocalGatewayRoutesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubSearchTransitGatewayMulticastGroups(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "SearchTransitGatewayMulticastGroupsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubSearchTransitGatewayRoutes(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "SearchTransitGatewayRoutesResponse"},
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

func (h *Handler) handleStubStartNetworkInsightsAccessScopeAnalysis(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "StartNetworkInsightsAccessScopeAnalysisResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubStartNetworkInsightsAnalysis(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "StartNetworkInsightsAnalysisResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubStartVpcEndpointServicePrivateDNSVerification(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "StartVpcEndpointServicePrivateDnsVerificationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubTerminateClientVpnConnections(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "TerminateClientVpnConnectionsResponse"},
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

func (h *Handler) handleStubUpdateCapacityManagerOrganizationsAccess(
	_ url.Values,
	reqID string,
) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "UpdateCapacityManagerOrganizationsAccessResponse"},
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

func (h *Handler) handleStubWithdrawByoipCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "WithdrawByoipCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// ---- Additional IPv4/IPv6 stub handlers (SDK naming uses Ipv4/Ipv6) ----

func (h *Handler) handleStubCreatePublicIpv4Pool(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "CreatePublicIpv4PoolResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeletePublicIpv4Pool(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeletePublicIpv4PoolResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDeprovisionPublicIpv4PoolCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DeprovisionPublicIpv4PoolCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribeIpv6Pools(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribeIpv6PoolsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubDescribePublicIpv4Pools(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "DescribePublicIpv4PoolsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubGetAssociatedIpv6PoolCidrs(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "GetAssociatedIpv6PoolCidrsResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleStubProvisionPublicIpv4PoolCidr(_ url.Values, reqID string) (any, error) {
	return &stubResponse{
		XMLName:   xml.Name{Local: "ProvisionPublicIpv4PoolCidrResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}
