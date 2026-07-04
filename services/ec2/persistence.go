package ec2

import (
	"encoding/json"
	"log/slog"
)

// snapTGWMcastAssoc is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWMcastAssoc = TransitGatewayMulticastDomainAssociation

// snapRIExchange is a type alias used in backendSnapshot to keep line lengths manageable.
type snapRIExchange = ReservedInstancesExchange

// snapTGWPeeringAtt is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWPeeringAtt = TransitGatewayPeeringAttachment

// snapTGWVpcAtt is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWVpcAtt = TransitGatewayVpcAttachment

// snapLGWVifGroupAssoc is a type alias used in backendSnapshot to keep line lengths manageable.
type snapLGWVifGroupAssoc = LocalGatewayRouteTableVirtualInterfaceGroupAssociation

// snapLGWVpcAssoc is a type alias used in backendSnapshot to keep line lengths manageable.
type snapLGWVpcAssoc = LocalGatewayRouteTableVpcAssociation

// snapTGWMeterPolicy is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWMeterPolicy = TransitGatewayMeteringPolicy

// snapTGWMeterPolicyEntry is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWMeterPolicyEntry = TransitGatewayMeteringPolicyEntry

// snapTGWMcastGroupEntry is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWMcastGroupEntry = TransitGatewayMulticastGroupEntry

// snapClassicLinkInstance is a type alias used in backendSnapshot to keep line lengths manageable.
type snapClassicLinkInstance = ClassicLinkInstance

// snapIpamVerifyToken is a type alias used in backendSnapshot to keep line lengths manageable.
type snapIpamVerifyToken = IpamExternalResourceVerificationToken

// snapTGWRTProp is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWRTProp = TransitGatewayRouteTablePropagation

// snapInterruptibleCRAlloc is a type alias used in backendSnapshot to keep line lengths manageable.
type snapInterruptibleCRAlloc = InterruptibleCapacityReservationAllocation

type backendSnapshot struct {
	SnapshotAttributes             map[string]map[string]string                    `json:"snapshotAttributes"`
	ImageDeprecated                map[string]string                               `json:"imageDeprecated"`
	VPCs                           map[string]*VPC                                 `json:"vpcs,omitempty"`
	NatGateways                    map[string]*NatGateway                          `json:"natGateways,omitempty"`
	KeyPairs                       map[string]*KeyPair                             `json:"keyPairs,omitempty"`
	Volumes                        map[string]*Volume                              `json:"volumes,omitempty"`
	Addresses                      map[string]*Address                             `json:"addresses,omitempty"`
	InternetGateways               map[string]*InternetGateway                     `json:"internetGateways"`
	SecurityGroups                 map[string]*SecurityGroup                       `json:"securityGroups"`
	Instances                      map[string]*Instance                            `json:"instances,omitempty"`
	Subnets                        map[string]*Subnet                              `json:"subnets,omitempty"`
	SpotRequests                   map[string]*SpotInstanceRequest                 `json:"spotRequests,omitempty"`
	PlacementGroups                map[string]*PlacementGroup                      `json:"placementGroups"`
	Images                         map[string]*AMIStub                             `json:"images,omitempty"`
	ImageUsageReports              map[string]*ImageUsageReport                    `json:"imageUsageReports"`
	LaunchTemplates                map[string]*LaunchTemplate                      `json:"launchTemplates"`
	VpcEndpoints                   map[string]*VpcEndpoint                         `json:"vpcEndpoints,omitempty"`
	Snapshots                      map[string]*Snapshot                            `json:"snapshots,omitempty"`
	NetworkACLs                    map[string]*StoredNetworkACL                    `json:"networkACLs,omitempty"`
	TransitGateways                map[string]*TransitGateway                      `json:"transitGateways"`
	FlowLogs                       map[string]*FlowLog                             `json:"flowLogs,omitempty"`
	DhcpOptionSets                 map[string]*DhcpOptions                         `json:"dhcpOptionSets"`
	Tags                           map[string]map[string]string                    `json:"tags,omitempty"`
	AddressTransfers               map[string]*AddressTransfer                     `json:"addressTransfers"`
	CapacityReservations           map[string]*CapacityReservation                 `json:"capacityReservations"`
	ReservedInstancesExchanges     map[string]*snapRIExchange                      `json:"reservedInstancesExchanges"`
	TGWMulticastDomainAssociations map[string]*snapTGWMcastAssoc                   `json:"tgwMcastDomainAssoc"`
	TGWPeeringAttachments          map[string]*snapTGWPeeringAtt                   `json:"tgwPeeringAttachments"`
	TGWVpcAttachments              map[string]*snapTGWVpcAtt                       `json:"tgwVpcAttachments"`
	VpcEndpointConnections         map[string]*VpcEndpointConnection               `json:"vpcEndpointConnections"`
	VpcPeeringConnections          map[string]*VpcPeeringConnection                `json:"vpcPeeringConnections"`
	ByoipCidrs                     map[string]*ByoipCidr                           `json:"byoipCidrs,omitempty"`
	DedicatedHosts                 map[string]*Host                                `json:"dedicatedHosts"`
	VpnGateways                    map[string]*VpnGateway                          `json:"vpnGateways,omitempty"`
	CustomerGateways               map[string]*CustomerGateway                     `json:"customerGateways"`
	Ipams                          map[string]*Ipam                                `json:"ipams,omitempty"`
	IpamScopes                     map[string]*IpamScope                           `json:"ipamScopes,omitempty"`
	IpamPools                      map[string]*IpamPool                            `json:"ipamPools,omitempty"`
	IpamPoolCidrs                  map[string][]*IpamPoolCidr                      `json:"ipamPoolCidrs,omitempty"`
	IpamPoolAllocations            map[string]*IpamPoolAllocation                  `json:"ipamPoolAllocations"`
	IpamResourceDiscoveries        map[string]*IpamResourceDiscovery               `json:"ipamResourceDiscoveries"`
	IpamResourceDiscoveryAssocs    map[string]*IpamResourceDiscoveryAssociation    `json:"ipamResourceDiscoveryAssocs"`
	IpamByoasns                    map[string]*IpamByoasn                          `json:"ipamByoasns,omitempty"`
	IpamAsnAssociations            map[string]*IpamAsnAssociation                  `json:"ipamAsnAssocs,omitempty"`
	IpamVerificationTokens         map[string]*snapIpamVerifyToken                 `json:"ipamVerifyTokens,omitempty"`
	IpamResourceCidrs              map[string]*IpamResourceCidr                    `json:"ipamResourceCidrs,omitempty"`
	IpamPrefixListResolvers        map[string]*IpamPrefixListResolver              `json:"ipamPLResolvers,omitempty"`
	IpamPrefixListResolverVersions map[string][]int64                              `json:"ipamPLRVersions,omitempty"`
	IpamPrefixListResolverTargets  map[string]*IpamPrefixListResolverTarget        `json:"ipamPLRTargets,omitempty"`
	CarrierGateways                map[string]*CarrierGateway                      `json:"carrierGateways"`
	Fleets                         map[string]*Fleet                               `json:"fleets,omitempty"`
	NetworkInsightsPaths           map[string]*NetworkInsightsPath                 `json:"networkInsightsPaths"`
	ManagedPrefixLists             map[string]*ManagedPrefixList                   `json:"managedPrefixLists"`
	EgressOnlyIGWs                 map[string]*EgressOnlyInternetGateway           `json:"egressOnlyIGWs"`
	IamAssociations                map[string]*IamInstanceProfileAssociation       `json:"iamAssociations"`
	TgwRouteTables                 map[string]*TransitGatewayRouteTable            `json:"tgwRouteTables"`
	TgwRoutes                      map[string]*TransitGatewayRoute                 `json:"tgwRoutes,omitempty"`
	TgwRTAssociations              map[string]*TransitGatewayRouteTableAssociation `json:"tgwRTAssociations"`

	// TGW policy tables and route table announcements are kept in their own
	// gofmt alignment group: their long type names would otherwise widen the
	// tag column for the much larger field block above.
	TgwPolicyTables            map[string]*TransitGatewayPolicyTable            `json:"tgwPolicyTables,omitempty"`
	TgwPolicyTableAssociations map[string]*TransitGatewayPolicyTableAssociation `json:"tgwPTAssocs,omitempty"`
	TgwRouteTableAnnouncements map[string]*TransitGatewayRouteTableAnnouncement `json:"tgwRTAnn,omitempty"`

	ReservedInstancesModifications map[string]*ReservedInstancesModification     `json:"rim"`
	ReservedInstancesListings      map[string]*ReservedInstancesListing          `json:"reservedInstancesListings"`
	VpcCidrAssociations            map[string]*VpcCidrBlockAssociation           `json:"vpcCidrAssociations"`
	VpnConnections                 map[string]*VpnConnection                     `json:"vpnConnections"`
	VpcEndpointServiceConfigs      map[string]*VpcEndpointServiceConfig          `json:"vpcEndpointServiceConfigs"`
	SpotFleets                     map[string]*SpotFleetRequest                  `json:"spotFleets,omitempty"`
	SpotFleetHistory               map[string][]SpotFleetHistoryRecord           `json:"spotFleetHistory"`
	VolumeModifications            map[string]*VolumeModification                `json:"volumeModifications"`
	SnapshotTiers                  map[string]string                             `json:"snapshotTiers,omitempty"`
	NetworkInterfaces              map[string]*NetworkInterface                  `json:"networkInterfaces"`
	RouteTables                    map[string]*RouteTable                        `json:"routeTables,omitempty"`
	TrafficMirrorFilters           map[string]*TrafficMirrorFilter               `json:"trafficMirrorFilters"`
	VpcPeeringOptions              map[string]*PeeringConnectionOptions          `json:"vpcPeeringOptions"`
	SubnetCIDRAssociations         map[string][]*SubnetCIDRAssociation           `json:"subnetCIDRAssociations"`
	AddressAttributes              map[string]*AddressAttribute                  `json:"addressAttributes"`
	InstanceMonitoring             map[string]string                             `json:"instanceMonitoring"`
	InstanceCreditSpecs            map[string]string                             `json:"instanceCreditSpecs"`
	InstanceIMDSOptions            map[string]*IMDSOptions                       `json:"instanceIMDSOptions"`
	InstanceMetadataDefaults       *InstanceMetadataDefaults                     `json:"instanceMetadataDefaults"`
	InstanceEventNotifAttrs        *InstanceEventNotificationAttributes          `json:"instanceEventNotifAttrs"`
	NiPermissions                  map[string]*NetworkInterfacePermission        `json:"niPermissions,omitempty"`
	NiIPv6Addresses                map[string][]string                           `json:"niIPv6Addresses"`
	IDFormatSettings               map[string]bool                               `json:"idFormatSettings"`
	EndpointConnectionNotifs       map[string]*VpcEndpointConnectionNotification `json:"endpointConnectionNotifs"`
	VpcEndpointServicePermissions  map[string][]string                           `json:"vpcEpSvcPerms"`
	SnapshotLocks                  map[string]*SnapshotLock                      `json:"snapshotLocks,omitempty"`
	ReplaceRootVolumeTasks         map[string]*ReplaceRootVolumeTask             `json:"replaceRootVolumeTasks"`
	SubnetCIDRReservations         map[string][]*SubnetCIDRReservation           `json:"subnetCIDRReservations"`
	ImageDisabled                  map[string]bool                               `json:"imageDisabled,omitempty"`
	SgVpcAssociations              map[string]map[string]string                  `json:"sgVpcAssociations"`
	ImageDeregistrationProtection  map[string]bool                               `json:"imageDeregProtect"`
	ImageAttributes                map[string]map[string]string                  `json:"imageAttributes"`
	VgwRoutePropagation            map[string]bool                               `json:"vgwRoutePropagation"`
	ClientVpnEndpoints             map[string]*ClientVpnEndpoint                 `json:"clientVpnEndpoints"`
	TgwConnects                    map[string]*TransitGatewayConnect             `json:"tgwConnects,omitempty"`
	TgwConnectPeers                map[string]*TransitGatewayConnectPeer         `json:"tgwConnectPeers"`
	TgwPrefixListRefs              map[string]*TransitGatewayPrefixListReference `json:"tgwPrefixListRefs"`
	VerifiedAccessEndpoints        map[string]*VerifiedAccessEndpoint            `json:"verifiedAccessEndpoints"`
	VerifiedAccessGroups           map[string]*VerifiedAccessGroup               `json:"verifiedAccessGroups"`
	VerifiedAccessInstances        map[string]*VerifiedAccessInstance            `json:"verifiedAccessInstances"`
	VerifiedAccessTrustProviders   map[string]*VerifiedAccessTrustProvider       `json:"vatps"`

	// gopherstack-5o9 final EC2 parity sweep additions (kept in their own
	// alignment block so their longer field names don't widen gofmt's column
	// alignment for the rest of this struct).
	TgwRTPropagations          map[string]map[string]*snapTGWRTProp `json:"tgwRTPropagations,omitempty"`
	InterruptibleCRAllocations map[string]*snapInterruptibleCRAlloc `json:"interruptibleCRAllocations,omitempty"`
	MovingAddresses            map[string]*MovingAddressStatus      `json:"movingAddresses,omitempty"`

	InstanceConnectEndpoints           map[string]*InstanceConnectEndpoint            `json:"instanceConnectEndpoints"`
	InstanceEventWindows               map[string]*InstanceEventWindow                `json:"instanceEventWindows"`
	ImageImportTasks                   map[string]*ImageImportTask                    `json:"imageImportTasks"`
	SnapshotImportTasks                map[string]*SnapshotImportTask                 `json:"snapshotImportTasks"`
	RecycleBinImages                   map[string]*RecycleBinImage                    `json:"recycleBinImages"`
	RecycleBinSnapshots                map[string]*Snapshot                           `json:"recycleBinSnapshots"`
	RecycleBinVolumes                  map[string]*RecycleBinVolume                   `json:"recycleBinVolumes"`
	FastLaunchImages                   map[string]bool                                `json:"fastLaunchImages"`
	FastSnapshotRestores               map[string]bool                                `json:"fastSnapshotRestores"`
	VpnConnectionRoutes                map[string]*VpnConnectionRoute                 `json:"vpnConnectionRoutes"`
	SpotDatafeed                       *SpotDatafeed                                  `json:"spotDatafeed,omitempty"`
	VpcTenancy                         map[string]string                              `json:"vpcTenancy,omitempty"`
	TrafficMirrorFilterRules           map[string]*TrafficMirrorFilterRule            `json:"trafficMirrorFilterRules"`
	TrafficMirrorSessions              map[string]*TrafficMirrorSession               `json:"trafficMirrorSessions"`
	TrafficMirrorTargets               map[string]*TrafficMirrorTarget                `json:"trafficMirrorTargets"`
	NetworkInsightsAnalyses            map[string]*NetworkInsightsAnalysis            `json:"networkInsightsAnalyses"`
	NetworkInsightsAccessScopes        map[string]*NetworkInsightsAccessScope         `json:"networkInsightsAccessScopes"`
	NetworkInsightsAccessScopeAnalyses map[string]*NetworkInsightsAccessScopeAnalysis `json:"niasa"`
	ReservedInstances                  map[string]*ReservedInstance                   `json:"reservedInstances"`
	ReservedInstancesOfferings         map[string]*ReservedInstancesOffering          `json:"reservedInstancesOfferings"`
	RouteServers                       map[string]*RouteServer                        `json:"routeServers,omitempty"`
	RouteServerEndpoints               map[string]*RouteServerEndpoint                `json:"rsEndpoints,omitempty"`
	RouteServerPeers                   map[string]*RouteServerPeer                    `json:"routeServerPeers,omitempty"`
	RouteServerAssociations            map[string]*RouteServerAssociation             `json:"rsAssociations,omitempty"`
	RouteServerPropagations            map[string]*RouteServerPropagation             `json:"rsPropagations,omitempty"`
	LocalGateways                      map[string]*LocalGateway                       `json:"localGateways,omitempty"`
	LocalGatewayVirtualInterfaces      map[string]*LocalGatewayVirtualInterface       `json:"lgwVifs,omitempty"`
	LocalGatewayVirtualInterfaceGroups map[string]*LocalGatewayVirtualInterfaceGroup  `json:"lgwVifGroups,omitempty"`
	LocalGatewayRouteTables            map[string]*LocalGatewayRouteTable             `json:"lgwRouteTables,omitempty"`
	LocalGatewayRoutes                 map[string]*LocalGatewayRoute                  `json:"lgwRoutes,omitempty"`
	LocalGatewayRouteTableVpcAssocs    map[string]*snapLGWVpcAssoc                    `json:"lgwRtVpcAssocs,omitempty"`
	LocalGatewayRTVifGroupAssocs       map[string]*snapLGWVifGroupAssoc               `json:"lgwVifGroupAssocs,omitempty"`
	TgwMulticastDomains                map[string]*TransitGatewayMulticastDomain      `json:"tgwMcastDomains,omitempty"`
	TgwMulticastGroupEntries           map[string]*snapTGWMcastGroupEntry             `json:"tgwMcastGroupEnt,omitempty"`
	TgwMeteringPolicies                map[string]*snapTGWMeterPolicy                 `json:"tgwMeterPolicies,omitempty"`
	TgwMeteringPolicyEntries           map[string]*snapTGWMeterPolicyEntry            `json:"tgwMeterPolicyEnt,omitempty"`
	ClassicLinkInstances               map[string]*snapClassicLinkInstance            `json:"classicLinkInst,omitempty"`
	VpcBlockPublicAccessOptions        *VpcBlockPublicAccessOptions                   `json:"vpcBpaOptions,omitempty"`
	VpcBlockPublicAccessExclusions     map[string]*VpcBlockPublicAccessExclusion      `json:"vpcBpaExclusions,omitempty"`
	CapacityReservationFleets          map[string]*CapacityReservationFleet           `json:"crFleets,omitempty"`
	CapacityBlockOfferings             map[string]*CapacityBlockOffering              `json:"cbOfferings,omitempty"`
	CapacityBlockExtensionOfferings    map[string]*CapacityBlockExtensionOffering     `json:"cbExtOfferings,omitempty"`
	CapacityBlocks                     map[string]*CapacityBlock                      `json:"capacityBlocks,omitempty"`
	CapacityBlockExtensions            map[string]*CapacityBlockExtension             `json:"cbExtensions,omitempty"`
	CapacityReservationBillingReqs     map[string]*CapacityReservationBillingRequest  `json:"crBillingRequests,omitempty"`
	CapacityManagerDataExports         map[string]*CapacityManagerDataExport          `json:"cmDataExports,omitempty"`
	CapacityManagerState               *CapacityManagerState                          `json:"cmState,omitempty"`
	IpamPolicies                       map[string]*IpamPolicy                         `json:"ipamPolicies,omitempty"`
	IpamPolicyEnabledTargets           map[string]string                              `json:"ipamPolicyEnabled,omitempty"`
	VerifiedAccessEndpointPolicies     map[string]*VerifiedAccessPolicy               `json:"vaEndpointPolicies,omitempty"`
	VerifiedAccessGroupPolicies        map[string]*VerifiedAccessPolicy               `json:"vaGroupPolicies,omitempty"`
	FpgaImages                         map[string]*FpgaImage                          `json:"fpgaImages,omitempty"`

	// VerifiedAccessInstanceLoggingCfgs is kept in its own gofmt alignment
	// group (but still ahead of the string/slice/scalar fields below, to
	// preserve optimal field ordering): its long type name would otherwise
	// widen the tag column for the much larger field block above.
	VerifiedAccessInstanceLoggingCfgs map[string]*VerifiedAccessInstanceLoggingConfig `json:"vaLoggingCfgs,omitempty"`

	// Scheduled Instances / COIP / public IPv4-IPv6 pool / Allowed Images Settings / image
	// task / usage report fields are kept in their own gofmt alignment group for the same
	// reason as VerifiedAccessInstanceLoggingCfgs above.
	ScheduledInstances        map[string]*ScheduledInstance  `json:"scheduledInstances,omitempty"`
	ScheduledInstanceLaunched map[string]int32               `json:"schedInstLaunched,omitempty"`
	CoipPools                 map[string]*CoipPool           `json:"coipPools,omitempty"`
	CoipCidrs                 map[string]*CoipCidr           `json:"coipCidrs,omitempty"`
	Ipv4Pools                 map[string]*Ipv4Pool           `json:"ipv4Pools,omitempty"`
	Ipv6Pools                 map[string]*Ipv6Pool           `json:"ipv6Pools,omitempty"`
	AllowedImagesSettings     *AllowedImagesSettings         `json:"allowedImagesSettings,omitempty"`
	StoreImageTasks           map[string]*StoreImageTask     `json:"storeImageTasks,omitempty"`
	UsageReports              map[string]*UsageReport        `json:"usageReports,omitempty"`
	UsageReportEntries        map[string][]*UsageReportEntry `json:"usageReportEntries,omitempty"`
	InstanceProductCodes      map[string][]string            `json:"instanceProductCodes,omitempty"`

	// VM Import/Export, Bundle, Conversion Task, Trunk Interface, and Enclave Certificate
	// IAM Role fields.
	BundleTasks                map[string]*BundleTask                      `json:"bundleTasks,omitempty"`
	ConversionTasks            map[string]*ConversionTask                  `json:"conversionTasks,omitempty"`
	ExportTasks                map[string]*ExportTask                      `json:"exportTasks,omitempty"`
	ExportImageTasks           map[string]*ExportImageTaskRec              `json:"exportImageTasks,omitempty"`
	TrunkInterfaceAssociations map[string]*TrunkInterfaceAssociation       `json:"trunkInterfaceAssociations,omitempty"`
	EnclaveCertIamRoles        map[string][]*EnclaveCertIamRoleAssociation `json:"enclaveCertIamRoles,omitempty"`

	// Mac modification task / Secondary Network / Secondary Subnet / Secondary
	// Interface / Outpost LAG / Service Link Virtual Interface / SQL HA fields.
	MacModificationTasks         map[string]*MacModificationTask         `json:"macModificationTasks,omitempty"`
	SecondaryNetworks            map[string]*SecondaryNetwork            `json:"secondaryNetworks,omitempty"`
	SecondarySubnets             map[string]*SecondarySubnet             `json:"secondarySubnets,omitempty"`
	SecondaryInterfaces          map[string]*SecondaryInterface          `json:"secondaryInterfaces,omitempty"`
	ServiceLinkVirtualInterfaces map[string]*ServiceLinkVirtualInterface `json:"serviceLinkVirtualInterfaces,omitempty"`
	OutpostLags                  map[string]*OutpostLag                  `json:"outpostLags,omitempty"`
	AvailabilityZoneGroupOptIns  map[string]string                       `json:"azGroupOptIns,omitempty"`
	SQLHaRegistrations           map[string]*RegisteredSQLHaInstance     `json:"sqlHaRegistrations,omitempty"`
	SQLHaHistory                 map[string][]*RegisteredSQLHaInstance   `json:"sqlHaHistory,omitempty"`

	// parity-sweep-2: VPC Encryption Control, VPN Concentrator, Host Reservations,
	// Declarative Policies, AWS Network Performance.
	VpcEncryptionControls           map[string]*VpcEncryptionControl           `json:"vpcEncryptionControls,omitempty"`
	VpnConcentrators                map[string]*VpnConcentrator                `json:"vpnConcentrators,omitempty"`
	HostReservations                map[string]*HostReservation                `json:"hostReservations,omitempty"`
	DeclarativePoliciesReports      map[string]*DeclarativePoliciesReport      `json:"declPoliciesReports,omitempty"`
	NetworkPerformanceSubscriptions map[string]*NetworkPerformanceSubscription `json:"networkPerformanceSubs,omitempty"`

	IpamOrgAdminAccountID  string   `json:"ipamOrgAdminAcct,omitempty"`
	Region                 string   `json:"region,omitempty"`
	AccountID              string   `json:"accountID,omitempty"`
	FreePrivateIPs         []string `json:"freePrivateIPs"`
	NextPrivateIPIndex     int      `json:"nextPrivateIPIndex"`
	NextElasticIPIndex     int      `json:"nextElasticIPIndex"`
	EbsEncryptionByDefault bool     `json:"ebsEncryptionByDefault"`
	SerialConsoleAccess    bool     `json:"serialConsoleAccess"`
	// gopherstack-5o9 final EC2 parity sweep addition.
	ReachabilityAnalyzerOrgSharing bool `json:"reachabilityAnalyzerOrgSharing,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
//
//nolint:funlen // large state snapshot
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:                          b.instances,
		SecurityGroups:                     b.securityGroups,
		VPCs:                               b.vpcs,
		Subnets:                            b.subnets,
		KeyPairs:                           b.keyPairs,
		Volumes:                            b.volumes,
		Addresses:                          b.addresses,
		InternetGateways:                   b.internetGateways,
		RouteTables:                        b.routeTables,
		NatGateways:                        b.natGateways,
		NetworkInterfaces:                  b.networkInterfaces,
		SpotRequests:                       b.spotRequests,
		PlacementGroups:                    b.placementGroups,
		Images:                             b.images,
		ImageUsageReports:                  b.imageUsageReports,
		LaunchTemplates:                    b.launchTemplates,
		VpcEndpoints:                       b.vpcEndpoints,
		Snapshots:                          b.snapshots,
		NetworkACLs:                        b.networkACLs,
		TransitGateways:                    b.transitGateways,
		FlowLogs:                           b.flowLogs,
		DhcpOptionSets:                     b.dhcpOptionSets,
		Tags:                               b.tags,
		AddressTransfers:                   b.addressTransfers,
		CapacityReservations:               b.capacityReservations,
		ReservedInstancesExchanges:         b.reservedInstancesExchanges,
		TGWMulticastDomainAssociations:     b.tgwMulticastDomainAssociations,
		TGWPeeringAttachments:              b.tgwPeeringAttachments,
		TGWVpcAttachments:                  b.tgwVpcAttachments,
		VpcEndpointConnections:             b.vpcEndpointConnections,
		VpcPeeringConnections:              b.vpcPeeringConnections,
		ByoipCidrs:                         b.byoipCidrs,
		DedicatedHosts:                     b.dedicatedHosts,
		VpnGateways:                        b.vpnGateways,
		CustomerGateways:                   b.customerGateways,
		Ipams:                              b.ipams,
		IpamScopes:                         b.ipamScopes,
		IpamPools:                          b.ipamPools,
		IpamPoolCidrs:                      b.ipamPoolCidrs,
		IpamPoolAllocations:                b.ipamPoolAllocations,
		IpamResourceDiscoveries:            b.ipamResourceDiscoveries,
		IpamResourceDiscoveryAssocs:        b.ipamResourceDiscoveryAssocs,
		IpamByoasns:                        b.ipamByoasns,
		IpamAsnAssociations:                b.ipamAsnAssociations,
		IpamVerificationTokens:             b.ipamVerificationTokens,
		IpamResourceCidrs:                  b.ipamResourceCidrs,
		IpamPrefixListResolvers:            b.ipamPrefixListResolvers,
		IpamPrefixListResolverVersions:     b.ipamPrefixListResolverVersions,
		IpamPrefixListResolverTargets:      b.ipamPrefixListResolverTargets,
		CarrierGateways:                    b.carrierGateways,
		Fleets:                             b.fleets,
		NetworkInsightsPaths:               b.networkInsightsPaths,
		ManagedPrefixLists:                 b.managedPrefixLists,
		EbsEncryptionByDefault:             b.ebsEncryptionByDefault,
		SerialConsoleAccess:                b.serialConsoleAccess,
		FreePrivateIPs:                     b.freePrivateIPs,
		AccountID:                          b.AccountID,
		Region:                             b.Region,
		NextPrivateIPIndex:                 b.nextPrivateIPIndex,
		NextElasticIPIndex:                 b.nextElasticIPIndex,
		EgressOnlyIGWs:                     b.egressOnlyIGWs,
		IamAssociations:                    b.iamAssociations,
		TgwRouteTables:                     b.tgwRouteTables,
		TgwRoutes:                          b.tgwRoutes,
		TgwRTAssociations:                  b.tgwRTAssociations,
		TgwPolicyTables:                    b.tgwPolicyTables,
		TgwPolicyTableAssociations:         b.tgwPolicyTableAssociations,
		TgwRouteTableAnnouncements:         b.tgwRouteTableAnnouncements,
		VpcCidrAssociations:                b.vpcCidrAssociations,
		VpnConnections:                     b.vpnConnections,
		VpcEndpointServiceConfigs:          b.vpcEndpointServiceConfigs,
		SpotFleets:                         b.spotFleets,
		SpotFleetHistory:                   b.spotFleetHistory,
		VolumeModifications:                b.volumeModifications,
		SnapshotTiers:                      b.snapshotTiers,
		SnapshotAttributes:                 b.snapshotAttributes,
		SgVpcAssociations:                  b.sgVpcAssociations,
		VpcTenancy:                         b.vpcTenancy,
		VpcPeeringOptions:                  b.vpcPeeringOptions,
		SubnetCIDRAssociations:             b.subnetCIDRAssociations,
		AddressAttributes:                  b.addressAttributes,
		InstanceMonitoring:                 b.instanceMonitoring,
		InstanceCreditSpecs:                b.instanceCreditSpecs,
		InstanceIMDSOptions:                b.instanceIMDSOptions,
		InstanceMetadataDefaults:           b.instanceMetadataDefaults,
		InstanceEventNotifAttrs:            b.instanceEventNotifAttrs,
		NiPermissions:                      b.niPermissions,
		NiIPv6Addresses:                    b.niIPv6Addresses,
		IDFormatSettings:                   b.idFormatSettings,
		EndpointConnectionNotifs:           b.endpointConnectionNotifs,
		VpcEndpointServicePermissions:      b.vpcEndpointServicePermissions,
		SnapshotLocks:                      b.snapshotLocks,
		ReplaceRootVolumeTasks:             b.replaceRootVolumeTasks,
		SubnetCIDRReservations:             b.subnetCIDRReservations,
		ImageDisabled:                      b.imageDisabled,
		ImageDeprecated:                    b.imageDeprecated,
		ImageDeregistrationProtection:      b.imageDeregistrationProtection,
		ImageAttributes:                    b.imageAttributes,
		VgwRoutePropagation:                b.vgwRoutePropagation,
		ClientVpnEndpoints:                 b.clientVpnEndpoints,
		TgwConnects:                        b.tgwConnects,
		TgwConnectPeers:                    b.tgwConnectPeers,
		TgwPrefixListRefs:                  b.tgwPrefixListRefs,
		VerifiedAccessEndpoints:            b.verifiedAccessEndpoints,
		VerifiedAccessGroups:               b.verifiedAccessGroups,
		VerifiedAccessInstances:            b.verifiedAccessInstances,
		VerifiedAccessTrustProviders:       b.verifiedAccessTrustProviders,
		InstanceConnectEndpoints:           b.instanceConnectEndpoints,
		InstanceEventWindows:               b.instanceEventWindows,
		ImageImportTasks:                   b.imageImportTasks,
		SnapshotImportTasks:                b.snapshotImportTasks,
		RecycleBinImages:                   b.recycleBinImages,
		RecycleBinSnapshots:                b.recycleBinSnapshots,
		RecycleBinVolumes:                  b.recycleBinVolumes,
		FastLaunchImages:                   b.fastLaunchImages,
		FastSnapshotRestores:               b.fastSnapshotRestores,
		VpnConnectionRoutes:                b.vpnConnectionRoutes,
		SpotDatafeed:                       b.spotDatafeed,
		TrafficMirrorFilters:               b.trafficMirrorFilters,
		TrafficMirrorFilterRules:           b.trafficMirrorFilterRules,
		TrafficMirrorSessions:              b.trafficMirrorSessions,
		TrafficMirrorTargets:               b.trafficMirrorTargets,
		NetworkInsightsAnalyses:            b.networkInsightsAnalyses,
		NetworkInsightsAccessScopes:        b.networkInsightsAccessScopes,
		NetworkInsightsAccessScopeAnalyses: b.networkInsightsAccessScopeAnalyses,
		ReservedInstances:                  b.reservedInstances,
		ReservedInstancesOfferings:         b.reservedInstancesOfferings,
		ReservedInstancesListings:          b.reservedInstancesListings,
		ReservedInstancesModifications:     b.reservedInstancesModifications,
		RouteServers:                       b.routeServers,
		RouteServerEndpoints:               b.routeServerEndpoints,
		RouteServerPeers:                   b.routeServerPeers,
		RouteServerAssociations:            b.routeServerAssociations,
		RouteServerPropagations:            b.routeServerPropagations,
		LocalGateways:                      b.localGateways,
		LocalGatewayVirtualInterfaces:      b.localGatewayVirtualInterfaces,
		LocalGatewayVirtualInterfaceGroups: b.localGatewayVirtualInterfaceGroups,
		LocalGatewayRouteTables:            b.localGatewayRouteTables,
		LocalGatewayRoutes:                 b.localGatewayRoutes,
		LocalGatewayRouteTableVpcAssocs:    b.localGatewayRouteTableVpcAssociations,
		LocalGatewayRTVifGroupAssocs:       b.localGatewayRouteTableVifGroupAssociations,
		TgwMulticastDomains:                b.tgwMulticastDomains,
		TgwMulticastGroupEntries:           b.tgwMulticastGroupEntries,
		TgwMeteringPolicies:                b.tgwMeteringPolicies,
		TgwMeteringPolicyEntries:           b.tgwMeteringPolicyEntries,
		ClassicLinkInstances:               b.classicLinkInstances,
		VpcBlockPublicAccessOptions:        b.vpcBlockPublicAccessOptions,
		VpcBlockPublicAccessExclusions:     b.vpcBlockPublicAccessExclusions,
		CapacityReservationFleets:          b.capacityReservationFleets,
		CapacityBlockOfferings:             b.capacityBlockOfferings,
		CapacityBlockExtensionOfferings:    b.capacityBlockExtensionOfferings,
		CapacityBlocks:                     b.capacityBlocks,
		CapacityBlockExtensions:            b.capacityBlockExtensions,
		CapacityReservationBillingReqs:     b.capacityReservationBillingRequests,
		CapacityManagerDataExports:         b.capacityManagerDataExports,
		CapacityManagerState:               b.capacityManagerState,
		IpamPolicies:                       b.ipamPolicies,
		IpamPolicyEnabledTargets:           b.ipamPolicyEnabledTargets,
		IpamOrgAdminAccountID:              b.ipamOrgAdminAccountID,
		VerifiedAccessEndpointPolicies:     b.verifiedAccessEndpointPolicies,
		VerifiedAccessGroupPolicies:        b.verifiedAccessGroupPolicies,
		VerifiedAccessInstanceLoggingCfgs:  b.verifiedAccessInstanceLoggingConfigs,
		FpgaImages:                         b.fpgaImages,
		ScheduledInstances:                 b.scheduledInstances,
		ScheduledInstanceLaunched:          b.scheduledInstanceLaunched,
		CoipPools:                          b.coipPools,
		CoipCidrs:                          b.coipCidrs,
		Ipv4Pools:                          b.ipv4Pools,
		Ipv6Pools:                          b.ipv6Pools,
		AllowedImagesSettings:              b.allowedImagesSettings,
		StoreImageTasks:                    b.storeImageTasks,
		UsageReports:                       b.usageReports,
		UsageReportEntries:                 b.usageReportEntries,
		InstanceProductCodes:               b.instanceProductCodes,
		BundleTasks:                        b.bundleTasks,
		ConversionTasks:                    b.conversionTasks,
		ExportTasks:                        b.exportTasks,
		ExportImageTasks:                   b.exportImageTasks,
		TrunkInterfaceAssociations:         b.trunkInterfaceAssociations,
		EnclaveCertIamRoles:                b.enclaveCertIamRoles,
		MacModificationTasks:               b.macModificationTasks,
		SecondaryNetworks:                  b.secondaryNetworks,
		SecondarySubnets:                   b.secondarySubnets,
		SecondaryInterfaces:                b.secondaryInterfaces,
		ServiceLinkVirtualInterfaces:       b.serviceLinkVirtualInterfaces,
		OutpostLags:                        b.outpostLags,
		AvailabilityZoneGroupOptIns:        b.availabilityZoneGroupOptIns,
		SQLHaRegistrations:                 b.sqlHaRegistrations,
		SQLHaHistory:                       b.sqlHaHistory,
		VpcEncryptionControls:              b.vpcEncryptionControls,
		VpnConcentrators:                   b.vpnConcentrators,
		HostReservations:                   b.hostReservations,
		DeclarativePoliciesReports:         b.declarativePoliciesReports,
		NetworkPerformanceSubscriptions:    b.networkPerformanceSubscriptions,
		TgwRTPropagations:                  b.tgwRTPropagations,
		InterruptibleCRAllocations:         b.interruptibleCRAllocations,
		MovingAddresses:                    b.movingAddresses,
		ReachabilityAnalyzerOrgSharing:     b.reachabilityAnalyzerOrgSharing,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("ec2: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
//
//nolint:gocognit,gocyclo,cyclop,funlen // large state restore
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.initMissingMaps()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.restoreCoreFields(&snap)
	b.restoreExtendedFields(&snap)
	b.rebuildSecondaryIndexesLocked()

	if snap.EgressOnlyIGWs != nil {
		b.egressOnlyIGWs = snap.EgressOnlyIGWs
	} else {
		b.egressOnlyIGWs = make(map[string]*EgressOnlyInternetGateway)
	}
	if snap.IamAssociations != nil {
		b.iamAssociations = snap.IamAssociations
	} else {
		b.iamAssociations = make(map[string]*IamInstanceProfileAssociation)
	}
	if snap.TgwRouteTables != nil {
		b.tgwRouteTables = snap.TgwRouteTables
	} else {
		b.tgwRouteTables = make(map[string]*TransitGatewayRouteTable)
	}
	if snap.TgwRoutes != nil {
		b.tgwRoutes = snap.TgwRoutes
	} else {
		b.tgwRoutes = make(map[string]*TransitGatewayRoute)
	}
	if snap.TgwRTAssociations != nil {
		b.tgwRTAssociations = snap.TgwRTAssociations
	} else {
		b.tgwRTAssociations = make(map[string]*TransitGatewayRouteTableAssociation)
	}
	if snap.TgwPolicyTables != nil {
		b.tgwPolicyTables = snap.TgwPolicyTables
	} else {
		b.tgwPolicyTables = make(map[string]*TransitGatewayPolicyTable)
	}
	if snap.TgwPolicyTableAssociations != nil {
		b.tgwPolicyTableAssociations = snap.TgwPolicyTableAssociations
	} else {
		b.tgwPolicyTableAssociations = make(map[string]*TransitGatewayPolicyTableAssociation)
	}
	if snap.TgwRouteTableAnnouncements != nil {
		b.tgwRouteTableAnnouncements = snap.TgwRouteTableAnnouncements
	} else {
		b.tgwRouteTableAnnouncements = make(map[string]*TransitGatewayRouteTableAnnouncement)
	}
	if snap.VpcCidrAssociations != nil {
		b.vpcCidrAssociations = snap.VpcCidrAssociations
	} else {
		b.vpcCidrAssociations = make(map[string]*VpcCidrBlockAssociation)
	}
	if snap.VpnConnections != nil {
		b.vpnConnections = snap.VpnConnections
	} else {
		b.vpnConnections = make(map[string]*VpnConnection)
	}
	if snap.VpcEndpointServiceConfigs != nil {
		b.vpcEndpointServiceConfigs = snap.VpcEndpointServiceConfigs
	} else {
		b.vpcEndpointServiceConfigs = make(map[string]*VpcEndpointServiceConfig)
	}
	if snap.SpotFleets != nil {
		b.spotFleets = snap.SpotFleets
	} else {
		b.spotFleets = make(map[string]*SpotFleetRequest)
	}
	if snap.SpotFleetHistory != nil {
		b.spotFleetHistory = snap.SpotFleetHistory
	} else {
		b.spotFleetHistory = make(map[string][]SpotFleetHistoryRecord)
	}
	if snap.VolumeModifications != nil {
		b.volumeModifications = snap.VolumeModifications
	} else {
		b.volumeModifications = make(map[string]*VolumeModification)
	}
	if snap.SnapshotTiers != nil {
		b.snapshotTiers = snap.SnapshotTiers
	} else {
		b.snapshotTiers = make(map[string]string)
	}
	if snap.SnapshotAttributes != nil {
		b.snapshotAttributes = snap.SnapshotAttributes
	} else {
		b.snapshotAttributes = make(map[string]map[string]string)
	}
	if snap.SgVpcAssociations != nil {
		b.sgVpcAssociations = snap.SgVpcAssociations
	} else {
		b.sgVpcAssociations = make(map[string]map[string]string)
	}
	if snap.VpcTenancy != nil {
		b.vpcTenancy = snap.VpcTenancy
	} else {
		b.vpcTenancy = make(map[string]string)
	}
	if snap.VpcPeeringOptions != nil {
		b.vpcPeeringOptions = snap.VpcPeeringOptions
	} else {
		b.vpcPeeringOptions = make(map[string]*PeeringConnectionOptions)
	}
	if snap.SubnetCIDRAssociations != nil {
		b.subnetCIDRAssociations = snap.SubnetCIDRAssociations
	} else {
		b.subnetCIDRAssociations = make(map[string][]*SubnetCIDRAssociation)
	}
	if snap.AddressAttributes != nil {
		b.addressAttributes = snap.AddressAttributes
	} else {
		b.addressAttributes = make(map[string]*AddressAttribute)
	}
	if snap.InstanceMonitoring != nil {
		b.instanceMonitoring = snap.InstanceMonitoring
	} else {
		b.instanceMonitoring = make(map[string]string)
	}
	if snap.InstanceCreditSpecs != nil {
		b.instanceCreditSpecs = snap.InstanceCreditSpecs
	} else {
		b.instanceCreditSpecs = make(map[string]string)
	}
	if snap.InstanceIMDSOptions != nil {
		b.instanceIMDSOptions = snap.InstanceIMDSOptions
	} else {
		b.instanceIMDSOptions = make(map[string]*IMDSOptions)
	}
	b.instanceMetadataDefaults = snap.InstanceMetadataDefaults
	b.instanceEventNotifAttrs = snap.InstanceEventNotifAttrs
	if snap.NiPermissions != nil {
		b.niPermissions = snap.NiPermissions
	} else {
		b.niPermissions = make(map[string]*NetworkInterfacePermission)
	}
	if snap.NiIPv6Addresses != nil {
		b.niIPv6Addresses = snap.NiIPv6Addresses
	} else {
		b.niIPv6Addresses = make(map[string][]string)
	}
	if snap.IDFormatSettings != nil {
		b.idFormatSettings = snap.IDFormatSettings
	} else {
		b.idFormatSettings = make(map[string]bool)
	}
	if snap.EndpointConnectionNotifs != nil {
		b.endpointConnectionNotifs = snap.EndpointConnectionNotifs
	} else {
		b.endpointConnectionNotifs = make(map[string]*VpcEndpointConnectionNotification)
	}
	if snap.VpcEndpointServicePermissions != nil {
		b.vpcEndpointServicePermissions = snap.VpcEndpointServicePermissions
	} else {
		b.vpcEndpointServicePermissions = make(map[string][]string)
	}
	if snap.SnapshotLocks != nil {
		b.snapshotLocks = snap.SnapshotLocks
	} else {
		b.snapshotLocks = make(map[string]*SnapshotLock)
	}
	if snap.ReplaceRootVolumeTasks != nil {
		b.replaceRootVolumeTasks = snap.ReplaceRootVolumeTasks
	} else {
		b.replaceRootVolumeTasks = make(map[string]*ReplaceRootVolumeTask)
	}
	if snap.SubnetCIDRReservations != nil {
		b.subnetCIDRReservations = snap.SubnetCIDRReservations
	} else {
		b.subnetCIDRReservations = make(map[string][]*SubnetCIDRReservation)
	}
	if snap.ImageDisabled != nil {
		b.imageDisabled = snap.ImageDisabled
	} else {
		b.imageDisabled = make(map[string]bool)
	}
	if snap.ImageDeprecated != nil {
		b.imageDeprecated = snap.ImageDeprecated
	} else {
		b.imageDeprecated = make(map[string]string)
	}
	if snap.ImageDeregistrationProtection != nil {
		b.imageDeregistrationProtection = snap.ImageDeregistrationProtection
	} else {
		b.imageDeregistrationProtection = make(map[string]bool)
	}
	if snap.ImageAttributes != nil {
		b.imageAttributes = snap.ImageAttributes
	} else {
		b.imageAttributes = make(map[string]map[string]string)
	}
	if snap.VgwRoutePropagation != nil {
		b.vgwRoutePropagation = snap.VgwRoutePropagation
	} else {
		b.vgwRoutePropagation = make(map[string]bool)
	}
	if snap.ClientVpnEndpoints != nil {
		b.clientVpnEndpoints = snap.ClientVpnEndpoints
	} else {
		b.clientVpnEndpoints = make(map[string]*ClientVpnEndpoint)
	}
	if snap.TgwConnects != nil {
		b.tgwConnects = snap.TgwConnects
	} else {
		b.tgwConnects = make(map[string]*TransitGatewayConnect)
	}
	if snap.TgwConnectPeers != nil {
		b.tgwConnectPeers = snap.TgwConnectPeers
	} else {
		b.tgwConnectPeers = make(map[string]*TransitGatewayConnectPeer)
	}
	if snap.TgwPrefixListRefs != nil {
		b.tgwPrefixListRefs = snap.TgwPrefixListRefs
	} else {
		b.tgwPrefixListRefs = make(map[string]*TransitGatewayPrefixListReference)
	}
	if snap.VerifiedAccessEndpoints != nil {
		b.verifiedAccessEndpoints = snap.VerifiedAccessEndpoints
	} else {
		b.verifiedAccessEndpoints = make(map[string]*VerifiedAccessEndpoint)
	}
	if snap.VerifiedAccessGroups != nil {
		b.verifiedAccessGroups = snap.VerifiedAccessGroups
	} else {
		b.verifiedAccessGroups = make(map[string]*VerifiedAccessGroup)
	}
	if snap.VerifiedAccessInstances != nil {
		b.verifiedAccessInstances = snap.VerifiedAccessInstances
	} else {
		b.verifiedAccessInstances = make(map[string]*VerifiedAccessInstance)
	}
	if snap.VerifiedAccessTrustProviders != nil {
		b.verifiedAccessTrustProviders = snap.VerifiedAccessTrustProviders
	} else {
		b.verifiedAccessTrustProviders = make(map[string]*VerifiedAccessTrustProvider)
	}
	if snap.VerifiedAccessEndpointPolicies != nil {
		b.verifiedAccessEndpointPolicies = snap.VerifiedAccessEndpointPolicies
	} else {
		b.verifiedAccessEndpointPolicies = make(map[string]*VerifiedAccessPolicy)
	}
	if snap.VerifiedAccessGroupPolicies != nil {
		b.verifiedAccessGroupPolicies = snap.VerifiedAccessGroupPolicies
	} else {
		b.verifiedAccessGroupPolicies = make(map[string]*VerifiedAccessPolicy)
	}
	if snap.VerifiedAccessInstanceLoggingCfgs != nil {
		b.verifiedAccessInstanceLoggingConfigs = snap.VerifiedAccessInstanceLoggingCfgs
	} else {
		b.verifiedAccessInstanceLoggingConfigs = make(map[string]*VerifiedAccessInstanceLoggingConfig)
	}
	if snap.FpgaImages != nil {
		b.fpgaImages = snap.FpgaImages
	} else {
		b.fpgaImages = make(map[string]*FpgaImage)
	}
	if snap.InstanceConnectEndpoints != nil {
		b.instanceConnectEndpoints = snap.InstanceConnectEndpoints
	} else {
		b.instanceConnectEndpoints = make(map[string]*InstanceConnectEndpoint)
	}
	if snap.InstanceEventWindows != nil {
		b.instanceEventWindows = snap.InstanceEventWindows
	} else {
		b.instanceEventWindows = make(map[string]*InstanceEventWindow)
	}
	if snap.ImageImportTasks != nil {
		b.imageImportTasks = snap.ImageImportTasks
	} else {
		b.imageImportTasks = make(map[string]*ImageImportTask)
	}
	if snap.SnapshotImportTasks != nil {
		b.snapshotImportTasks = snap.SnapshotImportTasks
	} else {
		b.snapshotImportTasks = make(map[string]*SnapshotImportTask)
	}
	if snap.RecycleBinImages != nil {
		b.recycleBinImages = snap.RecycleBinImages
	} else {
		b.recycleBinImages = make(map[string]*RecycleBinImage)
	}
	if snap.RecycleBinSnapshots != nil {
		b.recycleBinSnapshots = snap.RecycleBinSnapshots
	} else {
		b.recycleBinSnapshots = make(map[string]*Snapshot)
	}
	if snap.RecycleBinVolumes != nil {
		b.recycleBinVolumes = snap.RecycleBinVolumes
	} else {
		b.recycleBinVolumes = make(map[string]*RecycleBinVolume)
	}
	if snap.FastLaunchImages != nil {
		b.fastLaunchImages = snap.FastLaunchImages
	} else {
		b.fastLaunchImages = make(map[string]bool)
	}
	if snap.FastSnapshotRestores != nil {
		b.fastSnapshotRestores = snap.FastSnapshotRestores
	} else {
		b.fastSnapshotRestores = make(map[string]bool)
	}
	if snap.VpnConnectionRoutes != nil {
		b.vpnConnectionRoutes = snap.VpnConnectionRoutes
	} else {
		b.vpnConnectionRoutes = make(map[string]*VpnConnectionRoute)
	}
	b.spotDatafeed = snap.SpotDatafeed
	if snap.TrafficMirrorFilters != nil {
		b.trafficMirrorFilters = snap.TrafficMirrorFilters
	} else {
		b.trafficMirrorFilters = make(map[string]*TrafficMirrorFilter)
	}
	if snap.TrafficMirrorFilterRules != nil {
		b.trafficMirrorFilterRules = snap.TrafficMirrorFilterRules
	} else {
		b.trafficMirrorFilterRules = make(map[string]*TrafficMirrorFilterRule)
	}
	if snap.TrafficMirrorSessions != nil {
		b.trafficMirrorSessions = snap.TrafficMirrorSessions
	} else {
		b.trafficMirrorSessions = make(map[string]*TrafficMirrorSession)
	}
	if snap.TrafficMirrorTargets != nil {
		b.trafficMirrorTargets = snap.TrafficMirrorTargets
	} else {
		b.trafficMirrorTargets = make(map[string]*TrafficMirrorTarget)
	}
	if snap.NetworkInsightsAnalyses != nil {
		b.networkInsightsAnalyses = snap.NetworkInsightsAnalyses
	} else {
		b.networkInsightsAnalyses = make(map[string]*NetworkInsightsAnalysis)
	}
	if snap.NetworkInsightsAccessScopes != nil {
		b.networkInsightsAccessScopes = snap.NetworkInsightsAccessScopes
	} else {
		b.networkInsightsAccessScopes = make(map[string]*NetworkInsightsAccessScope)
	}
	if snap.NetworkInsightsAccessScopeAnalyses != nil {
		b.networkInsightsAccessScopeAnalyses = snap.NetworkInsightsAccessScopeAnalyses
	} else {
		b.networkInsightsAccessScopeAnalyses = make(map[string]*NetworkInsightsAccessScopeAnalysis)
	}
	if snap.ReservedInstances != nil {
		b.reservedInstances = snap.ReservedInstances
	} else {
		b.reservedInstances = make(map[string]*ReservedInstance)
	}
	if snap.ReservedInstancesOfferings != nil {
		b.reservedInstancesOfferings = snap.ReservedInstancesOfferings
	} else {
		b.reservedInstancesOfferings = make(map[string]*ReservedInstancesOffering)
	}
	if snap.ReservedInstancesListings != nil {
		b.reservedInstancesListings = snap.ReservedInstancesListings
	} else {
		b.reservedInstancesListings = make(map[string]*ReservedInstancesListing)
	}
	if snap.ReservedInstancesModifications != nil {
		b.reservedInstancesModifications = snap.ReservedInstancesModifications
	} else {
		b.reservedInstancesModifications = make(map[string]*ReservedInstancesModification)
	}
	b.restoreRouteServerFields(&snap)
	b.restoreLocalGatewayFields(&snap)
	b.restoreTGWMulticastFields(&snap)
	b.restoreVpcConfigFields(&snap)
	b.restoreCapacityFamilyFields(&snap)
	b.restoreNewFamilyFields(&snap)
	b.restoreParitySweep2Fields(&snap)
	b.restoreParityFinalFields(&snap)

	return nil
}

// restoreParityFinalFields copies the final EC2 parity sweep
// (gopherstack-5o9) state — TGW route table propagations, interruptible
// Capacity Reservation allocations, moving-address status, and the
// Reachability Analyzer organization-sharing flag — from snap into b. Must be
// called with b.mu held for writing.
func (b *InMemoryBackend) restoreParityFinalFields(snap *backendSnapshot) {
	if snap.TgwRTPropagations != nil {
		b.tgwRTPropagations = snap.TgwRTPropagations
	} else {
		b.tgwRTPropagations = make(map[string]map[string]*TransitGatewayRouteTablePropagation)
	}

	if snap.InterruptibleCRAllocations != nil {
		b.interruptibleCRAllocations = snap.InterruptibleCRAllocations
	} else {
		b.interruptibleCRAllocations = make(map[string]*InterruptibleCapacityReservationAllocation)
	}

	if snap.MovingAddresses != nil {
		b.movingAddresses = snap.MovingAddresses
	} else {
		b.movingAddresses = make(map[string]*MovingAddressStatus)
	}

	b.reachabilityAnalyzerOrgSharing = snap.ReachabilityAnalyzerOrgSharing
}

// restoreParitySweep2Fields copies the VPC Encryption Control, VPN Concentrator,
// Host Reservation, Declarative Policies, and AWS Network Performance state maps
// from snap into b. Must be called with b.mu held for writing.
func (b *InMemoryBackend) restoreParitySweep2Fields(snap *backendSnapshot) {
	if snap.VpcEncryptionControls != nil {
		b.vpcEncryptionControls = snap.VpcEncryptionControls
	} else {
		b.vpcEncryptionControls = make(map[string]*VpcEncryptionControl)
	}

	if snap.VpnConcentrators != nil {
		b.vpnConcentrators = snap.VpnConcentrators
	} else {
		b.vpnConcentrators = make(map[string]*VpnConcentrator)
	}

	if snap.HostReservations != nil {
		b.hostReservations = snap.HostReservations
	} else {
		b.hostReservations = make(map[string]*HostReservation)
	}

	if snap.DeclarativePoliciesReports != nil {
		b.declarativePoliciesReports = snap.DeclarativePoliciesReports
	} else {
		b.declarativePoliciesReports = make(map[string]*DeclarativePoliciesReport)
	}

	if snap.NetworkPerformanceSubscriptions != nil {
		b.networkPerformanceSubscriptions = snap.NetworkPerformanceSubscriptions
	} else {
		b.networkPerformanceSubscriptions = make(map[string]*NetworkPerformanceSubscription)
	}
}

// restoreNewFamilyFields copies the Mac modification task / Secondary
// Network / Secondary Subnet / Secondary Interface / Outpost LAG / Service
// Link Virtual Interface / instance-attribute misc / SQL HA fields from snap
// into b. Must be called with b.mu held for writing.
func (b *InMemoryBackend) restoreNewFamilyFields(snap *backendSnapshot) {
	b.macModificationTasks = snap.MacModificationTasks
	b.secondaryNetworks = snap.SecondaryNetworks
	b.secondarySubnets = snap.SecondarySubnets
	b.secondaryInterfaces = snap.SecondaryInterfaces
	b.serviceLinkVirtualInterfaces = snap.ServiceLinkVirtualInterfaces
	b.outpostLags = snap.OutpostLags
	b.availabilityZoneGroupOptIns = snap.AvailabilityZoneGroupOptIns
	b.sqlHaRegistrations = snap.SQLHaRegistrations
	b.sqlHaHistory = snap.SQLHaHistory
}

// restoreCapacityFamilyFields copies the Capacity Reservation Fleet, Capacity
// Block, and Capacity Manager state from snap into b. Split out to keep
// Restore's growth in check. Must be called with b.mu held.
func (b *InMemoryBackend) restoreCapacityFamilyFields(snap *backendSnapshot) {
	if snap.CapacityReservationFleets != nil {
		b.capacityReservationFleets = snap.CapacityReservationFleets
	} else {
		b.capacityReservationFleets = make(map[string]*CapacityReservationFleet)
	}

	if snap.CapacityBlockOfferings != nil {
		b.capacityBlockOfferings = snap.CapacityBlockOfferings
	} else {
		b.capacityBlockOfferings = make(map[string]*CapacityBlockOffering)
	}

	if snap.CapacityBlockExtensionOfferings != nil {
		b.capacityBlockExtensionOfferings = snap.CapacityBlockExtensionOfferings
	} else {
		b.capacityBlockExtensionOfferings = make(map[string]*CapacityBlockExtensionOffering)
	}

	if snap.CapacityBlocks != nil {
		b.capacityBlocks = snap.CapacityBlocks
	} else {
		b.capacityBlocks = make(map[string]*CapacityBlock)
	}

	if snap.CapacityBlockExtensions != nil {
		b.capacityBlockExtensions = snap.CapacityBlockExtensions
	} else {
		b.capacityBlockExtensions = make(map[string]*CapacityBlockExtension)
	}

	if snap.CapacityReservationBillingReqs != nil {
		b.capacityReservationBillingRequests = snap.CapacityReservationBillingReqs
	} else {
		b.capacityReservationBillingRequests = make(map[string]*CapacityReservationBillingRequest)
	}

	if snap.CapacityManagerDataExports != nil {
		b.capacityManagerDataExports = snap.CapacityManagerDataExports
	} else {
		b.capacityManagerDataExports = make(map[string]*CapacityManagerDataExport)
	}

	if snap.CapacityManagerState != nil {
		b.capacityManagerState = snap.CapacityManagerState
	} else {
		b.capacityManagerState = &CapacityManagerState{Status: capacityManagerStatusDisabled}
	}
}

// restoreVpcConfigFields copies the VPC ClassicLink and Block Public Access
// state from snap into b. Split out to keep Restore's growth in check.
// Must be called with b.mu held.
func (b *InMemoryBackend) restoreVpcConfigFields(snap *backendSnapshot) {
	if snap.ClassicLinkInstances != nil {
		b.classicLinkInstances = snap.ClassicLinkInstances
	} else {
		b.classicLinkInstances = make(map[string]*ClassicLinkInstance)
	}

	if snap.VpcBlockPublicAccessOptions != nil {
		b.vpcBlockPublicAccessOptions = snap.VpcBlockPublicAccessOptions
	} else {
		b.vpcBlockPublicAccessOptions = &VpcBlockPublicAccessOptions{
			InternetGatewayBlockMode: vpcBPABlockModeOff,
			State:                    vpcBPAStateDefault,
			ExclusionsAllowed:        vpcBPAExclusionsAllowed,
			ManagedBy:                vpcBPAManagedByAccount,
		}
	}

	if snap.VpcBlockPublicAccessExclusions != nil {
		b.vpcBlockPublicAccessExclusions = snap.VpcBlockPublicAccessExclusions
	} else {
		b.vpcBlockPublicAccessExclusions = make(map[string]*VpcBlockPublicAccessExclusion)
	}
}

// restoreTGWMulticastFields copies the transit gateway multicast domain and
// metering policy state maps from snap into b. Split out to keep Restore's
// growth in check. Must be called with b.mu held.
func (b *InMemoryBackend) restoreTGWMulticastFields(snap *backendSnapshot) {
	if snap.TgwMulticastDomains != nil {
		b.tgwMulticastDomains = snap.TgwMulticastDomains
	} else {
		b.tgwMulticastDomains = make(map[string]*TransitGatewayMulticastDomain)
	}

	if snap.TgwMulticastGroupEntries != nil {
		b.tgwMulticastGroupEntries = snap.TgwMulticastGroupEntries
	} else {
		b.tgwMulticastGroupEntries = make(map[string]*TransitGatewayMulticastGroupEntry)
	}

	if snap.TgwMeteringPolicies != nil {
		b.tgwMeteringPolicies = snap.TgwMeteringPolicies
	} else {
		b.tgwMeteringPolicies = make(map[string]*TransitGatewayMeteringPolicy)
	}

	if snap.TgwMeteringPolicyEntries != nil {
		b.tgwMeteringPolicyEntries = snap.TgwMeteringPolicyEntries
	} else {
		b.tgwMeteringPolicyEntries = make(map[string]*TransitGatewayMeteringPolicyEntry)
	}
}

// restoreRouteServerFields copies the Route Server state maps from snap into
// b. Split out to keep Restore's growth in check. Must be called with b.mu held.
func (b *InMemoryBackend) restoreRouteServerFields(snap *backendSnapshot) {
	if snap.RouteServers != nil {
		b.routeServers = snap.RouteServers
	} else {
		b.routeServers = make(map[string]*RouteServer)
	}
	if snap.RouteServerEndpoints != nil {
		b.routeServerEndpoints = snap.RouteServerEndpoints
	} else {
		b.routeServerEndpoints = make(map[string]*RouteServerEndpoint)
	}
	if snap.RouteServerPeers != nil {
		b.routeServerPeers = snap.RouteServerPeers
	} else {
		b.routeServerPeers = make(map[string]*RouteServerPeer)
	}
	if snap.RouteServerAssociations != nil {
		b.routeServerAssociations = snap.RouteServerAssociations
	} else {
		b.routeServerAssociations = make(map[string]*RouteServerAssociation)
	}
	if snap.RouteServerPropagations != nil {
		b.routeServerPropagations = snap.RouteServerPropagations
	} else {
		b.routeServerPropagations = make(map[string]*RouteServerPropagation)
	}
}

// restoreLocalGatewayFields copies the Local Gateway state maps from snap into
// b. Split out to keep Restore's growth in check. Must be called with b.mu held.
func (b *InMemoryBackend) restoreLocalGatewayFields(snap *backendSnapshot) {
	if snap.LocalGateways != nil {
		b.localGateways = snap.LocalGateways
	} else {
		b.localGateways = make(map[string]*LocalGateway)
	}
	if snap.LocalGatewayVirtualInterfaces != nil {
		b.localGatewayVirtualInterfaces = snap.LocalGatewayVirtualInterfaces
	} else {
		b.localGatewayVirtualInterfaces = make(map[string]*LocalGatewayVirtualInterface)
	}
	if snap.LocalGatewayVirtualInterfaceGroups != nil {
		b.localGatewayVirtualInterfaceGroups = snap.LocalGatewayVirtualInterfaceGroups
	} else {
		b.localGatewayVirtualInterfaceGroups = make(map[string]*LocalGatewayVirtualInterfaceGroup)
	}
	if snap.LocalGatewayRouteTables != nil {
		b.localGatewayRouteTables = snap.LocalGatewayRouteTables
	} else {
		b.localGatewayRouteTables = make(map[string]*LocalGatewayRouteTable)
	}
	if snap.LocalGatewayRoutes != nil {
		b.localGatewayRoutes = snap.LocalGatewayRoutes
	} else {
		b.localGatewayRoutes = make(map[string]*LocalGatewayRoute)
	}
	if snap.LocalGatewayRouteTableVpcAssocs != nil {
		b.localGatewayRouteTableVpcAssociations = snap.LocalGatewayRouteTableVpcAssocs
	} else {
		b.localGatewayRouteTableVpcAssociations = make(map[string]*LocalGatewayRouteTableVpcAssociation)
	}
	if snap.LocalGatewayRTVifGroupAssocs != nil {
		b.localGatewayRouteTableVifGroupAssociations = snap.LocalGatewayRTVifGroupAssocs
	} else {
		b.localGatewayRouteTableVifGroupAssociations = make(
			map[string]*LocalGatewayRouteTableVirtualInterfaceGroupAssociation,
		)
	}
}

// restoreCoreFields copies the core map/bool/scalar fields from snap into b.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) restoreCoreFields(snap *backendSnapshot) {
	b.instances = snap.Instances
	b.securityGroups = snap.SecurityGroups
	b.vpcs = snap.VPCs
	b.subnets = snap.Subnets
	b.keyPairs = snap.KeyPairs
	b.volumes = snap.Volumes
	b.addresses = snap.Addresses
	b.internetGateways = snap.InternetGateways
	b.routeTables = snap.RouteTables
	b.natGateways = snap.NatGateways
	b.networkInterfaces = snap.NetworkInterfaces
	b.spotRequests = snap.SpotRequests
	b.placementGroups = snap.PlacementGroups
	b.images = snap.Images
	b.imageUsageReports = snap.ImageUsageReports
	b.launchTemplates = snap.LaunchTemplates
	b.vpcEndpoints = snap.VpcEndpoints
	b.snapshots = snap.Snapshots
	b.networkACLs = snap.NetworkACLs
	b.transitGateways = snap.TransitGateways
	b.flowLogs = snap.FlowLogs
	b.dhcpOptionSets = snap.DhcpOptionSets
	b.tags = snap.Tags
}

// restoreExtendedFields copies extended/appendix fields from snap into b.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) restoreExtendedFields(snap *backendSnapshot) {
	b.addressTransfers = snap.AddressTransfers
	b.capacityReservations = snap.CapacityReservations
	b.reservedInstancesExchanges = snap.ReservedInstancesExchanges
	b.tgwMulticastDomainAssociations = snap.TGWMulticastDomainAssociations
	b.tgwPeeringAttachments = snap.TGWPeeringAttachments
	b.tgwVpcAttachments = snap.TGWVpcAttachments
	b.vpcEndpointConnections = snap.VpcEndpointConnections
	b.vpcPeeringConnections = snap.VpcPeeringConnections
	b.byoipCidrs = snap.ByoipCidrs
	b.dedicatedHosts = snap.DedicatedHosts
	b.vpnGateways = snap.VpnGateways
	b.customerGateways = snap.CustomerGateways
	b.ipams = snap.Ipams
	b.ipamScopes = snap.IpamScopes
	b.ipamPools = snap.IpamPools
	b.ipamPoolCidrs = snap.IpamPoolCidrs
	b.ipamPoolAllocations = snap.IpamPoolAllocations
	b.ipamResourceDiscoveries = snap.IpamResourceDiscoveries
	b.ipamResourceDiscoveryAssocs = snap.IpamResourceDiscoveryAssocs
	b.ipamByoasns = snap.IpamByoasns
	b.ipamAsnAssociations = snap.IpamAsnAssociations
	b.ipamVerificationTokens = snap.IpamVerificationTokens
	b.ipamResourceCidrs = snap.IpamResourceCidrs
	b.ipamPrefixListResolvers = snap.IpamPrefixListResolvers
	b.ipamPrefixListResolverVersions = snap.IpamPrefixListResolverVersions
	b.ipamPrefixListResolverTargets = snap.IpamPrefixListResolverTargets
	b.ipamPolicies = snap.IpamPolicies
	b.ipamPolicyEnabledTargets = snap.IpamPolicyEnabledTargets
	b.ipamOrgAdminAccountID = snap.IpamOrgAdminAccountID
	b.carrierGateways = snap.CarrierGateways
	b.fleets = snap.Fleets
	b.networkInsightsPaths = snap.NetworkInsightsPaths
	b.managedPrefixLists = snap.ManagedPrefixLists
	b.ebsEncryptionByDefault = snap.EbsEncryptionByDefault
	b.serialConsoleAccess = snap.SerialConsoleAccess
	b.freePrivateIPs = snap.FreePrivateIPs
	b.AccountID = snap.AccountID
	b.Region = snap.Region
	b.nextPrivateIPIndex = snap.NextPrivateIPIndex
	b.nextElasticIPIndex = snap.NextElasticIPIndex
	b.restoreImageAndPoolFields(snap)
}

// restoreImageAndPoolFields copies the Scheduled Instances / COIP / public IPv4-IPv6 pool /
// Allowed Images Settings / image task / usage report fields from snap into b. Must be
// called with b.mu held for writing.
func (b *InMemoryBackend) restoreImageAndPoolFields(snap *backendSnapshot) {
	b.scheduledInstances = snap.ScheduledInstances
	b.scheduledInstanceLaunched = snap.ScheduledInstanceLaunched
	b.coipPools = snap.CoipPools
	b.coipCidrs = snap.CoipCidrs
	b.ipv4Pools = snap.Ipv4Pools
	b.ipv6Pools = snap.Ipv6Pools
	b.allowedImagesSettings = snap.AllowedImagesSettings
	b.storeImageTasks = snap.StoreImageTasks
	b.usageReports = snap.UsageReports
	b.usageReportEntries = snap.UsageReportEntries
	b.instanceProductCodes = snap.InstanceProductCodes
	b.bundleTasks = snap.BundleTasks
	b.conversionTasks = snap.ConversionTasks
	b.exportTasks = snap.ExportTasks
	b.exportImageTasks = snap.ExportImageTasks
	b.trunkInterfaceAssociations = snap.TrunkInterfaceAssociations
	b.enclaveCertIamRoles = snap.EnclaveCertIamRoles
}

// initMissingMaps ensures all map fields in the snapshot are non-nil.
// This prevents nil-map panics when the snapshot was created from a backend
// that never populated a particular resource type.
func (s *backendSnapshot) initMissingMaps() {
	s.initCoreMaps()
	s.initDeepDiveMaps()
	s.initNewOpsMaps()
}

// initCoreMaps initialises the original map fields.
func (s *backendSnapshot) initCoreMaps() {
	if s.Instances == nil {
		s.Instances = make(map[string]*Instance)
	}

	if s.SecurityGroups == nil {
		s.SecurityGroups = make(map[string]*SecurityGroup)
	}

	if s.VPCs == nil {
		s.VPCs = make(map[string]*VPC)
	}

	if s.Subnets == nil {
		s.Subnets = make(map[string]*Subnet)
	}

	if s.KeyPairs == nil {
		s.KeyPairs = make(map[string]*KeyPair)
	}

	if s.Volumes == nil {
		s.Volumes = make(map[string]*Volume)
	}

	if s.Addresses == nil {
		s.Addresses = make(map[string]*Address)
	}

	if s.InternetGateways == nil {
		s.InternetGateways = make(map[string]*InternetGateway)
	}

	if s.RouteTables == nil {
		s.RouteTables = make(map[string]*RouteTable)
	}

	if s.NatGateways == nil {
		s.NatGateways = make(map[string]*NatGateway)
	}

	if s.NetworkInterfaces == nil {
		s.NetworkInterfaces = make(map[string]*NetworkInterface)
	}

	if s.SpotRequests == nil {
		s.SpotRequests = make(map[string]*SpotInstanceRequest)
	}

	if s.PlacementGroups == nil {
		s.PlacementGroups = make(map[string]*PlacementGroup)
	}

	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}
}

// initMapIfNil initialises m to an empty map if it is nil.
func initMapIfNil[K comparable, V any](m *map[K]V) {
	if *m == nil {
		*m = make(map[K]V)
	}
}

func (s *backendSnapshot) initDeepDiveMaps() {
	initMapIfNil(&s.Images)
	initMapIfNil(&s.ImageUsageReports)
	initMapIfNil(&s.LaunchTemplates)
	initMapIfNil(&s.VpcEndpoints)
	initMapIfNil(&s.Snapshots)
	initMapIfNil(&s.NetworkACLs)
	initMapIfNil(&s.TransitGateways)
	initMapIfNil(&s.FlowLogs)
	initMapIfNil(&s.DhcpOptionSets)
}

// initNewOpsMaps initialises the map fields added for the new Accept/Advertise/Allocate operations.
func (s *backendSnapshot) initNewOpsMaps() {
	if s.AddressTransfers == nil {
		s.AddressTransfers = make(map[string]*AddressTransfer)
	}

	if s.CapacityReservations == nil {
		s.CapacityReservations = make(map[string]*CapacityReservation)
	}

	if s.ReservedInstancesExchanges == nil {
		s.ReservedInstancesExchanges = make(map[string]*ReservedInstancesExchange)
	}

	if s.TGWMulticastDomainAssociations == nil {
		s.TGWMulticastDomainAssociations = make(
			map[string]*TransitGatewayMulticastDomainAssociation,
		)
	}

	if s.TGWPeeringAttachments == nil {
		s.TGWPeeringAttachments = make(map[string]*TransitGatewayPeeringAttachment)
	}

	if s.TGWVpcAttachments == nil {
		s.TGWVpcAttachments = make(map[string]*TransitGatewayVpcAttachment)
	}

	if s.VpcEndpointConnections == nil {
		s.VpcEndpointConnections = make(map[string]*VpcEndpointConnection)
	}

	if s.VpcPeeringConnections == nil {
		s.VpcPeeringConnections = make(map[string]*VpcPeeringConnection)
	}

	if s.ByoipCidrs == nil {
		s.ByoipCidrs = make(map[string]*ByoipCidr)
	}

	if s.DedicatedHosts == nil {
		s.DedicatedHosts = make(map[string]*Host)
	}

	initMapIfNil(&s.TgwPolicyTables)
	initMapIfNil(&s.TgwPolicyTableAssociations)
	initMapIfNil(&s.TgwRouteTableAnnouncements)

	s.initAppendixMaps()
}

func (s *backendSnapshot) initAppendixMaps() {
	initMapIfNil(&s.VpnGateways)
	initMapIfNil(&s.CustomerGateways)
	initMapIfNil(&s.Ipams)
	initMapIfNil(&s.IpamScopes)
	initMapIfNil(&s.IpamPools)
	initMapIfNil(&s.IpamPoolCidrs)
	initMapIfNil(&s.IpamPoolAllocations)
	initMapIfNil(&s.IpamResourceDiscoveries)
	initMapIfNil(&s.IpamResourceDiscoveryAssocs)
	initMapIfNil(&s.IpamByoasns)
	initMapIfNil(&s.IpamAsnAssociations)
	initMapIfNil(&s.IpamVerificationTokens)
	initMapIfNil(&s.IpamResourceCidrs)
	initMapIfNil(&s.IpamPrefixListResolvers)
	initMapIfNil(&s.IpamPrefixListResolverVersions)
	initMapIfNil(&s.IpamPrefixListResolverTargets)
	initMapIfNil(&s.IpamPolicies)
	initMapIfNil(&s.IpamPolicyEnabledTargets)
	initMapIfNil(&s.CarrierGateways)
	initMapIfNil(&s.Fleets)
	initMapIfNil(&s.NetworkInsightsPaths)
	initMapIfNil(&s.ManagedPrefixLists)
	s.initImageAndPoolMaps()
}

// initImageAndPoolMaps initialises the map fields added for Scheduled Instances, COIP,
// public IPv4/IPv6 pools, Allowed Images Settings, and image tasks/usage reports.
func (s *backendSnapshot) initImageAndPoolMaps() {
	initMapIfNil(&s.ScheduledInstances)
	initMapIfNil(&s.ScheduledInstanceLaunched)
	initMapIfNil(&s.CoipPools)
	initMapIfNil(&s.CoipCidrs)
	initMapIfNil(&s.Ipv4Pools)
	initMapIfNil(&s.Ipv6Pools)
	initMapIfNil(&s.StoreImageTasks)
	initMapIfNil(&s.UsageReports)
	initMapIfNil(&s.UsageReportEntries)
	initMapIfNil(&s.InstanceProductCodes)
	initMapIfNil(&s.BundleTasks)
	initMapIfNil(&s.ConversionTasks)
	initMapIfNil(&s.ExportTasks)
	initMapIfNil(&s.ExportImageTasks)
	initMapIfNil(&s.TrunkInterfaceAssociations)
	initMapIfNil(&s.EnclaveCertIamRoles)
	initMapIfNil(&s.MacModificationTasks)
	initMapIfNil(&s.SecondaryNetworks)
	initMapIfNil(&s.SecondarySubnets)
	initMapIfNil(&s.SecondaryInterfaces)
	initMapIfNil(&s.ServiceLinkVirtualInterfaces)
	initMapIfNil(&s.OutpostLags)
	initMapIfNil(&s.AvailabilityZoneGroupOptIns)
	initMapIfNil(&s.SQLHaRegistrations)
	initMapIfNil(&s.SQLHaHistory)

	if s.AllowedImagesSettings == nil {
		s.AllowedImagesSettings = &AllowedImagesSettings{
			State:     allowedImagesStateDisabled,
			ManagedBy: allowedImagesManagedByAccnt,
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// It type-asserts the backend to check for Snapshot support so that alternative
// backend implementations that do not persist state still compile.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
// It type-asserts the backend to check for Restore support so that alternative
// backend implementations that do not persist state still compile.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
