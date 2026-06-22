package ec2

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapTGWMcastAssoc is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWMcastAssoc = TransitGatewayMulticastDomainAssociation

// snapRIExchange is a type alias used in backendSnapshot to keep line lengths manageable.
type snapRIExchange = ReservedInstancesExchange

// snapTGWPeeringAtt is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWPeeringAtt = TransitGatewayPeeringAttachment

// snapTGWVpcAtt is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWVpcAtt = TransitGatewayVpcAttachment

type backendSnapshot struct {
	SnapshotAttributes                 map[string]map[string]string                    `json:"snapshotAttributes"`
	ImageDeprecated                    map[string]string                               `json:"imageDeprecated"`
	VPCs                               map[string]*VPC                                 `json:"vpcs,omitempty"`
	NatGateways                        map[string]*NatGateway                          `json:"natGateways,omitempty"`
	KeyPairs                           map[string]*KeyPair                             `json:"keyPairs,omitempty"`
	Volumes                            map[string]*Volume                              `json:"volumes,omitempty"`
	Addresses                          map[string]*Address                             `json:"addresses,omitempty"`
	InternetGateways                   map[string]*InternetGateway                     `json:"internetGateways"`
	SecurityGroups                     map[string]*SecurityGroup                       `json:"securityGroups"`
	Instances                          map[string]*Instance                            `json:"instances,omitempty"`
	Subnets                            map[string]*Subnet                              `json:"subnets,omitempty"`
	SpotRequests                       map[string]*SpotInstanceRequest                 `json:"spotRequests,omitempty"`
	PlacementGroups                    map[string]*PlacementGroup                      `json:"placementGroups"`
	Images                             map[string]*AMIStub                             `json:"images,omitempty"`
	ImageUsageReports                  map[string]*ImageUsageReport                    `json:"imageUsageReports"`
	LaunchTemplates                    map[string]*LaunchTemplate                      `json:"launchTemplates"`
	VpcEndpoints                       map[string]*VpcEndpoint                         `json:"vpcEndpoints,omitempty"`
	Snapshots                          map[string]*Snapshot                            `json:"snapshots,omitempty"`
	NetworkACLs                        map[string]*StoredNetworkACL                    `json:"networkACLs,omitempty"`
	TransitGateways                    map[string]*TransitGateway                      `json:"transitGateways"`
	FlowLogs                           map[string]*FlowLog                             `json:"flowLogs,omitempty"`
	DhcpOptionSets                     map[string]*DhcpOptions                         `json:"dhcpOptionSets"`
	Tags                               map[string]map[string]string                    `json:"tags,omitempty"`
	AddressTransfers                   map[string]*AddressTransfer                     `json:"addressTransfers"`
	CapacityReservations               map[string]*CapacityReservation                 `json:"capacityReservations"`
	ReservedInstancesExchanges         map[string]*snapRIExchange                      `json:"reservedInstancesExchanges"`
	TGWMulticastDomainAssociations     map[string]*snapTGWMcastAssoc                   `json:"tgwMcastDomainAssoc"`
	TGWPeeringAttachments              map[string]*snapTGWPeeringAtt                   `json:"tgwPeeringAttachments"`
	TGWVpcAttachments                  map[string]*snapTGWVpcAtt                       `json:"tgwVpcAttachments"`
	VpcEndpointConnections             map[string]*VpcEndpointConnection               `json:"vpcEndpointConnections"`
	VpcPeeringConnections              map[string]*VpcPeeringConnection                `json:"vpcPeeringConnections"`
	ByoipCidrs                         map[string]*ByoipCidr                           `json:"byoipCidrs,omitempty"`
	DedicatedHosts                     map[string]*Host                                `json:"dedicatedHosts"`
	VpnGateways                        map[string]*VpnGateway                          `json:"vpnGateways,omitempty"`
	CustomerGateways                   map[string]*CustomerGateway                     `json:"customerGateways"`
	Ipams                              map[string]*Ipam                                `json:"ipams,omitempty"`
	IpamPools                          map[string]*IpamPool                            `json:"ipamPools,omitempty"`
	IpamPoolAllocations                map[string]*IpamPoolAllocation                  `json:"ipamPoolAllocations"`
	CarrierGateways                    map[string]*CarrierGateway                      `json:"carrierGateways"`
	Fleets                             map[string]*Fleet                               `json:"fleets,omitempty"`
	NetworkInsightsPaths               map[string]*NetworkInsightsPath                 `json:"networkInsightsPaths"`
	ManagedPrefixLists                 map[string]*ManagedPrefixList                   `json:"managedPrefixLists"`
	EgressOnlyIGWs                     map[string]*EgressOnlyInternetGateway           `json:"egressOnlyIGWs"`
	IamAssociations                    map[string]*IamInstanceProfileAssociation       `json:"iamAssociations"`
	TgwRouteTables                     map[string]*TransitGatewayRouteTable            `json:"tgwRouteTables"`
	TgwRoutes                          map[string]*TransitGatewayRoute                 `json:"tgwRoutes,omitempty"`
	TgwRTAssociations                  map[string]*TransitGatewayRouteTableAssociation `json:"tgwRTAssociations"`
	ReservedInstancesModifications     map[string]*ReservedInstancesModification       `json:"rim"`
	ReservedInstancesListings          map[string]*ReservedInstancesListing            `json:"reservedInstancesListings"`
	VpcCidrAssociations                map[string]*VpcCidrBlockAssociation             `json:"vpcCidrAssociations"`
	VpnConnections                     map[string]*VpnConnection                       `json:"vpnConnections"`
	VpcEndpointServiceConfigs          map[string]*VpcEndpointServiceConfig            `json:"vpcEndpointServiceConfigs"`
	SpotFleets                         map[string]*SpotFleetRequest                    `json:"spotFleets,omitempty"`
	SpotFleetHistory                   map[string][]SpotFleetHistoryRecord             `json:"spotFleetHistory"`
	VolumeModifications                map[string]*VolumeModification                  `json:"volumeModifications"`
	SnapshotTiers                      map[string]string                               `json:"snapshotTiers,omitempty"`
	NetworkInterfaces                  map[string]*NetworkInterface                    `json:"networkInterfaces"`
	RouteTables                        map[string]*RouteTable                          `json:"routeTables,omitempty"`
	TrafficMirrorFilters               map[string]*TrafficMirrorFilter                 `json:"trafficMirrorFilters"`
	VpcPeeringOptions                  map[string]*PeeringConnectionOptions            `json:"vpcPeeringOptions"`
	SubnetCIDRAssociations             map[string][]*SubnetCIDRAssociation             `json:"subnetCIDRAssociations"`
	AddressAttributes                  map[string]*AddressAttribute                    `json:"addressAttributes"`
	InstanceMonitoring                 map[string]string                               `json:"instanceMonitoring"`
	InstanceCreditSpecs                map[string]string                               `json:"instanceCreditSpecs"`
	InstanceIMDSOptions                map[string]*IMDSOptions                         `json:"instanceIMDSOptions"`
	InstanceMetadataDefaults           *InstanceMetadataDefaults                       `json:"instanceMetadataDefaults"`
	InstanceEventNotifAttrs            *InstanceEventNotificationAttributes            `json:"instanceEventNotifAttrs"`
	NiPermissions                      map[string]*NetworkInterfacePermission          `json:"niPermissions,omitempty"`
	NiIPv6Addresses                    map[string][]string                             `json:"niIPv6Addresses"`
	IDFormatSettings                   map[string]bool                                 `json:"idFormatSettings"`
	EndpointConnectionNotifs           map[string]*VpcEndpointConnectionNotification   `json:"endpointConnectionNotifs"`
	VpcEndpointServicePermissions      map[string][]string                             `json:"vpcEpSvcPerms"`
	SnapshotLocks                      map[string]*SnapshotLock                        `json:"snapshotLocks,omitempty"`
	ReplaceRootVolumeTasks             map[string]*ReplaceRootVolumeTask               `json:"replaceRootVolumeTasks"`
	SubnetCIDRReservations             map[string][]*SubnetCIDRReservation             `json:"subnetCIDRReservations"`
	ImageDisabled                      map[string]bool                                 `json:"imageDisabled,omitempty"`
	SgVpcAssociations                  map[string]map[string]string                    `json:"sgVpcAssociations"`
	ImageDeregistrationProtection      map[string]bool                                 `json:"imageDeregProtect"`
	ImageAttributes                    map[string]map[string]string                    `json:"imageAttributes"`
	VgwRoutePropagation                map[string]bool                                 `json:"vgwRoutePropagation"`
	ClientVpnEndpoints                 map[string]*ClientVpnEndpoint                   `json:"clientVpnEndpoints"`
	TgwConnects                        map[string]*TransitGatewayConnect               `json:"tgwConnects,omitempty"`
	TgwConnectPeers                    map[string]*TransitGatewayConnectPeer           `json:"tgwConnectPeers"`
	TgwPrefixListRefs                  map[string]*TransitGatewayPrefixListReference   `json:"tgwPrefixListRefs"`
	VerifiedAccessEndpoints            map[string]*VerifiedAccessEndpoint              `json:"verifiedAccessEndpoints"`
	VerifiedAccessGroups               map[string]*VerifiedAccessGroup                 `json:"verifiedAccessGroups"`
	VerifiedAccessInstances            map[string]*VerifiedAccessInstance              `json:"verifiedAccessInstances"`
	VerifiedAccessTrustProviders       map[string]*VerifiedAccessTrustProvider         `json:"vatps"`
	InstanceConnectEndpoints           map[string]*InstanceConnectEndpoint             `json:"instanceConnectEndpoints"`
	InstanceEventWindows               map[string]*InstanceEventWindow                 `json:"instanceEventWindows"`
	ImageImportTasks                   map[string]*ImageImportTask                     `json:"imageImportTasks"`
	SnapshotImportTasks                map[string]*SnapshotImportTask                  `json:"snapshotImportTasks"`
	RecycleBinImages                   map[string]*RecycleBinImage                     `json:"recycleBinImages"`
	RecycleBinSnapshots                map[string]*Snapshot                            `json:"recycleBinSnapshots"`
	RecycleBinVolumes                  map[string]*RecycleBinVolume                    `json:"recycleBinVolumes"`
	FastLaunchImages                   map[string]bool                                 `json:"fastLaunchImages"`
	FastSnapshotRestores               map[string]bool                                 `json:"fastSnapshotRestores"`
	VpnConnectionRoutes                map[string]*VpnConnectionRoute                  `json:"vpnConnectionRoutes"`
	SpotDatafeed                       *SpotDatafeed                                   `json:"spotDatafeed,omitempty"`
	VpcTenancy                         map[string]string                               `json:"vpcTenancy,omitempty"`
	TrafficMirrorFilterRules           map[string]*TrafficMirrorFilterRule             `json:"trafficMirrorFilterRules"`
	TrafficMirrorSessions              map[string]*TrafficMirrorSession                `json:"trafficMirrorSessions"`
	TrafficMirrorTargets               map[string]*TrafficMirrorTarget                 `json:"trafficMirrorTargets"`
	NetworkInsightsAnalyses            map[string]*NetworkInsightsAnalysis             `json:"networkInsightsAnalyses"`
	NetworkInsightsAccessScopes        map[string]*NetworkInsightsAccessScope          `json:"networkInsightsAccessScopes"`
	NetworkInsightsAccessScopeAnalyses map[string]*NetworkInsightsAccessScopeAnalysis  `json:"niasa"`
	ReservedInstances                  map[string]*ReservedInstance                    `json:"reservedInstances"`
	ReservedInstancesOfferings         map[string]*ReservedInstancesOffering           `json:"reservedInstancesOfferings"`
	Region                             string                                          `json:"region,omitempty"`
	AccountID                          string                                          `json:"accountID,omitempty"`
	FreePrivateIPs                     []string                                        `json:"freePrivateIPs"`
	NextPrivateIPIndex                 int                                             `json:"nextPrivateIPIndex"`
	NextElasticIPIndex                 int                                             `json:"nextElasticIPIndex"`
	EbsEncryptionByDefault             bool                                            `json:"ebsEncryptionByDefault"`
	SerialConsoleAccess                bool                                            `json:"serialConsoleAccess"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
//
//nolint:funlen // large state snapshot
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
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
		IpamPools:                          b.ipamPools,
		IpamPoolAllocations:                b.ipamPoolAllocations,
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
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ec2: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
//
//nolint:gocognit,gocyclo,cyclop,funlen // large state restore
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ec2", data, &snap); err != nil {
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

	return nil
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
	b.ipamPools = snap.IpamPools
	b.ipamPoolAllocations = snap.IpamPoolAllocations
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

	s.initAppendixMaps()
}

func (s *backendSnapshot) initAppendixMaps() {
	initMapIfNil(&s.VpnGateways)
	initMapIfNil(&s.CustomerGateways)
	initMapIfNil(&s.Ipams)
	initMapIfNil(&s.IpamPools)
	initMapIfNil(&s.IpamPoolAllocations)
	initMapIfNil(&s.CarrierGateways)
	initMapIfNil(&s.Fleets)
	initMapIfNil(&s.NetworkInsightsPaths)
	initMapIfNil(&s.ManagedPrefixLists)
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// It type-asserts the backend to check for Snapshot support so that alternative
// backend implementations that do not persist state still compile.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
// It type-asserts the backend to check for Restore support so that alternative
// backend implementations that do not persist state still compile.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
