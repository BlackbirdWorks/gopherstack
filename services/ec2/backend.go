package ec2

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Errors returned by the EC2 backend.
var (
	ErrInstanceNotFound      = errors.New("InvalidInstanceID.NotFound")
	ErrSecurityGroupNotFound = errors.New("InvalidGroup.NotFound")
	ErrVPCNotFound           = errors.New("InvalidVpcID.NotFound")
	ErrSubnetNotFound        = errors.New("InvalidSubnetID.NotFound")
	ErrInvalidParameter      = errors.New("InvalidParameterValue")
	ErrDuplicateSGName       = errors.New("InvalidGroup.Duplicate")
	ErrInvalidInstanceState  = errors.New("IncorrectInstanceState")
	ErrSpotFleetNotFound     = errors.New("InvalidSpotFleetRequestId.NotFound")
	ErrCIDRConflict          = errors.New("InvalidVpc.Conflict")
	ErrDryRunOperation       = errors.New("request would have succeeded, but DryRun flag is set")
	ErrDuplicatePermission   = errors.New("InvalidPermission.Duplicate")

	// ErrDependencyViolation is returned when an operation cannot complete
	// because another resource still depends on the target resource.
	ErrDependencyViolation = errors.New("DependencyViolation")

	// ErrVpcClassicLinkDisabled is returned by AttachClassicLinkVpc when the
	// target VPC has not been enabled for ClassicLink.
	ErrVpcClassicLinkDisabled = errors.New("VpcClassicLinkDisabled")

	// ErrClassicLinkInstanceNotFound is returned when a ClassicLink
	// instance/VPC linkage cannot be found (e.g. DetachClassicLinkVpc for an
	// instance that is not currently linked).
	ErrClassicLinkInstanceNotFound = errors.New("InvalidInstanceID.NotFound")

	// ErrVpcBlockPublicAccessExclusionNotFound is returned when a VPC Block
	// Public Access exclusion ID cannot be found.
	ErrVpcBlockPublicAccessExclusionNotFound = errors.New("InvalidVpcBlockPublicAccessExclusionId.NotFound")
)

// EC2 instance state codes as defined by the AWS EC2 API.
const (
	stateCodeRunning      = 16
	stateCodeTerminated   = 48
	stateCodeStopped      = 80
	stateCodePending      = 0
	stateCodeShuttingDown = 32
	stateCodeStopping     = 64

	// stateAvailable is the "available" state string shared by volumes, ENIs,
	// and other resources that are not currently in use.
	stateAvailable = "available"

	stateInUse              = "in-use"
	stateCancelled          = "cancelled"
	resourceTypeVPC         = "vpc"
	vpcDefaultName          = "vpc-default"
	archX8664               = "x86_64"
	resourceTypeFISInstance = "aws:ec2:instance"
	ec2BooleanFalse         = "false"

	// stateActive is the "active" state string used by peering connections,
	// capacity reservations, and spot instance requests.
	stateActive = "active"

	// lifecycleReconcileInterval is how often the reconciler advances transitional instance states.
	lifecycleReconcileInterval = 50 * time.Millisecond

	// cidrAllIPv4 is the IPv4 catch-all CIDR used in default security group egress rules.
	cidrAllIPv4 = "0.0.0.0/0"
)

// InstanceState represents the state of an EC2 instance.
type InstanceState struct {
	Name string `json:"name,omitempty"`
	Code int    `json:"code,omitempty"`
}

// Well-known instance states.
//
//nolint:gochecknoglobals // package-level sentinel values, analogous to exported errors
var (
	StateRunning      = InstanceState{Code: stateCodeRunning, Name: "running"}
	StateTerminated   = InstanceState{Code: stateCodeTerminated, Name: "terminated"}
	StateStopped      = InstanceState{Code: stateCodeStopped, Name: "stopped"}
	StatePending      = InstanceState{Code: stateCodePending, Name: "pending"}
	StateShuttingDown = InstanceState{Code: stateCodeShuttingDown, Name: "shutting-down"}
	StateStopping     = InstanceState{Code: stateCodeStopping, Name: "stopping"}
)

// Instance represents an EC2 instance (metadata only, no actual compute).
type Instance struct {
	LaunchTime            time.Time     `json:"launchTime"`
	TerminatedAt          time.Time     `json:"terminatedAt"`
	PublicDNSName         string        `json:"publicDNSName,omitempty"`
	KeyName               string        `json:"keyName,omitempty"`
	InstanceType          string        `json:"instanceType,omitempty"`
	ImageID               string        `json:"imageID,omitempty"`
	VPCID                 string        `json:"vpcID,omitempty"`
	SubnetID              string        `json:"subnetID,omitempty"`
	MetadataOptionsTokens string        `json:"metadataOptionsTokens,omitempty"`
	ID                    string        `json:"id,omitempty"`
	PrivateIP             string        `json:"privateIP,omitempty"`
	PublicIPAddress       string        `json:"publicIPAddress,omitempty"`
	MetadataOptionsState  string        `json:"metadataOptionsState,omitempty"`
	UserData              string        `json:"userData,omitempty"`
	SriovNetSupport       string        `json:"sriovNetSupport,omitempty"`
	ProviderID            string        `json:"providerID,omitempty"`
	SecurityGroups        []string      `json:"securityGroups,omitempty"`
	State                 InstanceState `json:"state"`
	SSHPort               int           `json:"sshPort,omitempty"`
	EnaSupport            bool          `json:"enaSupport,omitempty"`
}

// LaunchTemplate represents an EC2 launch template.
type LaunchTemplate struct {
	CreateTime           time.Time `json:"createTime"`
	ID                   string    `json:"id,omitempty"`
	Name                 string    `json:"name,omitempty"`
	ImageID              string    `json:"imageID,omitempty"`
	InstanceType         string    `json:"instanceType,omitempty"`
	CreatedBy            string    `json:"createdBy,omitempty"`
	DefaultVersionNumber int64     `json:"defaultVersionNumber"`
	LatestVersionNumber  int64     `json:"latestVersionNumber"`
}

// ImageUsageReport represents a synthetic AMI usage report entry.
type ImageUsageReport struct {
	GenerationDate string `json:"generationDate,omitempty"`
	ImageID        string `json:"imageID,omitempty"`
	State          string `json:"state,omitempty"`
}

// VpcEndpoint represents an EC2 VPC endpoint.
type VpcEndpoint struct {
	CreateTime      time.Time `json:"createTime"`
	ID              string    `json:"id,omitempty"`
	VPCID           string    `json:"vpcID,omitempty"`
	ServiceName     string    `json:"serviceName,omitempty"`
	State           string    `json:"state,omitempty"`
	VpcEndpointType string    `json:"vpcEndpointType,omitempty"`
	SubnetIDs       []string  `json:"subnetIDs,omitempty"`
	RouteTableIDs   []string  `json:"routeTableIDs,omitempty"`
}

// NetworkACL represents an EC2 network ACL.
type NetworkACL struct {
	ID             string   `json:"id,omitempty"`
	VPCID          string   `json:"vpcID,omitempty"`
	AssociationIDs []string `json:"associationIDs,omitempty"`
	IsDefault      bool     `json:"isDefault,omitempty"`
}

// InstanceStateChange records the state transition for a single instance.
// It is returned by StartInstances, StopInstances, and TerminateInstances so
// callers have accurate before/after information without hard-coding states.
type InstanceStateChange struct {
	InstanceID    string        `json:"instanceID,omitempty"`
	PreviousState InstanceState `json:"previousState"`
	CurrentState  InstanceState `json:"currentState"`
}

// SecurityGroupRule represents an inbound or outbound rule.
// Either IPRange or SourceGroupID is set; both can be empty for protocol-only rules.
type SecurityGroupRule struct {
	Protocol           string `json:"protocol,omitempty"`
	IPRange            string `json:"ipRange,omitempty"`
	SourceGroupID      string `json:"sourceGroupId,omitempty"`
	SourceGroupOwnerID string `json:"sourceGroupOwnerId,omitempty"`
	FromPort           int    `json:"fromPort,omitempty"`
	ToPort             int    `json:"toPort,omitempty"`
}

// SecurityGroup represents an EC2 security group.
type SecurityGroup struct {
	ID           string              `json:"id,omitempty"`
	Name         string              `json:"name,omitempty"`
	Description  string              `json:"description,omitempty"`
	VPCID        string              `json:"vpcID,omitempty"`
	IngressRules []SecurityGroupRule `json:"ingressRules,omitempty"`
	EgressRules  []SecurityGroupRule `json:"egressRules,omitempty"`
}

// VPC represents an EC2 VPC.
type VPC struct {
	ID        string `json:"id,omitempty"`
	CIDRBlock string `json:"cidrBlock,omitempty"`
	IsDefault bool   `json:"isDefault,omitempty"`

	// ClassicLinkEnabled reports whether the VPC has been enabled for
	// ClassicLink via EnableVpcClassicLink.
	ClassicLinkEnabled bool `json:"classicLinkEnabled,omitempty"`

	// ClassicLinkDNSSupported reports whether ClassicLink DNS support has been
	// enabled for the VPC via EnableVpcClassicLinkDnsSupport.
	ClassicLinkDNSSupported bool `json:"classicLinkDnsSupported,omitempty"`
}

// Subnet represents an EC2 Subnet.
type Subnet struct {
	ID                  string `json:"id,omitempty"`
	VPCID               string `json:"vpcID,omitempty"`
	CIDRBlock           string `json:"cidrBlock,omitempty"`
	AvailabilityZone    string `json:"availabilityZone,omitempty"`
	IsDefault           bool   `json:"isDefault,omitempty"`
	MapPublicIPOnLaunch bool   `json:"mapPublicIpOnLaunch,omitempty"`
}

// InMemoryBackend is the in-memory store for EC2 resources.
type InMemoryBackend struct {
	compute                        Compute
	dnsRegistrar                   DNSRegistrar
	addressTransfers               map[string]*AddressTransfer
	capacityReservations           map[string]*CapacityReservation
	vpcs                           map[string]*VPC
	subnets                        map[string]*Subnet
	keyPairs                       map[string]*KeyPair
	reservedInstancesExchanges     map[string]*ReservedInstancesExchange
	addresses                      map[string]*Address
	internetGateways               map[string]*InternetGateway
	natGateways                    map[string]*NatGateway
	routeTables                    map[string]*RouteTable
	placementGroups                map[string]*PlacementGroup
	spotRequests                   map[string]*SpotInstanceRequest
	instances                      map[string]*Instance
	images                         map[string]*AMIStub
	imageUsageReports              map[string]*ImageUsageReport
	launchTemplates                map[string]*LaunchTemplate
	vpcEndpoints                   map[string]*VpcEndpoint
	tags                           map[string]map[string]string
	securityGroups                 map[string]*SecurityGroup
	networkInterfaces              map[string]*NetworkInterface
	volumes                        map[string]*Volume
	tgwMulticastDomainAssociations map[string]*TransitGatewayMulticastDomainAssociation
	tgwPeeringAttachments          map[string]*TransitGatewayPeeringAttachment
	tgwVpcAttachments              map[string]*TransitGatewayVpcAttachment
	vpcEndpointConnections         map[string]*VpcEndpointConnection
	vpcPeeringConnections          map[string]*VpcPeeringConnection
	byoipCidrs                     map[string]*ByoipCidr
	dedicatedHosts                 map[string]*Host
	snapshots                      map[string]*Snapshot
	networkACLs                    map[string]*StoredNetworkACL
	transitGateways                map[string]*TransitGateway
	flowLogs                       map[string]*FlowLog
	dhcpOptionSets                 map[string]*DhcpOptions
	egressOnlyIGWs                 map[string]*EgressOnlyInternetGateway
	iamAssociations                map[string]*IamInstanceProfileAssociation
	tgwRouteTables                 map[string]*TransitGatewayRouteTable
	tgwRoutes                      map[string]*TransitGatewayRoute
	tgwRTAssociations              map[string]*TransitGatewayRouteTableAssociation
	tgwPolicyTables                map[string]*TransitGatewayPolicyTable
	tgwPolicyTableAssociations     map[string]*TransitGatewayPolicyTableAssociation
	tgwRouteTableAnnouncements     map[string]*TransitGatewayRouteTableAnnouncement
	vpcCidrAssociations            map[string]*VpcCidrBlockAssociation
	vpnGateways                    map[string]*VpnGateway
	customerGateways               map[string]*CustomerGateway
	vpnConnections                 map[string]*VpnConnection
	vpcEndpointServiceConfigs      map[string]*VpcEndpointServiceConfig
	ipams                          map[string]*Ipam
	ipamScopes                     map[string]*IpamScope
	ipamPools                      map[string]*IpamPool
	ipamPoolCidrs                  map[string][]*IpamPoolCidr
	ipamPoolAllocations            map[string]*IpamPoolAllocation
	ipamResourceDiscoveries        map[string]*IpamResourceDiscovery
	ipamResourceDiscoveryAssocs    map[string]*IpamResourceDiscoveryAssociation
	ipamByoasns                    map[string]*IpamByoasn
	ipamAsnAssociations            map[string]*IpamAsnAssociation
	ipamVerificationTokens         map[string]*IpamExternalResourceVerificationToken
	ipamResourceCidrs              map[string]*IpamResourceCidr
	ipamPrefixListResolvers        map[string]*IpamPrefixListResolver
	ipamPrefixListResolverVersions map[string][]int64
	ipamPrefixListResolverTargets  map[string]*IpamPrefixListResolverTarget
	ipamPolicies                   map[string]*IpamPolicy
	ipamPolicyEnabledTargets       map[string]string
	ipamOrgAdminAccountID          string
	spotFleets                     map[string]*SpotFleetRequest
	spotFleetHistory               map[string][]SpotFleetHistoryRecord
	// batch1 additions
	volumeModifications      map[string]*VolumeModification
	snapshotTiers            map[string]string
	snapshotAttributes       map[string]map[string]string
	sgVpcAssociations        map[string]map[string]string
	vpcTenancy               map[string]string
	vpcPeeringOptions        map[string]*PeeringConnectionOptions
	subnetCIDRAssociations   map[string][]*SubnetCIDRAssociation
	addressAttributes        map[string]*AddressAttribute
	instanceMonitoring       map[string]string
	instanceCreditSpecs      map[string]string
	instanceIMDSOptions      map[string]*IMDSOptions
	instanceMetadataDefaults *InstanceMetadataDefaults
	instanceEventNotifAttrs  *InstanceEventNotificationAttributes
	niPermissions            map[string]*NetworkInterfacePermission
	niIPv6Addresses          map[string][]string
	idFormatSettings         map[string]bool
	// batch2 additions
	endpointConnectionNotifs      map[string]*VpcEndpointConnectionNotification
	vpcEndpointServicePermissions map[string][]string
	snapshotLocks                 map[string]*SnapshotLock
	replaceRootVolumeTasks        map[string]*ReplaceRootVolumeTask
	subnetCIDRReservations        map[string][]*SubnetCIDRReservation
	imageDisabled                 map[string]bool
	imageDeprecated               map[string]string
	imageDeregistrationProtection map[string]bool
	imageAttributes               map[string]map[string]string
	vgwRoutePropagation           map[string]bool
	// batch4 additions
	managedPrefixLists           map[string]*ManagedPrefixList
	clientVpnEndpoints           map[string]*ClientVpnEndpoint
	tgwConnects                  map[string]*TransitGatewayConnect
	tgwConnectPeers              map[string]*TransitGatewayConnectPeer
	tgwPrefixListRefs            map[string]*TransitGatewayPrefixListReference
	verifiedAccessEndpoints      map[string]*VerifiedAccessEndpoint
	verifiedAccessGroups         map[string]*VerifiedAccessGroup
	verifiedAccessInstances      map[string]*VerifiedAccessInstance
	verifiedAccessTrustProviders map[string]*VerifiedAccessTrustProvider
	// batch3 additions
	instanceConnectEndpoints map[string]*InstanceConnectEndpoint
	instanceEventWindows     map[string]*InstanceEventWindow
	imageImportTasks         map[string]*ImageImportTask
	snapshotImportTasks      map[string]*SnapshotImportTask
	recycleBinImages         map[string]*RecycleBinImage
	recycleBinSnapshots      map[string]*Snapshot
	recycleBinVolumes        map[string]*RecycleBinVolume
	fastLaunchImages         map[string]bool
	fastSnapshotRestores     map[string]bool
	vpnConnectionRoutes      map[string]*VpnConnectionRoute
	spotDatafeed             *SpotDatafeed
	// batch5 additions
	trafficMirrorFilters               map[string]*TrafficMirrorFilter
	trafficMirrorFilterRules           map[string]*TrafficMirrorFilterRule
	trafficMirrorSessions              map[string]*TrafficMirrorSession
	trafficMirrorTargets               map[string]*TrafficMirrorTarget
	fleets                             map[string]*Fleet
	networkInsightsPaths               map[string]*NetworkInsightsPath
	networkInsightsAnalyses            map[string]*NetworkInsightsAnalysis
	networkInsightsAccessScopes        map[string]*NetworkInsightsAccessScope
	networkInsightsAccessScopeAnalyses map[string]*NetworkInsightsAccessScopeAnalysis
	carrierGateways                    map[string]*CarrierGateway
	reservedInstances                  map[string]*ReservedInstance
	reservedInstancesOfferings         map[string]*ReservedInstancesOffering
	reservedInstancesListings          map[string]*ReservedInstancesListing
	reservedInstancesModifications     map[string]*ReservedInstancesModification
	// route server additions
	routeServers            map[string]*RouteServer
	routeServerEndpoints    map[string]*RouteServerEndpoint
	routeServerPeers        map[string]*RouteServerPeer
	routeServerAssociations map[string]*RouteServerAssociation
	routeServerPropagations map[string]*RouteServerPropagation
	// local gateway additions
	localGateways                              map[string]*LocalGateway
	localGatewayVirtualInterfaces              map[string]*LocalGatewayVirtualInterface
	localGatewayVirtualInterfaceGroups         map[string]*LocalGatewayVirtualInterfaceGroup
	localGatewayRouteTables                    map[string]*LocalGatewayRouteTable
	localGatewayRoutes                         map[string]*LocalGatewayRoute
	localGatewayRouteTableVpcAssociations      map[string]*LocalGatewayRouteTableVpcAssociation
	localGatewayRouteTableVifGroupAssociations map[string]*LocalGatewayRouteTableVirtualInterfaceGroupAssociation
	// transit gateway multicast domain / metering policy additions
	tgwMulticastDomains      map[string]*TransitGatewayMulticastDomain
	tgwMulticastGroupEntries map[string]*TransitGatewayMulticastGroupEntry
	tgwMeteringPolicies      map[string]*TransitGatewayMeteringPolicy
	tgwMeteringPolicyEntries map[string]*TransitGatewayMeteringPolicyEntry
	// VPC ClassicLink / Block Public Access additions
	classicLinkInstances           map[string]*ClassicLinkInstance
	vpcBlockPublicAccessOptions    *VpcBlockPublicAccessOptions
	vpcBlockPublicAccessExclusions map[string]*VpcBlockPublicAccessExclusion
	// Capacity Reservation Fleet / Capacity Block / Capacity Manager additions
	capacityReservationFleets          map[string]*CapacityReservationFleet
	capacityBlockOfferings             map[string]*CapacityBlockOffering
	capacityBlockExtensionOfferings    map[string]*CapacityBlockExtensionOffering
	capacityBlocks                     map[string]*CapacityBlock
	capacityBlockExtensions            map[string]*CapacityBlockExtension
	capacityReservationBillingRequests map[string]*CapacityReservationBillingRequest
	capacityManagerDataExports         map[string]*CapacityManagerDataExport
	capacityManagerState               *CapacityManagerState
	// VerifiedAccess policy / logging additions
	verifiedAccessEndpointPolicies       map[string]*VerifiedAccessPolicy
	verifiedAccessGroupPolicies          map[string]*VerifiedAccessPolicy
	verifiedAccessInstanceLoggingConfigs map[string]*VerifiedAccessInstanceLoggingConfig
	// FPGA image additions
	fpgaImages map[string]*FpgaImage
	// Scheduled Instances additions
	scheduledInstances        map[string]*ScheduledInstance
	scheduledInstanceLaunched map[string]int32
	// COIP / Public IPv4 / IPv6 pool additions
	coipPools map[string]*CoipPool
	coipCidrs map[string]*CoipCidr
	ipv4Pools map[string]*Ipv4Pool
	ipv6Pools map[string]*Ipv6Pool
	// VM Import/Export, Bundle, and Conversion Task additions
	bundleTasks      map[string]*BundleTask
	conversionTasks  map[string]*ConversionTask
	exportTasks      map[string]*ExportTask
	exportImageTasks map[string]*ExportImageTaskRec
	// Trunk Interface / Enclave Certificate IAM Role additions
	trunkInterfaceAssociations map[string]*TrunkInterfaceAssociation
	enclaveCertIamRoles        map[string][]*EnclaveCertIamRoleAssociation
	// Allowed Images Settings / Store-Restore Image Task / Image Usage Report additions
	allowedImagesSettings     *AllowedImagesSettings
	storeImageTasks           map[string]*StoreImageTask
	usageReports              map[string]*UsageReport
	usageReportEntries        map[string][]*UsageReportEntry
	instanceProductCodes      map[string][]string
	mu                        *lockmetrics.RWMutex
	lifecycleStop             chan struct{}
	eniIDByAttachment         map[string]string
	eniIDsByInstance          map[string]map[string]struct{}
	instanceIDsByVPC          map[string]map[string]struct{}
	snapshotBlockPublicAccess string
	ebsDefaultKmsKeyID        string
	imageBlockPublicAccess    string
	defaultCreditSpec         string
	Region                    string `json:"region,omitempty"`
	AccountID                 string `json:"accountID,omitempty"`
	freePrivateIPs            []string
	nextPrivateIPIndex        int
	nextElasticIPIndex        int
	ebsEncryptionByDefault    bool
	serialConsoleAccess       bool
	lifecycleOnce             sync.Once
	lifecycleStopOnce         sync.Once
}

func newInMemoryBackendMaps() *InMemoryBackend {
	b := &InMemoryBackend{
		instances:                      make(map[string]*Instance),
		securityGroups:                 make(map[string]*SecurityGroup),
		vpcs:                           make(map[string]*VPC),
		subnets:                        make(map[string]*Subnet),
		keyPairs:                       make(map[string]*KeyPair),
		volumes:                        make(map[string]*Volume),
		addresses:                      make(map[string]*Address),
		internetGateways:               make(map[string]*InternetGateway),
		routeTables:                    make(map[string]*RouteTable),
		natGateways:                    make(map[string]*NatGateway),
		networkInterfaces:              make(map[string]*NetworkInterface),
		spotRequests:                   make(map[string]*SpotInstanceRequest),
		placementGroups:                make(map[string]*PlacementGroup),
		images:                         make(map[string]*AMIStub),
		imageUsageReports:              make(map[string]*ImageUsageReport),
		launchTemplates:                make(map[string]*LaunchTemplate),
		vpcEndpoints:                   make(map[string]*VpcEndpoint),
		tags:                           make(map[string]map[string]string),
		addressTransfers:               make(map[string]*AddressTransfer),
		capacityReservations:           make(map[string]*CapacityReservation),
		reservedInstancesExchanges:     make(map[string]*ReservedInstancesExchange),
		tgwMulticastDomainAssociations: make(map[string]*TransitGatewayMulticastDomainAssociation),
		tgwPeeringAttachments:          make(map[string]*TransitGatewayPeeringAttachment),
		tgwVpcAttachments:              make(map[string]*TransitGatewayVpcAttachment),
		vpcEndpointConnections:         make(map[string]*VpcEndpointConnection),
		vpcPeeringConnections:          make(map[string]*VpcPeeringConnection),
		byoipCidrs:                     make(map[string]*ByoipCidr),
		dedicatedHosts:                 make(map[string]*Host),
		snapshots:                      make(map[string]*Snapshot),
		networkACLs:                    make(map[string]*StoredNetworkACL),
		transitGateways:                make(map[string]*TransitGateway),
		flowLogs:                       make(map[string]*FlowLog),
		dhcpOptionSets:                 make(map[string]*DhcpOptions),
		egressOnlyIGWs:                 make(map[string]*EgressOnlyInternetGateway),
		iamAssociations:                make(map[string]*IamInstanceProfileAssociation),
		tgwRouteTables:                 make(map[string]*TransitGatewayRouteTable),
		tgwRoutes:                      make(map[string]*TransitGatewayRoute),
		tgwRTAssociations:              make(map[string]*TransitGatewayRouteTableAssociation),
		tgwPolicyTables:                make(map[string]*TransitGatewayPolicyTable),
		tgwPolicyTableAssociations:     make(map[string]*TransitGatewayPolicyTableAssociation),
		tgwRouteTableAnnouncements:     make(map[string]*TransitGatewayRouteTableAnnouncement),
		vpcCidrAssociations:            make(map[string]*VpcCidrBlockAssociation),
		vpnGateways:                    make(map[string]*VpnGateway),
		customerGateways:               make(map[string]*CustomerGateway),
		vpnConnections:                 make(map[string]*VpnConnection),
		vpcEndpointServiceConfigs:      make(map[string]*VpcEndpointServiceConfig),
		ipams:                          make(map[string]*Ipam),
		ipamScopes:                     make(map[string]*IpamScope),
		ipamPools:                      make(map[string]*IpamPool),
		ipamPoolCidrs:                  make(map[string][]*IpamPoolCidr),
		ipamPoolAllocations:            make(map[string]*IpamPoolAllocation),
		ipamResourceDiscoveries:        make(map[string]*IpamResourceDiscovery),
		ipamResourceDiscoveryAssocs:    make(map[string]*IpamResourceDiscoveryAssociation),
		instanceIDsByVPC:               make(map[string]map[string]struct{}),
		eniIDsByInstance:               make(map[string]map[string]struct{}),
		eniIDByAttachment:              make(map[string]string),
	}
	initCoreExtraMaps(b)
	initBatch5Maps(b)
	initBatch6Maps(b)
	initRouteServerMaps(b)
	initLocalGatewayMaps(b)
	initTGWMulticastMaps(b)
	initVpcConfigMaps(b)
	initCapacityFamilyMaps(b)
	initVerifiedAccessExtMaps(b)
	initFpgaImageMaps(b)
	b.resetIpamDiscoveryMapsLocked()
	b.resetIpamPolicyMapsLocked()
	b.resetScheduledInstanceMapsLocked()
	b.resetIPPoolMapsLocked()
	b.resetAllowedImagesSettingsLocked()
	b.resetImageTasksLocked()
	b.resetUsageReportMapsLocked()
	b.resetVMImportExportMapsLocked()
	b.resetTrunkEnclaveMapsLocked()
	b.instanceProductCodes = make(map[string][]string)

	return b
}

// initVerifiedAccessExtMaps initialises the VerifiedAccess policy/logging
// state maps (split out to keep newInMemoryBackendMaps under the funlen
// limit).
func initVerifiedAccessExtMaps(b *InMemoryBackend) {
	b.verifiedAccessEndpointPolicies = make(map[string]*VerifiedAccessPolicy)
	b.verifiedAccessGroupPolicies = make(map[string]*VerifiedAccessPolicy)
	b.verifiedAccessInstanceLoggingConfigs = make(map[string]*VerifiedAccessInstanceLoggingConfig)
}

// initFpgaImageMaps initialises the FPGA image state map (split out to keep
// newInMemoryBackendMaps under the funlen limit).
func initFpgaImageMaps(b *InMemoryBackend) {
	b.fpgaImages = make(map[string]*FpgaImage)
}

// initCoreExtraMaps initialises spot fleet, snapshot, IMDS, and batch4 state
// maps (split out to keep newInMemoryBackendMaps under the funlen limit).
func initCoreExtraMaps(b *InMemoryBackend) {
	b.spotFleets = make(map[string]*SpotFleetRequest)
	b.spotFleetHistory = make(map[string][]SpotFleetHistoryRecord)
	b.volumeModifications = make(map[string]*VolumeModification)
	b.snapshotTiers = make(map[string]string)
	b.snapshotAttributes = make(map[string]map[string]string)
	b.sgVpcAssociations = make(map[string]map[string]string)
	b.vpcTenancy = make(map[string]string)
	b.vpcPeeringOptions = make(map[string]*PeeringConnectionOptions)
	b.subnetCIDRAssociations = make(map[string][]*SubnetCIDRAssociation)
	b.addressAttributes = make(map[string]*AddressAttribute)
	b.instanceMonitoring = make(map[string]string)
	b.instanceCreditSpecs = make(map[string]string)
	b.instanceIMDSOptions = make(map[string]*IMDSOptions)
	b.niPermissions = make(map[string]*NetworkInterfacePermission)
	b.niIPv6Addresses = make(map[string][]string)
	b.idFormatSettings = make(map[string]bool)
	b.endpointConnectionNotifs = make(map[string]*VpcEndpointConnectionNotification)
	b.vpcEndpointServicePermissions = make(map[string][]string)
	b.snapshotLocks = make(map[string]*SnapshotLock)
	b.replaceRootVolumeTasks = make(map[string]*ReplaceRootVolumeTask)
	b.subnetCIDRReservations = make(map[string][]*SubnetCIDRReservation)
	b.imageDisabled = make(map[string]bool)
	b.imageDeprecated = make(map[string]string)
	b.imageDeregistrationProtection = make(map[string]bool)
	b.imageAttributes = make(map[string]map[string]string)
	b.vgwRoutePropagation = make(map[string]bool)
	b.managedPrefixLists = make(map[string]*ManagedPrefixList)
	b.clientVpnEndpoints = make(map[string]*ClientVpnEndpoint)
	b.tgwConnects = make(map[string]*TransitGatewayConnect)
	b.tgwConnectPeers = make(map[string]*TransitGatewayConnectPeer)
	b.tgwPrefixListRefs = make(map[string]*TransitGatewayPrefixListReference)
}

// initCapacityFamilyMaps initialises the Capacity Reservation Fleet, Capacity
// Block, and Capacity Manager state maps (split out to keep
// newInMemoryBackendMaps under the funlen limit).
func initCapacityFamilyMaps(b *InMemoryBackend) {
	b.capacityReservationFleets = make(map[string]*CapacityReservationFleet)
	b.capacityBlockOfferings = make(map[string]*CapacityBlockOffering)
	b.capacityBlockExtensionOfferings = make(map[string]*CapacityBlockExtensionOffering)
	b.capacityBlocks = make(map[string]*CapacityBlock)
	b.capacityBlockExtensions = make(map[string]*CapacityBlockExtension)
	b.capacityReservationBillingRequests = make(map[string]*CapacityReservationBillingRequest)
	b.capacityManagerDataExports = make(map[string]*CapacityManagerDataExport)
	b.capacityManagerState = &CapacityManagerState{Status: capacityManagerStatusDisabled}
}

// initVpcConfigMaps initialises the VPC ClassicLink and Block Public Access
// state maps (split out to keep newInMemoryBackendMaps under the funlen limit).
func initVpcConfigMaps(b *InMemoryBackend) {
	b.classicLinkInstances = make(map[string]*ClassicLinkInstance)
	b.vpcBlockPublicAccessExclusions = make(map[string]*VpcBlockPublicAccessExclusion)
	b.vpcBlockPublicAccessOptions = &VpcBlockPublicAccessOptions{
		InternetGatewayBlockMode: vpcBPABlockModeOff,
		State:                    vpcBPAStateDefault,
		ExclusionsAllowed:        vpcBPAExclusionsAllowed,
		ManagedBy:                vpcBPAManagedByAccount,
	}
}

// initTGWMulticastMaps initialises the transit gateway multicast domain and
// metering policy state maps (split out to keep newInMemoryBackendMaps under
// the funlen limit).
func initTGWMulticastMaps(b *InMemoryBackend) {
	b.tgwMulticastDomains = make(map[string]*TransitGatewayMulticastDomain)
	b.tgwMulticastGroupEntries = make(map[string]*TransitGatewayMulticastGroupEntry)
	b.tgwMeteringPolicies = make(map[string]*TransitGatewayMeteringPolicy)
	b.tgwMeteringPolicyEntries = make(map[string]*TransitGatewayMeteringPolicyEntry)
}

// initLocalGatewayMaps initialises the Local Gateway state maps (split out to keep
// newInMemoryBackendMaps under the funlen limit).
func initLocalGatewayMaps(b *InMemoryBackend) {
	b.localGateways = make(map[string]*LocalGateway)
	b.localGatewayVirtualInterfaces = make(map[string]*LocalGatewayVirtualInterface)
	b.localGatewayVirtualInterfaceGroups = make(map[string]*LocalGatewayVirtualInterfaceGroup)
	b.localGatewayRouteTables = make(map[string]*LocalGatewayRouteTable)
	b.localGatewayRoutes = make(map[string]*LocalGatewayRoute)
	b.localGatewayRouteTableVpcAssociations = make(map[string]*LocalGatewayRouteTableVpcAssociation)
	b.localGatewayRouteTableVifGroupAssociations = make(
		map[string]*LocalGatewayRouteTableVirtualInterfaceGroupAssociation,
	)
}

// initBatch6Maps initialises the verified-access, import-task, recycle-bin,
// fast-launch and VPN-route maps (split out to keep newInMemoryBackendMaps
// under the funlen limit).
func initBatch6Maps(b *InMemoryBackend) {
	b.verifiedAccessEndpoints = make(map[string]*VerifiedAccessEndpoint)
	b.verifiedAccessGroups = make(map[string]*VerifiedAccessGroup)
	b.verifiedAccessInstances = make(map[string]*VerifiedAccessInstance)
	b.verifiedAccessTrustProviders = make(map[string]*VerifiedAccessTrustProvider)
	b.instanceConnectEndpoints = make(map[string]*InstanceConnectEndpoint)
	b.instanceEventWindows = make(map[string]*InstanceEventWindow)
	b.imageImportTasks = make(map[string]*ImageImportTask)
	b.snapshotImportTasks = make(map[string]*SnapshotImportTask)
	b.recycleBinImages = make(map[string]*RecycleBinImage)
	b.recycleBinSnapshots = make(map[string]*Snapshot)
	b.recycleBinVolumes = make(map[string]*RecycleBinVolume)
	b.fastLaunchImages = make(map[string]bool)
	b.fastSnapshotRestores = make(map[string]bool)
	b.vpnConnectionRoutes = make(map[string]*VpnConnectionRoute)
}

func initBatch5Maps(b *InMemoryBackend) {
	b.trafficMirrorFilters = make(map[string]*TrafficMirrorFilter)
	b.trafficMirrorFilterRules = make(map[string]*TrafficMirrorFilterRule)
	b.trafficMirrorSessions = make(map[string]*TrafficMirrorSession)
	b.trafficMirrorTargets = make(map[string]*TrafficMirrorTarget)
	b.fleets = make(map[string]*Fleet)
	b.networkInsightsPaths = make(map[string]*NetworkInsightsPath)
	b.networkInsightsAnalyses = make(map[string]*NetworkInsightsAnalysis)
	b.networkInsightsAccessScopes = make(map[string]*NetworkInsightsAccessScope)
	b.networkInsightsAccessScopeAnalyses = make(map[string]*NetworkInsightsAccessScopeAnalysis)
	b.carrierGateways = make(map[string]*CarrierGateway)
	b.reservedInstances = make(map[string]*ReservedInstance)
	b.reservedInstancesOfferings = make(map[string]*ReservedInstancesOffering)
	b.reservedInstancesListings = make(map[string]*ReservedInstancesListing)
	b.reservedInstancesModifications = make(map[string]*ReservedInstancesModification)
}

// initRouteServerMaps initialises the VPC Route Server state maps (split out
// to keep newInMemoryBackendMaps under the funlen limit).
func initRouteServerMaps(b *InMemoryBackend) {
	b.routeServers = make(map[string]*RouteServer)
	b.routeServerEndpoints = make(map[string]*RouteServerEndpoint)
	b.routeServerPeers = make(map[string]*RouteServerPeer)
	b.routeServerAssociations = make(map[string]*RouteServerAssociation)
	b.routeServerPropagations = make(map[string]*RouteServerPropagation)
}

// NewInMemoryBackend creates a new InMemoryBackend with a default VPC and subnet.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := newInMemoryBackendMaps()
	b.AccountID = accountID
	b.Region = region
	b.mu = lockmetrics.New("ec2")
	b.lifecycleStop = make(chan struct{})
	b.initDefaults()

	return b
}

// StartLifecycleReconciler starts the background goroutine that advances
// instances through their transitional states (pending→running, stopping→stopped,
// shutting-down→terminated). It is started by the production provider; tests
// drive lifecycle transitions deterministically via TickLifecycleForTest and so
// deliberately do NOT start the background ticker (which would otherwise race
// with their direct ticks and state assertions). Idempotent — safe to call
// multiple times; only the first call starts the goroutine.
func (b *InMemoryBackend) StartLifecycleReconciler() {
	b.lifecycleOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(lifecycleReconcileInterval)
			defer ticker.Stop()

			for {
				select {
				case <-b.lifecycleStop:
					return
				case <-ticker.C:
					b.reconcileInstanceLifecycle()
				}
			}
		}()
	})
}

// StopLifecycleReconciler signals the background lifecycle goroutine (if any) to
// exit. Idempotent and safe to call even if the reconciler was never started.
func (b *InMemoryBackend) StopLifecycleReconciler() {
	b.lifecycleStopOnce.Do(func() {
		close(b.lifecycleStop)
	})
}

// reconcileInstanceLifecycle advances all instances in transitional states to their
// next stable state. It is also called directly by tests via TickLifecycleForTest.
func (b *InMemoryBackend) reconcileInstanceLifecycle() {
	b.mu.Lock("reconcileInstanceLifecycle")
	defer b.mu.Unlock()

	for _, inst := range b.instances {
		switch inst.State {
		case StatePending:
			inst.State = StateRunning
		case StateStopping:
			inst.State = StateStopped
		case StateShuttingDown:
			inst.State = StateTerminated
		}
	}
}

// initDefaults pre-populates a default VPC, subnet, and security group.
func (b *InMemoryBackend) initDefaults() {
	defaultVPCID := vpcDefaultName
	b.vpcs[defaultVPCID] = &VPC{
		ID:        defaultVPCID,
		CIDRBlock: "172.31.0.0/16",
		IsDefault: true,
	}

	defaultSubnetID := "subnet-default"
	b.subnets[defaultSubnetID] = &Subnet{
		ID:               defaultSubnetID,
		VPCID:            defaultVPCID,
		CIDRBlock:        "172.31.0.0/20",
		AvailabilityZone: b.Region + "a",
		IsDefault:        true,
	}

	defaultSGID := "sg-default"
	b.securityGroups[defaultSGID] = &SecurityGroup{
		ID:          defaultSGID,
		Name:        "default",
		Description: "default VPC security group",
		VPCID:       defaultVPCID,
	}
}

// RunInstances creates one or more EC2 instance stubs.
func (b *InMemoryBackend) RunInstances(
	imageID, instanceType, subnetID string,
	count int,
) ([]*Instance, error) {
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	if count < 1 {
		count = 1
	}

	b.mu.Lock("RunInstances")
	defer b.mu.Unlock()

	if subnetID == "" {
		subnetID = b.findDefaultSubnetID()
	} else if _, ok := b.subnets[subnetID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	vpcID := ""
	mapPublicIP := false

	if sub, ok := b.subnets[subnetID]; ok {
		vpcID = sub.VPCID
		mapPublicIP = sub.MapPublicIPOnLaunch
	}

	// No capacity hint — user-derived values in the make capacity position
	// trigger CodeQL go/slice-memory-allocation-excessive-size even after
	// clamping. count is only used for the loop count below (safe).
	//nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	instances := make([]*Instance, 0)

	for range count {
		id := "i-" + uuid.New().String()[:17]
		inst := &Instance{
			ID:           id,
			ImageID:      imageID,
			InstanceType: instanceType,
			// AWS state machine: pending → running via reconciler goroutine.
			State:      StatePending,
			VPCID:      vpcID,
			SubnetID:   subnetID,
			LaunchTime: time.Now(),
			EnaSupport: true,
		}
		inst.PrivateIP = b.allocPrivateIP()
		if mapPublicIP {
			inst.PublicIPAddress = b.allocElasticIP()
			inst.PublicDNSName = fmt.Sprintf("ec2-%s.compute-1.amazonaws.com",
				strings.ReplaceAll(inst.PublicIPAddress, ".", "-"))
		}
		eniID := "eni-" + uuid.New().String()[:17]
		attachID := "eni-attach-" + uuid.New().String()[:8]
		b.networkInterfaces[eniID] = &NetworkInterface{
			ID:              eniID,
			SubnetID:        subnetID,
			VPCID:           vpcID,
			PrivateIP:       inst.PrivateIP,
			InstanceID:      id,
			AttachmentID:    attachID,
			DeviceIndex:     0,
			Status:          stateInUse,
			SourceDestCheck: true,
		}
		b.instances[id] = inst
		b.indexInstanceLocked(inst)
		b.indexENILocked(eniID, b.networkInterfaces[eniID])
		instances = append(instances, inst)
	}

	return instances, nil
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS
// server. It mirrors the optional registrar interfaces used by RDS, OpenSearch
// and friends so the EC2 docker-compute provider can publish synthetic
// AWS-style instance hostnames (e.g. ec2-1-2-3-4.compute-1.amazonaws.com) that
// resolve to the host the container is reachable on.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// WithCompute installs an optional Compute provider. When non-nil, RunInstances,
// TerminateInstances, StartInstances and StopInstances will call into the
// provider after updating in-memory state. Passing nil disables the hook.
func (b *InMemoryBackend) WithCompute(c Compute) {
	b.mu.Lock("WithCompute")
	defer b.mu.Unlock()
	b.compute = c
}

// Compute returns the currently installed Compute provider (may be nil).
//
//nolint:ireturn // returning the configured interface is the intent
func (b *InMemoryBackend) Compute() Compute {
	b.mu.RLock("Compute")
	defer b.mu.RUnlock()

	return b.compute
}

// SetDNSRegistrar wires an embedded DNS server so synthetic instance
// hostnames produced by the Compute provider can be resolved by callers
// outside the gopherstack process.
func (b *InMemoryBackend) SetDNSRegistrar(r DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = r
}

// DNSRegistrar returns the configured DNS registrar (may be nil).
//
//nolint:ireturn // returning the configured interface is the intent
func (b *InMemoryBackend) DNSRegistrar() DNSRegistrar {
	b.mu.RLock("DNSRegistrar")
	defer b.mu.RUnlock()

	return b.dnsRegistrar
}

// LookupKeyPairAuthorizedKey returns the OpenSSH authorized_keys-format public
// key for the named key pair, or empty string when the key pair is unknown or
// has no derivable public key. Used by Compute providers to seed the launched
// container's authorized_keys.
func (b *InMemoryBackend) LookupKeyPairAuthorizedKey(name string) string {
	b.mu.RLock("LookupKeyPairAuthorizedKey")
	defer b.mu.RUnlock()

	kp, ok := b.keyPairs[name]
	if !ok {
		return ""
	}

	return kp.PublicKey
}

// SetComputeResult merges a LaunchResult onto an existing instance and its
// primary ENI. Empty string fields in the result are ignored. Returns
// ErrInstanceNotFound when the instance has been removed before the compute
// provider returned.
func (b *InMemoryBackend) SetComputeResult(instanceID string, r LaunchResult) error {
	b.mu.Lock("SetComputeResult")
	defer b.mu.Unlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if r.ProviderID != "" {
		inst.ProviderID = r.ProviderID
	}

	if r.PrivateIP != "" {
		oldPrivate := inst.PrivateIP
		inst.PrivateIP = r.PrivateIP
		// keep primary ENI in sync so DescribeNetworkInterfaces shows the
		// container's actual address
		for eniID := range b.eniIDsByInstance[instanceID] {
			if eni, exists := b.networkInterfaces[eniID]; exists && eni.PrivateIP == oldPrivate {
				eni.PrivateIP = r.PrivateIP
			}
		}
	}

	if r.PublicIPAddress != "" {
		inst.PublicIPAddress = r.PublicIPAddress
	}

	if r.PublicDNSName != "" {
		inst.PublicDNSName = r.PublicDNSName
	}

	if r.SSHPort != 0 {
		inst.SSHPort = r.SSHPort
	}

	return nil
}

// LookupInstanceProviderID returns the ProviderID stored on an instance, or
// empty string when the instance is unknown.
func (b *InMemoryBackend) LookupInstanceProviderID(instanceID string) string {
	b.mu.RLock("LookupInstanceProviderID")
	defer b.mu.RUnlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return ""
	}

	return inst.ProviderID
}

// LookupInstancePublicDNSName returns the synthetic public DNS name assigned
// to an instance, or empty string when the instance is unknown or has none.
func (b *InMemoryBackend) LookupInstancePublicDNSName(instanceID string) string {
	b.mu.RLock("LookupInstancePublicDNSName")
	defer b.mu.RUnlock()

	inst, ok := b.instances[instanceID]
	if !ok {
		return ""
	}

	return inst.PublicDNSName
}

// findDefaultSubnetID returns the ID of the default subnet, or empty string if none.
// Must be called with b.mu held.
func (b *InMemoryBackend) findDefaultSubnetID() string {
	for id, s := range b.subnets {
		if s.IsDefault {
			return id
		}
	}

	return ""
}

// DescribeInstances returns instances, optionally filtered by IDs or state.
// When ids are provided, lookups are O(len(ids)) via the instance map rather
// than scanning every instance in the backend.
func (b *InMemoryBackend) DescribeInstances(ids []string, state string) []*Instance {
	b.mu.RLock("DescribeInstances")

	if len(ids) > 0 {
		out := make([]*Instance, 0, len(ids))

		for _, id := range ids {
			inst, ok := b.instances[id]
			if !ok {
				continue
			}

			if state != "" && inst.State.Name != state {
				continue
			}

			cp := *inst
			out = append(out, &cp)
		}

		b.mu.RUnlock()

		return out
	}

	// Collect matching pointers under the read lock (no allocations), then copy
	// outside the lock to narrow the critical section for concurrent writers.
	ptrs := make([]*Instance, 0, len(b.instances))
	for _, inst := range b.instances {
		if state == "" || inst.State.Name == state {
			ptrs = append(ptrs, inst)
		}
	}
	b.mu.RUnlock()

	out := make([]*Instance, len(ptrs))
	for i, inst := range ptrs {
		cp := *inst
		out[i] = &cp
	}

	return out
}

// TerminateInstances transitions instances to shutting-down then terminated.
// Returns the previous and current state for each instance.
// Terminated instances remain visible (matching AWS ~1 hour grace period)
// until the janitor sweeps them.
func (b *InMemoryBackend) TerminateInstances(ids []string) ([]*InstanceStateChange, error) {
	b.mu.Lock("TerminateInstances")
	defer b.mu.Unlock()

	var result []*InstanceStateChange

	for _, id := range ids {
		inst, ok := b.instances[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		prev := inst.State
		// AWS state machine: any state → shutting-down → terminated (reconciler advances).
		// Resource cleanup (ENIs, EIPs, volumes) happens immediately so callers
		// do not observe dangling attachments, but state advances asynchronously.
		inst.State = StateShuttingDown
		inst.TerminatedAt = time.Now()
		result = append(result, &InstanceStateChange{
			InstanceID:    id,
			PreviousState: prev,
			CurrentState:  inst.State,
		})

		// Mirror AWS behaviour: when the backing instance of a spot request is
		// terminated, the request transitions to "closed" (not stateCancelled).
		for _, req := range b.spotRequests {
			if req.InstanceID == id && req.State == stateActive {
				req.State = "closed"
				req.CancelledAt = time.Now()
			}
		}

		// Delete all ENIs attached to the terminated instance and recycle their
		// private IPs. This mirrors the AWS behaviour where all network interfaces
		// are deleted when an instance is terminated (deleteOnTermination=true is
		// the default for all network interfaces attached at launch).
		eniIDs := b.eniIDsByInstance[id]
		for eniID := range eniIDs {
			eni, exists := b.networkInterfaces[eniID]
			if !exists {
				continue
			}

			b.deindexENILocked(eniID, eni)
			b.recycleENIIPsLocked(eni)
			delete(b.networkInterfaces, eniID)
			delete(b.tags, eniID)
		}
		b.deindexInstanceLocked(inst)

		// Detach any EBS volumes whose attachment refers to this instance so they
		// return to "available" state. AWS deletes the root volume by default
		// (deleteOnTermination=true) but detaches additional volumes; since we do
		// not track the deleteOnTermination flag per attachment, detaching all of
		// them is the safe, non-destructive equivalent.
		// Also disassociate any Elastic IPs associated with the terminated instance.
		b.detachVolumesAndEIPsLocked(id)
	}

	return result, nil
}

// DescribeSecurityGroups returns security groups, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the security-group map
// rather than scanning every group in the backend.
func (b *InMemoryBackend) DescribeSecurityGroups(ids []string) []*SecurityGroup {
	b.mu.RLock("DescribeSecurityGroups")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*SecurityGroup, 0, len(ids))

		for _, id := range ids {
			sg, ok := b.securityGroups[id]
			if !ok {
				continue
			}

			cp := *sg
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*SecurityGroup, 0, len(b.securityGroups))

	for _, sg := range b.securityGroups {
		cp := *sg
		out = append(out, &cp)
	}

	return out
}

// CreateSecurityGroup creates a new security group and returns its ID.
func (b *InMemoryBackend) CreateSecurityGroup(
	name, description, vpcID string,
) (*SecurityGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: GroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSecurityGroup")
	defer b.mu.Unlock()

	if vpcID != "" {
		if _, ok := b.vpcs[vpcID]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
		}
	}

	for _, sg := range b.securityGroups {
		if sg.Name == name && sg.VPCID == vpcID {
			return nil, fmt.Errorf(
				"%w: group named %s already exists in VPC %s",
				ErrDuplicateSGName,
				name,
				vpcID,
			)
		}
	}

	id := "sg-" + uuid.New().String()[:17]
	sg := &SecurityGroup{
		ID:          id,
		Name:        name,
		Description: description,
		VPCID:       vpcID,
		// Real AWS creates new security groups with a default allow-all egress rule.
		EgressRules: []SecurityGroupRule{
			{Protocol: "-1", IPRange: cidrAllIPv4},
		},
	}
	b.securityGroups[id] = sg

	return sg, nil
}

// DeleteSecurityGroup removes a security group by ID.
func (b *InMemoryBackend) DeleteSecurityGroup(id string) error {
	b.mu.Lock("DeleteSecurityGroup")
	defer b.mu.Unlock()

	if _, ok := b.securityGroups[id]; !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, id)
	}

	delete(b.securityGroups, id)
	delete(b.tags, id)

	return nil
}

// DescribeVpcs returns VPCs, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the VPC map rather than
// scanning every VPC in the backend.
func (b *InMemoryBackend) DescribeVpcs(ids []string) []*VPC {
	b.mu.RLock("DescribeVpcs")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*VPC, 0, len(ids))

		for _, id := range ids {
			v, ok := b.vpcs[id]
			if !ok {
				continue
			}

			cp := *v
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*VPC, 0, len(b.vpcs))

	for _, v := range b.vpcs {
		cp := *v
		out = append(out, &cp)
	}

	return out
}

// CreateVpc creates a new VPC with the given CIDR block.
func (b *InMemoryBackend) CreateVpc(cidr string) (*VPC, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: CidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVpc")
	defer b.mu.Unlock()

	for _, existing := range b.vpcs {
		if cidrsOverlap(cidr, existing.CIDRBlock) {
			return nil, fmt.Errorf("%w: CIDR %s overlaps with existing VPC %s (%s)",
				ErrCIDRConflict, cidr, existing.ID, existing.CIDRBlock)
		}
	}

	id := "vpc-" + uuid.New().String()[:17]
	v := &VPC{
		ID:        id,
		CIDRBlock: cidr,
	}
	b.vpcs[id] = v

	return v, nil
}

// cascadeDeleteVpcIGWsLocked removes all internet gateways that have an
// attachment to the given VPC. Must be called with b.mu held.
func (b *InMemoryBackend) cascadeDeleteVpcIGWsLocked(vpcID string) {
	for igwID, igw := range b.internetGateways {
		for _, att := range igw.Attachments {
			if att.VPCID == vpcID {
				delete(b.internetGateways, igwID)
				delete(b.tags, igwID)

				break
			}
		}
	}
}

// DeleteVpc removes a VPC by ID, cascade-deleting all dependent resources
// (instances, internet gateways, NAT gateways, route tables, security groups,
// network interfaces, and subnets) along with their tags.
func (b *InMemoryBackend) DeleteVpc(id string) error {
	b.mu.Lock("DeleteVpc")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, id)
	}

	// Cascade: terminate instances belonging to this VPC.
	for instID, inst := range b.instances {
		if inst.VPCID == id {
			inst.State = StateTerminated
			inst.TerminatedAt = time.Now()
			delete(b.tags, instID)
			b.detachVolumesAndEIPsLocked(instID)
		}
	}

	// Cascade: detach and delete internet gateways attached to this VPC.
	b.cascadeDeleteVpcIGWsLocked(id)

	// Cascade: delete NAT gateways in subnets belonging to this VPC.
	for ngwID, ngw := range b.natGateways {
		if sub, ok := b.subnets[ngw.SubnetID]; ok && sub.VPCID == id {
			b.recycleIPLocked(ngw.PrivateIP)
			delete(b.natGateways, ngwID)
			delete(b.tags, ngwID)
		}
	}

	// Cascade: remove route tables belonging to this VPC.
	for rtID, rt := range b.routeTables {
		if rt.VPCID == id {
			delete(b.routeTables, rtID)
			delete(b.tags, rtID)
		}
	}

	// Cascade: remove security groups belonging to this VPC.
	for sgID, sg := range b.securityGroups {
		if sg.VPCID == id {
			delete(b.securityGroups, sgID)
			delete(b.tags, sgID)
		}
	}

	// Cascade: remove network interfaces belonging to this VPC.
	for eniID, eni := range b.networkInterfaces {
		if eni.VPCID == id {
			b.recycleENIIPsLocked(eni)
			delete(b.networkInterfaces, eniID)
			delete(b.tags, eniID)
		}
	}

	// Cascade: remove subnets belonging to this VPC.
	for subnetID, subnet := range b.subnets {
		if subnet.VPCID == id {
			delete(b.subnets, subnetID)
			delete(b.tags, subnetID)
		}
	}

	delete(b.vpcs, id)
	delete(b.tags, id)

	return nil
}

// DescribeSubnets returns subnets, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the subnet map rather
// than scanning every subnet in the backend.
func (b *InMemoryBackend) DescribeSubnets(ids []string) []*Subnet {
	b.mu.RLock("DescribeSubnets")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*Subnet, 0, len(ids))

		for _, id := range ids {
			s, ok := b.subnets[id]
			if !ok {
				continue
			}

			cp := *s
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*Subnet, 0, len(b.subnets))

	for _, s := range b.subnets {
		cp := *s
		out = append(out, &cp)
	}

	return out
}

// CreateSubnet creates a new subnet in the given VPC.
func (b *InMemoryBackend) CreateSubnet(vpcID, cidr, az string) (*Subnet, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: CidrBlock is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSubnet")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	if az == "" {
		az = b.Region + "a"
	}

	vpc := b.vpcs[vpcID]
	if !cidrContains(vpc.CIDRBlock, cidr) {
		return nil, fmt.Errorf("%w: subnet CIDR %s is not within VPC CIDR %s",
			ErrInvalidParameter, cidr, vpc.CIDRBlock)
	}

	for _, existing := range b.subnets {
		if existing.VPCID == vpcID && cidrsOverlap(cidr, existing.CIDRBlock) {
			return nil, fmt.Errorf("%w: CIDR %s overlaps with existing subnet %s (%s)",
				ErrCIDRConflict, cidr, existing.ID, existing.CIDRBlock)
		}
	}

	id := "subnet-" + uuid.New().String()[:17]
	s := &Subnet{
		ID:               id,
		VPCID:            vpcID,
		CIDRBlock:        cidr,
		AvailabilityZone: az,
	}
	b.subnets[id] = s

	return s, nil
}

// DeleteSubnet removes a subnet by ID, cascade-deleting any instances, NAT
// gateways, and network interfaces in that subnet along with their tags.
func (b *InMemoryBackend) DeleteSubnet(id string) error {
	b.mu.Lock("DeleteSubnet")
	defer b.mu.Unlock()

	if _, ok := b.subnets[id]; !ok {
		return fmt.Errorf("%w: %s", ErrSubnetNotFound, id)
	}

	// Cascade: terminate instances in this subnet.
	for instID, inst := range b.instances {
		if inst.SubnetID == id {
			inst.State = StateTerminated
			inst.TerminatedAt = time.Now()
			delete(b.tags, instID)
			b.detachVolumesAndEIPsLocked(instID)
		}
	}

	// Cascade: delete NAT gateways in this subnet.
	for ngwID, ngw := range b.natGateways {
		if ngw.SubnetID == id {
			b.recycleIPLocked(ngw.PrivateIP)
			delete(b.natGateways, ngwID)
			delete(b.tags, ngwID)
		}
	}

	// Cascade: remove network interfaces in this subnet.
	for eniID, eni := range b.networkInterfaces {
		if eni.SubnetID == id {
			b.recycleENIIPsLocked(eni)
			delete(b.networkInterfaces, eniID)
			delete(b.tags, eniID)
		}
	}

	delete(b.subnets, id)
	delete(b.tags, id)

	return nil
}

// TagEntry holds a single resource-tag association returned by DescribeTags.
type TagEntry struct {
	ResourceID   string `json:"resourceID,omitempty"`
	ResourceType string `json:"resourceType,omitempty"`
	Key          string `json:"key,omitempty"`
	Value        string `json:"value,omitempty"`
}

// resourceTypeByID infers the EC2 resource type from the ID prefix.
func resourceTypeByID(id string) string {
	prefixes := []struct {
		prefix string
		rtype  string
	}{
		{"i-", "instance"},
		{"sg-", "security-group"},
		{"vpc-", resourceTypeVPC},
		{"subnet-", "subnet"},
		{"vol-", "volume"},
		{"igw-", "internet-gateway"},
		{"rtb-", "route-table"},
		{"nat-", "natgateway"},
		{"eipalloc-", "elastic-ip"},
	}

	for _, e := range prefixes {
		if strings.HasPrefix(id, e.prefix) {
			return e.rtype
		}
	}

	return "resource"
}

// recycleIPLocked adds ip to the free list if it is an auto-allocated
// 172.31.x.y address and the free list has capacity remaining.
// Must be called with b.mu held.
func (b *InMemoryBackend) recycleIPLocked(ip string) {
	if strings.HasPrefix(ip, "172.31.") && len(b.freePrivateIPs) < maxFreePrivateIPs {
		b.freePrivateIPs = append(b.freePrivateIPs, ip)
	}
}

// recycleENIIPsLocked returns the primary and secondary auto-allocated private
// IPs from an ENI back to the free list so they can be reused.
// Must be called with b.mu held.
func (b *InMemoryBackend) recycleENIIPsLocked(eni *NetworkInterface) {
	b.recycleIPLocked(eni.PrivateIP)

	for _, ip := range eni.SecondaryPrivateIPs {
		b.recycleIPLocked(ip)
	}
}

// detachVolumesAndEIPsLocked clears volume attachments and Elastic IP
// associations that reference instanceID so that no dangling references remain
// after the instance transitions to the terminated state.
// Must be called with b.mu held.
func (b *InMemoryBackend) detachVolumesAndEIPsLocked(instanceID string) {
	for _, vol := range b.volumes {
		if vol.Attachment != nil && vol.Attachment.InstanceID == instanceID {
			vol.Attachment = nil
			vol.State = stateAvailable
		}
	}

	for _, addr := range b.addresses {
		if addr.InstanceID == instanceID {
			addr.AssociationID = ""
			addr.InstanceID = ""
		}
	}
}

// resourceExistsLocked reports whether id refers to any known EC2 resource.
// Must be called with b.mu held.
func (b *InMemoryBackend) resourceExistsLocked(id string) bool {
	if _, ok := b.instances[id]; ok {
		return true
	}

	if _, ok := b.securityGroups[id]; ok {
		return true
	}

	if _, ok := b.vpcs[id]; ok {
		return true
	}

	if _, ok := b.subnets[id]; ok {
		return true
	}

	if _, ok := b.keyPairs[id]; ok {
		return true
	}

	if _, ok := b.volumes[id]; ok {
		return true
	}

	if _, ok := b.addresses[id]; ok {
		return true
	}

	if _, ok := b.internetGateways[id]; ok {
		return true
	}

	if _, ok := b.routeTables[id]; ok {
		return true
	}

	if _, ok := b.natGateways[id]; ok {
		return true
	}

	if _, ok := b.networkInterfaces[id]; ok {
		return true
	}

	if _, ok := b.spotRequests[id]; ok {
		return true
	}

	if _, ok := b.placementGroups[id]; ok {
		return true
	}

	return false
}

// CreateTags adds or updates tags on one or more resources.
// Returns ErrInvalidParameter if any resource ID does not exist.
// All IDs are validated before any tags are written, making the operation atomic
// with respect to failures: either all resources are tagged or none are.
func (b *InMemoryBackend) CreateTags(resourceIDs []string, tags map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	// Pre-validate: all resource IDs must exist before any tags are written.
	for _, id := range resourceIDs {
		if !b.resourceExistsLocked(id) {
			return fmt.Errorf("%w: resource %s does not exist", ErrInvalidParameter, id)
		}
	}

	for _, id := range resourceIDs {
		if b.tags[id] == nil {
			b.tags[id] = make(map[string]string)
		}

		maps.Copy(b.tags[id], tags)
	}

	return nil
}

// DeleteTags removes the specified tag keys from one or more resources.
// If keys is empty, the operation is a no-op (EC2 requires at least one tag key).
// Empty per-resource tag maps are removed after deletions.
func (b *InMemoryBackend) DeleteTags(resourceIDs []string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, id := range resourceIDs {
		if b.tags[id] == nil {
			continue
		}

		for _, k := range keys {
			delete(b.tags[id], k)
		}

		if len(b.tags[id]) == 0 {
			delete(b.tags, id)
		}
	}

	return nil
}

// TagsForResource returns a copy of the tags currently set on the given
// resource, or an empty map when nothing is tagged. Safe for concurrent use.
func (b *InMemoryBackend) TagsForResource(resourceID string) map[string]string {
	b.mu.RLock("TagsForResource")
	defer b.mu.RUnlock()

	src, ok := b.tags[resourceID]
	if !ok || len(src) == 0 {
		return map[string]string{}
	}

	out := make(map[string]string, len(src))
	maps.Copy(out, src)

	return out
}

// DescribeTags returns all tag entries, optionally filtered by resource IDs.
func (b *InMemoryBackend) DescribeTags(resourceIDs []string) []TagEntry {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(resourceIDs))
	for _, id := range resourceIDs {
		filterSet[id] = true
	}

	var entries []TagEntry

	for resourceID, tagMap := range b.tags {
		if len(filterSet) > 0 && !filterSet[resourceID] {
			continue
		}

		resType := resourceTypeByID(resourceID)

		for k, v := range tagMap {
			entries = append(entries, TagEntry{
				ResourceID:   resourceID,
				ResourceType: resType,
				Key:          k,
				Value:        v,
			})
		}
	}

	return entries
}

// cidrsOverlap reports whether two CIDR blocks overlap.
// Malformed CIDRs are treated as non-overlapping to avoid panics on bad input.
func cidrsOverlap(a, b string) bool {
	_, netA, err1 := net.ParseCIDR(a)
	_, netB, err2 := net.ParseCIDR(b)

	if err1 != nil || err2 != nil {
		return false
	}

	return netA.Contains(netB.IP) || netB.Contains(netA.IP)
}

// cidrContains reports whether outer fully contains inner.
func cidrContains(outer, inner string) bool {
	_, outerNet, err1 := net.ParseCIDR(outer)
	_, innerNet, err2 := net.ParseCIDR(inner)

	if err1 != nil || err2 != nil {
		return false
	}

	// outer must contain inner's base address and inner's broadcast address
	ones1, _ := outerNet.Mask.Size()
	ones2, _ := innerNet.Mask.Size()

	return outerNet.Contains(innerNet.IP) && ones1 <= ones2
}
