package ec2

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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

	// ErrInvalidUserData is returned when RunInstances / ModifyInstanceAttribute
	// user data is not valid base64 or exceeds the 16 KiB decoded-size limit.
	ErrInvalidUserData = errors.New("InvalidUserData.Malformed")
	// ErrMissingParameter is returned when a required parameter (e.g. the
	// ModifyInstanceAttribute attribute selector) is absent.
	ErrMissingParameter = errors.New("MissingParameter")
	// ErrInvalidPaginationToken is returned when a NextToken is forged, tampered
	// with, or otherwise fails HMAC verification.
	ErrInvalidPaginationToken = errors.New("InvalidPaginationToken")
	// ErrOperationNotPermitted is returned when an operation is blocked by an
	// instance-level API protection attribute (disableApiTermination /
	// disableApiStop), mirroring AWS's OperationNotPermitted error code.
	ErrOperationNotPermitted = errors.New("OperationNotPermitted")
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
	TerminatedAt            time.Time                  `json:"terminatedAt"`
	LaunchTime              time.Time                  `json:"launchTime"`
	EventStartTimeOverrides map[string]time.Time       `json:"eventStartTimeOverrides,omitempty"`
	CapacityReservationSpec CapacityReservationSpec    `json:"capacityReservationSpecification"`
	MaintenanceOptions      InstanceMaintenanceOptions `json:"maintenanceOptions"`
	PublicDNSName           string                     `json:"publicDNSName,omitempty"`
	MetadataOptionsTokens   string                     `json:"metadataOptionsTokens,omitempty"`
	MetadataOptionsState    string                     `json:"metadataOptionsState,omitempty"`
	VPCID                   string                     `json:"vpcID,omitempty"`
	ID                      string                     `json:"id,omitempty"`
	PrivateIP               string                     `json:"privateIP,omitempty"`
	PublicIPAddress         string                     `json:"publicIPAddress,omitempty"`
	SubnetID                string                     `json:"subnetID,omitempty"`
	UserData                string                     `json:"userData,omitempty"`
	SriovNetSupport         string                     `json:"sriovNetSupport,omitempty"`
	ProviderID              string                     `json:"providerID,omitempty"`
	// InstanceInitiatedShutdownBehavior is "stop" (default) or "terminate".
	InstanceInitiatedShutdownBehavior string `json:"instanceInitiatedShutdownBehavior,omitempty"`
	// NetworkPerformanceOptions carries the bandwidth-weighting mode.
	NetworkPerformanceOptions InstanceNetworkPerformanceOptions `json:"networkPerformanceOptions"`
	KeyName                   string                            `json:"keyName,omitempty"`
	InstanceType              string                            `json:"instanceType,omitempty"`
	// StateTransitionReason mirrors AWS's legacy <reason> element (e.g.
	// "User initiated (2016-05-...)").
	StateTransitionReason string `json:"stateTransitionReason,omitempty"`
	ImageID               string `json:"imageID,omitempty"`
	// StateReasonCode/StateReasonMessage mirror AWS's <stateReason> element,
	// populated on user-initiated stop/terminate and cleared on start.
	StateReasonMessage    string                `json:"stateReasonMessage,omitempty"`
	StateReasonCode       string                `json:"stateReasonCode,omitempty"`
	Placement             InstancePlacement     `json:"placement"`
	SecurityGroups        []string              `json:"securityGroups,omitempty"`
	State                 InstanceState         `json:"state"`
	PrivateDNSNameOptions PrivateDNSNameOptions `json:"privateDnsNameOptions"`
	CPUOptions            CPUOptions            `json:"cpuOptions"`
	SSHPort               int                   `json:"sshPort,omitempty"`
	EnaSupport            bool                  `json:"enaSupport,omitempty"`
	// DisableAPITermination / DisableAPIStop gate TerminateInstances /
	// StopInstances respectively (ModifyInstanceAttribute-settable).
	DisableAPITermination bool `json:"disableApiTermination,omitempty"`
	DisableAPIStop        bool `json:"disableApiStop,omitempty"`
	EBSOptimized          bool `json:"ebsOptimized,omitempty"`
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
//
// Description is metadata only: it does not participate in a rule's identity
// for authorize/revoke/duplicate-detection purposes (see ruleKey), matching
// AWS's UpdateSecurityGroupRuleDescriptions* semantics of changing a rule's
// description without it becoming a "different" rule.
type SecurityGroupRule struct {
	Protocol           string `json:"protocol,omitempty"`
	IPRange            string `json:"ipRange,omitempty"`
	SourceGroupID      string `json:"sourceGroupId,omitempty"`
	SourceGroupOwnerID string `json:"sourceGroupOwnerId,omitempty"`
	Description        string `json:"description,omitempty"`
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
	Attributes              map[string]bool `json:"attributes,omitempty"`
	ID                      string          `json:"id,omitempty"`
	CIDRBlock               string          `json:"cidrBlock,omitempty"`
	IsDefault               bool            `json:"isDefault,omitempty"`
	ClassicLinkEnabled      bool            `json:"classicLinkEnabled,omitempty"`
	ClassicLinkDNSSupported bool            `json:"classicLinkDnsSupported,omitempty"`
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
	capacityReservations           *store.Table[CapacityReservation]
	vpcs                           *store.Table[VPC]
	subnets                        *store.Table[Subnet]
	keyPairs                       *store.Table[KeyPair]
	reservedInstancesExchanges     *store.Table[ReservedInstancesExchange]
	addresses                      *store.Table[Address]
	internetGateways               *store.Table[InternetGateway]
	natGateways                    *store.Table[NatGateway]
	routeTables                    *store.Table[RouteTable]
	placementGroups                *store.Table[PlacementGroup]
	spotRequests                   *store.Table[SpotInstanceRequest]
	instances                      *store.Table[Instance]
	images                         *store.Table[AMIStub]
	imageUsageReports              *store.Table[ImageUsageReport]
	launchTemplates                *store.Table[LaunchTemplate]
	vpcEndpoints                   *store.Table[VpcEndpoint]
	tags                           map[string]map[string]string
	securityGroups                 *store.Table[SecurityGroup]
	networkInterfaces              *store.Table[NetworkInterface]
	volumes                        *store.Table[Volume]
	tgwMulticastDomainAssociations *store.Table[TransitGatewayMulticastDomainAssociation]
	tgwPeeringAttachments          *store.Table[TransitGatewayPeeringAttachment]
	tgwVpcAttachments              *store.Table[TransitGatewayVpcAttachment]
	vpcEndpointConnections         *store.Table[VpcEndpointConnection]
	vpcPeeringConnections          *store.Table[VpcPeeringConnection]
	byoipCidrs                     *store.Table[ByoipCidr]
	dedicatedHosts                 *store.Table[Host]
	snapshots                      *store.Table[Snapshot]
	networkACLs                    *store.Table[StoredNetworkACL]
	transitGateways                *store.Table[TransitGateway]
	flowLogs                       *store.Table[FlowLog]
	dhcpOptionSets                 *store.Table[DhcpOptions]
	egressOnlyIGWs                 *store.Table[EgressOnlyInternetGateway]
	iamAssociations                *store.Table[IamInstanceProfileAssociation]
	tgwRouteTables                 *store.Table[TransitGatewayRouteTable]
	tgwRoutes                      *store.Table[TransitGatewayRoute]
	tgwRTAssociations              *store.Table[TransitGatewayRouteTableAssociation]
	tgwPolicyTables                *store.Table[TransitGatewayPolicyTable]
	tgwPolicyTableAssociations     *store.Table[TransitGatewayPolicyTableAssociation]
	tgwRouteTableAnnouncements     *store.Table[TransitGatewayRouteTableAnnouncement]
	vpcCidrAssociations            map[string]*VpcCidrBlockAssociation
	vpnGateways                    *store.Table[VpnGateway]
	customerGateways               *store.Table[CustomerGateway]
	vpnConnections                 *store.Table[VpnConnection]
	vpcEndpointServiceConfigs      *store.Table[VpcEndpointServiceConfig]
	ipams                          *store.Table[Ipam]
	ipamScopes                     *store.Table[IpamScope]
	ipamPools                      *store.Table[IpamPool]
	ipamPoolCidrs                  map[string][]*IpamPoolCidr
	ipamPoolAllocations            *store.Table[IpamPoolAllocation]
	ipamResourceDiscoveries        *store.Table[IpamResourceDiscovery]
	ipamResourceDiscoveryAssocs    *store.Table[IpamResourceDiscoveryAssociation]
	ipamByoasns                    *store.Table[IpamByoasn]
	ipamAsnAssociations            *store.Table[IpamAsnAssociation]
	ipamVerificationTokens         *store.Table[IpamExternalResourceVerificationToken]
	ipamResourceCidrs              *store.Table[IpamResourceCidr]
	ipamPrefixListResolvers        *store.Table[IpamPrefixListResolver]
	ipamPrefixListResolverVersions map[string][]int64
	ipamPrefixListResolverTargets  *store.Table[IpamPrefixListResolverTarget]
	ipamPolicies                   *store.Table[IpamPolicy]
	ipamPolicyEnabledTargets       map[string]string
	ipamOrgAdminAccountID          string
	spotFleets                     *store.Table[SpotFleetRequest]
	spotFleetHistory               map[string][]SpotFleetHistoryRecord
	// batch1 additions
	volumeModifications      *store.Table[VolumeModification]
	snapshotTiers            map[string]string
	snapshotAttributes       map[string]map[string]string
	sgVpcAssociations        map[string]map[string]string
	vpcTenancy               map[string]string
	vpcPeeringOptions        map[string]*PeeringConnectionOptions
	subnetCIDRAssociations   map[string][]*SubnetCIDRAssociation
	addressAttributes        *store.Table[AddressAttribute]
	instanceMonitoring       map[string]string
	instanceCreditSpecs      map[string]string
	instanceIMDSOptions      map[string]*IMDSOptions
	instanceMetadataDefaults *InstanceMetadataDefaults
	instanceEventNotifAttrs  *InstanceEventNotificationAttributes
	niPermissions            *store.Table[NetworkInterfacePermission]
	niIPv6Addresses          map[string][]string
	idFormatSettings         map[string]bool
	// batch2 additions
	endpointConnectionNotifs      *store.Table[VpcEndpointConnectionNotification]
	vpcEndpointServicePermissions map[string][]string
	snapshotLocks                 *store.Table[SnapshotLock]
	replaceRootVolumeTasks        *store.Table[ReplaceRootVolumeTask]
	subnetCIDRReservations        map[string][]*SubnetCIDRReservation
	imageDisabled                 map[string]bool
	imageDeprecated               map[string]string
	imageDeregistrationProtection map[string]bool
	imageAttributes               map[string]map[string]string
	vgwRoutePropagation           map[string]bool
	// batch4 additions
	managedPrefixLists           *store.Table[ManagedPrefixList]
	clientVpnEndpoints           *store.Table[ClientVpnEndpoint]
	tgwConnects                  *store.Table[TransitGatewayConnect]
	tgwConnectPeers              *store.Table[TransitGatewayConnectPeer]
	tgwPrefixListRefs            *store.Table[TransitGatewayPrefixListReference]
	verifiedAccessEndpoints      *store.Table[VerifiedAccessEndpoint]
	verifiedAccessGroups         *store.Table[VerifiedAccessGroup]
	verifiedAccessInstances      *store.Table[VerifiedAccessInstance]
	verifiedAccessTrustProviders *store.Table[VerifiedAccessTrustProvider]
	// batch3 additions
	instanceConnectEndpoints *store.Table[InstanceConnectEndpoint]
	instanceEventWindows     *store.Table[InstanceEventWindow]
	imageImportTasks         *store.Table[ImageImportTask]
	snapshotImportTasks      *store.Table[SnapshotImportTask]
	recycleBinImages         *store.Table[RecycleBinImage]
	recycleBinSnapshots      *store.Table[Snapshot]
	recycleBinVolumes        *store.Table[RecycleBinVolume]
	fastLaunchImages         map[string]bool
	fastSnapshotRestores     map[string]bool
	vpnConnectionRoutes      *store.Table[VpnConnectionRoute]
	spotDatafeed             *SpotDatafeed
	// batch5 additions
	trafficMirrorFilters               *store.Table[TrafficMirrorFilter]
	trafficMirrorFilterRules           *store.Table[TrafficMirrorFilterRule]
	trafficMirrorSessions              *store.Table[TrafficMirrorSession]
	trafficMirrorTargets               *store.Table[TrafficMirrorTarget]
	fleets                             *store.Table[Fleet]
	networkInsightsPaths               *store.Table[NetworkInsightsPath]
	networkInsightsAnalyses            *store.Table[NetworkInsightsAnalysis]
	networkInsightsAccessScopes        *store.Table[NetworkInsightsAccessScope]
	networkInsightsAccessScopeAnalyses *store.Table[NetworkInsightsAccessScopeAnalysis]
	carrierGateways                    *store.Table[CarrierGateway]
	reservedInstances                  *store.Table[ReservedInstance]
	reservedInstancesOfferings         *store.Table[ReservedInstancesOffering]
	reservedInstancesListings          *store.Table[ReservedInstancesListing]
	reservedInstancesModifications     *store.Table[ReservedInstancesModification]
	// route server additions
	routeServers            *store.Table[RouteServer]
	routeServerEndpoints    *store.Table[RouteServerEndpoint]
	routeServerPeers        *store.Table[RouteServerPeer]
	routeServerAssociations *store.Table[RouteServerAssociation]
	routeServerPropagations *store.Table[RouteServerPropagation]
	// local gateway additions
	localGateways                              *store.Table[LocalGateway]
	localGatewayVirtualInterfaces              *store.Table[LocalGatewayVirtualInterface]
	localGatewayVirtualInterfaceGroups         *store.Table[LocalGatewayVirtualInterfaceGroup]
	localGatewayRouteTables                    *store.Table[LocalGatewayRouteTable]
	localGatewayRoutes                         *store.Table[LocalGatewayRoute]
	localGatewayRouteTableVpcAssociations      *store.Table[LocalGatewayRouteTableVpcAssociation]
	localGatewayRouteTableVifGroupAssociations *store.Table[LocalGatewayRouteTableVirtualInterfaceGroupAssociation]
	// transit gateway multicast domain / metering policy additions
	tgwMulticastDomains      *store.Table[TransitGatewayMulticastDomain]
	tgwMulticastGroupEntries *store.Table[TransitGatewayMulticastGroupEntry]
	tgwMeteringPolicies      *store.Table[TransitGatewayMeteringPolicy]
	tgwMeteringPolicyEntries *store.Table[TransitGatewayMeteringPolicyEntry]
	// VPC ClassicLink / Block Public Access additions
	classicLinkInstances           *store.Table[ClassicLinkInstance]
	vpcBlockPublicAccessOptions    *VpcBlockPublicAccessOptions
	vpcBlockPublicAccessExclusions *store.Table[VpcBlockPublicAccessExclusion]
	// Capacity Reservation Fleet / Capacity Block / Capacity Manager additions
	capacityReservationFleets          *store.Table[CapacityReservationFleet]
	capacityBlockOfferings             *store.Table[CapacityBlockOffering]
	capacityBlockExtensionOfferings    *store.Table[CapacityBlockExtensionOffering]
	capacityBlocks                     *store.Table[CapacityBlock]
	capacityBlockExtensions            *store.Table[CapacityBlockExtension]
	capacityReservationBillingRequests *store.Table[CapacityReservationBillingRequest]
	capacityManagerDataExports         *store.Table[CapacityManagerDataExport]
	capacityManagerState               *CapacityManagerState
	// VerifiedAccess policy / logging additions
	verifiedAccessEndpointPolicies       map[string]*VerifiedAccessPolicy
	verifiedAccessGroupPolicies          map[string]*VerifiedAccessPolicy
	verifiedAccessInstanceLoggingConfigs *store.Table[VerifiedAccessInstanceLoggingConfig]
	// FPGA image additions
	fpgaImages *store.Table[FpgaImage]
	// Scheduled Instances additions
	scheduledInstances        *store.Table[ScheduledInstance]
	scheduledInstanceLaunched map[string]int32
	// COIP / Public IPv4 / IPv6 pool additions
	coipPools *store.Table[CoipPool]
	coipCidrs *store.Table[CoipCidr]
	ipv4Pools *store.Table[Ipv4Pool]
	ipv6Pools *store.Table[Ipv6Pool]
	// VM Import/Export, Bundle, and Conversion Task additions
	bundleTasks      *store.Table[BundleTask]
	conversionTasks  *store.Table[ConversionTask]
	exportTasks      *store.Table[ExportTask]
	exportImageTasks *store.Table[ExportImageTaskRec]
	// Trunk Interface / Enclave Certificate IAM Role additions
	trunkInterfaceAssociations *store.Table[TrunkInterfaceAssociation]
	enclaveCertIamRoles        map[string][]*EnclaveCertIamRoleAssociation
	// Allowed Images Settings / Store-Restore Image Task / Image Usage Report additions
	allowedImagesSettings *AllowedImagesSettings
	storeImageTasks       *store.Table[StoreImageTask]
	usageReports          *store.Table[UsageReport]
	usageReportEntries    map[string][]*UsageReportEntry
	instanceProductCodes  map[string][]string
	// Mac Host / Mac modification task additions
	macModificationTasks *store.Table[MacModificationTask]
	// Secondary Network / Secondary Subnet / Secondary Interface / Outpost LAG /
	// Service Link Virtual Interface additions
	secondaryNetworks            *store.Table[SecondaryNetwork]
	secondarySubnets             *store.Table[SecondarySubnet]
	secondaryInterfaces          *store.Table[SecondaryInterface]
	serviceLinkVirtualInterfaces *store.Table[ServiceLinkVirtualInterface]
	outpostLags                  *store.Table[OutpostLag]
	// Instance-attribute misc cluster additions (AZ group opt-in, SQL HA)
	availabilityZoneGroupOptIns map[string]string
	sqlHaRegistrations          *store.Table[RegisteredSQLHaInstance]
	sqlHaHistory                map[string][]*RegisteredSQLHaInstance
	// parity-sweep-2 additions: VPC Encryption Control, VPN Concentrator, Host
	// Reservations, Declarative Policies, AWS Network Performance
	vpcEncryptionControls           *store.Table[VpcEncryptionControl]
	vpnConcentrators                *store.Table[VpnConcentrator]
	hostReservations                *store.Table[HostReservation]
	declarativePoliciesReports      *store.Table[DeclarativePoliciesReport]
	networkPerformanceSubscriptions *store.Table[NetworkPerformanceSubscription]
	// gopherstack-5o9 final EC2 parity sweep additions
	tgwRTPropagations          map[string]map[string]*TransitGatewayRouteTablePropagation
	interruptibleCRAllocations *store.Table[InterruptibleCapacityReservationAllocation]
	movingAddresses            *store.Table[MovingAddressStatus]
	// registry lets Reset collapse the ~150 converted resource maps' lifecycle
	// to one call (registry.ResetAll()) instead of hand-rolled re-initialization
	// of each map. See store_setup.go for every Table registration.
	registry                       *store.Registry
	mu                             *lockmetrics.RWMutex
	lifecycleStop                  chan struct{}
	eniIDByAttachment              map[string]string
	eniIDsByInstance               map[string]map[string]struct{}
	instanceIDsByVPC               map[string]map[string]struct{}
	subnetIDsByVPC                 map[string]map[string]struct{}
	routeTableIDsByVPC             map[string]map[string]struct{}
	sgIDsByVPC                     map[string]map[string]struct{}
	natGatewayIDsByVPC             map[string]map[string]struct{}
	eniIDsByVPC                    map[string]map[string]struct{}
	snapshotBlockPublicAccess      string
	ebsDefaultKmsKeyID             string
	imageBlockPublicAccess         string
	defaultCreditSpec              string
	Region                         string `json:"region,omitempty"`
	AccountID                      string `json:"accountID,omitempty"`
	freePrivateIPs                 []string
	nextPrivateIPIndex             int
	nextElasticIPIndex             int
	ebsEncryptionByDefault         bool
	serialConsoleAccess            bool
	reachabilityAnalyzerOrgSharing bool
	lifecycleOnce                  sync.Once
	lifecycleStopOnce              sync.Once
}

func newInMemoryBackendMaps() *InMemoryBackend {
	b := &InMemoryBackend{
		registry:            store.NewRegistry(),
		tags:                make(map[string]map[string]string),
		addressTransfers:    make(map[string]*AddressTransfer),
		vpcCidrAssociations: make(map[string]*VpcCidrBlockAssociation),
		ipamPoolCidrs:       make(map[string][]*IpamPoolCidr),
		instanceIDsByVPC:    make(map[string]map[string]struct{}),
		eniIDsByInstance:    make(map[string]map[string]struct{}),
		eniIDByAttachment:   make(map[string]string),
	}
	registerAllTables(b)
	initCoreExtraMaps(b)
	initBatch6Maps(b)
	initVpcConfigMaps(b)
	initCapacityFamilyMaps(b)
	initVerifiedAccessExtMaps(b)
	initParityFinalMaps(b)
	initSecondaryIndexMaps(b)
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
	b.resetMacHostMapsLocked()
	b.resetSecondaryNetworkMapsLocked()
	b.resetInstanceAttrMapsLocked()
	b.resetSQLHaMapsLocked()

	return b
}

// initVerifiedAccessExtMaps initialises the VerifiedAccess policy/logging
// state maps (split out to keep newInMemoryBackendMaps under the funlen
// limit).
func initVerifiedAccessExtMaps(b *InMemoryBackend) {
	b.verifiedAccessEndpointPolicies = make(map[string]*VerifiedAccessPolicy)
	b.verifiedAccessGroupPolicies = make(map[string]*VerifiedAccessPolicy)
}

// initCoreExtraMaps initialises spot fleet, snapshot, IMDS, and batch4 state
// maps (split out to keep newInMemoryBackendMaps under the funlen limit).
func initCoreExtraMaps(b *InMemoryBackend) {
	b.spotFleetHistory = make(map[string][]SpotFleetHistoryRecord)
	b.snapshotTiers = make(map[string]string)
	b.snapshotAttributes = make(map[string]map[string]string)
	b.sgVpcAssociations = make(map[string]map[string]string)
	b.vpcTenancy = make(map[string]string)
	b.vpcPeeringOptions = make(map[string]*PeeringConnectionOptions)
	b.subnetCIDRAssociations = make(map[string][]*SubnetCIDRAssociation)
	b.instanceMonitoring = make(map[string]string)
	b.instanceCreditSpecs = make(map[string]string)
	b.instanceIMDSOptions = make(map[string]*IMDSOptions)
	b.niIPv6Addresses = make(map[string][]string)
	b.idFormatSettings = make(map[string]bool)
	b.vpcEndpointServicePermissions = make(map[string][]string)
	b.subnetCIDRReservations = make(map[string][]*SubnetCIDRReservation)
	b.imageDisabled = make(map[string]bool)
	b.imageDeprecated = make(map[string]string)
	b.imageDeregistrationProtection = make(map[string]bool)
	b.imageAttributes = make(map[string]map[string]string)
	b.vgwRoutePropagation = make(map[string]bool)
}

// initCapacityFamilyMaps initialises the Capacity Reservation Fleet, Capacity
// Block, and Capacity Manager state maps (split out to keep
// newInMemoryBackendMaps under the funlen limit).
func initCapacityFamilyMaps(b *InMemoryBackend) {
	b.capacityManagerState = &CapacityManagerState{Status: capacityManagerStatusDisabled}
}

// initVpcConfigMaps initialises the VPC ClassicLink and Block Public Access
// state maps (split out to keep newInMemoryBackendMaps under the funlen limit).
func initVpcConfigMaps(b *InMemoryBackend) {
	b.vpcBlockPublicAccessOptions = &VpcBlockPublicAccessOptions{
		InternetGatewayBlockMode: vpcBPABlockModeOff,
		State:                    vpcBPAStateDefault,
		ExclusionsAllowed:        vpcBPAExclusionsAllowed,
		ManagedBy:                vpcBPAManagedByAccount,
	}
}

// initBatch6Maps initialises the verified-access, import-task, recycle-bin,
// fast-launch and VPN-route maps (split out to keep newInMemoryBackendMaps
// under the funlen limit).
func initBatch6Maps(b *InMemoryBackend) {
	b.fastLaunchImages = make(map[string]bool)
	b.fastSnapshotRestores = make(map[string]bool)
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
//
// The goroutine exits when ctx is cancelled OR when StopLifecycleReconciler is
// called, whichever comes first.
func (b *InMemoryBackend) StartLifecycleReconciler(ctx context.Context) {
	b.lifecycleOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(lifecycleReconcileInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
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
// Performance: takes a cheap read-lock pass first to bail early when nothing is
// transitional, avoiding a write-lock acquisition on every 50ms tick.
func (b *InMemoryBackend) reconcileInstanceLifecycle() {
	// Fast path: read-lock to detect any transitional instance.
	b.mu.RLock("reconcileInstanceLifecycle-check")
	hasTransitional := false
	for _, inst := range b.instances.All() {
		switch inst.State {
		case StatePending, StateStopping, StateShuttingDown:
			hasTransitional = true
		}
		if hasTransitional {
			break
		}
	}
	b.mu.RUnlock()

	if !hasTransitional {
		return
	}

	// Slow path: write-lock to advance transitional instances.
	b.mu.Lock("reconcileInstanceLifecycle")
	defer b.mu.Unlock()

	for _, inst := range b.instances.All() {
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
	b.vpcs.Put(&VPC{
		ID:        defaultVPCID,
		CIDRBlock: "172.31.0.0/16",
		IsDefault: true,
	})

	defaultSubnetID := "subnet-default"
	b.subnets.Put(&Subnet{
		ID:               defaultSubnetID,
		VPCID:            defaultVPCID,
		CIDRBlock:        "172.31.0.0/20",
		AvailabilityZone: b.Region + "a",
		IsDefault:        true,
	})
	b.indexSubnetLocked(defaultSubnetID, defaultVPCID)

	defaultSGID := "sg-default"
	b.securityGroups.Put(&SecurityGroup{
		ID:          defaultSGID,
		Name:        "default",
		Description: "default VPC security group",
		VPCID:       defaultVPCID,
	})
	b.indexSGLocked(defaultSGID, defaultVPCID)
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
	} else if _, ok := b.subnets.Get(subnetID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	vpcID := ""
	mapPublicIP := false
	availabilityZone := ""

	if sub, ok := b.subnets.Get(subnetID); ok {
		vpcID = sub.VPCID
		mapPublicIP = sub.MapPublicIPOnLaunch
		availabilityZone = sub.AvailabilityZone
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
		inst.Placement.AvailabilityZone = availabilityZone
		inst.PrivateIP = b.allocPrivateIP()
		if mapPublicIP {
			inst.PublicIPAddress = b.allocElasticIP()
			inst.PublicDNSName = fmt.Sprintf("ec2-%s.compute-1.amazonaws.com",
				strings.ReplaceAll(inst.PublicIPAddress, ".", "-"))
		}
		eniID := "eni-" + uuid.New().String()[:17]
		attachID := "eni-attach-" + uuid.New().String()[:8]
		b.networkInterfaces.Put(&NetworkInterface{
			ID:              eniID,
			SubnetID:        subnetID,
			VPCID:           vpcID,
			PrivateIP:       inst.PrivateIP,
			InstanceID:      id,
			AttachmentID:    attachID,
			DeviceIndex:     0,
			Status:          stateInUse,
			SourceDestCheck: true,
		})
		b.instances.Put(inst)
		b.indexInstanceLocked(inst)
		eni, _ := b.networkInterfaces.Get(eniID)
		b.indexENILocked(eniID, eni)
		b.indexENIByVPCLocked(eniID, eni)
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

	kp, ok := b.keyPairs.Get(name)
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

	inst, ok := b.instances.Get(instanceID)
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
			if eni, exists := b.networkInterfaces.Get(eniID); exists && eni.PrivateIP == oldPrivate {
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

	inst, ok := b.instances.Get(instanceID)
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

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return ""
	}

	return inst.PublicDNSName
}

// findDefaultSubnetID returns the ID of the default subnet, or empty string if none.
// Must be called with b.mu held.
func (b *InMemoryBackend) findDefaultSubnetID() string {
	for _, s := range b.subnets.All() {
		id := subnetsKeyFn(s)
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
			inst, ok := b.instances.Get(id)
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
	ptrs := make([]*Instance, 0, b.instances.Len())
	for _, inst := range b.instances.All() {
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
		inst, ok := b.instances.Get(id)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}

		if inst.DisableAPITermination {
			return nil, fmt.Errorf(
				"%w: the instance %s may not be terminated. "+
					"Modify its 'disableApiTermination' instance attribute and try again",
				ErrOperationNotPermitted, id)
		}

		prev := inst.State
		// AWS state machine: any state → shutting-down → terminated (reconciler advances).
		// Resource cleanup (ENIs, EIPs, volumes) happens immediately so callers
		// do not observe dangling attachments, but state advances asynchronously.
		inst.State = StateShuttingDown
		inst.TerminatedAt = time.Now()
		inst.StateReasonCode = "Client.UserInitiatedShutdown"
		inst.StateReasonMessage = "Client.UserInitiatedShutdown: User initiated shutdown"
		inst.StateTransitionReason = fmt.Sprintf(
			"User initiated (%s)", time.Now().UTC().Format("2006-01-02 15:04:05 GMT"),
		)
		result = append(result, &InstanceStateChange{
			InstanceID:    id,
			PreviousState: prev,
			CurrentState:  inst.State,
		})

		// Mirror AWS behaviour: when the backing instance of a spot request is
		// terminated, the request transitions to "closed" (not stateCancelled).
		for _, req := range b.spotRequests.All() {
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
			eni, exists := b.networkInterfaces.Get(eniID)
			if !exists {
				continue
			}

			b.deindexENILocked(eniID, eni)
			b.deindexENIByVPCLocked(eniID, eni)
			b.recycleENIIPsLocked(eni)
			b.networkInterfaces.Delete(eniID)
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
			sg, ok := b.securityGroups.Get(id)
			if !ok {
				continue
			}

			cp := *sg
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*SecurityGroup, 0, b.securityGroups.Len())

	for _, sg := range b.securityGroups.All() {
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
		if _, ok := b.vpcs.Get(vpcID); !ok {
			return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
		}
	}

	for _, sg := range b.securityGroups.All() {
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
	b.securityGroups.Put(sg)
	b.indexSGLocked(id, vpcID)

	return sg, nil
}

// DeleteSecurityGroup removes a security group by ID.
func (b *InMemoryBackend) DeleteSecurityGroup(id string) error {
	b.mu.Lock("DeleteSecurityGroup")
	defer b.mu.Unlock()

	sg, ok := b.securityGroups.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, id)
	}

	b.deindexSGLocked(id, sg.VPCID)
	b.securityGroups.Delete(id)
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
			v, ok := b.vpcs.Get(id)
			if !ok {
				continue
			}

			cp := *v
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*VPC, 0, b.vpcs.Len())

	for _, v := range b.vpcs.All() {
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

	for _, existing := range b.vpcs.All() {
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
	b.vpcs.Put(v)

	return v, nil
}

// cascadeDeleteVpcIGWsLocked removes all internet gateways that have an
// attachment to the given VPC. Must be called with b.mu held.
func (b *InMemoryBackend) cascadeDeleteVpcIGWsLocked(vpcID string) {
	for _, igw := range b.internetGateways.All() {
		igwID := internetGatewaysKeyFn(igw)
		for _, att := range igw.Attachments {
			if att.VPCID == vpcID {
				b.internetGateways.Delete(igwID)
				delete(b.tags, igwID)

				break
			}
		}
	}
}

// DeleteVpc removes a VPC by ID, cascade-deleting all dependent resources
// (instances, internet gateways, NAT gateways, route tables, security groups,
// network interfaces, and subnets) along with their tags.
// Uses secondary indexes for instances, subnets, route tables, and security groups
// to avoid O(n_all) scans for each resource type.
func (b *InMemoryBackend) DeleteVpc(id string) error {
	b.mu.Lock("DeleteVpc")
	defer b.mu.Unlock()

	if _, ok := b.vpcs.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, id)
	}

	// Cascade: terminate instances belonging to this VPC via secondary index.
	for instID := range b.instanceIDsByVPC[id] {
		if inst, ok := b.instances.Get(instID); ok {
			inst.State = StateTerminated
			inst.TerminatedAt = time.Now()
			delete(b.tags, instID)
			b.detachVolumesAndEIPsLocked(instID)
		}
	}
	delete(b.instanceIDsByVPC, id)

	// Cascade: detach and delete internet gateways attached to this VPC.
	b.cascadeDeleteVpcIGWsLocked(id)

	// Cascade: delete NAT gateways belonging to this VPC via secondary index,
	// avoiding a full-map scan under the write lock.
	for ngwID := range b.natGatewayIDsByVPC[id] {
		if ngw, ok := b.natGateways.Get(ngwID); ok {
			b.recycleIPLocked(ngw.PrivateIP)
			b.natGateways.Delete(ngwID)
			delete(b.tags, ngwID)
		}
	}
	delete(b.natGatewayIDsByVPC, id)

	// Cascade: remove route tables belonging to this VPC via secondary index.
	for rtID := range b.routeTableIDsByVPC[id] {
		b.routeTables.Delete(rtID)
		delete(b.tags, rtID)
	}
	delete(b.routeTableIDsByVPC, id)

	// Cascade: remove security groups belonging to this VPC via secondary index.
	for sgID := range b.sgIDsByVPC[id] {
		b.securityGroups.Delete(sgID)
		delete(b.tags, sgID)
	}
	delete(b.sgIDsByVPC, id)

	// Cascade: remove network interfaces belonging to this VPC via secondary
	// index, avoiding a full-map scan under the write lock.
	for eniID := range b.eniIDsByVPC[id] {
		if eni, ok := b.networkInterfaces.Get(eniID); ok {
			b.recycleENIIPsLocked(eni)
			b.deindexENILocked(eniID, eni)
			b.networkInterfaces.Delete(eniID)
			delete(b.tags, eniID)
		}
	}
	delete(b.eniIDsByVPC, id)

	// Cascade: remove subnets belonging to this VPC via secondary index.
	for subnetID := range b.subnetIDsByVPC[id] {
		b.subnets.Delete(subnetID)
		delete(b.tags, subnetID)
	}
	delete(b.subnetIDsByVPC, id)
	b.vpcs.Delete(id)
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
			s, ok := b.subnets.Get(id)
			if !ok {
				continue
			}

			cp := *s
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*Subnet, 0, b.subnets.Len())

	for _, s := range b.subnets.All() {
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

	if _, ok := b.vpcs.Get(vpcID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	if az == "" {
		az = b.Region + "a"
	}

	vpc, _ := b.vpcs.Get(vpcID)
	if !cidrContains(vpc.CIDRBlock, cidr) {
		return nil, fmt.Errorf("%w: subnet CIDR %s is not within VPC CIDR %s",
			ErrInvalidParameter, cidr, vpc.CIDRBlock)
	}

	for _, existing := range b.subnets.All() {
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
	b.subnets.Put(s)
	b.indexSubnetLocked(id, vpcID)

	return s, nil
}

// DeleteSubnet removes a subnet by ID, cascade-deleting any instances, NAT
// gateways, and network interfaces in that subnet along with their tags.
func (b *InMemoryBackend) DeleteSubnet(id string) error {
	b.mu.Lock("DeleteSubnet")
	defer b.mu.Unlock()

	if _, ok := b.subnets.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrSubnetNotFound, id)
	}

	// Cascade: terminate instances in this subnet.
	for _, inst := range b.instances.All() {
		instID := instancesKeyFn(inst)
		if inst.SubnetID == id {
			inst.State = StateTerminated
			inst.TerminatedAt = time.Now()
			delete(b.tags, instID)
			b.detachVolumesAndEIPsLocked(instID)
		}
	}

	// Cascade: delete NAT gateways in this subnet.
	for _, ngw := range b.natGateways.All() {
		ngwID := natGatewaysKeyFn(ngw)
		if ngw.SubnetID == id {
			b.recycleIPLocked(ngw.PrivateIP)
			b.deindexNatGatewayLocked(ngw)
			b.natGateways.Delete(ngwID)
			delete(b.tags, ngwID)
		}
	}

	// Cascade: remove network interfaces in this subnet.
	for _, eni := range b.networkInterfaces.All() {
		eniID := networkInterfacesKeyFn(eni)
		if eni.SubnetID == id {
			b.recycleENIIPsLocked(eni)
			b.deindexENILocked(eniID, eni)
			b.deindexENIByVPCLocked(eniID, eni)
			b.networkInterfaces.Delete(eniID)
			delete(b.tags, eniID)
		}
	}

	subnet, _ := b.subnets.Get(id)
	b.deindexSubnetLocked(id, subnet.VPCID)
	b.subnets.Delete(id)
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

// resourceTypeByID and resourceExistsLocked live in backend_resource_types.go
// (split out: the full taggable-resource-type table is large and orthogonal
// to the rest of this file).

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
	for _, vol := range b.volumes.All() {
		if vol.Attachment != nil && vol.Attachment.InstanceID == instanceID {
			vol.Attachment = nil
			vol.State = stateAvailable
		}
	}

	for _, addr := range b.addresses.All() {
		if addr.InstanceID == instanceID {
			addr.AssociationID = ""
			addr.InstanceID = ""
		}
	}
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

	// Only build the filter set when callers actually supply IDs; avoids an
	// unnecessary allocation on the common unfiltered path.
	var filterSet map[string]bool
	if len(resourceIDs) > 0 {
		filterSet = make(map[string]bool, len(resourceIDs))
		for _, id := range resourceIDs {
			filterSet[id] = true
		}
	}

	var entries []TagEntry

	for resourceID, tagMap := range b.tags {
		if filterSet != nil && !filterSet[resourceID] {
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
