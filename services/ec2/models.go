package ec2

import "time"

// Shared state constants consolidated from across the backend op-family
// files (formerly batch1 constants).
const (
	stateCompleted          = "completed"
	stateAssociated         = "associated"
	snapshotProgress100     = "100%"
	stateBlockAllSharing    = "block-all-sharing"
	defaultVPCCIDR          = "172.31.0.0/16"
	defaultSubnetCIDR       = "172.31.16.0/20"
	stateMonitoringEnabled  = "enabled"
	stateMonitoringDisabled = "disabled"
	progressComplete        = int64(100)
)

// VolumeModification tracks a ModifyVolume operation.
type VolumeModification struct {
	StartTime         time.Time `json:"startTime"`
	EndTime           time.Time `json:"endTime"`
	VolumeID          string    `json:"volumeID,omitempty"`
	ModificationState string    `json:"modificationState,omitempty"`
	TargetVolumeType  string    `json:"targetVolumeType,omitempty"`
	OrigVolumeType    string    `json:"origVolumeType,omitempty"`
	TargetSize        int       `json:"targetSize,omitempty"`
	OrigSize          int       `json:"origSize,omitempty"`
	TargetIops        int       `json:"targetIops,omitempty"`
	OrigIops          int       `json:"origIops,omitempty"`
	Progress          int64     `json:"progress"`
}

// NetworkInterfacePermission is a permission granted on a network interface.
type NetworkInterfacePermission struct {
	PermissionID       string `json:"permissionID,omitempty"`
	NetworkInterfaceID string `json:"networkInterfaceID,omitempty"`
	AwsAccountID       string `json:"awsAccountID,omitempty"`
	AwsService         string `json:"awsService,omitempty"`
	Permission         string `json:"permission,omitempty"`
	State              string `json:"state,omitempty"`
}

// PeeringConnectionOptions holds DNS/routing options for a VPC peering connection.
type PeeringConnectionOptions struct {
	AllowDNSResolutionFromRemoteVPC            bool `json:"allowDnsResolutionFromRemoteVpc,omitempty"`
	AllowEgressFromLocalClassicLinkToRemoteVPC bool `json:"allowEgressFromLocalClassicLinkToRemoteVpc,omitempty"`
	AllowEgressFromLocalVPCToRemoteClassicLink bool `json:"allowEgressFromLocalVpcToRemoteClassicLink,omitempty"`
}

// AddressAttribute holds domain-name attributes for an Elastic IP.
type AddressAttribute struct {
	AllocationID string `json:"allocationID,omitempty"`
	PublicIP     string `json:"publicIP,omitempty"`
	DomainName   string `json:"domainName,omitempty"`
}

// InstanceMetadataDefaults holds account-level IMDS defaults.
type InstanceMetadataDefaults struct {
	HTTPTokens              string `json:"httpTokens,omitempty"`
	HTTPEndpoint            string `json:"httpEndpoint,omitempty"`
	InstanceMetadataTags    string `json:"instanceMetadataTags,omitempty"`
	HTTPPutResponseHopLimit int    `json:"httpPutResponseHopLimit,omitempty"`
}

// SubnetCIDRAssociation tracks an IPv6 CIDR associated with a subnet.
type SubnetCIDRAssociation struct {
	AssociationID string `json:"associationID,omitempty"`
	IPv6CIDRBlock string `json:"ipv6CidrBlock,omitempty"`
	State         string `json:"state,omitempty"`
}

// Image state constants (formerly batch2 constants).
const (
	stateAvailableImg  = "available"
	stateDisabledImg   = "disabled"
	stateDeprecatedImg = "deprecated"
)

// Misc formerly-batch2 constants.
const (
	defaultEBSKmsKeyAlias    = "alias/aws/ebs"
	stateImageUnblocked      = "unblocked"
	stateImageBlockNew       = "block-new-sharing"
	stateDefaultCredit       = "standard"
	addressTransferOfferDays = 3
)

// VpcEndpointConnectionNotification tracks a VPC endpoint connection notification.
type VpcEndpointConnectionNotification struct {
	ConnectionNotificationID    string   `json:"connectionNotificationID,omitempty"`
	ServiceID                   string   `json:"serviceID,omitempty"`
	VpcEndpointID               string   `json:"vpcEndpointID,omitempty"`
	ConnectionNotificationARN   string   `json:"connectionNotificationARN,omitempty"`
	ConnectionNotificationType  string   `json:"connectionNotificationType,omitempty"`
	ConnectionNotificationState string   `json:"connectionNotificationState,omitempty"`
	ConnectionEvents            []string `json:"connectionEvents,omitempty"`
}

// SnapshotLock holds lock state for an EBS snapshot.
type SnapshotLock struct {
	LockCreatedOn    time.Time `json:"lockCreatedOn"`
	LockExpiresOn    time.Time `json:"lockExpiresOn"`
	SnapshotID       string    `json:"snapshotID,omitempty"`
	LockState        string    `json:"lockState,omitempty"`
	LockDurationDays int       `json:"lockDurationDays,omitempty"`
}

// ReplaceRootVolumeTask tracks a replace-root-volume operation.
type ReplaceRootVolumeTask struct {
	StartTime               time.Time `json:"startTime"`
	CompleteTime            time.Time `json:"completeTime"`
	ReplaceRootVolumeTaskID string    `json:"replaceRootVolumeTaskID,omitempty"`
	InstanceID              string    `json:"instanceID,omitempty"`
	TaskState               string    `json:"taskState,omitempty"`
	SnapshotID              string    `json:"snapshotID,omitempty"`
}

// SubnetCIDRReservation tracks a CIDR reservation within a subnet.
type SubnetCIDRReservation struct {
	SubnetCIDRReservationID string `json:"subnetCidrReservationID,omitempty"`
	SubnetID                string `json:"subnetID,omitempty"`
	CIDR                    string `json:"cidr,omitempty"`
	ReservationType         string `json:"reservationType,omitempty"`
	Description             string `json:"description,omitempty"`
	OwnerID                 string `json:"ownerID,omitempty"`
	State                   string `json:"state,omitempty"`
}

// Recycle-bin / fast-launch state constants (formerly batch3 constants).
const (
	stateInRecycleBin      = "InRecycleBin"
	stateRestoring         = "restoring"
	statePending           = "pending"
	stateTaskCompleted     = "completed"
	stateEnabledFastLaunch = "enabled"
	magicSplitLen          = 2
)

// InstanceConnectEndpoint represents an EC2 Instance Connect Endpoint.
type InstanceConnectEndpoint struct {
	CreateTime                 time.Time `json:"createTime"`
	InstanceConnectEndpointID  string    `json:"instanceConnectEndpointId,omitempty"`
	InstanceConnectEndpointARN string    `json:"instanceConnectEndpointArn,omitempty"`
	SubnetID                   string    `json:"subnetId,omitempty"`
	VPCID                      string    `json:"vpcId,omitempty"`
	State                      string    `json:"state,omitempty"`
	SecurityGroupIDs           []string  `json:"securityGroupIds,omitempty"`
	PreserveClientIP           bool      `json:"preserveClientIp,omitempty"`
}

// InstanceEventWindow represents a scheduled maintenance window for instances.
type InstanceEventWindow struct {
	InstanceEventWindowID string `json:"instanceEventWindowId,omitempty"`
	Name                  string `json:"name,omitempty"`
	CronExpression        string `json:"cronExpression,omitempty"`
	State                 string `json:"state,omitempty"`

	// InstanceIDs and DedicatedHostIDs hold the association targets added via
	// AssociateInstanceEventWindow and removed via DisassociateInstanceEventWindow.
	InstanceIDs      []string `json:"instanceIds,omitempty"`
	DedicatedHostIDs []string `json:"dedicatedHostIds,omitempty"`
}

// SpotDatafeed holds the spot instance data feed subscription settings.
type SpotDatafeed struct {
	Bucket string `json:"bucket,omitempty"`
	Prefix string `json:"prefix,omitempty"`
	State  string `json:"state,omitempty"`
}

// RecycleBinImage holds a soft-deleted AMI.
type RecycleBinImage struct {
	RecycleBinEnterTime time.Time `json:"recycleBinEnterTime"`
	RecycleBinExitTime  time.Time `json:"recycleBinExitTime"`
	ImageID             string    `json:"imageId,omitempty"`
	Name                string    `json:"name,omitempty"`
}

// RecycleBinVolume holds a soft-deleted EBS volume.
type RecycleBinVolume struct {
	RecycleBinEnterTime time.Time `json:"recycleBinEnterTime"`
	RecycleBinExitTime  time.Time `json:"recycleBinExitTime"`
	VolumeID            string    `json:"volumeId,omitempty"`
}

// ImageImportTask holds status for an ImportImage task.
type ImageImportTask struct {
	ImportTaskID string `json:"importTaskId,omitempty"`
	Description  string `json:"description,omitempty"`
	Architecture string `json:"architecture,omitempty"`
	Platform     string `json:"platform,omitempty"`
	Status       string `json:"status,omitempty"`
}

// SnapshotImportTask holds status for an ImportSnapshot task.
type SnapshotImportTask struct {
	ImportTaskID string `json:"importTaskId,omitempty"`
	Description  string `json:"description,omitempty"`
	SnapshotID   string `json:"snapshotId,omitempty"`
	Status       string `json:"status,omitempty"`
}

// ManagedPrefixList represents a managed prefix list.
type ManagedPrefixList struct {
	PrefixListID   string            `json:"prefixListId,omitempty"`
	PrefixListName string            `json:"prefixListName,omitempty"`
	PrefixListArn  string            `json:"prefixListArn,omitempty"`
	AddressFamily  string            `json:"addressFamily,omitempty"`
	State          string            `json:"state,omitempty"`
	OwnerID        string            `json:"ownerId,omitempty"`
	Entries        []PrefixListEntry `json:"entries,omitempty"`
	Version        int64             `json:"version"`
	MaxEntries     int               `json:"maxEntries,omitempty"`
}

// PrefixListEntry holds a single CIDR entry in a managed prefix list.
type PrefixListEntry struct {
	Cidr        string `json:"cidr,omitempty"`
	Description string `json:"description,omitempty"`
}

// ClientVpnTargetNetwork represents an associated target network for a Client VPN endpoint.
type ClientVpnTargetNetwork struct {
	AssociationID       string   `json:"associationId,omitempty"`
	SubnetID            string   `json:"subnetId,omitempty"`
	VPCID               string   `json:"vpcId,omitempty"`
	ClientVpnEndpointID string   `json:"clientVpnEndpointId,omitempty"`
	Status              string   `json:"status,omitempty"`
	SecurityGroups      []string `json:"securityGroups,omitempty"`
}

// ClientVpnEndpoint represents an EC2 Client VPN endpoint.
type ClientVpnEndpoint struct {
	ServerCertificateArn      string                    `json:"serverCertificateArn,omitempty"`
	DNSName                   string                    `json:"dnsName,omitempty"`
	Status                    string                    `json:"status,omitempty"`
	Description               string                    `json:"description,omitempty"`
	ClientCidrBlock           string                    `json:"clientCidrBlock,omitempty"`
	ClientVpnEndpointID       string                    `json:"clientVpnEndpointId,omitempty"`
	VpnProtocol               string                    `json:"vpnProtocol,omitempty"`
	TransportProtocol         string                    `json:"transportProtocol,omitempty"`
	VPCID                     string                    `json:"vpcId,omitempty"`
	CertificateRevocationList string                    `json:"certificateRevocationList,omitempty"`
	CreationTime              string                    `json:"creationTime,omitempty"`
	SelfServicePortalURL      string                    `json:"selfServicePortalUrl,omitempty"`
	DNSServers                []string                  `json:"dnsServers,omitempty"`
	SecurityGroupIDs          []string                  `json:"securityGroupIds,omitempty"`
	TargetNetworks            []*ClientVpnTargetNetwork `json:"targetNetworks,omitempty"`
	Routes                    []ClientVpnRoute          `json:"routes,omitempty"`
	AuthRules                 []ClientVpnAuthRule       `json:"authRules,omitempty"`
	SessionTimeoutHours       int32                     `json:"sessionTimeoutHours,omitempty"`
	VpnPort                   int32                     `json:"vpnPort,omitempty"`
	SplitTunnel               bool                      `json:"splitTunnel,omitempty"`
}

// ClientVpnRoute holds a single route for a Client VPN endpoint.
type ClientVpnRoute struct {
	DestinationCidr string `json:"destinationCidr,omitempty"`
	Status          string `json:"status,omitempty"`
	Description     string `json:"description,omitempty"`
	// Origin indicates how the route was added: "add-route" for routes created
	// via CreateClientVpnRoute, "associate" for routes auto-added when a target
	// network is associated.
	Origin string `json:"origin,omitempty"`
	// TargetSubnet is the ID of the subnet through which traffic is routed.
	TargetSubnet string `json:"targetSubnet,omitempty"`
}

// ClientVpnAuthRule holds a single authorization rule for a Client VPN endpoint.
type ClientVpnAuthRule struct {
	Cidr        string `json:"cidr,omitempty"`
	Status      string `json:"status,omitempty"`
	Description string `json:"description,omitempty"`
	// GroupID is the Active Directory group ID the rule grants access to, if any.
	GroupID string `json:"groupId,omitempty"`
	// AccessAll indicates the rule grants access to all clients (true when
	// GroupID is empty).
	AccessAll bool `json:"accessAll,omitempty"`
}

// ClientVpnConnection represents an active client connection to a Client VPN
// endpoint. No API in this backend creates connections (a real client would
// need to actually establish an OpenVPN session), so DescribeClientVpnConnections
// always returns an empty set — that is the correct AWS shape when no clients
// are connected.
type ClientVpnConnection struct {
	ConnectionID              string `json:"connectionId,omitempty"`
	ClientVpnEndpointID       string `json:"clientVpnEndpointId,omitempty"`
	Username                  string `json:"username,omitempty"`
	ClientIP                  string `json:"clientIp,omitempty"`
	CommonName                string `json:"commonName,omitempty"`
	ConnectionEstablishedTime string `json:"connectionEstablishedTime,omitempty"`
	Status                    string `json:"status,omitempty"`
}

// TransitGatewayConnect represents a TGW connect attachment.
type TransitGatewayConnect struct {
	TransitGatewayAttachmentID          string `json:"transitGatewayAttachmentId,omitempty"`
	TransportTransitGatewayAttachmentID string `json:"transportTransitGatewayAttachmentId,omitempty"`
	TransitGatewayID                    string `json:"transitGatewayId,omitempty"`
	State                               string `json:"state,omitempty"`
}

// TransitGatewayConnectPeer represents a TGW connect peer.
type TransitGatewayConnectPeer struct {
	TransitGatewayConnectPeerID string   `json:"transitGatewayConnectPeerId,omitempty"`
	TransitGatewayAttachmentID  string   `json:"transitGatewayAttachmentId,omitempty"`
	PeerAddress                 string   `json:"peerAddress,omitempty"`
	State                       string   `json:"state,omitempty"`
	InsideCidrBlocks            []string `json:"insideCidrBlocks,omitempty"`
}

// TransitGatewayPrefixListReference represents a TGW prefix list reference.
type TransitGatewayPrefixListReference struct {
	PrefixListID               string `json:"prefixListId,omitempty"`
	TransitGatewayRouteTableID string `json:"transitGatewayRouteTableId,omitempty"`
	State                      string `json:"state,omitempty"`
	TransitGatewayAttachmentID string `json:"transitGatewayAttachmentId,omitempty"`
	Blackhole                  bool   `json:"blackhole,omitempty"`
}

// VerifiedAccessEndpoint represents a Verified Access endpoint.
type VerifiedAccessEndpoint struct {
	VerifiedAccessEndpointID string `json:"verifiedAccessEndpointId,omitempty"`
	VerifiedAccessGroupID    string `json:"verifiedAccessGroupId,omitempty"`
	Status                   string `json:"status,omitempty"`
	Description              string `json:"description,omitempty"`
	EndpointType             string `json:"endpointType,omitempty"`
}

// VerifiedAccessGroup represents a Verified Access group.
type VerifiedAccessGroup struct {
	VerifiedAccessGroupID    string `json:"verifiedAccessGroupId,omitempty"`
	VerifiedAccessInstanceID string `json:"verifiedAccessInstanceId,omitempty"`
	Status                   string `json:"status,omitempty"`
	Description              string `json:"description,omitempty"`
}

// VerifiedAccessInstance represents a Verified Access instance.
type VerifiedAccessInstance struct {
	VerifiedAccessInstanceID string   `json:"verifiedAccessInstanceId,omitempty"`
	Status                   string   `json:"status,omitempty"`
	Description              string   `json:"description,omitempty"`
	AttachedTrustProviderIDs []string `json:"attachedTrustProviderIds,omitempty"`
}

// VerifiedAccessTrustProvider represents a Verified Access trust provider.
type VerifiedAccessTrustProvider struct {
	VerifiedAccessTrustProviderID string `json:"verifiedAccessTrustProviderId,omitempty"`
	TrustProviderType             string `json:"trustProviderType,omitempty"`
	Status                        string `json:"status,omitempty"`
	Description                   string `json:"description,omitempty"`
}

// Traffic mirror / fleet / network insights / carrier gateway / reserved instances (formerly batch5).
const (
	stateByoipAdvertised   = "advertised"
	stateAnalysisSucceeded = "succeeded"
	fleetTypeDefault       = "maintain"
)

type TrafficMirrorFilter struct {
	TrafficMirrorFilterID string                     `json:"trafficMirrorFilterId,omitempty"`
	Description           string                     `json:"description,omitempty"`
	NetworkServices       []string                   `json:"networkServices,omitempty"`
	IngressFilterRules    []*TrafficMirrorFilterRule `json:"ingressFilterRules,omitempty"`
	EgressFilterRules     []*TrafficMirrorFilterRule `json:"egressFilterRules,omitempty"`
}

// TrafficMirrorPortRange holds a source or destination port range assigned
// to a traffic mirror filter rule.

type TrafficMirrorPortRange struct {
	FromPort int `json:"fromPort,omitempty"`
	ToPort   int `json:"toPort,omitempty"`
}

// TrafficMirrorPortRangePair carries the optional source and destination
// port ranges supplied when creating a traffic mirror filter rule.

type TrafficMirrorPortRangePair struct {
	Source      *TrafficMirrorPortRange
	Destination *TrafficMirrorPortRange
}

// TrafficMirrorFilterRule holds a single traffic mirror filter rule.

type TrafficMirrorFilterRule struct {
	DestinationPortRange      *TrafficMirrorPortRange `json:"destinationPortRange,omitempty"`
	SourcePortRange           *TrafficMirrorPortRange `json:"sourcePortRange,omitempty"`
	TrafficMirrorFilterRuleID string                  `json:"trafficMirrorFilterRuleId,omitempty"`
	TrafficMirrorFilterID     string                  `json:"trafficMirrorFilterId,omitempty"`
	RuleAction                string                  `json:"ruleAction,omitempty"`
	TrafficDirection          string                  `json:"trafficDirection,omitempty"`
	DestinationCidrBlock      string                  `json:"destinationCidrBlock,omitempty"`
	SourceCidrBlock           string                  `json:"sourceCidrBlock,omitempty"`
	Description               string                  `json:"description,omitempty"`
	RuleNumber                int                     `json:"ruleNumber,omitempty"`
	Protocol                  int                     `json:"protocol,omitempty"`
}

// TrafficMirrorSession holds a traffic mirror session.

type TrafficMirrorSession struct {
	TrafficMirrorSessionID string `json:"trafficMirrorSessionId,omitempty"`
	NetworkInterfaceID     string `json:"networkInterfaceId,omitempty"`
	OwnerID                string `json:"ownerId,omitempty"`
	TrafficMirrorTargetID  string `json:"trafficMirrorTargetId,omitempty"`
	TrafficMirrorFilterID  string `json:"trafficMirrorFilterId,omitempty"`
	Description            string `json:"description,omitempty"`
	SessionNumber          int    `json:"sessionNumber,omitempty"`
	PacketLength           int    `json:"packetLength,omitempty"`
	VirtualNetworkID       int    `json:"virtualNetworkId,omitempty"`
}

// TrafficMirrorTarget holds a traffic mirror target.

type TrafficMirrorTarget struct {
	TrafficMirrorTargetID         string `json:"trafficMirrorTargetId,omitempty"`
	NetworkInterfaceID            string `json:"networkInterfaceId,omitempty"`
	NetworkLoadBalancerArn        string `json:"networkLoadBalancerArn,omitempty"`
	GatewayLoadBalancerEndpointID string `json:"gatewayLoadBalancerEndpointId,omitempty"`
	OwnerID                       string `json:"ownerId,omitempty"`
	Type                          string `json:"type,omitempty"`
	Description                   string `json:"description,omitempty"`
}

// ---- EC2 Fleet ----

// Fleet holds an EC2 Fleet.

type Fleet struct {
	FleetID                          string `json:"fleetId,omitempty"`
	FleetState                       string `json:"fleetState,omitempty"`
	FleetType                        string `json:"fleetType,omitempty"`
	TargetCapacityUnitType           string `json:"targetCapacityUnitType,omitempty"`
	ExcessCapacityTerminationPolicy  string `json:"excessCapacityTerminationPolicy,omitempty"`
	TotalTargetCapacity              int    `json:"totalTargetCapacity,omitempty"`
	OnDemandTargetCapacity           int    `json:"onDemandTargetCapacity,omitempty"`
	SpotTargetCapacity               int    `json:"spotTargetCapacity,omitempty"`
	TerminateInstancesWithExpiration bool   `json:"terminateInstancesWithExpiration,omitempty"`
}

// ---- Network Insights ----

// NetworkInsightsPath holds a network insights path.

type NetworkInsightsPath struct {
	NetworkInsightsPathID  string `json:"networkInsightsPathId,omitempty"`
	NetworkInsightsPathArn string `json:"networkInsightsPathArn,omitempty"`
	SourceID               string `json:"sourceId,omitempty"`
	DestinationID          string `json:"destinationId,omitempty"`
	Protocol               string `json:"protocol,omitempty"`
	DestinationPort        int    `json:"destinationPort,omitempty"`
}

// NetworkInsightsAnalysis holds a network insights analysis.

type NetworkInsightsAnalysis struct {
	NetworkInsightsAnalysisID string `json:"networkInsightsAnalysisId,omitempty"`
	NetworkInsightsPathID     string `json:"networkInsightsPathId,omitempty"`
	Status                    string `json:"status,omitempty"`
	NetworkPathFound          bool   `json:"networkPathFound,omitempty"`
}

// NetworkInsightsAccessScope holds a network insights access scope.

type NetworkInsightsAccessScope struct {
	NetworkInsightsAccessScopeID  string `json:"networkInsightsAccessScopeId,omitempty"`
	NetworkInsightsAccessScopeArn string `json:"networkInsightsAccessScopeArn,omitempty"`
}

// NetworkInsightsAccessScopeAnalysis holds an access scope analysis.

type NetworkInsightsAccessScopeAnalysis struct {
	NetworkInsightsAccessScopeAnalysisID string `json:"networkInsightsAccessScopeAnalysisId,omitempty"`
	NetworkInsightsAccessScopeID         string `json:"networkInsightsAccessScopeId,omitempty"`
	Status                               string `json:"status,omitempty"`
	AnalyzedEniCount                     int    `json:"analyzedEniCount,omitempty"`
}

// ---- Carrier Gateways ----

// CarrierGateway holds a carrier gateway.

type CarrierGateway struct {
	CarrierGatewayID string `json:"carrierGatewayId,omitempty"`
	VpcID            string `json:"vpcId,omitempty"`
	State            string `json:"state,omitempty"`
	OwnerID          string `json:"ownerId,omitempty"`
}

// ---- Reserved Instances ----

// ReservedInstance holds a reserved instance.

type ReservedInstance struct {
	ReservedInstancesID string  `json:"reservedInstancesId,omitempty"`
	InstanceType        string  `json:"instanceType,omitempty"`
	AvailabilityZone    string  `json:"availabilityZone,omitempty"`
	ProductDescription  string  `json:"productDescription,omitempty"`
	State               string  `json:"state,omitempty"`
	OfferingType        string  `json:"offeringType,omitempty"`
	InstanceCount       int     `json:"instanceCount,omitempty"`
	Duration            int64   `json:"duration"`
	FixedPrice          float64 `json:"fixedPrice"`
	UsagePrice          float64 `json:"usagePrice"`
}

// ReservedInstancesOffering holds a reserved instances offering.

type ReservedInstancesOffering struct {
	ReservedInstancesOfferingID string  `json:"reservedInstancesOfferingId,omitempty"`
	InstanceType                string  `json:"instanceType,omitempty"`
	AvailabilityZone            string  `json:"availabilityZone,omitempty"`
	ProductDescription          string  `json:"productDescription,omitempty"`
	OfferingType                string  `json:"offeringType,omitempty"`
	Duration                    int64   `json:"duration"`
	FixedPrice                  float64 `json:"fixedPrice"`
	UsagePrice                  float64 `json:"usagePrice"`
}

// ReservedInstancesListing holds a reserved instances listing.

type ReservedInstancesListing struct {
	ReservedInstancesListingID string `json:"reservedInstancesListingId,omitempty"`
	ReservedInstancesID        string `json:"reservedInstancesId,omitempty"`
	Status                     string `json:"status,omitempty"`
	StatusMessage              string `json:"statusMessage,omitempty"`
}

// ReservedInstancesModification holds a reserved instances modification.

type ReservedInstancesModification struct {
	ReservedInstancesModificationID string `json:"reservedInstancesModificationId,omitempty"`
	Status                          string `json:"status,omitempty"`
	StatusMessage                   string `json:"statusMessage,omitempty"`
}

// QueuedPurchaseDeletionResult holds one ReservedInstance ID's outcome from
// DeleteQueuedReservedInstances. This backend's PurchaseReservedInstancesOffering
// has no future-dated purchase mode, so every Reserved Instance starts "active" --
// a real ID here always fails with reserved-instances-not-in-queued-state
// (matching real AWS refusing to delete an active reservation); an unknown ID
// fails with reserved-instances-id-invalid.
type QueuedPurchaseDeletionResult struct {
	ReservedInstancesID string
	ErrorCode           string
	ErrorMessage        string
	Failed              bool
}

// ---- Traffic Mirror backend methods ----

// Network ACL / launch template / snapshot / vpc-attr constants (formerly refinement2).

// Snapshot represents an EBS snapshot.
type Snapshot struct {
	StartTime   time.Time `json:"startTime"`
	VolumeID    string    `json:"volumeID,omitempty"`
	SnapshotID  string    `json:"snapshotID,omitempty"`
	Description string    `json:"description,omitempty"`
	State       string    `json:"state,omitempty"`
	Progress    string    `json:"progress,omitempty"`
	KmsKeyID    string    `json:"kmsKeyId,omitempty"`
	OwnerID     string    `json:"ownerID,omitempty"`
	VolumeSize  int       `json:"volumeSize,omitempty"`
	Encrypted   bool      `json:"encrypted,omitempty"`
}

// NACLEntry represents a single NACL rule entry.

type NACLEntry struct {
	Protocol   string `json:"protocol,omitempty"`
	CIDRBlock  string `json:"cidrBlock,omitempty"`
	RuleAction string `json:"ruleAction,omitempty"`
	RuleNumber int    `json:"ruleNumber,omitempty"`
	FromPort   int    `json:"fromPort,omitempty"`
	ToPort     int    `json:"toPort,omitempty"`
	Egress     bool   `json:"egress,omitempty"`
}

// StoredNetworkACL represents a persisted Network ACL (created explicitly via CreateNetworkAcl).

type StoredNetworkACL struct {
	ID             string      `json:"id,omitempty"`
	VPCID          string      `json:"vpcID,omitempty"`
	AssociationIDs []string    `json:"associationIDs,omitempty"`
	Entries        []NACLEntry `json:"entries,omitempty"`
	IsDefault      bool        `json:"isDefault,omitempty"`
}

// SecurityGroupRuleDetail represents a full security group rule record as returned by DescribeSecurityGroupRules.

type SecurityGroupRuleDetail struct {
	SecurityGroupRuleID string `json:"securityGroupRuleID,omitempty"`
	GroupID             string `json:"groupID,omitempty"`
	Protocol            string `json:"protocol,omitempty"`
	CIDRIPv4            string `json:"cidrIpv4,omitempty"`
	Description         string `json:"description,omitempty"`
	FromPort            int    `json:"fromPort,omitempty"`
	ToPort              int    `json:"toPort,omitempty"`
	IsEgress            bool   `json:"isEgress,omitempty"`
}

// Additional errors for refinement 2 operations.

const naclDefaultDenyRuleNumber = 32767

// VPC attribute name constants.

const (
	attrEnableDNSSupport          = "enableDnsSupport"
	attrEnableDNSHostnames        = "enableDnsHostnames"
	attrMapPublicIPOnLaunch       = "mapPublicIpOnLaunch"
	attrEnableResourceNameDNSARec = "enableResourceNameDnsARecordOnLaunch"
)

// ---- Snapshots ----
