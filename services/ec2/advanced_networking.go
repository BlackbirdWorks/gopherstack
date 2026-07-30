package ec2

import (
	"errors"
)

// ---- Errors ----

var (
	// ErrVpnConnectionNotFound is returned when a VPN connection ID does not exist.
	ErrVpnConnectionNotFound = errors.New("InvalidVpnConnectionID.NotFound")
	// ErrVpnGatewayNotFound is returned when a VPN gateway ID does not exist.
	ErrVpnGatewayNotFound = errors.New("InvalidVpnGatewayID.NotFound")
	// ErrCustomerGatewayNotFound is returned when a customer gateway ID does not exist.
	ErrCustomerGatewayNotFound = errors.New("InvalidCustomerGatewayID.NotFound")
	// ErrVpnTunnelNotFound is returned when a VPN tunnel's outside IP address does not match
	// any tunnel on the given VPN connection.
	ErrVpnTunnelNotFound = errors.New("InvalidParameterValue")
	// ErrVpcEndpointServiceNotFound is returned when a VPC endpoint service config ID does not exist.
	ErrVpcEndpointServiceNotFound = errors.New("InvalidVpcEndpointService.NotFound")
	// ErrIpamNotFound is returned when an IPAM ID does not exist.
	ErrIpamNotFound = errors.New("InvalidIpamId.NotFound")
	// ErrIpamPoolNotFound is returned when an IPAM pool ID does not exist.
	ErrIpamPoolNotFound = errors.New("InvalidIpamPoolId.NotFound")
	// ErrIpamAllocationNotFound is returned when an IPAM pool allocation ID does not exist.
	ErrIpamAllocationNotFound = errors.New("InvalidIpamPoolAllocationId.NotFound")
	// ErrIpamScopeNotFound is returned when an IPAM scope ID does not exist.
	ErrIpamScopeNotFound = errors.New("InvalidIpamScopeId.NotFound")
	// ErrIpamScopeDefault is returned when an attempt is made to delete a default IPAM scope.
	ErrIpamScopeDefault = errors.New("IncorrectState")
	// ErrIpamPoolCidrNotFound is returned when a CIDR is not provisioned to an IPAM pool.
	ErrIpamPoolCidrNotFound = errors.New("InvalidParameterValue")
	// ErrIpamResourceDiscoveryNotFound is returned when an IPAM resource discovery ID does not exist.
	ErrIpamResourceDiscoveryNotFound = errors.New("InvalidIpamResourceDiscoveryId.NotFound")
)

// ---- Constants ----

const (
	// vpnTypeIPSec is the only VPN connection type currently supported by AWS.
	vpnTypeIPSec = "ipsec.1"
	// attachmentStateAttached is the state of a VPN gateway once attached to a VPC.
	attachmentStateAttached = "attached"
	// attachmentStateDetached is the state of a VPN gateway once detached from a VPC.
	attachmentStateDetached = "detached"
	// ipv4Shift is the bit-size of an IPv4 address, used when calculating CIDR offsets.
	ipv4Shift = 32
	// octetMask is used to extract a single byte from an IPv4 uint32 representation.
	octetMask = 8
	// ipv4Len is the length of an IPv4 address in bytes.
	ipv4Len = 4
	// octet3Shift is the left-shift for the most significant byte (byte 0) of an IPv4 uint32.
	octet3Shift = octetMask * 3
	// octet2Shift is the left-shift for byte 1 of an IPv4 uint32.
	octet2Shift = octetMask * 2

	// vpnTunnelOutsideIPBase1 and vpnTunnelOutsideIPBase2 are the /24 blocks (from the
	// documentation-only TEST-NET-2/TEST-NET-3 ranges, RFC 5737) used to synthesize distinct
	// public tunnel outside IP addresses for the two tunnels of a VPN connection.
	vpnTunnelOutsideIPBase1 = "203.0.113."
	vpnTunnelOutsideIPBase2 = "198.51.100."
	// vpnTunnelOctetRange bounds the generated last octet of a tunnel outside IP to a valid,
	// non-zero, non-broadcast host address within its /24 block.
	vpnTunnelOctetRange = 253
	// vpnTunnelInsideCIDRRange bounds the generated 169.254.x.0/30 tunnel-inside block so the
	// third octet stays within a valid byte range while leaving room for the paired tunnel.
	vpnTunnelInsideCIDRRange = 60
	// vpnTunnelInsideCIDRStep spaces successive /30 blocks (4 addresses each) apart.
	vpnTunnelInsideCIDRStep = 4
	// vpnPhase1LifetimeSeconds and vpnPhase2LifetimeSeconds are the AWS default IKE Phase 1/2
	// SA lifetimes applied to newly-created tunnels.
	vpnPhase1LifetimeSeconds = 28800
	vpnPhase2LifetimeSeconds = 3600
	// vpnRekeyMarginTimeSeconds is the AWS default rekey margin.
	vpnRekeyMarginTimeSeconds = 540
	// vpnDPDTimeoutSeconds is the AWS default Dead Peer Detection timeout.
	vpnDPDTimeoutSeconds = 30
	// vpnPreSharedKeyLength is the length of generated tunnel pre-shared keys.
	vpnPreSharedKeyLength = 24

	// IPAM lifecycle states, mirroring the AWS IpamState/IpamPoolState/IpamScopeState enums.
	ipamStateCreateComplete = "create-complete"
	ipamStateModifyComplete = "modify-complete"
	ipamStateDeleteComplete = "delete-complete"

	// ipamScopeTypePublic and ipamScopeTypePrivate are the two IpamScopeType values.
	ipamScopeTypePublic  = "public"
	ipamScopeTypePrivate = "private"

	// ipamPoolCidrStateProvisioned is the steady-state IpamPoolCidrState once a CIDR is provisioned.
	ipamPoolCidrStateProvisioned = "provisioned"

	// ipamResourceDiscoveryAssocStatus is the steady-state ResourceDiscoveryAssociationStatus.
	ipamResourceDiscoveryAssocStatus = "active"
)

// ---- Data types ----

// VpnGateway represents a Virtual Private Gateway (VGW).
type VpnGateway struct {
	VpnGatewayID    string `json:"vpnGatewayId,omitempty"`
	State           string `json:"state,omitempty"`
	Type            string `json:"type,omitempty"`
	AttachedVPCID   string `json:"attachedVpcId,omitempty"`
	AttachmentState string `json:"attachmentState,omitempty"`
}

// CustomerGateway represents a customer gateway.
type CustomerGateway struct {
	CustomerGatewayID string `json:"customerGatewayId,omitempty"`
	State             string `json:"state,omitempty"`
	Type              string `json:"type,omitempty"`
	BgpAsn            string `json:"bgpAsn,omitempty"`
	IPAddress         string `json:"ipAddress,omitempty"`
}

// VpnConnection represents a VPN connection.
type VpnConnection struct {
	VpnConnectionID              string               `json:"vpnConnectionId,omitempty"`
	State                        string               `json:"state,omitempty"`
	CustomerGatewayID            string               `json:"customerGatewayId,omitempty"`
	VpnGatewayID                 string               `json:"vpnGatewayId,omitempty"`
	TransitGatewayID             string               `json:"transitGatewayId,omitempty"`
	Type                         string               `json:"type,omitempty"`
	Category                     string               `json:"category,omitempty"`
	CustomerGatewayConfiguration string               `json:"customerGatewayConfiguration,omitempty"`
	VgwTelemetry                 []VgwTelemetry       `json:"vgwTelemetry,omitempty"`
	Options                      VpnConnectionOptions `json:"options"`
}

// VpnTunnelOption represents the negotiated configuration of one of a VPN connection's two
// IPsec tunnels.
type VpnTunnelOption struct {
	OutsideIPAddress       string   `json:"outsideIpAddress,omitempty"`
	TunnelInsideCIDR       string   `json:"tunnelInsideCidr,omitempty"`
	PreSharedKey           string   `json:"preSharedKey,omitempty"`
	DPDTimeoutAction       string   `json:"dpdTimeoutAction,omitempty"`
	StartupAction          string   `json:"startupAction,omitempty"`
	CertificateARN         string   `json:"certificateArn,omitempty"`
	IKEVersions            []string `json:"ikeVersions,omitempty"`
	Phase1LifetimeSeconds  int32    `json:"phase1LifetimeSeconds,omitempty"`
	Phase2LifetimeSeconds  int32    `json:"phase2LifetimeSeconds,omitempty"`
	RekeyMarginTimeSeconds int32    `json:"rekeyMarginTimeSeconds,omitempty"`
	DPDTimeoutSeconds      int32    `json:"dpdTimeoutSeconds,omitempty"`
}

// VpnConnectionOptions holds the negotiated options of a VPN connection, including its two
// IPsec tunnels.
type VpnConnectionOptions struct {
	LocalIPv4NetworkCIDR  string            `json:"localIpv4NetworkCidr,omitempty"`
	RemoteIPv4NetworkCIDR string            `json:"remoteIpv4NetworkCidr,omitempty"`
	TunnelOptions         []VpnTunnelOption `json:"tunnelOptions,omitempty"`
	StaticRoutesOnly      bool              `json:"staticRoutesOnly,omitempty"`
}

// VgwTelemetry reports the status of one tunnel as observed from the VPN gateway side.
type VgwTelemetry struct {
	OutsideIPAddress   string `json:"outsideIpAddress,omitempty"`
	Status             string `json:"status,omitempty"`
	StatusMessage      string `json:"statusMessage,omitempty"`
	LastStatusChange   string `json:"lastStatusChange,omitempty"`
	CertificateARN     string `json:"certificateArn,omitempty"`
	AcceptedRouteCount int32  `json:"acceptedRouteCount,omitempty"`
}

// VpnTunnelOptionsModify holds the subset of tunnel fields that ModifyVpnTunnelOptions may
// change. Zero values (empty string / 0 / nil slice) mean "leave unchanged".
type VpnTunnelOptionsModify struct {
	TunnelInsideCIDR       string
	PreSharedKey           string
	DPDTimeoutAction       string
	StartupAction          string
	IKEVersions            []string
	Phase1LifetimeSeconds  int32
	Phase2LifetimeSeconds  int32
	RekeyMarginTimeSeconds int32
	DPDTimeoutSeconds      int32
}

// VpnConnectionDeviceType describes a customer gateway device vendor/platform/software
// combination that AWS publishes sample configurations for.
type VpnConnectionDeviceType struct {
	VpnConnectionDeviceTypeID string `json:"vpnConnectionDeviceTypeId,omitempty"`
	Vendor                    string `json:"vendor,omitempty"`
	Platform                  string `json:"platform,omitempty"`
	Software                  string `json:"software,omitempty"`
}

// VpnTunnelMaintenanceDetails reports pending AWS-initiated tunnel endpoint maintenance.
// This mock never schedules maintenance, so PendingMaintenance is always "false".
type VpnTunnelMaintenanceDetails struct {
	PendingMaintenance string `json:"pendingMaintenance,omitempty"`
}

// VpnTunnelReplacementStatus is the result of GetVpnTunnelReplacementStatus.
type VpnTunnelReplacementStatus struct {
	VpnConnectionID           string
	TransitGatewayID          string
	VpnGatewayID              string
	CustomerGatewayID         string
	VpnTunnelOutsideIPAddress string
	MaintenanceDetails        VpnTunnelMaintenanceDetails
}

// VpcEndpointServiceConfig represents a VPC endpoint service configuration.
type VpcEndpointServiceConfig struct {
	ServiceID               string   `json:"serviceId,omitempty"`
	ServiceName             string   `json:"serviceName,omitempty"`
	ServiceType             string   `json:"serviceType,omitempty"`
	PrivateDNSNameState     string   `json:"privateDnsNameState,omitempty"`
	PayerResponsibility     string   `json:"payerResponsibility,omitempty"`
	NetworkLoadBalancerARNs []string `json:"networkLoadBalancerArns,omitempty"`
	AcceptanceRequired      bool     `json:"acceptanceRequired,omitempty"`
}

// Ipam represents an AWS IPAM instance.
type Ipam struct {
	PrivateDefaultScopeID                 string   `json:"privateDefaultScopeId,omitempty"`
	DefaultResourceDiscoveryAssociationID string   `json:"defaultResourceDiscoveryAssociationId,omitempty"`
	State                                 string   `json:"state,omitempty"`
	Region                                string   `json:"region,omitempty"`
	OwnerID                               string   `json:"ownerId,omitempty"`
	Description                           string   `json:"description,omitempty"`
	PublicDefaultScopeID                  string   `json:"publicDefaultScopeId,omitempty"`
	Tier                                  string   `json:"tier,omitempty"`
	IpamARN                               string   `json:"ipamArn,omitempty"`
	IpamID                                string   `json:"ipamId,omitempty"`
	DefaultResourceDiscoveryID            string   `json:"defaultResourceDiscoveryId,omitempty"`
	OperatingRegions                      []string `json:"operatingRegions,omitempty"`
	ScopeCount                            int32    `json:"scopeCount,omitempty"`
	ResourceDiscoveryAssociationCount     int32    `json:"resourceDiscoveryAssociationCount,omitempty"`
}

// IpamOptions holds optional parameters accepted by CreateIpam and ModifyIpam.
type IpamOptions struct {
	Description      string
	Tier             string
	OperatingRegions []string
}

// IpamScope represents an IPAM scope: a private or public routing domain within an IPAM.
type IpamScope struct {
	IpamScopeID   string `json:"ipamScopeId,omitempty"`
	IpamScopeARN  string `json:"ipamScopeArn,omitempty"`
	IpamID        string `json:"ipamId,omitempty"`
	IpamScopeType string `json:"ipamScopeType,omitempty"`
	State         string `json:"state,omitempty"`
	Description   string `json:"description,omitempty"`
	PoolCount     int32  `json:"poolCount,omitempty"`
	IsDefault     bool   `json:"isDefault,omitempty"`
}

// IpamPool represents an IPAM pool.
type IpamPool struct {
	IpamPoolID                     string `json:"ipamPoolId,omitempty"`
	IpamPoolARN                    string `json:"ipamPoolArn,omitempty"`
	IpamID                         string `json:"ipamId,omitempty"`
	IpamScopeID                    string `json:"ipamScopeId,omitempty"`
	SourceIpamPoolID               string `json:"sourceIpamPoolId,omitempty"`
	State                          string `json:"state,omitempty"`
	Locale                         string `json:"locale,omitempty"`
	AddressFamily                  string `json:"addressFamily,omitempty"`
	Cidr                           string `json:"cidr,omitempty"`
	Description                    string `json:"description,omitempty"`
	AutoImport                     bool   `json:"autoImport,omitempty"`
	PubliclyAdvertisable           bool   `json:"publiclyAdvertisable,omitempty"`
	AllocationMinNetmaskLength     int32  `json:"allocationMinNetmaskLength,omitempty"`
	AllocationMaxNetmaskLength     int32  `json:"allocationMaxNetmaskLength,omitempty"`
	AllocationDefaultNetmaskLength int32  `json:"allocationDefaultNetmaskLength,omitempty"`
}

// IpamPoolOptions holds optional parameters accepted by CreateIpamPool and ModifyIpamPool.
type IpamPoolOptions struct {
	IpamScopeID                    string
	Description                    string
	AutoImport                     bool
	PubliclyAdvertisable           bool
	AllocationMinNetmaskLength     int32
	AllocationMaxNetmaskLength     int32
	AllocationDefaultNetmaskLength int32
}

// IpamPoolCidr represents a CIDR range provisioned to an IPAM pool via ProvisionIpamPoolCidr.
type IpamPoolCidr struct {
	Cidr  string `json:"cidr,omitempty"`
	State string `json:"state,omitempty"`
}

// IpamPoolAllocation represents an allocated CIDR from an IPAM pool.
type IpamPoolAllocation struct {
	IpamPoolAllocationID string `json:"ipamPoolAllocationId,omitempty"`
	IpamPoolID           string `json:"ipamPoolId,omitempty"`
	Cidr                 string `json:"cidr,omitempty"`
	Description          string `json:"description,omitempty"`
	ResourceType         string `json:"resourceType,omitempty"`
	ResourceID           string `json:"resourceId,omitempty"`
	ResourceOwner        string `json:"resourceOwner,omitempty"`
	// ResourceRegion is the AWS region of the allocated resource, populated
	// from the backend's own region at allocation time.
	ResourceRegion string `json:"resourceRegion,omitempty"`
}

// IpamAllocationOptions holds optional parameters accepted by AllocateIpamPoolCidr.
type IpamAllocationOptions struct {
	Description   string
	ResourceType  string
	ResourceID    string
	ResourceOwner string
}

// IpamResourceDiscovery represents an IPAM resource discovery, which scans a set of operating
// regions for resources whose CIDRs should be tracked by an IPAM.
type IpamResourceDiscovery struct {
	IpamResourceDiscoveryID  string   `json:"ipamResourceDiscoveryId,omitempty"`
	IpamResourceDiscoveryARN string   `json:"ipamResourceDiscoveryArn,omitempty"`
	OwnerID                  string   `json:"ownerId,omitempty"`
	Region                   string   `json:"ipamResourceDiscoveryRegion,omitempty"`
	State                    string   `json:"state,omitempty"`
	Description              string   `json:"description,omitempty"`
	OperatingRegions         []string `json:"operatingRegions,omitempty"`
	IsDefault                bool     `json:"isDefault,omitempty"`
}

// IpamResourceDiscoveryAssociation represents the association between an IPAM and a resource
// discovery (every IPAM has at least its own default resource discovery associated).
type IpamResourceDiscoveryAssociation struct {
	IpamResourceDiscoveryAssociationID  string `json:"ipamResourceDiscoveryAssociationId,omitempty"`
	IpamResourceDiscoveryAssociationARN string `json:"ipamResourceDiscoveryAssociationArn,omitempty"`
	IpamID                              string `json:"ipamId,omitempty"`
	IpamARN                             string `json:"ipamArn,omitempty"`
	IpamRegion                          string `json:"ipamRegion,omitempty"`
	IpamResourceDiscoveryID             string `json:"ipamResourceDiscoveryId,omitempty"`
	OwnerID                             string `json:"ownerId,omitempty"`
	ResourceDiscoveryStatus             string `json:"resourceDiscoveryStatus,omitempty"`
	State                               string `json:"state,omitempty"`
	IsDefault                           bool   `json:"isDefault,omitempty"`
}

// ---- Reset helpers ----

// resetAdvancedNetworkingMapsLocked re-initialises all advanced-networking maps.
// Must be called with b.mu held.
func (b *InMemoryBackend) resetAdvancedNetworkingMapsLocked() {
	b.ipamPoolCidrs = make(map[string][]*IpamPoolCidr)
	b.spotFleetHistory = make(map[string][]SpotFleetHistoryRecord)
}
