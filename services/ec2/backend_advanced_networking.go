package ec2

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
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
	IpamResourceDiscoveryID  string `json:"ipamResourceDiscoveryId,omitempty"`
	IpamResourceDiscoveryARN string `json:"ipamResourceDiscoveryArn,omitempty"`
	OwnerID                  string `json:"ownerId,omitempty"`
	State                    string `json:"state,omitempty"`
	Description              string `json:"description,omitempty"`
	IsDefault                bool   `json:"isDefault,omitempty"`
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
	ResourceDiscoveryStatus             string `json:"resourceDiscoveryStatus,omitempty"`
	State                               string `json:"state,omitempty"`
	IsDefault                           bool   `json:"isDefault,omitempty"`
}

// ---- Reset helpers ----

// resetAdvancedNetworkingMapsLocked re-initialises all advanced-networking maps.
// Must be called with b.mu held.
func (b *InMemoryBackend) resetAdvancedNetworkingMapsLocked() {
	b.vpnGateways = make(map[string]*VpnGateway)
	b.customerGateways = make(map[string]*CustomerGateway)
	b.vpnConnections = make(map[string]*VpnConnection)
	b.vpcEndpointServiceConfigs = make(map[string]*VpcEndpointServiceConfig)
	b.ipams = make(map[string]*Ipam)
	b.ipamScopes = make(map[string]*IpamScope)
	b.ipamPools = make(map[string]*IpamPool)
	b.ipamPoolCidrs = make(map[string][]*IpamPoolCidr)
	b.ipamPoolAllocations = make(map[string]*IpamPoolAllocation)
	b.ipamResourceDiscoveries = make(map[string]*IpamResourceDiscovery)
	b.ipamResourceDiscoveryAssocs = make(map[string]*IpamResourceDiscoveryAssociation)
	b.spotFleets = make(map[string]*SpotFleetRequest)
	b.spotFleetHistory = make(map[string][]SpotFleetHistoryRecord)
}

// ---- VPN Gateways ----

// CreateVpnGateway creates a new virtual private gateway.
func (b *InMemoryBackend) CreateVpnGateway(gatewayType string) (*VpnGateway, error) {
	if gatewayType == "" {
		gatewayType = vpnTypeIPSec
	}

	b.mu.Lock("CreateVpnGateway")
	defer b.mu.Unlock()

	vgw := &VpnGateway{
		VpnGatewayID: "vgw-" + uuid.New().String()[:8],
		State:        stateAvailable,
		Type:         gatewayType,
	}
	b.vpnGateways[vgw.VpnGatewayID] = vgw

	cp := *vgw

	return &cp, nil
}

// DescribeVpnGateways returns virtual private gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpnGateways(ids []string) []*VpnGateway {
	b.mu.RLock("DescribeVpnGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*VpnGateway, 0, len(b.vpnGateways))

	for _, vgw := range b.vpnGateways {
		if len(idSet) > 0 && !idSet[vgw.VpnGatewayID] {
			continue
		}

		cp := *vgw
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].VpnGatewayID < out[j].VpnGatewayID
	})

	return out
}

// DeleteVpnGateway removes a VPN gateway.
func (b *InMemoryBackend) DeleteVpnGateway(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VpnGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpnGateway")
	defer b.mu.Unlock()

	if _, ok := b.vpnGateways[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, id)
	}

	delete(b.vpnGateways, id)

	return nil
}

// AttachVpnGateway attaches a VPN gateway to a VPC.
func (b *InMemoryBackend) AttachVpnGateway(vgwID, vpcID string) error {
	if vgwID == "" {
		return fmt.Errorf("%w: VpnGatewayId is required", ErrInvalidParameter)
	}

	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AttachVpnGateway")
	defer b.mu.Unlock()

	vgw, ok := b.vpnGateways[vgwID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, vgwID)
	}

	if _, exists := b.vpcs[vpcID]; !exists {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	vgw.AttachedVPCID = vpcID
	vgw.AttachmentState = attachmentStateAttached

	return nil
}

// DetachVpnGateway detaches a VPN gateway from a VPC.
func (b *InMemoryBackend) DetachVpnGateway(vgwID, vpcID string) error {
	if vgwID == "" {
		return fmt.Errorf("%w: VpnGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DetachVpnGateway")
	defer b.mu.Unlock()

	vgw, ok := b.vpnGateways[vgwID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, vgwID)
	}

	_ = vpcID
	vgw.AttachedVPCID = ""
	vgw.AttachmentState = attachmentStateDetached

	return nil
}

// ---- Customer Gateways ----

// CreateCustomerGateway creates a new customer gateway.
func (b *InMemoryBackend) CreateCustomerGateway(
	gatewayType, ipAddress, bgpAsn string,
) (*CustomerGateway, error) {
	if ipAddress == "" {
		return nil, fmt.Errorf("%w: IpAddress is required", ErrInvalidParameter)
	}

	if gatewayType == "" {
		gatewayType = vpnTypeIPSec
	}

	if bgpAsn == "" {
		bgpAsn = "65000"
	}

	b.mu.Lock("CreateCustomerGateway")
	defer b.mu.Unlock()

	cgw := &CustomerGateway{
		CustomerGatewayID: "cgw-" + uuid.New().String()[:8],
		State:             stateAvailable,
		Type:              gatewayType,
		BgpAsn:            bgpAsn,
		IPAddress:         ipAddress,
	}
	b.customerGateways[cgw.CustomerGatewayID] = cgw

	cp := *cgw

	return &cp, nil
}

// DescribeCustomerGateways returns customer gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeCustomerGateways(ids []string) []*CustomerGateway {
	b.mu.RLock("DescribeCustomerGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*CustomerGateway, 0, len(b.customerGateways))

	for _, cgw := range b.customerGateways {
		if len(idSet) > 0 && !idSet[cgw.CustomerGatewayID] {
			continue
		}

		cp := *cgw
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CustomerGatewayID < out[j].CustomerGatewayID
	})

	return out
}

// DeleteCustomerGateway removes a customer gateway.
func (b *InMemoryBackend) DeleteCustomerGateway(id string) error {
	if id == "" {
		return fmt.Errorf("%w: CustomerGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCustomerGateway")
	defer b.mu.Unlock()

	if _, ok := b.customerGateways[id]; !ok {
		return fmt.Errorf("%w: %s", ErrCustomerGatewayNotFound, id)
	}

	delete(b.customerGateways, id)

	return nil
}

// ---- VPN Connections ----

// CreateVpnConnection creates a new VPN connection between a customer gateway and VPN gateway.
func (b *InMemoryBackend) CreateVpnConnection(
	connType, customerGatewayID, vpnGatewayID string,
) (*VpnConnection, error) {
	if customerGatewayID == "" {
		return nil, fmt.Errorf("%w: CustomerGatewayId is required", ErrInvalidParameter)
	}

	if vpnGatewayID == "" {
		return nil, fmt.Errorf("%w: VpnGatewayId is required", ErrInvalidParameter)
	}

	if connType == "" {
		connType = vpnTypeIPSec
	}

	b.mu.Lock("CreateVpnConnection")
	defer b.mu.Unlock()

	if _, ok := b.customerGateways[customerGatewayID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrCustomerGatewayNotFound, customerGatewayID)
	}

	if _, ok := b.vpnGateways[vpnGatewayID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, vpnGatewayID)
	}

	conn := &VpnConnection{
		VpnConnectionID:   "vpn-" + uuid.New().String()[:8],
		State:             stateAvailable,
		CustomerGatewayID: customerGatewayID,
		VpnGatewayID:      vpnGatewayID,
		Type:              connType,
		Category:          "VPN",
	}
	conn.Options = VpnConnectionOptions{TunnelOptions: generateVpnTunnels(len(b.vpnConnections))}
	conn.VgwTelemetry = vgwTelemetryFromTunnels(conn.Options.TunnelOptions)
	conn.CustomerGatewayConfiguration = buildCustomerGatewayConfiguration(conn)

	b.vpnConnections[conn.VpnConnectionID] = conn

	return copyVpnConnection(conn), nil
}

// DescribeVpnConnections returns VPN connections, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpnConnections(ids []string) []*VpnConnection {
	b.mu.RLock("DescribeVpnConnections")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*VpnConnection, 0, len(b.vpnConnections))

	for _, conn := range b.vpnConnections {
		if len(idSet) > 0 && !idSet[conn.VpnConnectionID] {
			continue
		}

		out = append(out, copyVpnConnection(conn))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].VpnConnectionID < out[j].VpnConnectionID
	})

	return out
}

// DeleteVpnConnection removes a VPN connection along with any static routes registered against it.
func (b *InMemoryBackend) DeleteVpnConnection(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpnConnection")
	defer b.mu.Unlock()

	if _, ok := b.vpnConnections[id]; !ok {
		return fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, id)
	}

	delete(b.vpnConnections, id)

	prefix := id + ":"
	for key := range b.vpnConnectionRoutes {
		if strings.HasPrefix(key, prefix) {
			delete(b.vpnConnectionRoutes, key)
		}
	}

	return nil
}

// GetVpnConnectionRoutes returns the static routes registered against a VPN connection.
func (b *InMemoryBackend) GetVpnConnectionRoutes(vpnConnectionID string) []*VpnConnectionRoute {
	b.mu.RLock("GetVpnConnectionRoutes")
	defer b.mu.RUnlock()

	prefix := vpnConnectionID + ":"
	out := make([]*VpnConnectionRoute, 0)

	for key, route := range b.vpnConnectionRoutes {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		cp := *route
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DestinationCIDR < out[j].DestinationCIDR
	})

	return out
}

// copyVpnConnection returns a deep copy of a VPN connection, including its slice-valued fields,
// so callers cannot mutate backend state through the returned pointer.
func copyVpnConnection(conn *VpnConnection) *VpnConnection {
	cp := *conn

	cp.Options.TunnelOptions = make([]VpnTunnelOption, len(conn.Options.TunnelOptions))
	for i, t := range conn.Options.TunnelOptions {
		tc := t
		tc.IKEVersions = append([]string(nil), t.IKEVersions...)
		cp.Options.TunnelOptions[i] = tc
	}

	cp.VgwTelemetry = append([]VgwTelemetry(nil), conn.VgwTelemetry...)

	return &cp
}

// indexOfVpnTunnel returns the index of the tunnel matching outsideIPAddress, or -1 if none matches.
func indexOfVpnTunnel(tunnels []VpnTunnelOption, outsideIPAddress string) int {
	for i, t := range tunnels {
		if t.OutsideIPAddress == outsideIPAddress {
			return i
		}
	}

	return -1
}

// generateVpnTunnels synthesizes the two IPsec tunnels AWS always provisions for a new VPN
// connection. connIndex (the number of VPN connections that already exist) is used to vary the
// generated addressing across connections so tunnels don't collide.
func generateVpnTunnels(connIndex int) []VpnTunnelOption {
	octet1 := connIndex%vpnTunnelOctetRange + 1
	octet2 := (connIndex+1)%vpnTunnelOctetRange + 1
	insideBlock := (connIndex % vpnTunnelInsideCIDRRange) * vpnTunnelInsideCIDRStep

	return []VpnTunnelOption{
		newVpnTunnel(vpnTunnelOutsideIPBase1+strconv.Itoa(octet1), insideBlock),
		newVpnTunnel(vpnTunnelOutsideIPBase2+strconv.Itoa(octet2), insideBlock+vpnTunnelInsideCIDRStep),
	}
}

// newVpnTunnel builds a single tunnel's default configuration.
func newVpnTunnel(outsideIP string, insideBlock int) VpnTunnelOption {
	return VpnTunnelOption{
		OutsideIPAddress:       outsideIP,
		TunnelInsideCIDR:       fmt.Sprintf("169.254.%d.0/30", insideBlock),
		PreSharedKey:           uuid.New().String()[:vpnPreSharedKeyLength],
		Phase1LifetimeSeconds:  vpnPhase1LifetimeSeconds,
		Phase2LifetimeSeconds:  vpnPhase2LifetimeSeconds,
		RekeyMarginTimeSeconds: vpnRekeyMarginTimeSeconds,
		DPDTimeoutSeconds:      vpnDPDTimeoutSeconds,
		DPDTimeoutAction:       "clear",
		StartupAction:          "add",
		IKEVersions:            []string{"ikev1", "ikev2"},
	}
}

// vgwTelemetryFromTunnels builds the initial per-tunnel telemetry for a newly-created VPN
// connection. Tunnels start DOWN since no real customer gateway peer ever connects.
func vgwTelemetryFromTunnels(tunnels []VpnTunnelOption) []VgwTelemetry {
	now := time.Now().UTC().Format(time.RFC3339)
	out := make([]VgwTelemetry, 0, len(tunnels))

	for _, t := range tunnels {
		out = append(out, VgwTelemetry{
			OutsideIPAddress:   t.OutsideIPAddress,
			Status:             "DOWN",
			StatusMessage:      "IPSEC IS DOWN",
			AcceptedRouteCount: 0,
			LastStatusChange:   now,
		})
	}

	return out
}

// buildCustomerGatewayConfiguration renders the vendor-neutral sample XML configuration blob
// AWS returns in the customerGatewayConfiguration field of a VPN connection.
func buildCustomerGatewayConfiguration(conn *VpnConnection) string {
	var sb strings.Builder

	sb.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	fmt.Fprintf(&sb, "<vpn_connection id=\"%s\">\n", conn.VpnConnectionID)
	fmt.Fprintf(&sb, "  <customer_gateway_id>%s</customer_gateway_id>\n", conn.CustomerGatewayID)

	if conn.VpnGatewayID != "" {
		fmt.Fprintf(&sb, "  <vpn_gateway_id>%s</vpn_gateway_id>\n", conn.VpnGatewayID)
	}

	if conn.TransitGatewayID != "" {
		fmt.Fprintf(&sb, "  <transit_gateway_id>%s</transit_gateway_id>\n", conn.TransitGatewayID)
	}

	fmt.Fprintf(&sb, "  <vpn_connection_type>%s</vpn_connection_type>\n", conn.Type)

	for i, t := range conn.Options.TunnelOptions {
		fmt.Fprintf(&sb, "  <ipsec_tunnel index=\"%d\">\n", i+1)
		fmt.Fprintf(
			&sb,
			"    <customer_gateway><tunnel_outside_address><ip_address>%s</ip_address>"+
				"</tunnel_outside_address></customer_gateway>\n",
			t.OutsideIPAddress,
		)
		fmt.Fprintf(&sb, "    <ike><pre_shared_key>%s</pre_shared_key></ike>\n", t.PreSharedKey)
		sb.WriteString("  </ipsec_tunnel>\n")
	}

	sb.WriteString("</vpn_connection>\n")

	return sb.String()
}

// ModifyVpnConnectionOptions updates the negotiated local/remote IPv4 network CIDRs and the
// static-routes-only flag of a VPN connection. Empty strings and a nil staticRoutesOnly leave
// the corresponding field unchanged.
func (b *InMemoryBackend) ModifyVpnConnectionOptions(
	vpnConnectionID, localIPv4CIDR, remoteIPv4CIDR string, staticRoutesOnly *bool,
) (*VpnConnection, error) {
	if vpnConnectionID == "" {
		return nil, fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpnConnectionOptions")
	defer b.mu.Unlock()

	conn, ok := b.vpnConnections[vpnConnectionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	if localIPv4CIDR != "" {
		conn.Options.LocalIPv4NetworkCIDR = localIPv4CIDR
	}

	if remoteIPv4CIDR != "" {
		conn.Options.RemoteIPv4NetworkCIDR = remoteIPv4CIDR
	}

	if staticRoutesOnly != nil {
		conn.Options.StaticRoutesOnly = *staticRoutesOnly
	}

	return copyVpnConnection(conn), nil
}

// ModifyVpnTunnelOptions updates the configuration of a single tunnel of a VPN connection,
// identified by its outside IP address. Zero-valued fields in opts leave the corresponding
// tunnel field unchanged.
func (b *InMemoryBackend) ModifyVpnTunnelOptions(
	vpnConnectionID, outsideIPAddress string, opts VpnTunnelOptionsModify,
) (*VpnConnection, error) {
	if vpnConnectionID == "" || outsideIPAddress == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and VpnTunnelOutsideIpAddress are required", ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyVpnTunnelOptions")
	defer b.mu.Unlock()

	conn, ok := b.vpnConnections[vpnConnectionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	idx := indexOfVpnTunnel(conn.Options.TunnelOptions, outsideIPAddress)
	if idx < 0 {
		return nil, fmt.Errorf(
			"%w: no tunnel with outside IP address %s on %s",
			ErrVpnTunnelNotFound, outsideIPAddress, vpnConnectionID,
		)
	}

	applyVpnTunnelOptionsModify(&conn.Options.TunnelOptions[idx], opts)

	return copyVpnConnection(conn), nil
}

// applyVpnTunnelOptionsModify merges non-zero fields of opts onto an existing tunnel.
func applyVpnTunnelOptionsModify(t *VpnTunnelOption, opts VpnTunnelOptionsModify) {
	if opts.TunnelInsideCIDR != "" {
		t.TunnelInsideCIDR = opts.TunnelInsideCIDR
	}

	if opts.PreSharedKey != "" {
		t.PreSharedKey = opts.PreSharedKey
	}

	if opts.Phase1LifetimeSeconds > 0 {
		t.Phase1LifetimeSeconds = opts.Phase1LifetimeSeconds
	}

	if opts.Phase2LifetimeSeconds > 0 {
		t.Phase2LifetimeSeconds = opts.Phase2LifetimeSeconds
	}

	if opts.RekeyMarginTimeSeconds > 0 {
		t.RekeyMarginTimeSeconds = opts.RekeyMarginTimeSeconds
	}

	if opts.DPDTimeoutSeconds > 0 {
		t.DPDTimeoutSeconds = opts.DPDTimeoutSeconds
	}

	if opts.DPDTimeoutAction != "" {
		t.DPDTimeoutAction = opts.DPDTimeoutAction
	}

	if opts.StartupAction != "" {
		t.StartupAction = opts.StartupAction
	}

	if len(opts.IKEVersions) > 0 {
		t.IKEVersions = append([]string(nil), opts.IKEVersions...)
	}
}

// ModifyVpnTunnelCertificate provisions a private-certificate-based authentication certificate
// for a single tunnel of a VPN connection, identified by its outside IP address.
func (b *InMemoryBackend) ModifyVpnTunnelCertificate(
	vpnConnectionID, outsideIPAddress string,
) (*VpnConnection, error) {
	if vpnConnectionID == "" || outsideIPAddress == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and VpnTunnelOutsideIpAddress are required", ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyVpnTunnelCertificate")
	defer b.mu.Unlock()

	conn, ok := b.vpnConnections[vpnConnectionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	idx := indexOfVpnTunnel(conn.Options.TunnelOptions, outsideIPAddress)
	if idx < 0 {
		return nil, fmt.Errorf(
			"%w: no tunnel with outside IP address %s on %s",
			ErrVpnTunnelNotFound, outsideIPAddress, vpnConnectionID,
		)
	}

	certARN := fmt.Sprintf("arn:aws:acm:%s:%s:certificate/%s", b.Region, b.AccountID, uuid.New().String())
	conn.Options.TunnelOptions[idx].CertificateARN = certARN

	for i := range conn.VgwTelemetry {
		if conn.VgwTelemetry[i].OutsideIPAddress == outsideIPAddress {
			conn.VgwTelemetry[i].CertificateARN = certARN
		}
	}

	return copyVpnConnection(conn), nil
}

// GetVpnConnectionDeviceTypes returns the static catalog of customer gateway device
// vendor/platform/software combinations AWS publishes sample configurations for.
func (b *InMemoryBackend) GetVpnConnectionDeviceTypes() []VpnConnectionDeviceType {
	return []VpnConnectionDeviceType{
		{
			VpnConnectionDeviceTypeID: "cisco-systems-inc-cisco-ios-15",
			Vendor:                    "Cisco Systems, Inc.",
			Platform:                  "Cisco ISR Series Router",
			Software:                  "IOS 12.4",
		},
		{
			VpnConnectionDeviceTypeID: "cisco-systems-inc-cisco-asa-9",
			Vendor:                    "Cisco Systems, Inc.",
			Platform:                  "Cisco ASA Series Router",
			Software:                  "ASA 9.x",
		},
		{
			VpnConnectionDeviceTypeID: "juniper-networks-inc-junos-srx-12",
			Vendor:                    "Juniper Networks, Inc.",
			Platform:                  "J-Series Routers",
			Software:                  "JunOS 12.x",
		},
		{
			VpnConnectionDeviceTypeID: "fortinet-inc-fortigate-40-plus-series-5",
			Vendor:                    "Fortinet, Inc.",
			Platform:                  "Fortigate 40+ Series",
			Software:                  "FortiOS 5.x",
		},
		{
			VpnConnectionDeviceTypeID: "palo-alto-networks-inc-pan-os-8",
			Vendor:                    "Palo Alto Networks, Inc.",
			Platform:                  "PA Series Router",
			Software:                  "PAN-OS 8.x",
		},
		{
			VpnConnectionDeviceTypeID: "checkpoint-r80-10",
			Vendor:                    "Check Point Software Technologies Ltd.",
			Platform:                  "Security Gateway",
			Software:                  "R80.10",
		},
		{
			VpnConnectionDeviceTypeID: "generic-vendor-x-generic-platform-generic-version",
			Vendor:                    "Generic",
			Platform:                  "Generic",
			Software:                  "Vendor Agnostic",
		},
	}
}

// GetVpnConnectionDeviceSampleConfiguration generates a sample vendor configuration for a VPN
// connection's tunnels, targeting the given device type and IKE version.
func (b *InMemoryBackend) GetVpnConnectionDeviceSampleConfiguration(
	vpnConnectionID, deviceTypeID, ikeVersion string,
) (string, error) {
	if vpnConnectionID == "" {
		return "", fmt.Errorf("%w: VpnConnectionId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetVpnConnectionDeviceSampleConfiguration")
	defer b.mu.RUnlock()

	conn, ok := b.vpnConnections[vpnConnectionID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	if deviceTypeID == "" {
		deviceTypeID = "generic-vendor-x-generic-platform-generic-version"
	}

	if ikeVersion == "" {
		ikeVersion = "ikev2"
	}

	var sb strings.Builder

	fmt.Fprintf(&sb, "! Sample configuration for device type %s (IKE %s)\n", deviceTypeID, ikeVersion)
	fmt.Fprintf(&sb, "! Generated for VPN connection %s\n", conn.VpnConnectionID)

	for i, t := range conn.Options.TunnelOptions {
		fmt.Fprintf(&sb, "\n! --- Tunnel %d ---\n", i+1)
		fmt.Fprintf(&sb, "crypto vpn tunnel outside-address %s\n", t.OutsideIPAddress)
		fmt.Fprintf(&sb, "crypto vpn tunnel inside-cidr %s\n", t.TunnelInsideCIDR)
		fmt.Fprintf(&sb, "crypto vpn ike pre-shared-key %s\n", t.PreSharedKey)
		fmt.Fprintf(&sb, "crypto vpn ike lifetime %d\n", t.Phase1LifetimeSeconds)
		fmt.Fprintf(&sb, "crypto vpn ipsec lifetime %d\n", t.Phase2LifetimeSeconds)
	}

	return sb.String(), nil
}

// GetVpnTunnelReplacementStatus reports whether AWS-initiated tunnel endpoint maintenance is
// pending for a single tunnel of a VPN connection. This mock never schedules maintenance.
func (b *InMemoryBackend) GetVpnTunnelReplacementStatus(
	vpnConnectionID, outsideIPAddress string,
) (*VpnTunnelReplacementStatus, error) {
	if vpnConnectionID == "" || outsideIPAddress == "" {
		return nil, fmt.Errorf(
			"%w: VpnConnectionId and VpnTunnelOutsideIpAddress are required", ErrInvalidParameter,
		)
	}

	b.mu.RLock("GetVpnTunnelReplacementStatus")
	defer b.mu.RUnlock()

	conn, ok := b.vpnConnections[vpnConnectionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnConnectionID)
	}

	if indexOfVpnTunnel(conn.Options.TunnelOptions, outsideIPAddress) < 0 {
		return nil, fmt.Errorf(
			"%w: no tunnel with outside IP address %s on %s",
			ErrVpnTunnelNotFound, outsideIPAddress, vpnConnectionID,
		)
	}

	return &VpnTunnelReplacementStatus{
		VpnConnectionID:           conn.VpnConnectionID,
		TransitGatewayID:          conn.TransitGatewayID,
		VpnGatewayID:              conn.VpnGatewayID,
		CustomerGatewayID:         conn.CustomerGatewayID,
		VpnTunnelOutsideIPAddress: outsideIPAddress,
		MaintenanceDetails:        VpnTunnelMaintenanceDetails{PendingMaintenance: "false"},
	}, nil
}

// ---- VPC Peering: Reject ----

// RejectVpcPeeringConnection rejects a pending VPC peering connection.
func (b *InMemoryBackend) RejectVpcPeeringConnection(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VpcPeeringConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectVpcPeeringConnection")
	defer b.mu.Unlock()

	pc, ok := b.vpcPeeringConnections[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpcPeeringConnectionNotFound, id)
	}

	pc.State = "rejected"

	return nil
}

// ---- VPC Endpoint Service Configurations ----

// CreateVpcEndpointServiceConfiguration creates a new VPC endpoint service configuration.
func (b *InMemoryBackend) CreateVpcEndpointServiceConfiguration(
	acceptanceRequired bool, nlbARNs []string,
) (*VpcEndpointServiceConfig, error) {
	b.mu.Lock("CreateVpcEndpointServiceConfiguration")
	defer b.mu.Unlock()

	svcID := "vpce-svc-" + uuid.New().String()[:8]
	svcName := "com.amazonaws.vpce." + b.Region + "." + svcID

	cfg := &VpcEndpointServiceConfig{
		ServiceID:               svcID,
		ServiceName:             svcName,
		ServiceType:             "Interface",
		AcceptanceRequired:      acceptanceRequired,
		NetworkLoadBalancerARNs: nlbARNs,
	}
	b.vpcEndpointServiceConfigs[svcID] = cfg

	cp := *cfg
	cp.NetworkLoadBalancerARNs = append([]string(nil), cfg.NetworkLoadBalancerARNs...)

	return &cp, nil
}

// DescribeVpcEndpointServiceConfigurations returns endpoint service configs, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpcEndpointServiceConfigurations(
	ids []string,
) []*VpcEndpointServiceConfig {
	b.mu.RLock("DescribeVpcEndpointServiceConfigurations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*VpcEndpointServiceConfig, 0, len(b.vpcEndpointServiceConfigs))

	for _, cfg := range b.vpcEndpointServiceConfigs {
		if len(idSet) > 0 && !idSet[cfg.ServiceID] {
			continue
		}

		cp := *cfg
		cp.NetworkLoadBalancerARNs = append([]string(nil), cfg.NetworkLoadBalancerARNs...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ServiceID < out[j].ServiceID
	})

	return out
}

// DeleteVpcEndpointServiceConfigurations removes VPC endpoint service configurations by IDs.
func (b *InMemoryBackend) DeleteVpcEndpointServiceConfigurations(ids []string) error {
	b.mu.Lock("DeleteVpcEndpointServiceConfigurations")
	defer b.mu.Unlock()

	for _, id := range ids {
		if _, ok := b.vpcEndpointServiceConfigs[id]; !ok {
			return fmt.Errorf("%w: %s", ErrVpcEndpointServiceNotFound, id)
		}
	}

	for _, id := range ids {
		delete(b.vpcEndpointServiceConfigs, id)
	}

	return nil
}

// ModifyVpcEndpointServiceConfiguration updates acceptance required for a service config.
func (b *InMemoryBackend) ModifyVpcEndpointServiceConfiguration(
	id string,
	acceptanceRequired bool,
) error {
	if id == "" {
		return fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVpcEndpointServiceConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.vpcEndpointServiceConfigs[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpcEndpointServiceNotFound, id)
	}

	cfg.AcceptanceRequired = acceptanceRequired

	return nil
}

// StartVpcEndpointServicePrivateDNSVerification initiates (and, for this
// in-memory backend, immediately completes) private DNS name verification for
// a VPC endpoint service configuration.
func (b *InMemoryBackend) StartVpcEndpointServicePrivateDNSVerification(id string) error {
	if id == "" {
		return fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("StartVpcEndpointServicePrivateDnsVerification")
	defer b.mu.Unlock()

	cfg, ok := b.vpcEndpointServiceConfigs[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpcEndpointServiceNotFound, id)
	}

	cfg.PrivateDNSNameState = "verified"

	return nil
}

// ---- IPAM ----

// ipamOpts returns the first IpamOptions in opts, or a zero value if none was given.
func ipamOpts(opts []IpamOptions) IpamOptions {
	if len(opts) > 0 {
		return opts[0]
	}

	return IpamOptions{}
}

// CreateIpam creates a new IPAM instance, along with its default public/private scopes and
// default resource discovery, mirroring real AWS behavior.
func (b *InMemoryBackend) CreateIpam(opts ...IpamOptions) (*Ipam, error) {
	o := ipamOpts(opts)

	b.mu.Lock("CreateIpam")
	defer b.mu.Unlock()

	ipamID := "ipam-" + uuid.New().String()[:8]

	privScope := &IpamScope{
		IpamScopeID:   "ipam-scope-" + uuid.New().String()[:8],
		IpamScopeARN:  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-scope/",
		IpamID:        ipamID,
		IpamScopeType: ipamScopeTypePrivate,
		IsDefault:     true,
		State:         ipamStateCreateComplete,
	}
	privScope.IpamScopeARN += privScope.IpamScopeID
	b.ipamScopes[privScope.IpamScopeID] = privScope

	pubScope := &IpamScope{
		IpamScopeID:   "ipam-scope-" + uuid.New().String()[:8],
		IpamScopeARN:  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-scope/",
		IpamID:        ipamID,
		IpamScopeType: ipamScopeTypePublic,
		IsDefault:     true,
		State:         ipamStateCreateComplete,
	}
	pubScope.IpamScopeARN += pubScope.IpamScopeID
	b.ipamScopes[pubScope.IpamScopeID] = pubScope

	discovery := &IpamResourceDiscovery{
		IpamResourceDiscoveryID: "ipam-res-disco-" + uuid.New().String()[:8],
		OwnerID:                 b.AccountID,
		IsDefault:               true,
		State:                   ipamStateCreateComplete,
	}
	discovery.IpamResourceDiscoveryARN = "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
		":ipam-resource-discovery/" + discovery.IpamResourceDiscoveryID
	b.ipamResourceDiscoveries[discovery.IpamResourceDiscoveryID] = discovery

	assoc := &IpamResourceDiscoveryAssociation{
		IpamResourceDiscoveryAssociationID: "ipam-res-disco-assoc-" + uuid.New().String()[:8],
		IpamID:                             ipamID,
		IpamRegion:                         b.Region,
		IpamResourceDiscoveryID:            discovery.IpamResourceDiscoveryID,
		IsDefault:                          true,
		ResourceDiscoveryStatus:            ipamResourceDiscoveryAssocStatus,
		State:                              ipamStateCreateComplete,
	}
	assoc.IpamResourceDiscoveryAssociationARN = "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
		":ipam-resource-discovery-association/" + assoc.IpamResourceDiscoveryAssociationID
	b.ipamResourceDiscoveryAssocs[assoc.IpamResourceDiscoveryAssociationID] = assoc

	ipam := &Ipam{
		IpamID:                                ipamID,
		IpamARN:                               "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam/" + ipamID,
		State:                                 ipamStateCreateComplete,
		Region:                                b.Region,
		OwnerID:                               b.AccountID,
		Description:                           o.Description,
		OperatingRegions:                      append([]string(nil), o.OperatingRegions...),
		Tier:                                  o.Tier,
		PublicDefaultScopeID:                  pubScope.IpamScopeID,
		PrivateDefaultScopeID:                 privScope.IpamScopeID,
		ScopeCount:                            2, //nolint:mnd // AWS always creates exactly the 2 default scopes
		DefaultResourceDiscoveryID:            discovery.IpamResourceDiscoveryID,
		DefaultResourceDiscoveryAssociationID: assoc.IpamResourceDiscoveryAssociationID,
		ResourceDiscoveryAssociationCount:     1,
	}
	if ipam.Tier == "" {
		ipam.Tier = "advanced"
	}

	b.ipams[ipamID] = ipam

	cp := *ipam
	cp.OperatingRegions = append([]string(nil), ipam.OperatingRegions...)

	return &cp, nil
}

// DescribeIpams returns IPAM instances, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpams(ids []string) []*Ipam {
	b.mu.RLock("DescribeIpams")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*Ipam, 0, len(b.ipams))

	for _, ipam := range b.ipams {
		if len(idSet) > 0 && !idSet[ipam.IpamID] {
			continue
		}

		cp := *ipam
		cp.OperatingRegions = append([]string(nil), ipam.OperatingRegions...)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamID < out[j].IpamID
	})

	return out
}

// ModifyIpam updates an IPAM's description, operating regions, or tier.
func (b *InMemoryBackend) ModifyIpam(id string, opts IpamOptions) (*Ipam, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpam")
	defer b.mu.Unlock()

	ipam, ok := b.ipams[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, id)
	}

	if opts.Description != "" {
		ipam.Description = opts.Description
	}

	if len(opts.OperatingRegions) > 0 {
		ipam.OperatingRegions = append([]string(nil), opts.OperatingRegions...)
	}

	if opts.Tier != "" {
		ipam.Tier = opts.Tier
	}

	ipam.State = ipamStateModifyComplete

	cp := *ipam
	cp.OperatingRegions = append([]string(nil), ipam.OperatingRegions...)

	return &cp, nil
}

// DeleteIpam removes an IPAM instance and its default scopes/resource discovery.
func (b *InMemoryBackend) DeleteIpam(id string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpam")
	defer b.mu.Unlock()

	ipam, ok := b.ipams[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrIpamNotFound, id)
	}

	delete(b.ipamScopes, ipam.PublicDefaultScopeID)
	delete(b.ipamScopes, ipam.PrivateDefaultScopeID)
	delete(b.ipamResourceDiscoveries, ipam.DefaultResourceDiscoveryID)
	delete(b.ipamResourceDiscoveryAssocs, ipam.DefaultResourceDiscoveryAssociationID)
	delete(b.ipams, id)

	return nil
}

// ---- IPAM Scopes ----

// CreateIpamScope creates an additional (non-default) private IPAM scope.
func (b *InMemoryBackend) CreateIpamScope(ipamID, description string) (*IpamScope, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIpamScope")
	defer b.mu.Unlock()

	ipam, ok := b.ipams[ipamID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	scopeID := "ipam-scope-" + uuid.New().String()[:8]
	scope := &IpamScope{
		IpamScopeID:   scopeID,
		IpamScopeARN:  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-scope/" + scopeID,
		IpamID:        ipamID,
		IpamScopeType: ipamScopeTypePrivate,
		State:         ipamStateCreateComplete,
		Description:   description,
	}
	b.ipamScopes[scopeID] = scope
	ipam.ScopeCount++

	cp := *scope

	return &cp, nil
}

// DescribeIpamScopes returns IPAM scopes, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamScopes(ids []string) []*IpamScope {
	b.mu.RLock("DescribeIpamScopes")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamScope, 0, len(b.ipamScopes))

	for _, scope := range b.ipamScopes {
		if len(idSet) > 0 && !idSet[scope.IpamScopeID] {
			continue
		}

		cp := *scope
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamScopeID < out[j].IpamScopeID
	})

	return out
}

// ModifyIpamScope updates an IPAM scope's description.
func (b *InMemoryBackend) ModifyIpamScope(id, description string) (*IpamScope, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamScopeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamScope")
	defer b.mu.Unlock()

	scope, ok := b.ipamScopes[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamScopeNotFound, id)
	}

	scope.Description = description
	scope.State = ipamStateModifyComplete

	cp := *scope

	return &cp, nil
}

// DeleteIpamScope removes a non-default IPAM scope. Default scopes cannot be deleted.
func (b *InMemoryBackend) DeleteIpamScope(id string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamScopeId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamScope")
	defer b.mu.Unlock()

	scope, ok := b.ipamScopes[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrIpamScopeNotFound, id)
	}

	if scope.IsDefault {
		return fmt.Errorf("%w: default IPAM scopes cannot be deleted", ErrIpamScopeDefault)
	}

	if ipam, ipamOK := b.ipams[scope.IpamID]; ipamOK {
		ipam.ScopeCount--
	}

	delete(b.ipamScopes, id)

	return nil
}

// ---- IPAM Pools ----

// ipamPoolOpts returns the first IpamPoolOptions in opts, or a zero value if none was given.
func ipamPoolOpts(opts []IpamPoolOptions) IpamPoolOptions {
	if len(opts) > 0 {
		return opts[0]
	}

	return IpamPoolOptions{}
}

// CreateIpamPool creates a new IPAM pool under the given IPAM (resolved to its default scope
// for addressFamily). If cidr is non-empty it is immediately provisioned to the pool, matching
// the CreateIpamPool ProvisionedCidrs request parameter.
func (b *InMemoryBackend) CreateIpamPool(
	ipamID, addressFamily, locale, cidr string, opts ...IpamPoolOptions,
) (*IpamPool, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	if addressFamily == "" {
		addressFamily = "ipv4"
	}

	o := ipamPoolOpts(opts)

	b.mu.Lock("CreateIpamPool")
	defer b.mu.Unlock()

	ipam, ok := b.ipams[ipamID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	scopeID := o.IpamScopeID
	if scopeID == "" {
		if addressFamily == "ipv6" {
			scopeID = ipam.PublicDefaultScopeID
		} else {
			scopeID = ipam.PrivateDefaultScopeID
		}
	}

	poolID := "ipam-pool-" + uuid.New().String()[:8]
	pool := &IpamPool{
		IpamPoolID:                     poolID,
		IpamPoolARN:                    "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":ipam-pool/" + poolID,
		IpamID:                         ipamID,
		IpamScopeID:                    scopeID,
		State:                          ipamStateCreateComplete,
		Locale:                         locale,
		AddressFamily:                  addressFamily,
		Cidr:                           cidr,
		Description:                    o.Description,
		AutoImport:                     o.AutoImport,
		PubliclyAdvertisable:           o.PubliclyAdvertisable,
		AllocationMinNetmaskLength:     o.AllocationMinNetmaskLength,
		AllocationMaxNetmaskLength:     o.AllocationMaxNetmaskLength,
		AllocationDefaultNetmaskLength: o.AllocationDefaultNetmaskLength,
	}
	b.ipamPools[poolID] = pool

	if cidr != "" {
		b.ipamPoolCidrs[poolID] = []*IpamPoolCidr{{Cidr: cidr, State: ipamPoolCidrStateProvisioned}}
	}

	cp := *pool

	return &cp, nil
}

// DescribeIpamPools returns IPAM pools, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamPools(ids []string) []*IpamPool {
	b.mu.RLock("DescribeIpamPools")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamPool, 0, len(b.ipamPools))

	for _, pool := range b.ipamPools {
		if len(idSet) > 0 && !idSet[pool.IpamPoolID] {
			continue
		}

		cp := *pool
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPoolID < out[j].IpamPoolID
	})

	return out
}

// ModifyIpamPool updates mutable attributes of an IPAM pool.
func (b *InMemoryBackend) ModifyIpamPool(id string, opts IpamPoolOptions) (*IpamPool, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamPool")
	defer b.mu.Unlock()

	pool, ok := b.ipamPools[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, id)
	}

	if opts.Description != "" {
		pool.Description = opts.Description
	}

	pool.AutoImport = opts.AutoImport

	if opts.AllocationMinNetmaskLength > 0 {
		pool.AllocationMinNetmaskLength = opts.AllocationMinNetmaskLength
	}

	if opts.AllocationMaxNetmaskLength > 0 {
		pool.AllocationMaxNetmaskLength = opts.AllocationMaxNetmaskLength
	}

	if opts.AllocationDefaultNetmaskLength > 0 {
		pool.AllocationDefaultNetmaskLength = opts.AllocationDefaultNetmaskLength
	}

	pool.State = ipamStateModifyComplete

	cp := *pool

	return &cp, nil
}

// DeleteIpamPool removes an IPAM pool and its provisioned CIDRs.
func (b *InMemoryBackend) DeleteIpamPool(id string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamPool")
	defer b.mu.Unlock()

	if _, ok := b.ipamPools[id]; !ok {
		return fmt.Errorf("%w: %s", ErrIpamPoolNotFound, id)
	}

	delete(b.ipamPools, id)
	delete(b.ipamPoolCidrs, id)

	return nil
}

// ---- IPAM Pool CIDR provisioning ----

// ProvisionIpamPoolCidr adds a CIDR range to an IPAM pool's provisioned space.
func (b *InMemoryBackend) ProvisionIpamPoolCidr(poolID, cidr string) (*IpamPoolCidr, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("ProvisionIpamPoolCidr")
	defer b.mu.Unlock()

	if _, ok := b.ipamPools[poolID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	entry := &IpamPoolCidr{Cidr: cidr, State: ipamPoolCidrStateProvisioned}
	b.ipamPoolCidrs[poolID] = append(b.ipamPoolCidrs[poolID], entry)

	cp := *entry

	return &cp, nil
}

// DeprovisionIpamPoolCidr removes a previously provisioned CIDR range from an IPAM pool.
func (b *InMemoryBackend) DeprovisionIpamPoolCidr(poolID, cidr string) (*IpamPoolCidr, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeprovisionIpamPoolCidr")
	defer b.mu.Unlock()

	if _, ok := b.ipamPools[poolID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	cidrs := b.ipamPoolCidrs[poolID]

	for i, c := range cidrs {
		if c.Cidr == cidr {
			removed := *c
			b.ipamPoolCidrs[poolID] = append(cidrs[:i], cidrs[i+1:]...)

			return &removed, nil
		}
	}

	return nil, fmt.Errorf("%w: %s is not provisioned to pool %s", ErrIpamPoolCidrNotFound, cidr, poolID)
}

// GetIpamPoolCidrs returns the CIDR ranges provisioned to an IPAM pool.
func (b *InMemoryBackend) GetIpamPoolCidrs(poolID string) []*IpamPoolCidr {
	b.mu.RLock("GetIpamPoolCidrs")
	defer b.mu.RUnlock()

	cidrs := b.ipamPoolCidrs[poolID]
	out := make([]*IpamPoolCidr, 0, len(cidrs))

	for _, c := range cidrs {
		cp := *c
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Cidr < out[j].Cidr
	})

	return out
}

// autoCIDRLocked generates a unique CIDR from the given poolCidr and netmask length.
// Must be called with b.mu held.
func (b *InMemoryBackend) autoCIDRLocked(poolCidr string, netmaskLength int) (string, error) {
	const defaultPoolCIDR = "10.0.0.0/8"
	const defaultNetmask = 24

	if poolCidr == "" {
		poolCidr = defaultPoolCIDR
	}

	if netmaskLength <= 0 {
		netmaskLength = defaultNetmask
	}

	_, network, err := net.ParseCIDR(poolCidr)
	if err != nil {
		return "", fmt.Errorf("%w: invalid pool CIDR %s", ErrInvalidParameter, poolCidr)
	}

	existingCount := len(b.ipamPoolAllocations)

	ip := network.IP.To4()
	if ip == nil {
		ip = make(net.IP, ipv4Len)
	}

	ipInt := uint32(
		ip[0],
	)<<octet3Shift | uint32(
		ip[1],
	)<<octet2Shift | uint32(
		ip[2],
	)<<octetMask | uint32(
		ip[3],
	)
	shift := max(ipv4Shift-netmaskLength, 0)

	//nolint:gosec // existingCount is small; integer overflow is acceptable in mock context
	ipInt += uint32(existingCount) << uint(shift)
	//nolint:gosec // byte truncation is intentional: each octet is extracted from a 32-bit IP integer
	shiftedIP := net.IP{
		byte(ipInt >> octet3Shift),
		byte(ipInt >> octet2Shift),
		byte(ipInt >> octetMask),
		byte(ipInt),
	}

	return fmt.Sprintf("%s/%d", shiftedIP.String(), netmaskLength), nil
}

// ---- IPAM Pool Allocations ----

// ipamAllocOpts returns the first IpamAllocationOptions in opts, or a zero value if none given.
func ipamAllocOpts(opts []IpamAllocationOptions) IpamAllocationOptions {
	if len(opts) > 0 {
		return opts[0]
	}

	return IpamAllocationOptions{}
}

// AllocateIpamPoolCidr allocates a CIDR from an IPAM pool.
// If cidr is empty, one is auto-generated from the pool's network space.
func (b *InMemoryBackend) AllocateIpamPoolCidr(
	poolID, cidr string,
	netmaskLength int,
	opts ...IpamAllocationOptions,
) (*IpamPoolAllocation, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	o := ipamAllocOpts(opts)

	b.mu.Lock("AllocateIpamPoolCidr")
	defer b.mu.Unlock()

	pool, ok := b.ipamPools[poolID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	allocCidr := cidr
	if allocCidr == "" {
		var err error

		allocCidr, err = b.autoCIDRLocked(pool.Cidr, netmaskLength)
		if err != nil {
			return nil, err
		}
	}

	alloc := &IpamPoolAllocation{
		IpamPoolAllocationID: "ipam-alloc-" + uuid.New().String()[:8],
		IpamPoolID:           poolID,
		Cidr:                 allocCidr,
		Description:          o.Description,
		ResourceType:         o.ResourceType,
		ResourceID:           o.ResourceID,
		ResourceOwner:        o.ResourceOwner,
	}
	if alloc.ResourceType == "" {
		alloc.ResourceType = "custom"
	}

	b.ipamPoolAllocations[alloc.IpamPoolAllocationID] = alloc

	cp := *alloc

	return &cp, nil
}

// GetIpamPoolAllocations returns allocations for an IPAM pool, optionally filtered to a
// single allocation ID.
func (b *InMemoryBackend) GetIpamPoolAllocations(poolID, allocationID string) ([]*IpamPoolAllocation, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPoolAllocations")
	defer b.mu.RUnlock()

	if _, ok := b.ipamPools[poolID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, poolID)
	}

	out := make([]*IpamPoolAllocation, 0, len(b.ipamPoolAllocations))

	for _, alloc := range b.ipamPoolAllocations {
		if alloc.IpamPoolID != poolID {
			continue
		}

		if allocationID != "" && alloc.IpamPoolAllocationID != allocationID {
			continue
		}

		cp := *alloc
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPoolAllocationID < out[j].IpamPoolAllocationID
	})

	return out, nil
}

// ReleaseIpamPoolAllocation releases an IPAM pool allocation.
func (b *InMemoryBackend) ReleaseIpamPoolAllocation(poolID, allocationID string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: IpamPoolAllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReleaseIpamPoolAllocation")
	defer b.mu.Unlock()

	alloc, ok := b.ipamPoolAllocations[allocationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrIpamAllocationNotFound, allocationID)
	}

	if poolID != "" && alloc.IpamPoolID != poolID {
		return fmt.Errorf("%w: %s", ErrIpamAllocationNotFound, allocationID)
	}

	delete(b.ipamPoolAllocations, allocationID)

	return nil
}

// ---- IPAM Resource Discoveries ----

// DescribeIpamResourceDiscoveries returns IPAM resource discoveries, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamResourceDiscoveries(ids []string) []*IpamResourceDiscovery {
	b.mu.RLock("DescribeIpamResourceDiscoveries")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamResourceDiscovery, 0, len(b.ipamResourceDiscoveries))

	for _, d := range b.ipamResourceDiscoveries {
		if len(idSet) > 0 && !idSet[d.IpamResourceDiscoveryID] {
			continue
		}

		cp := *d
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamResourceDiscoveryID < out[j].IpamResourceDiscoveryID
	})

	return out
}

// DescribeIpamResourceDiscoveryAssociations returns IPAM resource discovery associations,
// optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamResourceDiscoveryAssociations(
	ids []string,
) []*IpamResourceDiscoveryAssociation {
	b.mu.RLock("DescribeIpamResourceDiscoveryAssociations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamResourceDiscoveryAssociation, 0, len(b.ipamResourceDiscoveryAssocs))

	for _, a := range b.ipamResourceDiscoveryAssocs {
		if len(idSet) > 0 && !idSet[a.IpamResourceDiscoveryAssociationID] {
			continue
		}

		cp := *a
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamResourceDiscoveryAssociationID < out[j].IpamResourceDiscoveryAssociationID
	})

	return out
}
