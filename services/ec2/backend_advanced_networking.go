package ec2

import (
	"errors"
	"fmt"
	"net"
	"sort"

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
	VpnConnectionID   string `json:"vpnConnectionId,omitempty"`
	State             string `json:"state,omitempty"`
	CustomerGatewayID string `json:"customerGatewayId,omitempty"`
	VpnGatewayID      string `json:"vpnGatewayId,omitempty"`
	Type              string `json:"type,omitempty"`
}

// VpcEndpointServiceConfig represents a VPC endpoint service configuration.
type VpcEndpointServiceConfig struct {
	ServiceID               string   `json:"serviceId,omitempty"`
	ServiceName             string   `json:"serviceName,omitempty"`
	ServiceType             string   `json:"serviceType,omitempty"`
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
	}
	b.vpnConnections[conn.VpnConnectionID] = conn

	cp := *conn

	return &cp, nil
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

		cp := *conn
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].VpnConnectionID < out[j].VpnConnectionID
	})

	return out
}

// DeleteVpnConnection removes a VPN connection.
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

	return nil
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
