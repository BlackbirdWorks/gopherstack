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
)

// ---- Data types ----

// VpnGateway represents a Virtual Private Gateway (VGW).
type VpnGateway struct {
	VpnGatewayID    string `json:"vpnGatewayId"`
	State           string `json:"state"`
	Type            string `json:"type"`
	AttachedVPCID   string `json:"attachedVpcId,omitempty"`
	AttachmentState string `json:"attachmentState,omitempty"`
}

// CustomerGateway represents a customer gateway.
type CustomerGateway struct {
	CustomerGatewayID string `json:"customerGatewayId"`
	State             string `json:"state"`
	Type              string `json:"type"`
	BgpAsn            string `json:"bgpAsn"`
	IPAddress         string `json:"ipAddress"`
}

// VpnConnection represents a VPN connection.
type VpnConnection struct {
	VpnConnectionID   string `json:"vpnConnectionId"`
	State             string `json:"state"`
	CustomerGatewayID string `json:"customerGatewayId"`
	VpnGatewayID      string `json:"vpnGatewayId"`
	Type              string `json:"type"`
}

// VpcEndpointServiceConfig represents a VPC endpoint service configuration.
type VpcEndpointServiceConfig struct {
	ServiceID               string   `json:"serviceId"`
	ServiceName             string   `json:"serviceName"`
	ServiceType             string   `json:"serviceType"`
	NetworkLoadBalancerARNs []string `json:"networkLoadBalancerArns"`
	AcceptanceRequired      bool     `json:"acceptanceRequired"`
}

// Ipam represents an AWS IPAM instance.
type Ipam struct {
	IpamID  string `json:"ipamId"`
	IpamARN string `json:"ipamArn"`
	State   string `json:"state"`
	Region  string `json:"region"`
}

// IpamPool represents an IPAM pool.
type IpamPool struct {
	IpamPoolID    string `json:"ipamPoolId"`
	IpamPoolARN   string `json:"ipamPoolArn"`
	IpamID        string `json:"ipamId"`
	State         string `json:"state"`
	Locale        string `json:"locale"`
	AddressFamily string `json:"addressFamily"`
	Cidr          string `json:"cidr"`
}

// IpamPoolAllocation represents an allocated CIDR from an IPAM pool.
type IpamPoolAllocation struct {
	IpamPoolAllocationID string `json:"ipamPoolAllocationId"`
	Cidr                 string `json:"cidr"`
	Description          string `json:"description"`
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
	b.ipamPools = make(map[string]*IpamPool)
	b.ipamPoolAllocations = make(map[string]*IpamPoolAllocation)
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

// CreateIpam creates a new IPAM instance.
func (b *InMemoryBackend) CreateIpam() (*Ipam, error) {
	b.mu.Lock("CreateIpam")
	defer b.mu.Unlock()

	ipamID := "ipam-" + uuid.New().String()[:8]
	ipam := &Ipam{
		IpamID:  ipamID,
		IpamARN: "arn:aws:ec2::" + b.AccountID + ":ipam/" + ipamID,
		State:   stateAvailable,
		Region:  b.Region,
	}
	b.ipams[ipamID] = ipam

	cp := *ipam

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
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamID < out[j].IpamID
	})

	return out
}

// DeleteIpam removes an IPAM instance.
func (b *InMemoryBackend) DeleteIpam(id string) error {
	if id == "" {
		return fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpam")
	defer b.mu.Unlock()

	if _, ok := b.ipams[id]; !ok {
		return fmt.Errorf("%w: %s", ErrIpamNotFound, id)
	}

	delete(b.ipams, id)

	return nil
}

// CreateIpamPool creates a new IPAM pool.
func (b *InMemoryBackend) CreateIpamPool(
	ipamID, addressFamily, locale, cidr string,
) (*IpamPool, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	if addressFamily == "" {
		addressFamily = "ipv4"
	}

	b.mu.Lock("CreateIpamPool")
	defer b.mu.Unlock()

	if _, ok := b.ipams[ipamID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	poolID := "ipam-pool-" + uuid.New().String()[:8]
	pool := &IpamPool{
		IpamPoolID:    poolID,
		IpamPoolARN:   "arn:aws:ec2::" + b.AccountID + ":ipam-pool/" + poolID,
		IpamID:        ipamID,
		State:         stateAvailable,
		Locale:        locale,
		AddressFamily: addressFamily,
		Cidr:          cidr,
	}
	b.ipamPools[poolID] = pool

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

// DeleteIpamPool removes an IPAM pool.
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

	return nil
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

	ipInt := uint32(ip[0])<<octet3Shift | uint32(ip[1])<<octet2Shift | uint32(ip[2])<<octetMask | uint32(ip[3])
	shift := max(ipv4Shift-netmaskLength, 0)

	//nolint:gosec // existingCount is small; integer overflow is acceptable in mock context
	ipInt += uint32(existingCount) << uint(shift)
	shiftedIP := net.IP{
		byte(ipInt >> octet3Shift),
		byte(ipInt >> octet2Shift),
		byte(ipInt >> octetMask),
		byte(ipInt),
	}

	return fmt.Sprintf("%s/%d", shiftedIP.String(), netmaskLength), nil
}

// AllocateIpamPoolCidr allocates a CIDR from an IPAM pool.
// If cidr is empty, one is auto-generated from the pool's network space.
func (b *InMemoryBackend) AllocateIpamPoolCidr(
	poolID, cidr string,
	netmaskLength int,
) (*IpamPoolAllocation, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IpamPoolId is required", ErrInvalidParameter)
	}

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
		Cidr:                 allocCidr,
	}
	b.ipamPoolAllocations[alloc.IpamPoolAllocationID] = alloc

	cp := *alloc

	return &cp, nil
}

// GetIpamPoolCidrs returns allocations for an IPAM pool.
func (b *InMemoryBackend) GetIpamPoolCidrs(poolID string) []*IpamPoolAllocation {
	b.mu.RLock("GetIpamPoolCidrs")
	defer b.mu.RUnlock()

	out := make([]*IpamPoolAllocation, 0, len(b.ipamPoolAllocations))

	for _, alloc := range b.ipamPoolAllocations {
		cp := *alloc
		out = append(out, &cp)
	}

	_ = poolID // in a full implementation we'd associate allocations to pools

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPoolAllocationID < out[j].IpamPoolAllocationID
	})

	return out
}

// ReleaseIpamPoolAllocation releases an IPAM pool allocation.
func (b *InMemoryBackend) ReleaseIpamPoolAllocation(poolID, allocationID string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: IpamPoolAllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ReleaseIpamPoolAllocation")
	defer b.mu.Unlock()

	_ = poolID

	if _, ok := b.ipamPoolAllocations[allocationID]; !ok {
		return fmt.Errorf("%w: %s", ErrIpamAllocationNotFound, allocationID)
	}

	delete(b.ipamPoolAllocations, allocationID)

	return nil
}
