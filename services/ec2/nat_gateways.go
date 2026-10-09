package ec2

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

// ErrNatGatewayNotFound is returned when a NAT gateway is not found.
var ErrNatGatewayNotFound = errors.New("InvalidNatGatewayID.NotFound")

// natGatewayConnectivityTypePublic is the only ConnectivityType this backend
// creates: CreateNatGateway always requires an AllocationId (an Elastic IP),
// which is the defining trait of a public NAT gateway in real AWS. Private
// NAT gateways (no AllocationId, ConnectivityType=private) are not modeled —
// see PARITY.md.
const natGatewayConnectivityTypePublic = "public"

// natGatewayStateDeleted matches types.NatGatewayStateDeleted (ec2@v1.329.0
// types/enums.go), the tombstone state DeleteNatGateway leaves behind.
const natGatewayStateDeleted = "deleted"

// NatGateway represents an EC2 NAT Gateway.
type NatGateway struct {
	CreateTime          time.Time            `json:"createTime"`
	DrainingAddresses   map[string]time.Time `json:"drainingAddresses,omitempty"`
	PrivateIP           string               `json:"privateIP,omitempty"`
	State               string               `json:"state,omitempty"`
	AvailabilityZone    string               `json:"availabilityZone,omitempty"`
	AllocationID        string               `json:"allocationID,omitempty"`
	SubnetID            string               `json:"subnetID,omitempty"`
	PublicIP            string               `json:"publicIP,omitempty"`
	AssociationID       string               `json:"associationID,omitempty"`
	VPCID               string               `json:"vpcID,omitempty"`
	ConnectivityType    string               `json:"connectivityType,omitempty"`
	ID                  string               `json:"id,omitempty"`
	AutoScalingIPs      string               `json:"autoScalingIPs,omitempty"`
	AutoProvisionZones  string               `json:"autoProvisionZones,omitempty"`
	AvailabilityMode    string               `json:"availabilityMode,omitempty"`
	RouteTableID        string               `json:"routeTableID,omitempty"`
	ZoneAddresses       []NatGatewayAddress  `json:"zoneAddresses,omitempty"`
	SecondaryPrivateIPs []string             `json:"secondaryPrivateIPs,omitempty"`
	SecondaryAddresses  []NatGatewayAddress  `json:"secondaryAddresses,omitempty"`
}

// NatGatewayAddress represents one secondary (non-primary) EIP association on
// a public NAT gateway, as added by AssociateNatGatewayAddress.
type NatGatewayAddress struct {
	AllocationID  string `json:"allocationID,omitempty"`
	AssociationID string `json:"associationID,omitempty"`
	PrivateIP     string `json:"privateIP,omitempty"`
	PublicIP      string `json:"publicIP,omitempty"`
	// AvailabilityZone and Auto are set only on regional NAT gateway addresses.
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	Auto             bool   `json:"auto,omitempty"`
}

// CreateNatGateway creates a new NAT Gateway.
func (b *InMemoryBackend) CreateNatGateway(
	subnetID, allocationID string, tags map[string]string,
) (*NatGateway, error) {
	b.mu.Lock("CreateNatGateway")
	defer b.mu.Unlock()

	subnet, ok := b.subnets.Get(subnetID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	addr, ok := b.addresses.Get(allocationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAddressNotFound, allocationID)
	}

	id := newNATGatewayID()
	ngw := &NatGateway{
		ID:               id,
		SubnetID:         subnetID,
		VPCID:            subnet.VPCID,
		AvailabilityZone: subnet.AvailabilityZone,
		AvailabilityMode: natGatewayModeZonal,
		AllocationID:     allocationID,
		AssociationID:    newEIPAssociationID(),
		PublicIP:         addr.PublicIP,
		PrivateIP:        b.allocPrivateIP(),
		State:            stateAvailable,
		ConnectivityType: natGatewayConnectivityTypePublic,
		CreateTime:       time.Now(),
	}
	b.natGateways.Put(ngw)
	b.indexNatGatewayLocked(ngw)
	b.setTagsLocked(id, tags)

	return ngw, nil
}

// DeleteNatGateway removes a NAT Gateway and recycles every private IP it
// holds: the primary address, any secondary EIP associations, and any
// secondary private IPs assigned via AssignPrivateNatGatewayAddress. A
// tombstone in state "deleted" is kept so a subsequent by-ID Describe still
// finds it (real AWS keeps a deleted NAT gateway describable for a period;
// terraform-provider-aws's delete waiter polls by ID and treats a NotFound
// response as a fatal error instead of "done").
func (b *InMemoryBackend) DeleteNatGateway(id string) error {
	b.mu.Lock("DeleteNatGateway")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNatGatewayNotFound, id)
	}

	b.recycleIPLocked(ngw.PrivateIP)

	for _, sa := range ngw.SecondaryAddresses {
		b.recycleIPLocked(sa.PrivateIP)
	}

	for _, ip := range ngw.SecondaryPrivateIPs {
		b.recycleIPLocked(ip)
	}

	b.releaseRegionalAddressesLocked(ngw)
	b.deleteRegionalNatRouteTableLocked(ngw)

	b.deindexNatGatewayLocked(ngw)
	b.natGateways.Delete(id)
	delete(b.tags, id)

	cp := *ngw
	cp.State = natGatewayStateDeleted
	pruneExpiredTombstones(b.natGatewayTombstones, time.Now())
	b.natGatewayTombstones[id] = tombstone[NatGateway]{value: &cp, deletedAt: time.Now()}

	return nil
}

// DescribeNatGateways returns NAT Gateways, optionally filtered by IDs. When
// ids are provided, a tombstone is returned for any of them that was
// recently deleted (see DeleteNatGateway); an unfiltered Describe never
// surfaces tombstones, matching real AWS's list-vs-get behavior.
func (b *InMemoryBackend) DescribeNatGateways(ids []string) []*NatGateway {
	b.mu.Lock("DescribeNatGateways")
	defer b.mu.Unlock()

	b.purgeDrainedNatAddressesLocked()

	if len(ids) > 0 {
		return describeWithTombstones(b.natGateways.All(), b.natGatewayTombstones, ids,
			func(n *NatGateway) string { return n.ID })
	}

	out := make([]*NatGateway, 0, b.natGateways.Len())

	for _, ngw := range b.natGateways.All() {
		out = append(out, copyNatGateway(ngw))
	}

	return out
}

// DisassociateNatGatewayAddress removes secondary EIP associations (added via
// AssociateNatGatewayAddress) from a public NAT gateway by AssociationId,
// recycling their private IPs. Returns the updated NAT gateway.
func (b *InMemoryBackend) DisassociateNatGatewayAddress(
	natGatewayID string, associationIDs []string,
) (*NatGateway, error) {
	return b.DisassociateNatGatewayAddressDrain(natGatewayID, associationIDs, 0)
}

// DisassociateNatGatewayAddressDrain adds MaxDrainDurationSeconds: when positive the
// addresses stay "disassociating" until the drain elapses.
func (b *InMemoryBackend) DisassociateNatGatewayAddressDrain(
	natGatewayID string, associationIDs []string, maxDrainSeconds int,
) (*NatGateway, error) {
	if maxDrainSeconds < 0 {
		return nil, fmt.Errorf("%w: MaxDrainDurationSeconds must not be negative", ErrInvalidParameter)
	}

	if natGatewayID == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if len(associationIDs) == 0 {
		return nil, fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateNatGatewayAddress")
	defer b.mu.Unlock()

	b.purgeDrainedNatAddressesLocked()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNatGatewayNotFound, natGatewayID)
	}

	for _, assocID := range associationIDs {
		if zi := slices.IndexFunc(ngw.ZoneAddresses, func(a NatGatewayAddress) bool {
			return a.AssociationID == assocID
		}); zi >= 0 {
			if maxDrainSeconds > 0 {
				ngw.markDrainingLocked(assocID, maxDrainSeconds)
			} else {
				b.releaseZoneAddressLocked(ngw.ZoneAddresses[zi])
				ngw.ZoneAddresses = slices.Delete(ngw.ZoneAddresses, zi, zi+1)
			}

			continue
		}

		idx := -1

		for i, sa := range ngw.SecondaryAddresses {
			if sa.AssociationID == assocID {
				idx = i

				break
			}
		}

		if idx == -1 {
			return nil, fmt.Errorf("%w: %s", ErrAssociationNotFound, assocID)
		}

		if maxDrainSeconds > 0 {
			ngw.markDrainingLocked(assocID, maxDrainSeconds)

			continue
		}

		b.recycleIPLocked(ngw.SecondaryAddresses[idx].PrivateIP)
		ngw.SecondaryAddresses = append(
			ngw.SecondaryAddresses[:idx], ngw.SecondaryAddresses[idx+1:]...,
		)
	}

	return copyNatGateway(ngw), nil
}

func copyNatGateway(ngw *NatGateway) *NatGateway {
	cp := *ngw
	cp.SecondaryAddresses = slices.Clone(ngw.SecondaryAddresses)
	cp.SecondaryPrivateIPs = slices.Clone(ngw.SecondaryPrivateIPs)
	cp.ZoneAddresses = slices.Clone(ngw.ZoneAddresses)
	cp.DrainingAddresses = maps.Clone(ngw.DrainingAddresses)

	return &cp
}

func (n *NatGateway) markDrainingLocked(key string, seconds int) {
	if n.DrainingAddresses == nil {
		n.DrainingAddresses = make(map[string]time.Time)
	}

	n.DrainingAddresses[key] = time.Now().Add(time.Duration(seconds) * time.Second)
}

// purgeDrainedNatAddressesLocked releases secondary addresses whose drain has
// elapsed. Must be called with b.mu held for writing.
func (b *InMemoryBackend) purgeDrainedNatAddressesLocked() {
	now := time.Now()

	for _, ngw := range b.natGateways.All() {
		if len(ngw.DrainingAddresses) == 0 {
			continue
		}

		ngw.SecondaryAddresses = slices.DeleteFunc(ngw.SecondaryAddresses, func(a NatGatewayAddress) bool {
			due, draining := ngw.DrainingAddresses[a.AssociationID]
			if !draining || due.After(now) {
				return false
			}

			b.recycleIPLocked(a.PrivateIP)
			delete(ngw.DrainingAddresses, a.AssociationID)

			return true
		})

		ngw.ZoneAddresses = slices.DeleteFunc(ngw.ZoneAddresses, func(a NatGatewayAddress) bool {
			due, draining := ngw.DrainingAddresses[a.AssociationID]
			if !draining || due.After(now) {
				return false
			}

			b.releaseZoneAddressLocked(a)
			delete(ngw.DrainingAddresses, a.AssociationID)

			return true
		})

		ngw.SecondaryPrivateIPs = slices.DeleteFunc(ngw.SecondaryPrivateIPs, func(ip string) bool {
			due, draining := ngw.DrainingAddresses[ip]
			if !draining || due.After(now) {
				return false
			}

			delete(ngw.DrainingAddresses, ip)

			return true
		})
	}
}

// AssociateNatGatewayAddress associates one or more additional Elastic IP
// allocations with a public NAT gateway, allocating a fresh private IP and
// AssociationId for each. Returns the updated NAT gateway.
func (b *InMemoryBackend) AssociateNatGatewayAddress(
	natGatewayID string, allocationIDs []string,
) (*NatGateway, error) {
	return b.AssociateNatGatewayAddressInZone(natGatewayID, "", "", allocationIDs)
}

// AssociateNatGatewayAddressInZone is AssociateNatGatewayAddress plus the
// AvailabilityZone/AvailabilityZoneId that regional NAT gateways require.
func (b *InMemoryBackend) AssociateNatGatewayAddressInZone(
	natGatewayID, azName, azID string, allocationIDs []string,
) (*NatGateway, error) {
	if natGatewayID == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if len(allocationIDs) == 0 {
		return nil, fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateNatGatewayAddress")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNatGatewayNotFound, natGatewayID)
	}

	if ngw.AvailabilityMode == natGatewayModeRegional {
		if err := b.associateRegionalAddressLocked(ngw, azName, azID, allocationIDs); err != nil {
			return nil, err
		}

		return copyNatGateway(ngw), nil
	}

	if azName != "" || azID != "" {
		return nil, fmt.Errorf("%w: AvailabilityZone applies to regional NAT gateways only", ErrInvalidParameter)
	}

	for _, allocID := range allocationIDs {
		addr, addrOK := b.addresses.Get(allocID)
		if !addrOK {
			return nil, fmt.Errorf("%w: %s", ErrAddressNotFound, allocID)
		}

		ngw.SecondaryAddresses = append(ngw.SecondaryAddresses, NatGatewayAddress{
			AllocationID:  allocID,
			AssociationID: newEIPAssociationID(),
			PrivateIP:     b.allocPrivateIP(),
			PublicIP:      addr.PublicIP,
		})
	}

	cp := *ngw
	cp.SecondaryAddresses = append([]NatGatewayAddress(nil), ngw.SecondaryAddresses...)

	return &cp, nil
}

// AssignPrivateNatGatewayAddress assigns secondary private IPs to a NAT
// gateway, appending them to the gateway's real address state and returning
// the updated gateway. ips is used when non-empty; otherwise count new IPs
// are auto-allocated (at least 1, matching the real API's implicit default
// of one address when neither PrivateIpAddressCount nor PrivateIpAddresses
// is given). See UnassignPrivateNatGatewayAddress (nat_gateways.go) for the
// inverse.
func (b *InMemoryBackend) AssignPrivateNatGatewayAddress(
	natGatewayID string, count int, ips []string,
) (*NatGateway, error) {
	if natGatewayID == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssignPrivateNatGatewayAddress")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, natGatewayID)
	}

	if len(ips) > 0 {
		ngw.SecondaryPrivateIPs = append(ngw.SecondaryPrivateIPs, ips...)

		return copyNatGateway(ngw), nil
	}

	if count < 1 {
		count = 1
	}

	for range count {
		ngw.SecondaryPrivateIPs = append(ngw.SecondaryPrivateIPs, b.allocPrivateIP())
	}

	return copyNatGateway(ngw), nil
}

// ---- NAT gateway address management ----

// UnassignPrivateNatGatewayAddress removes previously assigned secondary
// private IPs from a NAT gateway, mutating the existing NAT gateway's address
// state.
func (b *InMemoryBackend) UnassignPrivateNatGatewayAddress(
	natGatewayID string, privateIPs []string,
) (*NatGateway, error) {
	return b.UnassignPrivateNatGatewayAddressDrain(natGatewayID, privateIPs, 0)
}

// UnassignPrivateNatGatewayAddressDrain adds MaxDrainDurationSeconds: when positive the
// private IPs stay "unassigning" until the drain elapses.
func (b *InMemoryBackend) UnassignPrivateNatGatewayAddressDrain(
	natGatewayID string, privateIPs []string, maxDrainSeconds int,
) (*NatGateway, error) {
	if maxDrainSeconds < 0 {
		return nil, fmt.Errorf("%w: MaxDrainDurationSeconds must not be negative", ErrInvalidParameter)
	}

	if natGatewayID == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if len(privateIPs) == 0 {
		return nil, fmt.Errorf("%w: PrivateIpAddress is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnassignPrivateNatGatewayAddress")
	defer b.mu.Unlock()

	b.purgeDrainedNatAddressesLocked()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNatGatewayNotFound, natGatewayID)
	}

	if maxDrainSeconds > 0 {
		for _, ip := range privateIPs {
			if slices.Contains(ngw.SecondaryPrivateIPs, ip) {
				ngw.markDrainingLocked(ip, maxDrainSeconds)
			}
		}

		return copyNatGateway(ngw), nil
	}

	remove := make(map[string]bool, len(privateIPs))
	for _, ip := range privateIPs {
		remove[ip] = true
	}

	kept := ngw.SecondaryPrivateIPs[:0:0]

	for _, ip := range ngw.SecondaryPrivateIPs {
		if !remove[ip] {
			kept = append(kept, ip)
		}
	}

	ngw.SecondaryPrivateIPs = kept

	return copyNatGateway(ngw), nil
}
