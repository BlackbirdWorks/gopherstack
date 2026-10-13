package ec2

import (
	"cmp"
	"fmt"
	"slices"
	"time"
)

const (
	natGatewayModeZonal    = "zonal"
	natGatewayModeRegional = "regional"
	natFeatureEnabled      = "enabled"
	natFeatureDisabled     = "disabled"
)

// NatGatewayZoneRequest is one AvailabilityZoneAddress of CreateNatGateway.
type NatGatewayZoneRequest struct {
	AvailabilityZone   string
	AvailabilityZoneID string
	AllocationIDs      []string
}

// NatGatewayMode returns the effective availability mode ("zonal" for legacy rows).
func (n *NatGateway) NatGatewayMode() string {
	if n.AvailabilityMode == "" {
		return natGatewayModeZonal
	}

	return n.AvailabilityMode
}

// resolveRegionalZoneLocked maps an AZ name or ID to a zone name of this region.
func (b *InMemoryBackend) resolveRegionalZoneLocked(name, id string) (string, error) {
	for _, az := range b.DescribeAvailabilityZones("") {
		if (name != "" && az == name) || (id != "" && availabilityZoneID(az) == id) {
			return az, nil
		}
	}

	return "", fmt.Errorf("%w: invalid Availability Zone %q%q", ErrInvalidParameter, name, id)
}

// CreateRegionalNatGateway creates a regional (multi-AZ) public NAT gateway in
// a VPC. Without zones it auto-provisions one EIP per AZ that has a subnet.
func (b *InMemoryBackend) CreateRegionalNatGateway(
	vpcID string, zones []NatGatewayZoneRequest, tags map[string]string,
) (*NatGateway, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required for a regional NAT gateway", ErrInvalidParameter)
	}

	b.mu.Lock("CreateRegionalNatGateway")
	defer b.mu.Unlock()

	if _, ok := b.vpcs.Get(vpcID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	ngw := &NatGateway{
		ID:               newNATGatewayID(),
		VPCID:            vpcID,
		State:            stateAvailable,
		ConnectivityType: natGatewayConnectivityTypePublic,
		AvailabilityMode: natGatewayModeRegional,
		CreateTime:       time.Now(),
	}

	if len(zones) == 0 {
		ngw.AutoProvisionZones = natFeatureEnabled
		ngw.AutoScalingIPs = natFeatureEnabled
		b.autoProvisionZonesLocked(ngw)
	} else {
		ngw.AutoProvisionZones = natFeatureDisabled
		ngw.AutoScalingIPs = natFeatureDisabled

		if err := b.addExplicitZonesLocked(ngw, zones); err != nil {
			return nil, err
		}
	}

	b.createRegionalNatRouteTableLocked(ngw)
	b.natGateways.Put(ngw)
	b.indexNatGatewayLocked(ngw)
	b.setTagsLocked(ngw.ID, tags)

	return copyNatGateway(ngw), nil
}

func (b *InMemoryBackend) addExplicitZonesLocked(ngw *NatGateway, zones []NatGatewayZoneRequest) error {
	seen := make(map[string]struct{}, len(zones))
	staged := make([]NatGatewayAddress, 0, len(zones))

	for _, z := range zones {
		az, err := b.resolveRegionalZoneLocked(z.AvailabilityZone, z.AvailabilityZoneID)
		if err != nil {
			return err
		}

		if _, dup := seen[az]; dup {
			return fmt.Errorf("%w: duplicate Availability Zone %s", ErrInvalidParameter, az)
		}

		seen[az] = struct{}{}

		if len(z.AllocationIDs) == 0 {
			return fmt.Errorf("%w: AllocationId is required for %s", ErrInvalidParameter, az)
		}

		for _, allocID := range z.AllocationIDs {
			addr, ok := b.addresses.Get(allocID)
			if !ok {
				return fmt.Errorf("%w: %s", ErrAddressNotFound, allocID)
			}

			staged = append(staged, NatGatewayAddress{
				AllocationID:     allocID,
				PublicIP:         addr.PublicIP,
				AvailabilityZone: az,
			})
		}
	}

	for i := range staged {
		staged[i].AssociationID = newEIPAssociationID()
		staged[i].PrivateIP = b.allocPrivateIP()
	}

	ngw.ZoneAddresses = staged

	return nil
}

// autoProvisionZonesLocked gives ngw an auto-allocated EIP in every AZ where
// its VPC has a subnet and that it does not cover yet.
func (b *InMemoryBackend) autoProvisionZonesLocked(ngw *NatGateway) {
	covered := make(map[string]struct{}, len(ngw.ZoneAddresses))
	for _, a := range ngw.ZoneAddresses {
		covered[a.AvailabilityZone] = struct{}{}
	}

	for _, s := range b.subnets.All() {
		if s.VPCID != ngw.VPCID {
			continue
		}

		if _, ok := covered[s.AvailabilityZone]; ok {
			continue
		}

		covered[s.AvailabilityZone] = struct{}{}

		alloc := newEIPAllocationID()
		addr := &Address{AllocationID: alloc, PublicIP: b.allocElasticIP()}
		b.addresses.Put(addr)

		ngw.ZoneAddresses = append(ngw.ZoneAddresses, NatGatewayAddress{
			AllocationID:     alloc,
			AssociationID:    newEIPAssociationID(),
			PrivateIP:        b.allocPrivateIP(),
			PublicIP:         addr.PublicIP,
			AvailabilityZone: s.AvailabilityZone,
			Auto:             true,
		})
	}

	slices.SortStableFunc(ngw.ZoneAddresses, func(x, y NatGatewayAddress) int {
		return cmp.Compare(x.AvailabilityZone, y.AvailabilityZone)
	})
}

// expandRegionalNatGatewaysLocked runs after a subnet appears in vpcID.
func (b *InMemoryBackend) expandRegionalNatGatewaysLocked(vpcID string) {
	for _, ngw := range b.natGateways.All() {
		if ngw.VPCID == vpcID && ngw.AvailabilityMode == natGatewayModeRegional &&
			ngw.AutoProvisionZones == natFeatureEnabled {
			b.autoProvisionZonesLocked(ngw)
		}
	}
}

// retractRegionalNatGatewaysLocked drops auto-provisioned addresses in AZs
// where vpcID no longer has any subnet.
func (b *InMemoryBackend) retractRegionalNatGatewaysLocked(vpcID string) {
	live := make(map[string]struct{})

	for _, s := range b.subnets.All() {
		if s.VPCID == vpcID {
			live[s.AvailabilityZone] = struct{}{}
		}
	}

	for _, ngw := range b.natGateways.All() {
		if ngw.VPCID != vpcID || ngw.AvailabilityMode != natGatewayModeRegional ||
			ngw.AutoProvisionZones != natFeatureEnabled {
			continue
		}

		ngw.ZoneAddresses = slices.DeleteFunc(ngw.ZoneAddresses, func(a NatGatewayAddress) bool {
			if _, ok := live[a.AvailabilityZone]; ok || !a.Auto {
				return false
			}

			b.releaseZoneAddressLocked(a)

			return true
		})
	}
}

func (b *InMemoryBackend) releaseZoneAddressLocked(a NatGatewayAddress) {
	b.recycleIPLocked(a.PrivateIP)

	if a.Auto {
		b.addresses.Delete(a.AllocationID)
	}
}

func (b *InMemoryBackend) releaseRegionalAddressesLocked(ngw *NatGateway) {
	for _, a := range ngw.ZoneAddresses {
		b.releaseZoneAddressLocked(a)
	}
}

// associateRegionalAddressLocked adds EIPs to one AZ of a regional gateway.
func (b *InMemoryBackend) associateRegionalAddressLocked(
	ngw *NatGateway, azName, azID string, allocationIDs []string,
) error {
	if azName == "" && azID == "" {
		return fmt.Errorf("%w: AvailabilityZone or AvailabilityZoneId is required for a regional NAT gateway",
			ErrInvalidParameter)
	}

	az, err := b.resolveRegionalZoneLocked(azName, azID)
	if err != nil {
		return err
	}

	for _, allocID := range allocationIDs {
		addr, ok := b.addresses.Get(allocID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrAddressNotFound, allocID)
		}

		ngw.ZoneAddresses = append(ngw.ZoneAddresses, NatGatewayAddress{
			AllocationID:     allocID,
			AssociationID:    newEIPAssociationID(),
			PrivateIP:        b.allocPrivateIP(),
			PublicIP:         addr.PublicIP,
			AvailabilityZone: az,
		})
	}

	return nil
}
