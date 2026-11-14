package ocicore

import "slices"

// ServiceNicIn describes a VNIC created on behalf of another OCI service (for example a File Storage mount target).
type ServiceNicIn struct {
	CompartmentID string
	AD            string
	SubnetID      string
	DisplayName   string
	HostnameLabel string
	PrivateIP     string
	NsgIDs        []string
	// AssignPublicIP nil keeps the subnet default.
	AssignPublicIP *bool
	// ReservedPublicIPIDs are unassigned reserved public IPs attached to the primary private IP.
	ReservedPublicIPIDs []string
	// Ipv6Addresses and Ipv6SubnetCidrs each request one IPv6 per entry; AssignIPv6 asks for one when both are empty.
	Ipv6Addresses   []string
	Ipv6SubnetCidrs []string
	AssignIPv6      bool
}

// ServiceNic is the VNIC and primary private IP created by CreateServiceNic.
type ServiceNic struct {
	VnicID      string
	PrivateIPID string
	IPAddress   string
	// PublicIPs are the public IPv4 addresses on the primary private IP.
	PublicIPs     []string
	Ipv6Addresses []string
}

// CreateServiceNic creates a VNIC without a public IP and returns its primary private IP.
func (b *InMemoryBackend) CreateServiceNic(in ServiceNicIn) (ServiceNic, error) {
	b.mu.Lock("CreateServiceNic")
	defer b.mu.Unlock()

	public := in.AssignPublicIP
	if public == nil {
		no := false
		public = &no
	}

	reserved, aerr := b.reservedPublicIPs(in.ReservedPublicIPIDs)
	if aerr != nil {
		return ServiceNic{}, aerr
	}

	v, aerr := b.newVnic(CreateVnicIn{
		AssignPublicIP: public, AssignIpv6IP: &in.AssignIPv6, DisplayName: in.DisplayName,
		HostnameLabel: in.HostnameLabel, PrivateIP: in.PrivateIP, SubnetID: in.SubnetID, NsgIDs: in.NsgIDs,
		Ipv6Pairs: ipv6Pairs(in.Ipv6Addresses, in.Ipv6SubnetCidrs),
	}, in.CompartmentID, in.AD, false)
	if aerr != nil {
		return ServiceNic{}, aerr
	}

	out := ServiceNic{VnicID: v.ID, IPAddress: v.PrivateIP}

	primary := b.primaryIP(v.ID)
	out.PrivateIPID = primary.ID

	for _, ip := range reserved {
		b.assignPublic(ip, primary)
	}

	b.refreshVnic(v.ID)

	if pub := b.publicOf(primary.ID); pub != nil {
		out.PublicIPs = append(out.PublicIPs, pub.IPAddress)
	}

	for _, ip := range reserved {
		if ip.IPAddress != "" && !slices.Contains(out.PublicIPs, ip.IPAddress) {
			out.PublicIPs = append(out.PublicIPs, ip.IPAddress)
		}
	}

	out.Ipv6Addresses = b.vnicIpv6(v.ID)

	return out, nil
}

func ipv6Pairs(addrs, cidrs []string) []Ipv6Pair {
	pairs := make([]Ipv6Pair, 0, len(addrs)+len(cidrs))

	for _, a := range addrs {
		pairs = append(pairs, Ipv6Pair{Ipv6Address: a})
	}

	for _, c := range cidrs {
		pairs = append(pairs, Ipv6Pair{Ipv6SubnetCidr: c})
	}

	return pairs
}

func (b *InMemoryBackend) reservedPublicIPs(ids []string) ([]*PublicIP, *APIError) {
	out := make([]*PublicIP, 0, len(ids))

	for _, id := range ids {
		ip, aerr := getLive[*PublicIP](b, id)
		if aerr != nil {
			return nil, aerr
		}

		if ip.Lifetime != lifetimeReserved || ip.AssignedEntityID != "" || slices.Contains(out, ip) {
			return nil, errConflict("The public IP " + ip.ID + " is not an unassigned reserved public IP")
		}

		out = append(out, ip)
	}

	return out, nil
}

// DeleteServiceNic terminates a VNIC made by CreateServiceNic; unknown ids are ignored.
func (b *InMemoryBackend) DeleteServiceNic(vnicID string) {
	b.mu.Lock("DeleteServiceNic")
	defer b.mu.Unlock()

	if v, ok := b.res[vnicID].(*Vnic); ok && live(v) {
		b.endVnic(v)
	}
}
