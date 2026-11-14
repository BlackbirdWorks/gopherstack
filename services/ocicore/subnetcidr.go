package ocicore

import (
	"net/netip"
	"slices"
)

// subnetForUpdate resolves a subnet for a CIDR change: it must be AVAILABLE, writable and match the etag.
func (b *InMemoryBackend) subnetForUpdate(id, ifMatch string) (*Subnet, *Vcn, *APIError) {
	s, aerr := getLive[*Subnet](b, id)
	if aerr != nil {
		return nil, nil, aerr
	}

	if aerr = checkEtag(s, ifMatch); aerr != nil {
		return nil, nil, aerr
	}

	if s.LifecycleState != statAvailable {
		return nil, nil, errState("The subnet " + id + " is " + s.LifecycleState)
	}

	if aerr = b.vcnWritable(s.VcnID); aerr != nil {
		return nil, nil, aerr
	}

	v, aerr := getLive[*Vcn](b, s.VcnID)

	return s, v, aerr
}

func (b *InMemoryBackend) beginSubnetUpdate(s *Subnet, op string) string {
	b.moveTo(s, statUpdating, statAvailable)

	return b.newWorkRequest(op, s.CompartmentID, []WorkResource{
		{
			ActionType: wrActionUpdated,
			EntityType: entitySubnet,
			Identifier: s.ID,
			EntityURI:  apiBase + "/subnets/" + s.ID,
		},
	})
}

func (s *Subnet) setIpv4(blocks []string) {
	s.Ipv4CidrBlocks, s.CidrBlock = blocks, blocks[0]
	s.VirtualRouterIP = hostAt(netip.MustParsePrefix(blocks[0]), 1)
}

// AddIpv4SubnetCidr adds an IPv4 block to a subnet and returns the work request id.
func (b *InMemoryBackend) AddIpv4SubnetCidr(id, ifMatch, cidr string) (string, *APIError) {
	b.mu.Lock("AddIpv4SubnetCidr")
	defer b.mu.Unlock()

	s, v, aerr := b.subnetForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if cidr == "" {
		return "", errMissing("ipv4CidrBlock is required")
	}

	if len(s.Ipv4CidrBlocks) >= maxSubnetCidrs {
		return "", errLimit("A subnet can have at most 6 IPv4 CIDR blocks")
	}

	if _, aerr = b.subnetBlock(v, cidr, s.ID, prefixesOf(s.Ipv4CidrBlocks)); aerr != nil {
		return "", aerr
	}

	s.setIpv4(append(slices.Clone(s.Ipv4CidrBlocks), cidr))

	return b.beginSubnetUpdate(s, opAddSubnetCidr), nil
}

// ModifyIpv4SubnetCidr replaces one IPv4 block of a subnet and returns the work request id.
func (b *InMemoryBackend) ModifyIpv4SubnetCidr(id, ifMatch, original, updated string) (string, *APIError) {
	b.mu.Lock("ModifyIpv4SubnetCidr")
	defer b.mu.Unlock()

	s, v, aerr := b.subnetForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	i := slices.Index(s.Ipv4CidrBlocks, original)
	if i < 0 {
		return "", errParam("ipv4CidrBlock " + original + " is not an IPv4 block of the subnet")
	}

	if updated == "" || updated == original {
		return "", errParam("updatedIpv4CidrBlock must be given and differ from ipv4CidrBlock")
	}

	others := slices.Delete(slices.Clone(s.Ipv4CidrBlocks), i, i+1)

	next, aerr := b.subnetBlock(v, updated, s.ID, prefixesOf(others))
	if aerr != nil {
		return "", aerr
	}

	old := netip.MustParsePrefix(original)

	for a := range b.subnetIPsUsed(s) {
		if old.Contains(a) && !next.Contains(a) {
			return "", errParam("The IP address " + a.String() + " is in use and would be outside the new CIDR block")
		}
	}

	blocks := slices.Clone(s.Ipv4CidrBlocks)
	blocks[i] = updated
	s.setIpv4(blocks)

	return b.beginSubnetUpdate(s, opModifySubnetCidr), nil
}

// RemoveIpv4SubnetCidr removes an IPv4 block that holds no private IP and returns the work request id.
func (b *InMemoryBackend) RemoveIpv4SubnetCidr(id, ifMatch, cidr string) (string, *APIError) {
	b.mu.Lock("RemoveIpv4SubnetCidr")
	defer b.mu.Unlock()

	s, _, aerr := b.subnetForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	i := slices.Index(s.Ipv4CidrBlocks, cidr)
	if i < 0 {
		return "", errParam("ipv4CidrBlock " + cidr + " is not an IPv4 block of the subnet")
	}

	if len(s.Ipv4CidrBlocks) == 1 {
		return "", errParam("A subnet must keep at least one IPv4 CIDR block")
	}

	p := netip.MustParsePrefix(cidr)

	for a := range b.subnetIPsUsed(s) {
		if p.Contains(a) {
			return "", errConflict(
				"The CIDR block " + cidr + " cannot be removed: the IP address " + a.String() + " is in use",
			)
		}
	}

	s.setIpv4(slices.Delete(slices.Clone(s.Ipv4CidrBlocks), i, i+1))

	return b.beginSubnetUpdate(s, opRemoveSubnetCidr), nil
}

// AddIpv6SubnetCidr adds an IPv6 /64 from the VCN's prefixes to a subnet and returns the work request id.
func (b *InMemoryBackend) AddIpv6SubnetCidr(id, ifMatch, cidr string) (string, string, *APIError) {
	b.mu.Lock("AddIpv6SubnetCidr")
	defer b.mu.Unlock()

	s, v, aerr := b.subnetForUpdate(id, ifMatch)
	if aerr != nil {
		return "", "", aerr
	}

	if cidr == "" {
		return "", "", errMissing("ipv6CidrBlock is required")
	}

	if slices.Contains(s.Ipv6CidrBlocks, cidr) {
		return "", "", errConflict("The subnet already has the IPv6 prefix " + cidr)
	}

	if aerr = b.subnetV6Block(v, cidr, s.ID); aerr != nil {
		return "", "", aerr
	}

	s.setIpv6(append(slices.Clone(s.Ipv6CidrBlocks), cidr))

	return b.beginSubnetUpdate(s, opAddSubnetIpv6), s.Internal.Etag, nil
}

// RemoveIpv6SubnetCidr removes an IPv6 prefix from a subnet; at least one must remain.
func (b *InMemoryBackend) RemoveIpv6SubnetCidr(id, ifMatch, cidr string) (string, string, *APIError) {
	b.mu.Lock("RemoveIpv6SubnetCidr")
	defer b.mu.Unlock()

	s, _, aerr := b.subnetForUpdate(id, ifMatch)
	if aerr != nil {
		return "", "", aerr
	}

	i := slices.Index(s.Ipv6CidrBlocks, cidr)
	if i < 0 {
		return "", "", errParam("ipv6CidrBlock " + cidr + " is not an IPv6 prefix of the subnet")
	}

	if len(s.Ipv6CidrBlocks) == 1 {
		return "", "", errParam("At least one IPv6 CIDR block must remain on the subnet")
	}

	if aerr = b.ipv6PrefixFree(s, cidr); aerr != nil {
		return "", "", aerr
	}

	s.setIpv6(slices.Delete(slices.Clone(s.Ipv6CidrBlocks), i, i+1))

	return b.beginSubnetUpdate(s, opRemoveSubnetIpv6), s.Internal.Etag, nil
}

func (h *Handler) addIpv4SubnetCidr(q *req) error {
	var in struct {
		Ipv4CidrBlock string `json:"ipv4CidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.AddIpv4SubnetCidr(q.param("subnetId"), q.ifMatch(), in.Ipv4CidrBlock)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func (h *Handler) modifyIpv4SubnetCidr(q *req) error {
	var in struct {
		Ipv4CidrBlock        string `json:"ipv4CidrBlock"`
		UpdatedIpv4CidrBlock string `json:"updatedIpv4CidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.ModifyIpv4SubnetCidr(
		q.param("subnetId"),
		q.ifMatch(),
		in.Ipv4CidrBlock,
		in.UpdatedIpv4CidrBlock,
	)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func (h *Handler) removeIpv4SubnetCidr(q *req) error {
	var in struct {
		Ipv4CidrBlock string `json:"ipv4CidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.RemoveIpv4SubnetCidr(q.param("subnetId"), q.ifMatch(), in.Ipv4CidrBlock)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func (h *Handler) addIpv6SubnetCidr(q *req) error {
	var in struct {
		Ipv6CidrBlock string `json:"ipv6CidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, etag, aerr := h.Backend.AddIpv6SubnetCidr(q.param("subnetId"), q.ifMatch(), in.Ipv6CidrBlock)
	if aerr != nil {
		return aerr
	}

	return q.acceptCidrChange(wr, etag)
}

func (h *Handler) removeIpv6SubnetCidr(q *req) error {
	var in struct {
		Ipv6CidrBlock string `json:"ipv6CidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, etag, aerr := h.Backend.RemoveIpv6SubnetCidr(q.param("subnetId"), q.ifMatch(), in.Ipv6CidrBlock)
	if aerr != nil {
		return aerr
	}

	return q.acceptCidrChange(wr, etag)
}
