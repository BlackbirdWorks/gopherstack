package ocicore

import (
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strings"
)

const (
	ctCIDR      = "CIDR_BLOCK"
	ctService   = "SERVICE_CIDR_BLOCK"
	ctNSG       = "NETWORK_SECURITY_GROUP"
	maxSubnets  = 300
	maxSubnetSL = 5
	macSpace    = 1 << 24
)

var adRe = regexp.MustCompile(`^[A-Za-z]{4}:US-ASHBURN-AD-[1-3]$`)

// CreateSubnetIn is the CreateSubnet request body.
type CreateSubnetIn struct {
	TagsIn
	ProhibitInternetIngress *bool    `json:"prohibitInternetIngress"`
	ProhibitPublicIPOnVnic  *bool    `json:"prohibitPublicIpOnVnic"`
	DhcpOptionsID           string   `json:"dhcpOptionsId"`
	AvailabilityDomain      string   `json:"availabilityDomain"`
	CidrBlock               string   `json:"cidrBlock"`
	VcnID                   string   `json:"vcnId"`
	DisplayName             string   `json:"displayName"`
	DNSLabel                string   `json:"dnsLabel"`
	Ipv6CidrBlock           string   `json:"ipv6CidrBlock"`
	RouteTableID            string   `json:"routeTableId"`
	CompartmentID           string   `json:"compartmentId"`
	Ipv4CidrBlocks          []string `json:"ipv4CidrBlocks"`
	Ipv6CidrBlocks          []string `json:"ipv6CidrBlocks"`
	SecurityListIDs         []string `json:"securityListIds"`
}

// refInVcn checks that id names a live resource of type T in the given VCN.
func refInVcn[T resource](b *InMemoryBackend, id, vcnID, what string) *APIError {
	r, aerr := getLive[T](b, id)
	if aerr != nil {
		return aerr
	}

	if r.vcnRef() != vcnID {
		return errParam("The " + what + " " + id + " belongs to a different VCN")
	}

	return nil
}

func checkSecurityLists(b *InMemoryBackend, ids []string, vcnID string) ([]string, *APIError) {
	uniq := []string{}

	for _, id := range ids {
		if !slices.Contains(uniq, id) {
			uniq = append(uniq, id)
		}
	}

	if len(uniq) > maxSubnetSL {
		return nil, errParam("A subnet can have at most 5 security lists")
	}

	for _, id := range uniq {
		if aerr := refInVcn[*SecurityList](b, id, vcnID, "security list"); aerr != nil {
			return nil, aerr
		}
	}

	return uniq, nil
}

// subnetBlock validates one IPv4 block of a subnet: valid, inside a VCN block and clear of the other subnets.
func (b *InMemoryBackend) subnetBlock(v *Vcn, cidr, selfID string, taken []netip.Prefix) (netip.Prefix, *APIError) {
	p, aerr := parseV4(cidr, "Subnet CIDR block")
	if aerr != nil {
		return p, aerr
	}

	if !slices.ContainsFunc(v.CidrBlocks, func(c string) bool { return within(netip.MustParsePrefix(c), p) }) {
		return p, errParam("Subnet CIDR block " + cidr + " is not within the VCN CIDR blocks")
	}

	if slices.ContainsFunc(taken, p.Overlaps) {
		return p, errParam("Subnet CIDR block " + cidr + " overlaps another CIDR block of the subnet")
	}

	for _, s := range liveOf(b, inVcn[*Subnet](v.ID)) {
		if s.ID == selfID {
			continue
		}

		for _, c := range s.Ipv4CidrBlocks {
			if netip.MustParsePrefix(c).Overlaps(p) {
				return p, errParam("Subnet CIDR block " + cidr + " overlaps subnet " + s.ID)
			}
		}
	}

	return p, nil
}

func (b *InMemoryBackend) subnetCidrs(v *Vcn, in CreateSubnetIn) ([]netip.Prefix, *APIError) {
	cidrs := slices.Clone(in.Ipv4CidrBlocks)

	switch {
	case in.CidrBlock != "" && len(cidrs) == 0:
		cidrs = []string{in.CidrBlock}
	case in.CidrBlock != "" && !slices.Contains(cidrs, in.CidrBlock):
		return nil, errParam("cidrBlock must be one of ipv4CidrBlocks")
	case len(cidrs) == 0:
		return nil, errMissing("cidrBlock is required")
	}

	if len(cidrs) > maxSubnetCidrs {
		return nil, errLimit("A subnet can have at most 6 IPv4 CIDR blocks")
	}

	out := make([]netip.Prefix, 0, len(cidrs))

	for _, c := range cidrs {
		p, aerr := b.subnetBlock(v, c, "", out)
		if aerr != nil {
			return nil, aerr
		}

		out = append(out, p)
	}

	return out, nil
}

// vcnIpv6Prefixes lists every IPv6 prefix of the VCN a subnet /64 can come from.
func vcnIpv6Prefixes(v *Vcn) []string { return slices.Concat(v.Ipv6CidrBlocks, v.Ipv6PrivateCidrs) }

// subnetV6Block validates one /64 of a subnet against the VCN prefixes and the other subnets.
func (b *InMemoryBackend) subnetV6Block(v *Vcn, cidr, selfID string) *APIError {
	p, ok := parseAnyCidr(cidr)
	if !ok || !p.Addr().Is6() || p.Bits() != v6SubnetBits || p != p.Masked() {
		return errParam("ipv6CidrBlock must be a /64 IPv6 prefix")
	}

	if !slices.ContainsFunc(
		vcnIpv6Prefixes(v),
		func(c string) bool { return netip.MustParsePrefix(c).Contains(p.Addr()) },
	) {
		return errParam("ipv6CidrBlock " + cidr + " is not within the VCN IPv6 prefixes")
	}

	for _, s := range liveOf(b, inVcn[*Subnet](v.ID)) {
		if s.ID != selfID && slices.Contains(s.Ipv6CidrBlocks, cidr) {
			return errParam("ipv6CidrBlock " + cidr + " is already used by subnet " + s.ID)
		}
	}

	return nil
}

func (b *InMemoryBackend) subnetV6(v *Vcn, in CreateSubnetIn) ([]string, *APIError) {
	blocks := slices.Clone(in.Ipv6CidrBlocks)
	if in.Ipv6CidrBlock != "" && !slices.Contains(blocks, in.Ipv6CidrBlock) {
		blocks = slices.Insert(blocks, 0, in.Ipv6CidrBlock)
	}

	for i, c := range blocks {
		if slices.Contains(blocks[:i], c) {
			return nil, errParam("ipv6CidrBlocks lists " + c + " twice")
		}

		if aerr := b.subnetV6Block(v, c, ""); aerr != nil {
			return nil, aerr
		}
	}

	return blocks, nil
}

func (b *InMemoryBackend) subnetRefs(v *Vcn, in *CreateSubnetIn) *APIError {
	if in.RouteTableID == "" {
		in.RouteTableID = v.DefaultRouteTableID
	} else if aerr := refInVcn[*RouteTable](b, in.RouteTableID, v.ID, "route table"); aerr != nil {
		return aerr
	}

	if in.DhcpOptionsID == "" {
		in.DhcpOptionsID = v.DefaultDhcpOptionsID
	} else if aerr := refInVcn[*DhcpOptions](b, in.DhcpOptionsID, v.ID, "DHCP options"); aerr != nil {
		return aerr
	}

	if len(in.SecurityListIDs) == 0 {
		in.SecurityListIDs = []string{v.DefaultSecurityList}
	}

	ids, aerr := checkSecurityLists(b, in.SecurityListIDs, v.ID)
	in.SecurityListIDs = ids

	return aerr
}

func (b *InMemoryBackend) subnetDNS(v *Vcn, label string) (string, *APIError) {
	if label == "" {
		return "", nil
	}

	if aerr := validDNSLabel(label, "dnsLabel"); aerr != nil {
		return "", aerr
	}

	if v.DNSLabel == "" {
		return "", errParam("A subnet dnsLabel requires the VCN to have a dnsLabel")
	}

	dup := liveOf(b, func(s *Subnet) bool { return s.VcnID == v.ID && strings.EqualFold(s.DNSLabel, label) })
	if len(dup) > 0 {
		return "", errConflict("The dnsLabel " + label + " is already used by subnet " + dup[0].ID)
	}

	return strings.ToLower(label) + "." + v.VcnDomainName, nil
}

func routerMac(seq int64) string {
	h := fmt.Sprintf("%06X", seq%macSpace)

	return "00:00:17:" + h[0:2] + ":" + h[2:4] + ":" + h[4:6]
}

func (b *InMemoryBackend) subnetVcn(in CreateSubnetIn) (*Vcn, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, aerr
	}

	if in.VcnID == "" {
		return nil, errMissing("vcnId is required")
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, aerr
	}

	if in.AvailabilityDomain != "" && !adRe.MatchString(in.AvailabilityDomain) {
		return nil, errParam("availabilityDomain " + in.AvailabilityDomain + " is not a valid availability domain")
	}

	return getLive[*Vcn](b, in.VcnID)
}

// subnetAccess resolves prohibitPublicIpOnVnic and prohibitInternetIngress, which default together.
func subnetAccess(in CreateSubnetIn) ([2]bool, *APIError) {
	private := in.ProhibitPublicIPOnVnic != nil && *in.ProhibitPublicIPOnVnic
	ingress := private

	if in.ProhibitInternetIngress != nil {
		ingress = *in.ProhibitInternetIngress
	}

	if private && !ingress {
		return [2]bool{}, errParam("prohibitInternetIngress cannot be false when prohibitPublicIpOnVnic is true")
	}

	return [2]bool{private, ingress}, nil
}

// subnetChecks validates everything a new subnet depends on and returns its CIDR blocks and DNS domain.
func (b *InMemoryBackend) subnetChecks(v *Vcn, in *CreateSubnetIn) ([]netip.Prefix, []string, string, *APIError) {
	if aerr := b.vcnWritable(v.ID); aerr != nil {
		return nil, nil, "", aerr
	}

	ps, aerr := b.subnetCidrs(v, *in)
	if aerr != nil {
		return nil, nil, "", aerr
	}

	v6, aerr := b.subnetV6(v, *in)
	if aerr != nil {
		return nil, nil, "", aerr
	}

	domain, aerr := b.subnetDNS(v, in.DNSLabel)
	if aerr != nil {
		return nil, nil, "", aerr
	}

	if aerr = b.subnetRefs(v, in); aerr != nil {
		return nil, nil, "", aerr
	}

	if len(liveOf(b, inVcn[*Subnet](v.ID))) >= maxSubnets {
		return nil, nil, "", errLimit("The VCN subnet limit (300) has been reached")
	}

	return ps, v6, domain, nil
}

func prefixStrings(ps []netip.Prefix) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.String()
	}

	return out
}

// CreateSubnet creates a subnet in a VCN.
func (b *InMemoryBackend) CreateSubnet(in CreateSubnetIn) (resource, string, *APIError) {
	b.mu.Lock("CreateSubnet")
	defer b.mu.Unlock()

	v, aerr := b.subnetVcn(in)
	if aerr != nil {
		return nil, "", aerr
	}

	ps, v6, domain, aerr := b.subnetChecks(v, &in)
	if aerr != nil {
		return nil, "", aerr
	}

	access, aerr := subnetAccess(in)
	if aerr != nil {
		return nil, "", aerr
	}

	blocks := prefixStrings(ps)
	s := &Subnet{
		Base: b.newBase(kindSubnet, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID, CidrBlock: blocks[0],
		Ipv4CidrBlocks: blocks, DhcpOptionsID: in.DhcpOptionsID, RouteTableID: in.RouteTableID,
		SecurityListIDs: in.SecurityListIDs, DNSLabel: in.DNSLabel, SubnetDomainName: domain,
		ProhibitInternetIngress: access[1], ProhibitPublicIPOnVnic: access[0], VirtualRouterIP: hostAt(ps[0], 1),
	}
	s.VirtualRouterMac = routerMac(s.Internal.Seq)

	if in.AvailabilityDomain != "" {
		s.AvailabilityDomain = &in.AvailabilityDomain
	}

	s.setIpv6(v6)
	b.add(s)

	return s.view(), s.Internal.Etag, nil
}

// setIpv6 stores the subnet's IPv6 prefixes; the first one is the primary ipv6CidrBlock.
func (s *Subnet) setIpv6(blocks []string) {
	s.Ipv6CidrBlocks = blocks
	s.Ipv6CidrBlock, s.Ipv6VirtualRouterIP = "", ""

	if len(blocks) > 0 {
		s.Ipv6CidrBlock = blocks[0]
		s.Ipv6VirtualRouterIP = strings.TrimSuffix(blocks[0], "/64") + "1"
	}
}

// UpdateSubnetIn is the UpdateSubnet request body.
type UpdateSubnetIn struct {
	DhcpOptionsID   *string   `json:"dhcpOptionsId"`
	RouteTableID    *string   `json:"routeTableId"`
	CidrBlock       *string   `json:"cidrBlock"`
	Ipv6CidrBlock   *string   `json:"ipv6CidrBlock"`
	SecurityListIDs *[]string `json:"securityListIds"`
	Ipv6CidrBlocks  *[]string `json:"ipv6CidrBlocks"`
	UpdateIn
}

func optRef[T resource](b *InMemoryBackend, id *string, vcnID, what string) *APIError {
	if id == nil {
		return nil
	}

	return refInVcn[T](b, *id, vcnID, what)
}

// UpdateSubnet changes a subnet's name, tags, associations and CIDR blocks.
func (b *InMemoryBackend) UpdateSubnet(id, ifMatch string, in UpdateSubnetIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(s *Subnet) *APIError {
		if aerr := b.vcnWritable(s.VcnID); aerr != nil {
			return aerr
		}

		next := *s

		resized, aerr := b.applySubnetChanges(&next, in)
		if aerr != nil {
			return aerr
		}

		*s = next

		if resized {
			b.moveTo(s, statUpdating, statAvailable)
		}

		return nil
	})
}

// applySubnetChanges validates and applies an update to a copy of the subnet; it reports whether the CIDR changed.
func (b *InMemoryBackend) applySubnetChanges(next *Subnet, in UpdateSubnetIn) (bool, *APIError) {
	if aerr := optRef[*RouteTable](b, in.RouteTableID, next.VcnID, "route table"); aerr != nil {
		return false, aerr
	}

	if aerr := optRef[*DhcpOptions](b, in.DhcpOptionsID, next.VcnID, "DHCP options"); aerr != nil {
		return false, aerr
	}

	sls := next.SecurityListIDs

	if in.SecurityListIDs != nil {
		var aerr *APIError
		if sls, aerr = checkSecurityLists(b, *in.SecurityListIDs, next.VcnID); aerr != nil {
			return false, aerr
		}
	}

	if aerr := b.updateSubnetV6(next, in.Ipv6CidrBlock, in.Ipv6CidrBlocks); aerr != nil {
		return false, aerr
	}

	resized := in.CidrBlock != nil && *in.CidrBlock != next.CidrBlock
	if resized {
		if aerr := b.resizeSubnet(next, *in.CidrBlock); aerr != nil {
			return false, aerr
		}
	}

	next.SecurityListIDs = sls
	next.RouteTableID = cmpOr(in.RouteTableID, next.RouteTableID)
	next.DhcpOptionsID = cmpOr(in.DhcpOptionsID, next.DhcpOptionsID)

	return resized, nil
}

func (b *InMemoryBackend) subnetIPsUsed(s *Subnet) map[netip.Addr]bool {
	used := map[netip.Addr]bool{}

	for _, ip := range liveOf(b, func(x *PrivateIP) bool { return x.SubnetID == s.ID }) {
		if a, ok := parseAddr(ip.IPAddress); ok {
			used[a] = true
		}
	}

	return used
}

// resizeSubnet applies the documented UpdateSubnet cidrBlock rules to the subnet's primary IPv4 block.
func (b *InMemoryBackend) resizeSubnet(s *Subnet, cidr string) *APIError {
	v, aerr := getLive[*Vcn](b, s.VcnID)
	if aerr != nil {
		return aerr
	}

	next, aerr := b.subnetBlock(v, cidr, s.ID, prefixesOf(s.Ipv4CidrBlocks[1:]))
	if aerr != nil {
		return aerr
	}

	old := netip.MustParsePrefix(s.CidrBlock)
	if next.Addr() != old.Addr() {
		return errParam("The old and new CIDR blocks must use the same network address")
	}

	used := b.subnetIPsUsed(s)

	for a := range used {
		if old.Contains(a) && !next.Contains(a) {
			return errParam("The IP address " + a.String() + " is in use and would be outside the new CIDR block")
		}
	}

	if last := lastAddr(next); old.Contains(last) && used[last] {
		return errParam("The last address " + last.String() + " of the new CIDR block is in use")
	}

	s.CidrBlock = next.String()
	s.Ipv4CidrBlocks = slices.Clone(s.Ipv4CidrBlocks)
	s.Ipv4CidrBlocks[0] = s.CidrBlock

	return nil
}

func prefixesOf(cidrs []string) []netip.Prefix {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		out = append(out, netip.MustParsePrefix(c))
	}

	return out
}

func (b *InMemoryBackend) updateSubnetV6(s *Subnet, primary *string, list *[]string) *APIError {
	if primary == nil && list == nil {
		return nil
	}

	v, aerr := getLive[*Vcn](b, s.VcnID)
	if aerr != nil {
		return aerr
	}

	next := slices.Clone(s.Ipv6CidrBlocks)
	if list != nil {
		next = slices.Clone(*list)
	}

	if primary != nil && *primary != "" && !slices.Contains(next, *primary) {
		next = slices.Insert(next, 0, *primary)
	}

	for i, c := range next {
		if slices.Contains(next[:i], c) {
			return errParam("ipv6CidrBlocks lists " + c + " twice")
		}

		if !slices.Contains(s.Ipv6CidrBlocks, c) {
			if aerr = b.subnetV6Block(v, c, s.ID); aerr != nil {
				return aerr
			}
		}
	}

	for _, c := range s.Ipv6CidrBlocks {
		if !slices.Contains(next, c) {
			if aerr = b.ipv6PrefixFree(s, c); aerr != nil {
				return aerr
			}
		}
	}

	s.setIpv6(next)

	return nil
}

func cmpOr(p *string, def string) string {
	if p == nil {
		return def
	}

	return *p
}
