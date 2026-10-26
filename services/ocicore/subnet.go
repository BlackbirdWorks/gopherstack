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

func (b *InMemoryBackend) subnetCidr(v *Vcn, in CreateSubnetIn) (netip.Prefix, *APIError) {
	cidr := in.CidrBlock

	switch {
	case cidr == "" && len(in.Ipv4CidrBlocks) == 1:
		cidr = in.Ipv4CidrBlocks[0]
	case len(in.Ipv4CidrBlocks) > 1:
		return netip.Prefix{}, errParam("Only one IPv4 CIDR block per subnet is supported")
	case cidr == "":
		return netip.Prefix{}, errMissing("cidrBlock is required")
	}

	p, aerr := parseV4(cidr, "Subnet CIDR block")
	if aerr != nil {
		return p, aerr
	}

	inside := slices.ContainsFunc(v.CidrBlocks, func(c string) bool {
		vp := netip.MustParsePrefix(c)

		return vp.Bits() <= p.Bits() && vp.Contains(p.Addr())
	})
	if !inside {
		return p, errParam("Subnet CIDR block " + cidr + " is not within the VCN CIDR blocks")
	}

	for _, s := range liveOf(b, inVcn[*Subnet](v.ID)) {
		if netip.MustParsePrefix(s.CidrBlock).Overlaps(p) {
			return p, errParam("Subnet CIDR block " + cidr + " overlaps subnet " + s.ID)
		}
	}

	return p, nil
}

func (b *InMemoryBackend) subnetV6(v *Vcn, cidr string) *APIError {
	if cidr == "" {
		return nil
	}

	p, ok := parseAnyCidr(cidr)
	if !ok || !p.Addr().Is6() || p.Bits() != v6SubnetBits || p != p.Masked() {
		return errParam("ipv6CidrBlock must be a /64 IPv6 prefix")
	}

	if !slices.ContainsFunc(
		v.Ipv6CidrBlocks,
		func(c string) bool { return netip.MustParsePrefix(c).Contains(p.Addr()) },
	) {
		return errParam("ipv6CidrBlock " + cidr + " is not within the VCN IPv6 prefixes")
	}

	for _, s := range liveOf(b, inVcn[*Subnet](v.ID)) {
		if s.Ipv6CidrBlock == cidr {
			return errParam("ipv6CidrBlock " + cidr + " is already used by subnet " + s.ID)
		}
	}

	return nil
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

// subnetChecks validates everything a new subnet depends on and returns its CIDR and DNS domain.
func (b *InMemoryBackend) subnetChecks(v *Vcn, in *CreateSubnetIn) (netip.Prefix, string, *APIError) {
	p, aerr := b.subnetCidr(v, *in)
	if aerr != nil {
		return p, "", aerr
	}

	if aerr = b.subnetV6(v, in.Ipv6CidrBlock); aerr != nil {
		return p, "", aerr
	}

	domain, aerr := b.subnetDNS(v, in.DNSLabel)
	if aerr != nil {
		return p, "", aerr
	}

	if aerr = b.subnetRefs(v, in); aerr != nil {
		return p, "", aerr
	}

	if len(liveOf(b, inVcn[*Subnet](v.ID))) >= maxSubnets {
		return p, "", errLimit("The VCN subnet limit (300) has been reached")
	}

	return p, domain, nil
}

// CreateSubnet creates a subnet in a VCN.
func (b *InMemoryBackend) CreateSubnet(in CreateSubnetIn) (resource, string, *APIError) {
	b.mu.Lock("CreateSubnet")
	defer b.mu.Unlock()

	v, aerr := b.subnetVcn(in)
	if aerr != nil {
		return nil, "", aerr
	}

	p, domain, aerr := b.subnetChecks(v, &in)
	if aerr != nil {
		return nil, "", aerr
	}

	access, aerr := subnetAccess(in)
	if aerr != nil {
		return nil, "", aerr
	}

	s := &Subnet{
		Base: b.newBase(kindSubnet, in.CompartmentID, in.DisplayName, in.TagsIn), VcnID: v.ID, CidrBlock: p.String(),
		Ipv4CidrBlocks: []string{p.String()}, DhcpOptionsID: in.DhcpOptionsID, RouteTableID: in.RouteTableID,
		SecurityListIDs: in.SecurityListIDs, DNSLabel: in.DNSLabel, SubnetDomainName: domain,
		ProhibitInternetIngress: access[1], ProhibitPublicIPOnVnic: access[0], VirtualRouterIP: hostAt(p, 1),
	}
	s.VirtualRouterMac = routerMac(s.Internal.Seq)

	if in.AvailabilityDomain != "" {
		s.AvailabilityDomain = &in.AvailabilityDomain
	}

	if in.Ipv6CidrBlock != "" {
		s.Ipv6CidrBlock, s.Ipv6CidrBlocks = in.Ipv6CidrBlock, []string{in.Ipv6CidrBlock}
		s.Ipv6VirtualRouterIP = strings.TrimSuffix(in.Ipv6CidrBlock, "/64") + "1"
	}

	b.add(s)

	return s.view(), s.Internal.Etag, nil
}

// UpdateSubnetIn is the UpdateSubnet request body.
type UpdateSubnetIn struct {
	DhcpOptionsID   *string   `json:"dhcpOptionsId"`
	RouteTableID    *string   `json:"routeTableId"`
	CidrBlock       *string   `json:"cidrBlock"`
	SecurityListIDs *[]string `json:"securityListIds"`
	UpdateIn
}

func optRef[T resource](b *InMemoryBackend, id *string, vcnID, what string) *APIError {
	if id == nil {
		return nil
	}

	return refInVcn[T](b, *id, vcnID, what)
}

// UpdateSubnet changes a subnet's name, tags and associations.
func (b *InMemoryBackend) UpdateSubnet(id, ifMatch string, in UpdateSubnetIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(s *Subnet) *APIError {
		if in.CidrBlock != nil && *in.CidrBlock != s.CidrBlock {
			return errParam("Modifying a subnet CIDR block is not supported")
		}

		if aerr := optRef[*RouteTable](b, in.RouteTableID, s.VcnID, "route table"); aerr != nil {
			return aerr
		}

		if aerr := optRef[*DhcpOptions](b, in.DhcpOptionsID, s.VcnID, "DHCP options"); aerr != nil {
			return aerr
		}

		sls := s.SecurityListIDs

		if in.SecurityListIDs != nil {
			var aerr *APIError
			if sls, aerr = checkSecurityLists(b, *in.SecurityListIDs, s.VcnID); aerr != nil {
				return aerr
			}
		}

		s.SecurityListIDs = sls
		s.RouteTableID = cmpOr(in.RouteTableID, s.RouteTableID)
		s.DhcpOptionsID = cmpOr(in.DhcpOptionsID, s.DhcpOptionsID)

		return nil
	})
}

func cmpOr(p *string, def string) string {
	if p == nil {
		return def
	}

	return *p
}
