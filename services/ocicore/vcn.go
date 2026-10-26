package ocicore

import (
	"net/netip"
	"slices"
	"strings"
)

const vcnDomainSuffix = ".oraclevcn.com"

// CreateVcnIn is the CreateVcn request body.
type CreateVcnIn struct {
	TagsIn
	IsIpv6Enabled    *bool    `json:"isIpv6Enabled"`
	CompartmentID    string   `json:"compartmentId"`
	DisplayName      string   `json:"displayName"`
	CidrBlock        string   `json:"cidrBlock"`
	DNSLabel         string   `json:"dnsLabel"`
	CidrBlocks       []string `json:"cidrBlocks"`
	Ipv6PrivateCidrs []string `json:"ipv6PrivateCidrBlocks"`
}

func vcnCidrs(in CreateVcnIn) ([]string, *APIError) {
	cidrs := slices.Clone(in.CidrBlocks)

	switch {
	case in.CidrBlock != "" && len(cidrs) == 0:
		cidrs = []string{in.CidrBlock}
	case in.CidrBlock != "" && !slices.Contains(cidrs, in.CidrBlock):
		return nil, errParam("cidrBlock must be one of cidrBlocks")
	case len(cidrs) == 0:
		return nil, errMissing("cidrBlocks is required")
	}

	if len(cidrs) > maxVcnCidrs {
		return nil, errLimit("A VCN can have at most 16 IPv4 CIDR blocks")
	}

	var parsed []netip.Prefix

	for _, c := range cidrs {
		p, aerr := parseV4(c, "VCN CIDR block")
		if aerr != nil {
			return nil, aerr
		}

		if reservedV4(p) {
			return nil, errParam("VCN CIDR block " + c + " overlaps a reserved range")
		}

		if slices.ContainsFunc(parsed, p.Overlaps) {
			return nil, errParam("VCN CIDR block " + c + " overlaps another CIDR block of the VCN")
		}

		parsed = append(parsed, p)
	}

	return cidrs, nil
}

func validUlaBlocks(blocks []string) *APIError {
	ula := netip.MustParsePrefix("fc00::/7")

	for _, c := range blocks {
		p, ok := parseAnyCidr(c)
		if !ok || !p.Addr().Is6() || p.Bits() > v6SubnetBits || !ula.Contains(p.Addr()) {
			return errParam("ipv6PrivateCidrBlocks entry " + c + " must be an IPv6 ULA prefix of /64 or larger")
		}
	}

	return nil
}

func defaultIngress(vcnCidrs []string, v6 bool) []IngressRule {
	f := false
	three, four := 3, 4
	rules := []IngressRule{
		{Protocol: "6", Source: anyV4, SourceType: ctCIDR, IsStateless: &f,
			TCPOptions: &PortOptions{DestinationPortRange: &PortRange{Min: sshPort, Max: sshPort}}},
		{Protocol: "1", Source: anyV4, SourceType: ctCIDR, IsStateless: &f,
			IcmpOptions: &IcmpOptions{Type: three, Code: &four}},
	}

	for _, c := range vcnCidrs {
		rules = append(rules, IngressRule{Protocol: "1", Source: c, SourceType: ctCIDR, IsStateless: &f,
			IcmpOptions: &IcmpOptions{Type: three}})
	}

	if v6 {
		zero, two := 0, 2
		rules = append(rules,
			IngressRule{Protocol: "6", Source: anyV6, SourceType: ctCIDR, IsStateless: &f,
				TCPOptions: &PortOptions{DestinationPortRange: &PortRange{Min: sshPort, Max: sshPort}}},
			IngressRule{Protocol: "58", Source: anyV6, SourceType: ctCIDR, IsStateless: &f,
				IcmpOptions: &IcmpOptions{Type: two, Code: &zero}})
	}

	return rules
}

func defaultEgress(v6 bool) []EgressRule {
	f := false
	rules := []EgressRule{{Protocol: "all", Destination: anyV4, DestinationType: ctCIDR, IsStateless: &f}}

	if v6 {
		rules = append(
			rules,
			EgressRule{Protocol: "all", Destination: anyV6, DestinationType: ctCIDR, IsStateless: &f},
		)
	}

	return rules
}

// CreateVcn creates a VCN with its default route table, security list and DHCP options.
func (b *InMemoryBackend) CreateVcn(in CreateVcnIn) (resource, string, *APIError) {
	b.mu.Lock("CreateVcn")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	cidrs, aerr := vcnCidrs(in)
	if aerr != nil {
		return nil, "", aerr
	}

	if in.DNSLabel != "" {
		if aerr = validDNSLabel(in.DNSLabel, "dnsLabel"); aerr != nil {
			return nil, "", aerr
		}
	}

	if aerr = validUlaBlocks(in.Ipv6PrivateCidrs); aerr != nil {
		return nil, "", aerr
	}

	if len(liveOf(b, func(*Vcn) bool { return true })) >= maxVcns {
		return nil, "", errLimit("The region VCN limit (50) has been reached")
	}

	v := &Vcn{
		Base: b.newBase(kindVcn, in.CompartmentID, in.DisplayName, in.TagsIn), CidrBlock: cidrs[0], CidrBlocks: cidrs,
		DNSLabel: in.DNSLabel, Ipv6PrivateCidrs: in.Ipv6PrivateCidrs,
	}

	v6 := in.IsIpv6Enabled != nil && *in.IsIpv6Enabled
	if v6 {
		b.ipv6Seq++
		v.Ipv6CidrBlocks = []string{v6Block(b.ipv6Seq)}
	}

	search := []DhcpOption{{Type: "DomainNameServer", ServerType: "VcnLocalPlusInternet"}}
	domainType := domainCustom

	if in.DNSLabel != "" {
		v.VcnDomainName = strings.ToLower(in.DNSLabel) + vcnDomainSuffix
		search = append(search, DhcpOption{Type: "SearchDomain", SearchDomainNames: []string{v.VcnDomainName}})
		domainType = "VCN_DOMAIN"
	}

	name := v.DisplayName
	rt := &RouteTable{
		Base: b.newBase(kindRT, in.CompartmentID, "Default Route Table for "+name, TagsIn{}), VcnID: v.ID,
		RouteRules: []RouteRule{},
	}
	sl := &SecurityList{
		Base: b.newBase(kindSL, in.CompartmentID, "Default Security List for "+name, TagsIn{}), VcnID: v.ID,
		IngressRules: defaultIngress(cidrs, v6), EgressRules: defaultEgress(v6),
	}
	dh := &DhcpOptions{
		Base: b.newBase(kindDHCP, in.CompartmentID, "Default DHCP Options for "+name, TagsIn{}), VcnID: v.ID,
		Options: search, DomainNameType: domainType,
	}

	v.DefaultRouteTableID, v.DefaultSecurityList, v.DefaultDhcpOptionsID = rt.ID, sl.ID, dh.ID

	for _, r := range []resource{v, rt, sl, dh} {
		b.add(r)
	}

	return v.view(), v.Internal.Etag, nil
}

// UpdateVcnIn is the UpdateVcn request body.
type UpdateVcnIn = UpdateIn

// UpdateVcn changes a VCN's name and tags.
func (b *InMemoryBackend) UpdateVcn(id, ifMatch string, in UpdateVcnIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*Vcn) *APIError { return nil })
}

// updateWith is the shared update skeleton: state and etag checks, a type specific mutation, name and tags.
func updateWith[T resource](
	b *InMemoryBackend, id, ifMatch string, in UpdateIn, apply func(T) *APIError,
) (resource, string, *APIError) {
	b.mu.Lock("update")
	defer b.mu.Unlock()

	r, aerr := getLive[T](b, id)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(r, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = validName(*in.DisplayName); aerr != nil {
			return nil, "", aerr
		}
	}

	if aerr = apply(r); aerr != nil {
		return nil, "", aerr
	}

	b.updateCommon(r, in)

	return r.view(), r.base().Internal.Etag, nil
}
