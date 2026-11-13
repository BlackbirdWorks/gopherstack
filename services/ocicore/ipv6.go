package ocicore

import (
	"encoding/binary"
	"net/http"
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	kindIpv6        = "ipv6"
	maxIpv6PerVnic  = 32
	ipv6HostOffset  = 2
	ipv6HostBytes   = 8
	ipv6FullBits    = 128
	ipv6StateAssign = "ASSIGNED"
)

// Ipv6 is an IPv6 address inside a subnet's /64.
type Ipv6 struct {
	CidrPrefixLength *int `json:"cidrPrefixLength,omitempty"`
	Base
	IPAddress    string `json:"ipAddress"`
	SubnetID     string `json:"subnetId"`
	VnicID       string `json:"vnicId,omitempty"`
	IPState      string `json:"ipState"`
	Lifetime     string `json:"lifetime"`
	RouteTableID string `json:"routeTableId,omitempty"`
	Hostname     string `json:"hostname,omitempty"`
}

func (v *Ipv6) vcnRef() string { return "" }

func (v *Ipv6) view() resource {
	c := *v
	c.Internal = nil

	return &c
}

// addrAt returns the address n hosts into a /64.
func addrAt(p netip.Prefix, n uint64) netip.Addr {
	raw := p.Masked().Addr().As16()
	lo := binary.BigEndian.Uint64(raw[ipv6HostBytes:]) + n
	binary.BigEndian.PutUint64(raw[ipv6HostBytes:], lo)

	return netip.AddrFrom16(raw)
}

// ipv6PrefixFree refuses to release a subnet prefix while an IPv6 address of the subnet lives in it.
func (b *InMemoryBackend) ipv6PrefixFree(s *Subnet, cidr string) *APIError {
	p := netip.MustParsePrefix(cidr)

	for _, ip := range liveOf(b, func(x *Ipv6) bool { return x.SubnetID == s.ID }) {
		if a, ok := parseAddr(ip.IPAddress); ok && p.Contains(a) {
			return errConflict("The IPv6 prefix " + cidr + " is in use by IPv6 " + ip.ID)
		}
	}

	return nil
}

// CreateIpv6In is the CreateIpv6 request body.
type CreateIpv6In struct {
	TagsIn
	CidrPrefixLength *int    `json:"cidrPrefixLength"`
	Hostname         *string `json:"hostname"`
	DisplayName      string  `json:"displayName"`
	IPAddress        string  `json:"ipAddress"`
	VnicID           string  `json:"vnicId"`
	SubnetID         string  `json:"subnetId"`
	Lifetime         string  `json:"lifetime"`
	RouteTableID     string  `json:"routeTableId"`
	Ipv6SubnetCidr   string  `json:"ipv6SubnetCidr"`
}

// ipv6Target resolves the subnet and VNIC an IPv6 is created in.
func (b *InMemoryBackend) ipv6Target(in CreateIpv6In) (*Subnet, *Vnic, *APIError) {
	if in.VnicID == "" && in.SubnetID == "" {
		return nil, nil, errMissing("vnicId or subnetId is required")
	}

	var vnic *Vnic

	subnetID := in.SubnetID

	if in.VnicID != "" {
		v, aerr := getLive[*Vnic](b, in.VnicID)
		if aerr != nil {
			return nil, nil, aerr
		}

		if subnetID != "" && subnetID != v.SubnetID {
			return nil, nil, errParam("subnetId does not match the subnet of the VNIC")
		}

		vnic, subnetID = v, v.SubnetID
	}

	s, aerr := getLive[*Subnet](b, subnetID)
	if aerr != nil {
		return nil, nil, aerr
	}

	if len(s.Ipv6CidrBlocks) == 0 {
		return nil, nil, errParam("The subnet " + s.ID + " has no IPv6 prefix")
	}

	return s, vnic, nil
}

// pickIpv6 chooses the requested address, or the lowest free one, inside the subnet's /64 prefixes.
func (b *InMemoryBackend) pickIpv6(s *Subnet, cidr, want string) (string, *APIError) {
	prefixes := s.Ipv6CidrBlocks

	if cidr != "" {
		if !slices.Contains(prefixes, cidr) {
			return "", errParam("ipv6SubnetCidr " + cidr + " is not an IPv6 prefix of the subnet")
		}

		prefixes = []string{cidr}
	}

	used := map[string]bool{}
	for _, ip := range liveOf(b, func(x *Ipv6) bool { return x.SubnetID == s.ID }) {
		used[ip.IPAddress] = true
	}

	if want != "" {
		return wantedIpv6(prefixes, want, used)
	}

	for _, c := range prefixes {
		p := netip.MustParsePrefix(c)

		for n := uint64(ipv6HostOffset); ; n++ {
			if a := addrAt(p, n); !used[a.String()] {
				return a.String(), nil
			}
		}
	}

	return "", errLimit("The subnet " + s.ID + " has no free IPv6 addresses")
}

func wantedIpv6(prefixes []string, want string, used map[string]bool) (string, *APIError) {
	a, ok := parseAddr(want)
	if !ok || !a.Is6() {
		return "", errParam("ipAddress " + want + " is not a valid IPv6 address")
	}

	i := slices.IndexFunc(prefixes, func(c string) bool { return netip.MustParsePrefix(c).Contains(a) })
	if i < 0 {
		return "", errParam("ipAddress " + want + " is not within the subnet IPv6 prefix")
	}

	if base := netip.MustParsePrefix(prefixes[i]); a == base.Addr() || a == addrAt(base, 1) {
		return "", errParam("ipAddress " + want + " is reserved by the Networking service")
	}

	if used[a.String()] {
		return "", errConflict("The IPv6 address " + a.String() + " is already in use")
	}

	return a.String(), nil
}

func (b *InMemoryBackend) checkIpv6Hostname(s *Subnet, label *string, selfID string) *APIError {
	if label == nil || *label == "" {
		return nil
	}

	if !hostLabelRe.MatchString(*label) {
		return errParam("hostname must be a valid DNS label")
	}

	if s.DNSLabel == "" {
		return errParam("hostname requires the subnet " + s.ID + " to have a dnsLabel")
	}

	dup := liveOf(b, func(x *Ipv6) bool {
		return x.SubnetID == s.ID && x.ID != selfID && strings.EqualFold(x.Hostname, *label)
	})
	if len(dup) > 0 {
		return errConflict("The hostname " + *label + " is already used in subnet " + s.ID)
	}

	return nil
}

// ipv6Checks validates a CreateIpv6 request against its target and returns the lifetime to use.
func (b *InMemoryBackend) ipv6Checks(in CreateIpv6In, s *Subnet, vnic *Vnic) (string, *APIError) {
	life := cmpOrStr(in.Lifetime, lifetimeEphemeral)
	if life != lifetimeEphemeral && life != lifetimeReserved {
		return "", errParam("lifetime must be EPHEMERAL or RESERVED")
	}

	if vnic == nil && life != lifetimeReserved {
		return "", errParam("An IPv6 without a VNIC must be RESERVED")
	}

	if vnic != nil && len(liveOf(b, func(x *Ipv6) bool { return x.VnicID == vnic.ID })) >= maxIpv6PerVnic {
		return "", errLimit("A VNIC can have at most 32 IPv6 addresses")
	}

	if in.RouteTableID != "" {
		if aerr := refInVcn[*RouteTable](b, in.RouteTableID, s.VcnID, "route table"); aerr != nil {
			return "", aerr
		}
	}

	return life, b.checkIpv6Hostname(s, in.Hostname, "")
}

// newIpv6 validates and creates an IPv6; callers hold the write lock.
func (b *InMemoryBackend) newIpv6(in CreateIpv6In) (*Ipv6, *APIError) {
	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, aerr
	}

	if in.CidrPrefixLength != nil && *in.CidrPrefixLength != ipv6FullBits {
		return nil, errParam("cidrPrefixLength must be 128")
	}

	s, vnic, aerr := b.ipv6Target(in)
	if aerr != nil {
		return nil, aerr
	}

	life, aerr := b.ipv6Checks(in, s, vnic)
	if aerr != nil {
		return nil, aerr
	}

	addr, aerr := b.pickIpv6(s, in.Ipv6SubnetCidr, in.IPAddress)
	if aerr != nil {
		return nil, aerr
	}

	comp := s.CompartmentID
	if vnic != nil {
		comp = vnic.CompartmentID
	}

	ip := &Ipv6{
		Base: b.newBase(kindIpv6, comp, in.DisplayName, in.TagsIn), IPAddress: addr, SubnetID: s.ID,
		IPState: statAvailable, Lifetime: life, RouteTableID: in.RouteTableID, CidrPrefixLength: in.CidrPrefixLength,
	}
	if in.Hostname != nil {
		ip.Hostname = *in.Hostname
	}

	b.ready(ip)

	if vnic != nil {
		ip.VnicID, ip.IPState = vnic.ID, ipv6StateAssign
	}

	b.add(ip)
	b.refreshVnic(ip.VnicID)

	return ip, nil
}

// CreateIpv6 creates an IPv6 for a VNIC, or a reserved unassigned one for a subnet.
func (b *InMemoryBackend) CreateIpv6(in CreateIpv6In) (resource, string, *APIError) {
	ip, aerr := b.newIpv6(in)
	if aerr != nil {
		return nil, "", aerr
	}

	return ip.view(), ip.Internal.Etag, nil
}

// UpdateIpv6In is the UpdateIpv6 request body.
type UpdateIpv6In struct {
	VnicID       *string `json:"vnicId"`
	Hostname     *string `json:"hostname"`
	RouteTableID *string `json:"routeTableId"`
	Lifetime     *string `json:"lifetime"`
	UpdateIn
}

// UpdateIpv6 renames an IPv6, moves it to another VNIC of the same subnet, or changes its lifetime or route table.
func (b *InMemoryBackend) UpdateIpv6(id, ifMatch string, in UpdateIpv6In) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(ip *Ipv6) *APIError {
		s, aerr := getLive[*Subnet](b, ip.SubnetID)
		if aerr != nil {
			return aerr
		}

		next := *ip
		old := ip.VnicID

		if aerr = b.applyIpv6Update(s, &next, in); aerr != nil {
			return aerr
		}

		*ip = next

		b.refreshVnic(old)
		b.refreshVnic(ip.VnicID)

		return nil
	})
}

func applyIpv6Lifetime(ip *Ipv6, in UpdateIpv6In) *APIError {
	if in.Lifetime == nil {
		return nil
	}

	switch *in.Lifetime {
	case lifetimeReserved:
		ip.Lifetime = lifetimeReserved
	case lifetimeEphemeral:
		if ip.VnicID == "" && (in.VnicID == nil || *in.VnicID == "") {
			return errParam("An unassigned IPv6 cannot be EPHEMERAL")
		}

		ip.Lifetime = lifetimeEphemeral
	default:
		return errParam("lifetime must be EPHEMERAL or RESERVED")
	}

	return nil
}

func (b *InMemoryBackend) applyIpv6Update(s *Subnet, ip *Ipv6, in UpdateIpv6In) *APIError {
	if aerr := applyIpv6Lifetime(ip, in); aerr != nil {
		return aerr
	}

	if in.VnicID != nil && *in.VnicID != ip.VnicID {
		if ip.Lifetime == lifetimeEphemeral && ip.VnicID != "" {
			return errParam("An ephemeral IPv6 cannot be moved to another VNIC")
		}

		if aerr := b.moveIpv6(s, ip, *in.VnicID); aerr != nil {
			return aerr
		}
	}

	if in.RouteTableID != nil && *in.RouteTableID != "" {
		if aerr := refInVcn[*RouteTable](b, *in.RouteTableID, s.VcnID, "route table"); aerr != nil {
			return aerr
		}

		ip.RouteTableID = *in.RouteTableID
	}

	if in.Hostname == nil {
		return nil
	}

	if aerr := b.checkIpv6Hostname(s, in.Hostname, ip.ID); aerr != nil {
		return aerr
	}

	ip.Hostname = *in.Hostname

	return nil
}

func (b *InMemoryBackend) moveIpv6(s *Subnet, ip *Ipv6, vnicID string) *APIError {
	if vnicID == "" {
		return errParam("Use Ipv6VnicDetach to unassign an IPv6")
	}

	v, aerr := getLive[*Vnic](b, vnicID)
	if aerr != nil {
		return aerr
	}

	if v.SubnetID != s.ID {
		return errParam("The VNIC " + v.ID + " is in a different subnet")
	}

	if len(liveOf(b, func(x *Ipv6) bool { return x.VnicID == v.ID })) >= maxIpv6PerVnic {
		return errLimit("A VNIC can have at most 32 IPv6 addresses")
	}

	ip.VnicID, ip.IPState = v.ID, ipv6StateAssign

	return nil
}

// Ipv6VnicDetach unassigns a reserved IPv6 from its VNIC.
func (b *InMemoryBackend) Ipv6VnicDetach(id, ifMatch string) (resource, string, *APIError) {
	b.mu.Lock("Ipv6VnicDetach")
	defer b.mu.Unlock()

	ip, aerr := getLive[*Ipv6](b, id)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(ip, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	if ip.VnicID == "" {
		return nil, "", errState("The IPv6 " + id + " is not assigned to a VNIC")
	}

	if ip.Lifetime == lifetimeEphemeral {
		return nil, "", errParam("An ephemeral IPv6 cannot be unassigned: delete it instead")
	}

	old := ip.VnicID
	ip.VnicID, ip.IPState = "", statAvailable
	b.bump(ip)
	b.refreshVnic(old)

	return ip.view(), ip.Internal.Etag, nil
}

// endVnicIpv6 releases the IPv6 addresses of a VNIC that is being terminated.
func (b *InMemoryBackend) endVnicIpv6(vnicID string) {
	for _, ip := range liveOf(b, func(x *Ipv6) bool { return x.VnicID == vnicID }) {
		if ip.Lifetime == lifetimeReserved {
			ip.VnicID, ip.IPState = "", statAvailable
			b.bump(ip)

			continue
		}

		b.markTerminating(ip)
	}
}

func (b *InMemoryBackend) vnicIpv6(vnicID string) []string {
	ips := liveOf(b, func(x *Ipv6) bool { return x.VnicID == vnicID })
	out := make([]string, 0, len(ips))

	for _, ip := range ips {
		out = append(out, ip.IPAddress)
	}

	slices.Sort(out)

	return out
}

// Ipv6ListOpts holds the ListIpv6s filters.
type Ipv6ListOpts struct {
	IPAddress, SubnetID, VnicID, IPState, Lifetime, Page string
	Limit                                                int
}

// ListIpv6s lists IPv6 addresses by subnet, VNIC, or address plus subnet.
func (b *InMemoryBackend) ListIpv6s(o Ipv6ListOpts) (page.Page[resource], *APIError) {
	if o.SubnetID == "" && o.VnicID == "" {
		return page.Page[resource]{}, errMissing("subnetId or vnicId is required")
	}

	b.mu.RLock("ListIpv6s")
	defer b.mu.RUnlock()

	all := liveOf(b, func(x *Ipv6) bool {
		return (o.SubnetID == "" || x.SubnetID == o.SubnetID) && (o.VnicID == "" || x.VnicID == o.VnicID) &&
			(o.IPAddress == "" || x.IPAddress == o.IPAddress) && (o.IPState == "" || x.IPState == o.IPState) &&
			(o.Lifetime == "" || x.Lifetime == o.Lifetime)
	})
	sortRes(all, ListOpts{Order: orderAsc})

	pg := page.New(all, o.Page, o.Limit, defaultListLimit)
	out := make([]resource, 0, len(pg.Data))

	for _, ip := range pg.Data {
		out = append(out, ip.view())
	}

	return page.Page[resource]{Data: out, Next: pg.Next}, nil
}

func ipv6Entries() []entry {
	item := idPath("/ipv6", "ipv6Id")

	return []entry{
		{createOp((*InMemoryBackend).CreateIpv6), http.MethodPost, "/ipv6", "CreateIpv6"},
		{(*Handler).listIpv6s, http.MethodGet, "/ipv6", "ListIpv6s"},
		{getOp[*Ipv6]("ipv6Id"), http.MethodGet, item, "GetIpv6"},
		{updateOp("ipv6Id", (*InMemoryBackend).UpdateIpv6), http.MethodPut, item, "UpdateIpv6"},
		{deleteOp[*Ipv6]("ipv6Id"), http.MethodDelete, item, "DeleteIpv6"},
		{(*Handler).ipv6VnicDetach, http.MethodPost, item + "/actions/detach", "Ipv6VnicDetach"},
	}
}

func (h *Handler) listIpv6s(q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.ListIpv6s(Ipv6ListOpts{
		IPAddress: q.query("ipAddress"), SubnetID: q.query("subnetId"), VnicID: q.query("vnicId"),
		IPState: q.query("ipState"), Lifetime: q.query("lifetime"), Page: o.Page, Limit: o.Limit,
	})
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) ipv6VnicDetach(q *req) error {
	v, etag, aerr := h.Backend.Ipv6VnicDetach(q.param("ipv6Id"), q.ifMatch())

	return send(q, v, etag, aerr)
}
