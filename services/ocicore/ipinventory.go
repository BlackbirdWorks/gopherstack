package ocicore

import (
	"cmp"
	"math"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	addrPrivateV4 = "Private_IPv4"
	addrPublicV4  = "Oracle_Allocated_Public_IPv4"
	addrULAv6     = "ULA_IPv6"
	addrGUAv6     = "Oracle_Allocated_GUA_IPv6"

	invDone         = "DONE"
	reservedPerCidr = 3
	percent         = 100.0
	percentRound    = 100.0
	invRequestTail  = 24
	resourceKind    = "Resource"
	maxInvRequests  = 200
)

// InventoryDetails is the ListIpInventory request body.
type InventoryDetails struct {
	Utilization         *float64 `json:"utilization"`
	OverlappingVcnsOnly *bool    `json:"overlappingVcnsOnly"`
	OverrideFilters     *bool    `json:"overrideFilters"`
	PaginationOffset    *int     `json:"paginationOffset"`
	PaginationLimit     *int     `json:"paginationLimit"`
	SearchKeyword       string   `json:"searchKeyword"`
	SortBy              string   `json:"sortBy"`
	SortOrder           string   `json:"sortOrder"`
	RegionList          []string `json:"regionList"`
	CompartmentList     []string `json:"compartmentList"`
	AddressTypeList     []string `json:"addressTypeList"`
	ResourceTypeList    []string `json:"resourceTypeList"`
}

type invCidr struct {
	IPCidrBlock string  `json:"ipCidrBlock"`
	Utilization float64 `json:"utilization"`
}

type invAddress struct {
	IPAddress   string `json:"ipAddress"`
	VnicID      string `json:"vnicId"`
	VnicName    string `json:"vnicName"`
	DNSHostName string `json:"dnsHostName,omitempty"`
}

type invResource struct {
	ResourceName        string       `json:"resourceName"`
	ResourceType        string       `json:"resourceType"`
	MacAddress          string       `json:"macAddress"`
	Region              string       `json:"region"`
	CompartmentID       string       `json:"compartmentId"`
	IPAddressCollection []invAddress `json:"ipAddressCollection"`
}

type invSubnet struct {
	SubnetID                      string        `json:"subnetId"`
	SubnetName                    string        `json:"subnetName"`
	ResourceType                  string        `json:"resourceType"`
	DNSDomainName                 string        `json:"dnsDomainName,omitempty"`
	Region                        string        `json:"region"`
	CompartmentID                 string        `json:"compartmentId"`
	InventorySubnetCidrCollection []invCidr     `json:"inventorySubnetCidrCollection"`
	InventoryResourceSummary      []invResource `json:"inventoryResourceSummary"`
}

type invVcn struct {
	VcnID                           string      `json:"vcnId"`
	VcnName                         string      `json:"vcnName"`
	ResourceType                    string      `json:"resourceType"`
	DNSDomainName                   string      `json:"dnsDomainName,omitempty"`
	Region                          string      `json:"region"`
	CompartmentID                   string      `json:"compartmentId"`
	InventoryVcnCidrBlockCollection []invCidr   `json:"inventoryVcnCidrBlockCollection"`
	InventorySubnetCollection       []invSubnet `json:"inventorySubnetcollection"`
}

// InventoryOut is the IpInventoryCollection response.
type InventoryOut struct {
	LastUpdatedTimestamp   time.Time `json:"lastUpdatedTimestamp"`
	Message                string    `json:"message,omitempty"`
	InventoryVcnCollection []invVcn  `json:"inventoryVcnCollection"`
	Count                  int       `json:"count"`
	CompartmentsPerTenant  int       `json:"compartmentsPerTenant"`
}

// SubnetIPOut is one IP of the subnet inventory.
type SubnetIPOut struct {
	AssignedTime         *time.Time `json:"assignedTime,omitempty"`
	IsPrimary            *bool      `json:"isPrimary,omitempty"`
	IPID                 string     `json:"ipId"`
	IPAddress            string     `json:"ipAddress"`
	IPAddressLifetime    string     `json:"ipAddressLifetime,omitempty"`
	ParentCidr           string     `json:"parentCidr"`
	AssociatedPublicIP   string     `json:"associatedPublicIp,omitempty"`
	PublicIPLifetime     string     `json:"publicIpLifetime,omitempty"`
	AssociatedPublicPool string     `json:"associatedPublicIpPool,omitempty"`
	DNSHostName          string     `json:"dnsHostName,omitempty"`
	AssignedResourceName string     `json:"assignedResourceName,omitempty"`
	AssignedResourceType string     `json:"assignedResourceType,omitempty"`
	AddressType          string     `json:"addressType"`
}

// SubnetInventoryOut is the IpInventorySubnetResourceCollection response.
type SubnetInventoryOut struct {
	LastUpdatedTimestamp time.Time     `json:"lastUpdatedTimestamp"`
	CompartmentID        string        `json:"compartmentId"`
	Message              string        `json:"message,omitempty"`
	Items                []SubnetIPOut `json:"ipInventorySubnetResourceSummary"`
	Count                int           `json:"count"`
}

// CidrUtilizationItem is one entry of the subnet CIDR utilization.
type CidrUtilizationItem struct {
	Cidr        string  `json:"cidr"`
	AddressType string  `json:"addressType"`
	Utilization float64 `json:"utilization"`
}

// CidrUtilizationOut is the IpInventoryCidrUtilizationCollection response.
type CidrUtilizationOut struct {
	LastUpdatedTimestamp time.Time             `json:"lastUpdatedTimestamp"`
	CompartmentID        string                `json:"compartmentId"`
	Message              string                `json:"message,omitempty"`
	Items                []CidrUtilizationItem `json:"ipInventoryCidrUtilizationSummary"`
	Count                int                   `json:"count"`
}

// OverlapItem is one overlapping VCN CIDR pair.
type OverlapItem struct {
	OverlappingVcnID   string `json:"overlappingVcnId"`
	OverlappingVcnName string `json:"overlappingVcnName"`
	OverlappingCidr    string `json:"overlappingCidr"`
	Cidr               string `json:"cidr"`
}

// OverlapOut is the IpInventoryVcnOverlapCollection response.
type OverlapOut struct {
	LastUpdatedTimestamp time.Time     `json:"lastUpdatedTimestamp"`
	Message              string        `json:"message,omitempty"`
	Items                []OverlapItem `json:"ipInventoryVcnOverlapSummary"`
	OverlapCount         int           `json:"overlapCount"`
}

func titleCase(s string) string {
	if s == "" {
		return ""
	}

	return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
}

func round2(v float64) float64 { return math.Round(v*percentRound) / percentRound }

// v4Utilization is the share of a block's usable addresses (all but 3 reserved) that private IPs occupy.
func (b *InMemoryBackend) v4Utilization(s *Subnet, block string) float64 {
	p := netip.MustParsePrefix(block)
	usable := math.Ldexp(1, v4Bits-p.Bits()) - reservedPerCidr

	if usable <= 0 {
		return percent
	}

	used := len(liveOf(b, func(x *PrivateIP) bool {
		a, ok := parseAddr(x.IPAddress)

		return x.SubnetID == s.ID && ok && p.Contains(a)
	}))

	return round2(float64(used) / usable * percent)
}

func (b *InMemoryBackend) v6Utilization(s *Subnet, block string) float64 {
	p := netip.MustParsePrefix(block)
	used := len(liveOf(b, func(x *Ipv6) bool {
		a, ok := parseAddr(x.IPAddress)

		return x.SubnetID == s.ID && ok && p.Contains(a)
	}))

	return round2(float64(used) / math.Ldexp(1, ipv6FullBits-p.Bits()) * percent)
}

func (b *InMemoryBackend) v6AddressType(v *Vcn, block string) string {
	p := netip.MustParsePrefix(block)

	for _, c := range v.Ipv6PrivateCidrs {
		if netip.MustParsePrefix(c).Contains(p.Addr()) {
			return addrULAv6
		}
	}

	return addrGUAv6
}

// subnetCidrs reports the utilization of every IPv4 and IPv6 block of a subnet.
func (b *InMemoryBackend) subnetCidrUse(s *Subnet) []CidrUtilizationItem {
	out := make([]CidrUtilizationItem, 0, len(s.Ipv4CidrBlocks)+len(s.Ipv6CidrBlocks))

	for _, c := range s.Ipv4CidrBlocks {
		out = append(out, CidrUtilizationItem{Cidr: c, AddressType: addrPrivateV4, Utilization: b.v4Utilization(s, c)})
	}

	v, _ := b.res[s.VcnID].(*Vcn)

	for _, c := range s.Ipv6CidrBlocks {
		typ := addrGUAv6
		if v != nil {
			typ = b.v6AddressType(v, c)
		}

		out = append(out, CidrUtilizationItem{Cidr: c, AddressType: typ, Utilization: b.v6Utilization(s, c)})
	}

	return out
}

// GetSubnetCidrUtilization reports how full each block of a subnet is.
func (b *InMemoryBackend) GetSubnetCidrUtilization(id string) (CidrUtilizationOut, *APIError) {
	b.mu.RLock("GetSubnetCidrUtilization")
	defer b.mu.RUnlock()

	s, aerr := get[*Subnet](b, id)
	if aerr != nil {
		return CidrUtilizationOut{}, aerr
	}

	items := b.subnetCidrUse(s)

	return CidrUtilizationOut{
		LastUpdatedTimestamp: b.nowUTC(), CompartmentID: s.CompartmentID, Items: items, Count: len(items),
	}, nil
}

func (b *InMemoryBackend) ownerName(v *Vnic) string {
	for _, a := range liveOf(b, func(x *VnicAttachment) bool { return x.VnicID == v.ID }) {
		if inst, ok := b.res[a.InstanceID].(*Instance); ok {
			return inst.DisplayName
		}
	}

	return v.DisplayName
}

func (b *InMemoryBackend) blockOf(s *Subnet, a netip.Addr) string {
	for _, c := range s.Ipv4CidrBlocks {
		if netip.MustParsePrefix(c).Contains(a) {
			return c
		}
	}

	return ""
}

func (b *InMemoryBackend) privateInventory(s *Subnet, p *PrivateIP) SubnetIPOut {
	a, _ := parseAddr(p.IPAddress)
	out := SubnetIPOut{
		IPID: p.ID, IPAddress: p.IPAddress, ParentCidr: b.blockOf(s, a), AddressType: addrPrivateV4,
		IPAddressLifetime: titleCase(p.Lifetime),
		IsPrimary:         &p.IsPrimary,
	}

	if s.SubnetDomainName != "" && p.HostnameLabel != "" {
		out.DNSHostName = strings.ToLower(p.HostnameLabel) + "." + s.SubnetDomainName
	}

	if v, ok := b.res[p.VnicID].(*Vnic); ok {
		out.AssignedResourceName, out.AssignedResourceType = b.ownerName(v), resourceKind
		at := v.TimeCreated
		out.AssignedTime = &at
	}

	if pub := b.publicOf(p.ID); pub != nil {
		out.AssociatedPublicIP = pub.IPAddress
		out.PublicIPLifetime = titleCase(pub.Lifetime)
		out.AssociatedPublicPool = "ORACLE"
	}

	return out
}

// GetSubnetIPInventory lists every private IP and IPv6 address of a subnet with its owner.
func (b *InMemoryBackend) GetSubnetIPInventory(id string) (SubnetInventoryOut, *APIError) {
	b.mu.RLock("GetSubnetIPInventory")
	defer b.mu.RUnlock()

	s, aerr := get[*Subnet](b, id)
	if aerr != nil {
		return SubnetInventoryOut{}, aerr
	}

	ips := liveOf(b, func(x *PrivateIP) bool { return x.SubnetID == id })
	slices.SortFunc(ips, func(x, y *PrivateIP) int {
		return cmp.Or(x.TimeCreated.Compare(y.TimeCreated), cmp.Compare(x.Internal.Seq, y.Internal.Seq))
	})

	items := make([]SubnetIPOut, 0, len(ips))
	for _, p := range ips {
		items = append(items, b.privateInventory(s, p))
	}

	v, _ := b.res[s.VcnID].(*Vcn)

	for _, x := range liveOf(b, func(x *Ipv6) bool { return x.SubnetID == id }) {
		out := SubnetIPOut{
			IPID: x.ID, IPAddress: x.IPAddress, AddressType: addrGUAv6,
			IPAddressLifetime: titleCase(x.Lifetime),
		}

		if a, ok := parseAddr(x.IPAddress); ok && v != nil {
			for _, c := range s.Ipv6CidrBlocks {
				if netip.MustParsePrefix(c).Contains(a) {
					out.ParentCidr, out.AddressType = c, b.v6AddressType(v, c)
				}
			}
		}

		if vn, ok := b.res[x.VnicID].(*Vnic); ok {
			out.AssignedResourceName, out.AssignedResourceType = b.ownerName(vn), resourceKind
		}

		items = append(items, out)
	}

	return SubnetInventoryOut{
		LastUpdatedTimestamp: b.nowUTC(), CompartmentID: s.CompartmentID, Items: items, Count: len(items),
	}, nil
}

func (b *InMemoryBackend) subnetResources(s *Subnet) []invResource {
	vnics := liveOf(b, func(x *Vnic) bool { return x.SubnetID == s.ID })
	out := make([]invResource, 0, len(vnics))

	for _, v := range vnics {
		r := invResource{
			ResourceName: b.ownerName(v), ResourceType: resourceKind, MacAddress: v.MacAddress, Region: regionName,
			CompartmentID: v.CompartmentID,
		}

		for _, p := range liveOf(b, func(x *PrivateIP) bool { return x.VnicID == v.ID }) {
			host := ""
			if s.SubnetDomainName != "" && p.HostnameLabel != "" {
				host = strings.ToLower(p.HostnameLabel) + "." + s.SubnetDomainName
			}

			r.IPAddressCollection = append(r.IPAddressCollection,
				invAddress{IPAddress: p.IPAddress, VnicID: v.ID, VnicName: v.DisplayName, DNSHostName: host})
		}

		slices.SortFunc(
			r.IPAddressCollection,
			func(x, y invAddress) int { return cmp.Compare(x.IPAddress, y.IPAddress) },
		)
		out = append(out, r)
	}

	slices.SortFunc(out, func(x, y invResource) int { return cmp.Compare(x.ResourceName, y.ResourceName) })

	return out
}

func (b *InMemoryBackend) vcnInventory(v *Vcn) invVcn {
	out := invVcn{
		VcnID: v.ID, VcnName: v.DisplayName, ResourceType: "VCN", DNSDomainName: v.VcnDomainName, Region: regionName,
		CompartmentID: v.CompartmentID,
	}

	subnets := liveOf(b, inVcn[*Subnet](v.ID))
	sortRes(subnets, ListOpts{Order: orderAsc})

	for _, c := range slices.Concat(v.CidrBlocks, v.Ipv6CidrBlocks, v.Ipv6PrivateCidrs) {
		out.InventoryVcnCidrBlockCollection = append(out.InventoryVcnCidrBlockCollection,
			invCidr{IPCidrBlock: c, Utilization: vcnCidrCoverage(c, subnets)})
	}

	for _, s := range subnets {
		is := invSubnet{
			SubnetID: s.ID, SubnetName: s.DisplayName, ResourceType: "Subnet", DNSDomainName: s.SubnetDomainName,
			Region: regionName, CompartmentID: s.CompartmentID, InventoryResourceSummary: b.subnetResources(s),
		}
		for _, u := range b.subnetCidrUse(s) {
			is.InventorySubnetCidrCollection = append(is.InventorySubnetCidrCollection,
				invCidr{IPCidrBlock: u.Cidr, Utilization: u.Utilization})
		}

		out.InventorySubnetCollection = append(out.InventorySubnetCollection, is)
	}

	return out
}

// vcnCidrCoverage is the share of a VCN block's address space that subnets claim.
func vcnCidrCoverage(block string, subnets []*Subnet) float64 {
	p := netip.MustParsePrefix(block)
	total := math.Ldexp(1, p.Addr().BitLen()-p.Bits())

	var claimed float64

	for _, s := range subnets {
		for _, c := range slices.Concat(s.Ipv4CidrBlocks, s.Ipv6CidrBlocks) {
			if sp := netip.MustParsePrefix(c); within(p, sp) {
				claimed += math.Ldexp(1, sp.Addr().BitLen()-sp.Bits())
			}
		}
	}

	return round2(claimed / total * percent)
}

func (d InventoryDetails) wants(v *Vcn, regions bool) bool {
	return (!regions || slices.Contains(d.RegionList, regionName)) &&
		slices.Contains(d.CompartmentList, v.CompartmentID)
}

func (d InventoryDetails) filter(in invVcn, overlapping map[string]bool) (invVcn, bool) {
	if d.OverlappingVcnsOnly != nil && *d.OverlappingVcnsOnly && !overlapping[in.VcnID] {
		return in, false
	}

	if d.Utilization != nil {
		hot := slices.ContainsFunc(
			in.InventoryVcnCidrBlockCollection,
			func(c invCidr) bool { return c.Utilization >= *d.Utilization },
		)
		if !hot {
			return in, false
		}
	}

	if len(d.AddressTypeList) > 0 && !d.hasAddressType(in) {
		return in, false
	}

	if kw := strings.ToLower(d.SearchKeyword); kw != "" && !inventoryMatches(in, kw) {
		return in, false
	}

	return in, true
}

func (d InventoryDetails) hasAddressType(in invVcn) bool {
	want := func(t string) bool { return slices.Contains(d.AddressTypeList, t) }

	for _, c := range in.InventoryVcnCidrBlockCollection {
		p := netip.MustParsePrefix(c.IPCidrBlock)
		if (p.Addr().Is4() && want(addrPrivateV4)) || (p.Addr().Is6() && (want(addrGUAv6) || want(addrULAv6))) {
			return true
		}
	}

	return false
}

func inventoryMatches(in invVcn, kw string) bool {
	if strings.Contains(strings.ToLower(in.VcnName), kw) || strings.Contains(strings.ToLower(in.DNSDomainName), kw) {
		return true
	}

	for _, s := range in.InventorySubnetCollection {
		if strings.Contains(strings.ToLower(s.SubnetName), kw) ||
			strings.Contains(strings.ToLower(s.DNSDomainName), kw) {
			return true
		}

		for _, r := range s.InventoryResourceSummary {
			if strings.Contains(strings.ToLower(r.ResourceName), kw) {
				return true
			}

			for _, a := range r.IPAddressCollection {
				if strings.Contains(a.IPAddress, kw) || strings.Contains(strings.ToLower(a.DNSHostName), kw) {
					return true
				}
			}
		}
	}

	return false
}

func maxUtilization(v invVcn) float64 {
	var m float64
	for _, c := range v.InventoryVcnCidrBlockCollection {
		m = max(m, c.Utilization)
	}

	return m
}

func (d InventoryDetails) sorted(vcns []invVcn) {
	desc := strings.EqualFold(d.SortOrder, "DESC")

	slices.SortStableFunc(vcns, func(x, y invVcn) int {
		var r int

		switch d.SortBy {
		case "UTILIZATION":
			r = cmp.Compare(maxUtilization(x), maxUtilization(y))
		case "REGION":
			r = cmp.Compare(x.Region, y.Region)
		case "DNS_HOSTNAME":
			r = cmp.Compare(x.DNSDomainName, y.DNSDomainName)
		default:
			r = cmp.Compare(strings.ToLower(x.VcnName), strings.ToLower(y.VcnName))
		}

		if r == 0 {
			r = cmp.Or(
				cmp.Compare(strings.ToLower(x.VcnName), strings.ToLower(y.VcnName)),
				cmp.Compare(x.VcnID, y.VcnID),
			)
		}

		if desc {
			return -r
		}

		return r
	})
}

// overlappingVcns marks the VCNs whose CIDR blocks overlap those of another VCN in the selection.
func overlappingVcns(vcns []*Vcn) map[string]bool {
	out := map[string]bool{}

	for i, a := range vcns {
		for _, c := range vcns[i+1:] {
			if cidrsOverlap(a.CidrBlocks, c.CidrBlocks) {
				out[a.ID], out[c.ID] = true, true
			}
		}
	}

	return out
}

func validInventory(d InventoryDetails) *APIError {
	if len(d.RegionList) == 0 || len(d.CompartmentList) == 0 {
		return errMissing("regionList and compartmentList are required")
	}

	switch d.SortBy {
	case "", "DISPLAYNAME", "UTILIZATION", "DNS_HOSTNAME", "REGION":
	default:
		return errParam("sortBy must be DISPLAYNAME, UTILIZATION, DNS_HOSTNAME or REGION")
	}

	if d.SortOrder != "" && !strings.EqualFold(d.SortOrder, orderAsc) && !strings.EqualFold(d.SortOrder, "DESC") {
		return errParam("sortOrder must be ASC or DESC")
	}

	for _, t := range d.AddressTypeList {
		if !slices.Contains(
			[]string{addrPrivateV4, addrPublicV4, "BYOIP_IPv4", addrULAv6, addrGUAv6, "BYOIP_IPv6"},
			t,
		) {
			return errParam("addressTypeList entry " + t + " is not a valid address type")
		}
	}

	return nil
}

// buildInventory computes the IP inventory of the selected compartments; callers hold the lock.
func (b *InMemoryBackend) buildInventory(d InventoryDetails) InventoryOut {
	vcns := liveOf(b, func(x *Vcn) bool { return d.wants(x, true) })
	overlaps := overlappingVcns(vcns)
	items := make([]invVcn, 0, len(vcns))

	for _, v := range vcns {
		if in, ok := d.filter(b.vcnInventory(v), overlaps); ok {
			items = append(items, in)
		}
	}

	d.sorted(items)

	total := len(items)

	if d.PaginationOffset != nil && *d.PaginationOffset > 0 {
		items = items[min(*d.PaginationOffset, len(items)):]
	}

	if d.PaginationLimit != nil && *d.PaginationLimit >= 0 && *d.PaginationLimit < len(items) {
		items = items[:*d.PaginationLimit]
	}

	comps := map[string]bool{}
	for _, v := range vcns {
		comps[v.CompartmentID] = true
	}

	return InventoryOut{
		LastUpdatedTimestamp:   b.nowUTC(),
		InventoryVcnCollection: items,
		Count:                  total,
		CompartmentsPerTenant:  len(comps),
	}
}

// IPInventoryRequest is a stored ListIpInventory request that GetResourceIpInventory replays.
type IPInventoryRequest struct {
	Details InventoryDetails `json:"details"`
	Seq     int64            `json:"seq"`
}

// ListIPInventory computes the inventory immediately and remembers the request under a data request id.
func (b *InMemoryBackend) ListIPInventory(d InventoryDetails) (InventoryOut, string, *APIError) {
	if aerr := validInventory(d); aerr != nil {
		return InventoryOut{}, "", aerr
	}

	b.mu.Lock("ListIPInventory")
	defer b.mu.Unlock()

	for _, c := range d.CompartmentList {
		if aerr := b.checkCompartment(c); aerr != nil {
			return InventoryOut{}, "", aerr
		}
	}

	id := "ocid1.ipinventoryrequest.oc1." + RegionKey + "." + awsid.NewN("", invRequestTail)
	b.ipInvReqs[id] = &IPInventoryRequest{Details: d, Seq: b.nextSeq()}

	if len(b.ipInvReqs) > maxInvRequests {
		oldest := ""

		for k, r := range b.ipInvReqs {
			if oldest == "" || r.Seq < b.ipInvReqs[oldest].Seq {
				oldest = k
			}
		}

		delete(b.ipInvReqs, oldest)
	}

	return b.buildInventory(d), id, nil
}

// GetResourceIPInventory replays a stored ListIpInventory request against the current state.
func (b *InMemoryBackend) GetResourceIPInventory(dataRequestID string) (InventoryOut, *APIError) {
	b.mu.RLock("GetResourceIPInventory")
	defer b.mu.RUnlock()

	r, ok := b.ipInvReqs[dataRequestID]
	if !ok {
		return InventoryOut{}, errNotFound(dataRequestID)
	}

	return b.buildInventory(r.Details), nil
}

// OverlapDetails is the GetVcnOverlap request body.
type OverlapDetails struct {
	RegionList      []string `json:"regionList"`
	CompartmentList []string `json:"compartmentList"`
}

// GetVcnOverlap lists the VCNs of the selected compartments that overlap the CIDR blocks of one VCN.
func (b *InMemoryBackend) GetVcnOverlap(id string, d OverlapDetails) (OverlapOut, string, *APIError) {
	if len(d.RegionList) == 0 || len(d.CompartmentList) == 0 {
		return OverlapOut{}, "", errMissing("regionList and compartmentList are required")
	}

	b.mu.Lock("GetVcnOverlap")
	defer b.mu.Unlock()

	v, aerr := get[*Vcn](b, id)
	if aerr != nil {
		return OverlapOut{}, "", aerr
	}

	sel := InventoryDetails{RegionList: d.RegionList, CompartmentList: d.CompartmentList}
	others := liveOf(b, func(x *Vcn) bool { return x.ID != id && sel.wants(x, true) })
	sortRes(others, ListOpts{Order: orderAsc})

	out := OverlapOut{LastUpdatedTimestamp: b.nowUTC(), Items: []OverlapItem{}}

	for _, o := range others {
		for _, mine := range v.CidrBlocks {
			for _, theirs := range o.CidrBlocks {
				if netip.MustParsePrefix(mine).Overlaps(netip.MustParsePrefix(theirs)) {
					out.Items = append(out.Items, OverlapItem{
						OverlappingVcnID: o.ID, OverlappingVcnName: o.DisplayName, OverlappingCidr: theirs, Cidr: mine,
					})
				}
			}
		}
	}

	out.OverlapCount = len(out.Items)

	reqID := "ocid1.ipinventoryrequest.oc1." + RegionKey + "." + awsid.NewN("", invRequestTail)

	return out, reqID, nil
}

func (q *req) inventoryHeaders(dataRequestID string) {
	q.header("lifecycle-state", invDone)
	q.header("data-request-id", dataRequestID)
}

func inventoryEntries() []entry {
	return []entry{
		{(*Handler).listIPInventory, http.MethodPost, "/ipInventory", "ListIpInventory"},
		{(*Handler).getResourceIPInventory, http.MethodGet, idPath("/ipinventory/DataRequestId", "dataRequestId"),
			"GetResourceIpInventory"},
		{
			(*Handler).getSubnetIPInventory,
			http.MethodGet,
			idPath("/ipInventory/subnets", "subnetId"),
			"GetSubnetIpInventory",
		},
		{(*Handler).getSubnetCidrUtilization, http.MethodGet, idPath("/ipInventory/subnets", "subnetId") + "/cidrs",
			"GetSubnetCidrUtilization"},
		{
			(*Handler).getVcnOverlap,
			http.MethodPost,
			idPath("/ipInventory/vcns", "vcnId") + "/overlaps",
			"GetVcnOverlap",
		},
	}
}

func (h *Handler) listIPInventory(q *req) error {
	var in InventoryDetails
	if err := q.decode(&in); err != nil {
		return err
	}

	out, id, aerr := h.Backend.ListIPInventory(in)
	if aerr != nil {
		return aerr
	}

	q.inventoryHeaders(id)
	q.header("opc-total-items", strconv.Itoa(out.Count))

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) getResourceIPInventory(q *req) error {
	out, aerr := h.Backend.GetResourceIPInventory(q.param("dataRequestId"))
	if aerr != nil {
		return aerr
	}

	q.header("opc-total-items", strconv.Itoa(out.Count))

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) getSubnetIPInventory(q *req) error {
	out, aerr := h.Backend.GetSubnetIPInventory(q.param("subnetId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) getSubnetCidrUtilization(q *req) error {
	out, aerr := h.Backend.GetSubnetCidrUtilization(q.param("subnetId"))
	if aerr != nil {
		return aerr
	}

	q.header("opc-total-items", strconv.Itoa(out.Count))

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) getVcnOverlap(q *req) error {
	var in OverlapDetails
	if err := q.decode(&in); err != nil {
		return err
	}

	out, id, aerr := h.Backend.GetVcnOverlap(q.param("vcnId"), in)
	if aerr != nil {
		return aerr
	}

	q.inventoryHeaders(id)
	q.header("opc-total-items", strconv.Itoa(out.OverlapCount))

	return q.json(http.StatusOK, out, "")
}
