package ocicore

import (
	"net/http"
	"net/netip"
	"slices"
	"strconv"
)

const (
	kindVlan     = "vlan"
	maxVlanTag   = 4094
	vlanNicIndex = 0
)

// Vlan is a VLAN of a VCN; VNICs and private IPs can be created in it.
type Vlan struct {
	AvailabilityDomain *string `json:"availabilityDomain,omitempty"`
	Base
	VcnID        string   `json:"vcnId"`
	CidrBlock    string   `json:"cidrBlock"`
	RouteTableID string   `json:"routeTableId,omitempty"`
	NsgIDs       []string `json:"nsgIds"`
	VlanTag      int      `json:"vlanTag"`
}

func (v *Vlan) vcnRef() string { return v.VcnID }

func (v *Vlan) view() Resource {
	c := *v
	c.Internal = nil
	c.NsgIDs = slices.Clone(v.NsgIDs)

	if c.NsgIDs == nil {
		c.NsgIDs = []string{}
	}

	return &c
}

// CreateVlanIn is the CreateVlan request body.
type CreateVlanIn struct {
	TagsIn
	VlanTag            *int     `json:"vlanTag"`
	CompartmentID      string   `json:"compartmentId"`
	VcnID              string   `json:"vcnId"`
	CidrBlock          string   `json:"cidrBlock"`
	AvailabilityDomain string   `json:"availabilityDomain"`
	DisplayName        string   `json:"displayName"`
	RouteTableID       string   `json:"routeTableId"`
	NsgIDs             []string `json:"nsgIds"`
}

func (b *InMemoryBackend) checkVlanCidr(vcn *Vcn, cidr, selfID string) *APIError {
	p, err := netip.ParsePrefix(cidr)
	if err != nil || !p.Addr().Is4() || p.Masked() != p {
		return errParam("cidrBlock must be a valid IPv4 CIDR block")
	}

	if !slices.ContainsFunc(vcn.CidrBlocks, func(c string) bool { return within(netip.MustParsePrefix(c), p) }) {
		return errParam("cidrBlock " + cidr + " is not inside a CIDR block of VCN " + vcn.ID)
	}

	for _, s := range liveOf(b, inVcn[*Subnet](vcn.ID)) {
		if cidrsOverlap(s.Ipv4CidrBlocks, []string{cidr}) {
			return errConflict("cidrBlock " + cidr + " overlaps subnet " + s.ID)
		}
	}

	for _, o := range liveOf(b, func(x *Vlan) bool { return x.VcnID == vcn.ID && x.ID != selfID }) {
		if cidrsOverlap([]string{o.CidrBlock}, []string{cidr}) {
			return errConflict("cidrBlock " + cidr + " overlaps VLAN " + o.ID)
		}
	}

	return nil
}

func (b *InMemoryBackend) vlanTagFor(vcnID string, want *int) (int, *APIError) {
	used := map[int]bool{}
	for _, o := range liveOf(b, func(x *Vlan) bool { return x.VcnID == vcnID }) {
		used[o.VlanTag] = true
	}

	if want != nil {
		switch {
		case *want < 1 || *want > maxVlanTag:
			return 0, errParam("vlanTag must be between 1 and 4094; tag 0 is reserved")
		case used[*want]:
			return 0, errConflict("The VLAN tag " + strconv.Itoa(*want) + " is already used in the VCN")
		}

		return *want, nil
	}

	for t := 1; t <= maxVlanTag; t++ {
		if !used[t] {
			return t, nil
		}
	}

	return 0, errLimit("The VCN has no free VLAN tag")
}

// CreateVlan creates a VLAN.
func (b *InMemoryBackend) CreateVlan(in CreateVlanIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	vcn, aerr := getLive[*Vcn](b, in.VcnID)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = b.checkVlanCidr(vcn, in.CidrBlock, ""); aerr != nil {
		return nil, "", aerr
	}

	if in.AvailabilityDomain != "" && !adRe.MatchString(in.AvailabilityDomain) {
		return nil, "", errParam("availabilityDomain " + in.AvailabilityDomain + " is not a valid availability domain")
	}

	nsgs, aerr := b.checkNsgs(in.NsgIDs, vcn.ID)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = b.optRouteTable(in.RouteTableID, vcn.ID); aerr != nil {
		return nil, "", aerr
	}

	tag, aerr := b.vlanTagFor(vcn.ID, in.VlanTag)
	if aerr != nil {
		return nil, "", aerr
	}

	v := &Vlan{
		Base:  b.newBase(kindVlan, in.CompartmentID, in.DisplayName, in.TagsIn),
		VcnID: vcn.ID, CidrBlock: in.CidrBlock, RouteTableID: in.RouteTableID, NsgIDs: nsgs, VlanTag: tag,
	}
	if in.AvailabilityDomain != "" {
		v.AvailabilityDomain = &in.AvailabilityDomain
	}

	b.add(v)

	return v.view(), v.Internal.Etag, nil
}

// UpdateVlanIn is the UpdateVlan request body.
type UpdateVlanIn struct {
	UpdateIn
	NsgIDs       *[]string `json:"nsgIds"`
	RouteTableID *string   `json:"routeTableId"`
	CidrBlock    *string   `json:"cidrBlock"`
}

// UpdateVlan changes the name, tags, NSGs, route table and CIDR block of a VLAN.
func (b *InMemoryBackend) UpdateVlan(id, ifMatch string, in UpdateVlanIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(v *Vlan) *APIError {
		vcn, _ := get[*Vcn](b, v.VcnID)

		if in.NsgIDs != nil {
			nsgs, aerr := b.checkNsgs(*in.NsgIDs, v.VcnID)
			if aerr != nil {
				return aerr
			}

			v.NsgIDs = nsgs
		}

		if in.RouteTableID != nil {
			if aerr := b.optRouteTable(*in.RouteTableID, v.VcnID); aerr != nil {
				return aerr
			}

			v.RouteTableID = *in.RouteTableID
		}

		if in.CidrBlock == nil {
			return nil
		}

		if aerr := b.checkVlanCidr(vcn, *in.CidrBlock, v.ID); aerr != nil {
			return aerr
		}

		for _, ip := range liveOf(b, func(x *PrivateIP) bool { return x.VlanID == v.ID }) {
			if !netip.MustParsePrefix(*in.CidrBlock).Contains(netip.MustParseAddr(ip.IPAddress)) {
				return errConflict("Private IP " + ip.ID + " lies outside the new cidrBlock")
			}
		}

		v.CidrBlock = *in.CidrBlock

		return nil
	})
}

func (b *InMemoryBackend) deleteVlanCheck(v *Vlan) *APIError {
	if x := firstID(liveOf(b, func(p *PrivateIP) bool { return p.VlanID == v.ID })); x != "" {
		return errConflict("The VLAN " + v.ID + " still has private IP " + x)
	}

	if x := firstID(liveOf(b, func(n *Vnic) bool { return n.VlanID == v.ID })); x != "" {
		return errConflict("The VLAN " + v.ID + " still has VNIC " + x)
	}

	return nil
}

// vlanAddress picks the requested or lowest free address of a VLAN; the first two and the last are reserved.
func (b *InMemoryBackend) vlanAddress(v *Vlan, want string) (string, *APIError) {
	p := netip.MustParsePrefix(v.CidrBlock)
	used := map[string]bool{}

	for _, ip := range liveOf(b, func(x *PrivateIP) bool { return x.VlanID == v.ID }) {
		used[ip.IPAddress] = true
	}

	first, last := p.Addr(), lastAddr(p)

	if want != "" {
		a, err := netip.ParseAddr(want)

		switch {
		case err != nil || !p.Contains(a):
			return "", errParam("ipAddress " + want + " is not inside the VLAN cidrBlock " + v.CidrBlock)
		case a == first || a == first.Next() || a == last:
			return "", errParam("ipAddress " + want + " is reserved by the Networking service")
		case used[want]:
			return "", errConflict("The IP address " + want + " is already in use")
		}

		return want, nil
	}

	for a := first.Next().Next(); p.Contains(a) && a != last; a = a.Next() {
		if !used[a.String()] {
			return a.String(), nil
		}
	}

	return "", errLimit("The VLAN " + v.ID + " has no free IP addresses")
}

// createVlanPrivateIP reserves a private IP in a VLAN.
func (b *InMemoryBackend) createVlanPrivateIP(in CreatePrivateIPIn) (Resource, string, *APIError) {
	if in.VnicID != "" || in.SubnetID != "" {
		return nil, "", errParam("vlanId cannot be combined with vnicId or subnetId")
	}

	if in.HostnameLabel != "" || in.CidrPrefixLength != nil || in.Ipv4SubnetCidr != "" {
		return nil, "", errParam("hostnameLabel and CIDR allocation are not available for a VLAN private IP")
	}

	v, aerr := getLive[*Vlan](b, in.VlanID)
	if aerr != nil {
		return nil, "", aerr
	}

	if in.Lifetime != "" && in.Lifetime != lifetimeReserved {
		return nil, "", errParam("A VLAN private IP must be RESERVED")
	}

	if aerr = b.optRouteTable(in.RouteTableID, v.VcnID); aerr != nil {
		return nil, "", aerr
	}

	addr, aerr := b.vlanAddress(v, in.IPAddress)
	if aerr != nil {
		return nil, "", aerr
	}

	p := &PrivateIP{
		Base:         b.newBase(kindPrivateIP, v.CompartmentID, in.DisplayName, in.TagsIn),
		IPAddress:    addr,
		IPState:      statAvailable,
		Lifetime:     lifetimeReserved,
		VlanID:       v.ID,
		RouteTableID: in.RouteTableID,
	}
	b.ready(p)
	b.add(p)

	return p.view(), p.Internal.Etag, nil
}

// attachVlanVnic creates a VNIC in a VLAN of a bare metal instance.
func (b *InMemoryBackend) attachVlanVnic(inst *Instance, in AttachVnicIn, nic int) (Resource, string, *APIError) {
	vd := in.CreateVnicDetails

	if vd.SubnetID != "" {
		return nil, "", errParam("provide a vlanId instead of a subnetId, not both")
	}

	vlan, aerr := getLive[*Vlan](b, vd.VlanID)
	if aerr != nil {
		return nil, "", aerr
	}

	if sh, ok := shapeByName(inst.Shape); !ok || !sh.bareMetal {
		return nil, "", errParam("A VNIC can only be created in a VLAN for a bare metal instance")
	}

	if vlan.AvailabilityDomain != nil && *vlan.AvailabilityDomain != inst.AvailabilityDomain {
		return nil, "", errParam(
			"The VLAN " + vlan.ID + " is specific to availability domain " + *vlan.AvailabilityDomain,
		)
	}

	if slices.ContainsFunc(liveOf(b, func(x *VnicAttachment) bool { return x.InstanceID == inst.ID }),
		func(a *VnicAttachment) bool { return a.VlanID == vlan.ID }) {
		return nil, "", errConflict("The instance already has a VNIC from VLAN " + vlan.ID)
	}

	if aerr = validName(vd.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	v := &Vnic{
		Base:               b.newBase(kindVnic, inst.CompartmentID, vd.DisplayName, vd.TagsIn),
		AvailabilityDomain: inst.AvailabilityDomain, VlanID: vlan.ID, NsgIDs: slices.Clone(vlan.NsgIDs),
	}
	v.MacAddress = macPrefix + macTail(v.Internal.Seq)
	b.add(v)

	a := b.newVnicAttachment(inst, v, in.DisplayName, nic)
	a.VlanID, a.VlanTag = vlan.ID, vlan.VlanTag

	return a.view(), a.Internal.Etag, nil
}

func vlanEntries() []entry {
	vl := idPath("/vlans", "vlanId")

	return []entry{
		{createOp((*InMemoryBackend).CreateVlan), http.MethodPost, "/vlans", "CreateVlan"},
		{listOp[*Vlan](vlanFilter), http.MethodGet, "/vlans", "ListVlans"},
		{getOp[*Vlan]("vlanId"), http.MethodGet, vl, "GetVlan"},
		{updateOp("vlanId", (*InMemoryBackend).UpdateVlan), http.MethodPut, vl, "UpdateVlan"},
		{deleteOp[*Vlan]("vlanId"), http.MethodDelete, vl, "DeleteVlan"},
		{changeOp[*Vlan]("vlanId"), http.MethodPost, vl + "/actions/changeCompartment", "ChangeVlanCompartment"},
	}
}

func vlanFilter(q *req) func(Resource) bool {
	return fieldEq[*Vlan](q, "vcnId", func(x *Vlan) string { return x.VcnID })
}

func (b *InMemoryBackend) updateVlanVnic(v *Vnic, in UpdateVnicIn) *APIError {
	if in.HostnameLabel != nil {
		return errParam("hostnameLabel is not available for a VNIC in a VLAN")
	}

	if in.NsgIDs != nil {
		nsgs, aerr := b.checkNsgs(*in.NsgIDs, b.vlanVcn(v.VlanID))
		if aerr != nil {
			return aerr
		}

		v.NsgIDs = nsgs
	}

	if in.SkipSourceDestCheck != nil {
		v.SkipSourceDestCheck = *in.SkipSourceDestCheck
	}

	return nil
}

func (b *InMemoryBackend) vlanVcn(id string) string {
	vl, _ := b.res[id].(*Vlan)
	if vl == nil {
		return ""
	}

	return vl.VcnID
}
