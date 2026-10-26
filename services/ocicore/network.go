package ocicore

import (
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	entityPrivateIP = "PRIVATE_IP"
	maxNsgsPerVnic  = 5
	maxIPsPerVnic   = 32
	regionName      = "us-ashburn-1"
	macPrefix       = "02:00:17:"
	v4Bits          = 32
	bitsPerByte     = 8
	macByte         = 0xff
	macShift        = 16
)

var hostLabelRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

// CreateVnicIn is createVnicDetails.
type CreateVnicIn struct {
	TagsIn
	AssignPublicIP      *bool    `json:"assignPublicIp"`
	AssignIpv6IP        *bool    `json:"assignIpv6Ip"`
	SkipSourceDestCheck *bool    `json:"skipSourceDestCheck"`
	DisplayName         string   `json:"displayName"`
	HostnameLabel       string   `json:"hostnameLabel"`
	PrivateIP           string   `json:"privateIp"`
	SubnetID            string   `json:"subnetId"`
	VlanID              string   `json:"vlanId"`
	NsgIDs              []string `json:"nsgIds"`
}

func (b *InMemoryBackend) ready(r resource) {
	bs := r.base()
	bs.LifecycleState, bs.Internal.TransitionAt = statAvailable, nil
}

func lastAddr(p netip.Prefix) netip.Addr {
	a := p.Masked().Addr().As4()

	for i := p.Bits(); i < v4Bits; i++ {
		a[i/bitsPerByte] |= 1 << (bitsPerByte - 1 - i%bitsPerByte)
	}

	return netip.AddrFrom4(a)
}

// freeIP picks the requested or lowest free address of a subnet; the first two and the last address are reserved.
func (b *InMemoryBackend) freeIP(s *Subnet, want string) (string, *APIError) {
	p := netip.MustParsePrefix(s.CidrBlock)
	first := p.Masked().Addr()
	last := lastAddr(p)
	used := map[string]bool{}

	for _, ip := range liveOf(b, func(x *PrivateIP) bool { return x.SubnetID == s.ID }) {
		used[ip.IPAddress] = true
	}

	if want != "" {
		a, ok := parseAddr(want)
		if !ok || !p.Contains(a) {
			return "", errParam("privateIp " + want + " is not within the subnet CIDR block " + s.CidrBlock)
		}

		if a == first || a == first.Next() || a == last {
			return "", errParam("privateIp " + want + " is reserved by the Networking service")
		}

		if used[want] {
			return "", errConflict("The IP address " + want + " is already in use")
		}

		return want, nil
	}

	for a := first.Next().Next(); p.Contains(a) && a != last; a = a.Next() {
		if !used[a.String()] {
			return a.String(), nil
		}
	}

	return "", errLimit("The subnet " + s.ID + " has no free IP addresses")
}

func (b *InMemoryBackend) checkHostname(s *Subnet, label, selfID string) *APIError {
	if label == "" {
		return nil
	}

	if !hostLabelRe.MatchString(label) {
		return errParam("hostnameLabel must be a valid DNS label")
	}

	if s.DNSLabel == "" {
		return errParam("hostnameLabel requires the subnet " + s.ID + " to have a dnsLabel")
	}

	dup := liveOf(b, func(x *PrivateIP) bool {
		return x.SubnetID == s.ID && x.ID != selfID && strings.EqualFold(x.HostnameLabel, label)
	})
	if len(dup) > 0 {
		return errConflict("The hostnameLabel " + label + " is already used in subnet " + s.ID)
	}

	return nil
}

// checkNsgs validates NSG ids for a VNIC in the given VCN.
func (b *InMemoryBackend) checkNsgs(ids []string, vcnID string) ([]string, *APIError) {
	uniq := []string{}

	for _, id := range ids {
		if !slices.Contains(uniq, id) {
			uniq = append(uniq, id)
		}
	}

	if len(uniq) > maxNsgsPerVnic {
		return nil, errParam("A VNIC can be in at most 5 network security groups")
	}

	for _, id := range uniq {
		if aerr := refInVcn[*NetworkSecurityGroup](b, id, vcnID, "network security group"); aerr != nil {
			return nil, aerr
		}
	}

	return uniq, nil
}

func (b *InMemoryBackend) vnicSubnet(in CreateVnicIn, ad string) (*Subnet, *APIError) {
	if in.SubnetID == "" {
		return nil, errMissing("createVnicDetails.subnetId is required")
	}

	if in.VlanID != "" || (in.AssignIpv6IP != nil && *in.AssignIpv6IP) {
		return nil, errParam("vlanId and IPv6 address assignment are not supported")
	}

	s, aerr := getLive[*Subnet](b, in.SubnetID)
	if aerr != nil {
		return nil, aerr
	}

	if s.AvailabilityDomain != nil && *s.AvailabilityDomain != ad {
		return nil, errParam("The subnet " + s.ID + " is specific to availability domain " + *s.AvailabilityDomain)
	}

	if in.AssignPublicIP != nil && *in.AssignPublicIP && s.ProhibitPublicIPOnVnic {
		return nil, errParam("The subnet " + s.ID + " prohibits public IP addresses on VNICs")
	}

	return s, nil
}

// newVnic validates and then creates a VNIC with its primary private IP and optional ephemeral public IP.
func (b *InMemoryBackend) newVnic(in CreateVnicIn, compartment, ad string, primary bool) (*Vnic, *APIError) {
	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, aerr
	}

	s, aerr := b.vnicSubnet(in, ad)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = b.checkHostname(s, in.HostnameLabel, ""); aerr != nil {
		return nil, aerr
	}

	nsgs, aerr := b.checkNsgs(in.NsgIDs, s.VcnID)
	if aerr != nil {
		return nil, aerr
	}

	addr, aerr := b.freeIP(s, in.PrivateIP)
	if aerr != nil {
		return nil, aerr
	}

	v := &Vnic{
		Base:               b.newBase(kindVnic, compartment, in.DisplayName, in.TagsIn),
		AvailabilityDomain: ad, HostnameLabel: in.HostnameLabel, PrivateIP: addr, SubnetID: s.ID, NsgIDs: nsgs,
		IsPrimary: primary, SkipSourceDestCheck: in.SkipSourceDestCheck != nil && *in.SkipSourceDestCheck,
	}
	v.MacAddress = macPrefix + macTail(v.Internal.Seq)
	b.add(v)

	p := b.newPrivateIP(v, s, addr, in.HostnameLabel, true)

	if in.AssignPublicIP == nil && !s.ProhibitPublicIPOnVnic || in.AssignPublicIP != nil && *in.AssignPublicIP {
		ip := &PublicIP{
			Base: b.newBase(kindPublicIP, compartment, "", TagsIn{}), IPAddress: b.allocIP(),
			Lifetime: lifetimeEphemeral, Scope: scopeAD,
		}
		b.add(ip)
		b.assignPublic(ip, p)
		b.refreshVnic(v.ID)
	}

	return v, nil
}

func macTail(seq int64) string {
	return fmt.Sprintf("%02X:%02X:%02X", seq>>macShift&macByte, seq>>bitsPerByte&macByte, seq&macByte)
}

func (b *InMemoryBackend) newPrivateIP(v *Vnic, s *Subnet, addr, label string, primary bool) *PrivateIP {
	p := &PrivateIP{
		Base:               b.newBase(kindPrivateIP, v.CompartmentID, "", TagsIn{}),
		AvailabilityDomain: v.AvailabilityDomain, HostnameLabel: label, IPAddress: addr, IPState: statAssigned,
		Lifetime: lifetimeEphemeral, SubnetID: s.ID, VnicID: v.ID, IsPrimary: primary,
	}
	b.ready(p)
	b.add(p)

	return p
}

// Fqdn returns the fully qualified domain name of a private IP's hostname, or "" without a hostname label.
func (b *InMemoryBackend) Fqdn(privateIPID string) string {
	b.mu.RLock("Fqdn")
	defer b.mu.RUnlock()

	p, aerr := get[*PrivateIP](b, privateIPID)
	if aerr != nil || p.HostnameLabel == "" {
		return ""
	}

	s, aerr := get[*Subnet](b, p.SubnetID)
	if aerr != nil {
		return ""
	}

	return strings.ToLower(p.HostnameLabel) + "." + s.SubnetDomainName
}

func (b *InMemoryBackend) assignPublic(ip *PublicIP, p *PrivateIP) {
	ip.AssignedEntityID, ip.AssignedEntityType, ip.PrivateIPID = p.ID, entityPrivateIP, p.ID
	ip.LifecycleState, ip.Internal.TransitionAt = statAssigned, nil

	if ip.Lifetime == lifetimeEphemeral {
		ad := p.AvailabilityDomain
		ip.AvailabilityDomain = &ad
	}

	b.bump(ip)
}

// unassignPublic releases a public IP from its private IP: reserved ones return to the pool, ephemeral ones end.
func (b *InMemoryBackend) unassignPublic(ip *PublicIP) {
	vnicID := b.vnicOfPublic(ip)

	if ip.Lifetime == lifetimeEphemeral {
		b.markTerminating(ip)
	} else {
		ip.AssignedEntityID, ip.AssignedEntityType, ip.PrivateIPID, ip.LifecycleState = "", "", "", statAvailable
		b.bump(ip)
	}

	b.refreshVnic(vnicID)
}

func (b *InMemoryBackend) vnicOfPublic(ip *PublicIP) string {
	if p, ok := b.res[ip.PrivateIPID].(*PrivateIP); ok {
		return p.VnicID
	}

	return ""
}

func (b *InMemoryBackend) publicOf(privateID string) *PublicIP {
	ips := liveOf(
		b,
		func(x *PublicIP) bool { return x.PrivateIPID == privateID && x.AssignedEntityType == entityPrivateIP },
	)
	if len(ips) == 0 {
		return nil
	}

	return ips[0]
}

// refreshVnic recomputes the publicIp of a VNIC from its primary private IP.
func (b *InMemoryBackend) refreshVnic(vnicID string) {
	v, ok := b.res[vnicID].(*Vnic)
	if !ok {
		return
	}

	v.PublicIP = ""

	for _, p := range liveOf(b, func(x *PrivateIP) bool { return x.VnicID == vnicID && x.IsPrimary }) {
		if ip := b.publicOf(p.ID); ip != nil {
			v.PublicIP = ip.IPAddress
		}
	}

	b.bump(v)
}

// dropPrivateIP removes a private IP, releasing any public IP assigned to it.
func (b *InMemoryBackend) dropPrivateIP(p *PrivateIP) {
	if ip := b.publicOf(p.ID); ip != nil {
		b.unassignPublic(ip)
	}

	delete(b.res, p.ID)
}

// endVnic terminates a VNIC and settles its private IPs: ephemeral ones are dropped, reserved ones unassigned.
func (b *InMemoryBackend) endVnic(v *Vnic) {
	for _, p := range liveOf(b, func(x *PrivateIP) bool { return x.VnicID == v.ID }) {
		if p.Lifetime == lifetimeReserved && !p.IsPrimary {
			p.VnicID, p.IPState = "", statAvailable
			b.bump(p)

			continue
		}

		b.dropPrivateIP(p)
	}

	v.PublicIP = ""
	b.markTerminating(v)
}

func (b *InMemoryBackend) detachVnicAttachment(a *VnicAttachment) {
	b.moveTo(a, statDetaching, statDetached)

	if v, ok := b.res[a.VnicID].(*Vnic); ok && live(v) {
		b.endVnic(v)
	}
}

// AttachVnicIn is the AttachVnic request body.
type AttachVnicIn struct {
	CreateVnicDetails *CreateVnicIn `json:"createVnicDetails"`
	NicIndex          *int          `json:"nicIndex"`
	DisplayName       string        `json:"displayName"`
	InstanceID        string        `json:"instanceId"`
}

// AttachVnic attaches a new secondary VNIC to an instance.
func (b *InMemoryBackend) AttachVnic(in AttachVnicIn) (resource, string, *APIError) {
	b.mu.Lock("AttachVnic")
	defer b.mu.Unlock()

	if in.InstanceID == "" || in.CreateVnicDetails == nil {
		return nil, "", errMissing("instanceId and createVnicDetails are required")
	}

	inst, aerr := getLive[*Instance](b, in.InstanceID)
	if aerr != nil {
		return nil, "", aerr
	}

	if inst.LifecycleState != statRunning && inst.LifecycleState != statStopped {
		return nil, "", errState("The instance " + inst.ID + " is " + inst.LifecycleState)
	}

	atts := liveOf(b, func(a *VnicAttachment) bool { return a.InstanceID == inst.ID })
	if len(atts) >= inst.ShapeConfig.MaxVnicAttachments {
		return nil, "", errLimit("The shape " + inst.Shape + " supports at most " +
			strconv.Itoa(inst.ShapeConfig.MaxVnicAttachments) + " VNIC attachments")
	}

	nic := len(atts)

	if in.NicIndex != nil {
		nic = *in.NicIndex
	}

	if slices.ContainsFunc(atts, func(a *VnicAttachment) bool { return a.NicIndex == nic }) {
		return nil, "", errConflict("The NIC index " + strconv.Itoa(nic) + " is already in use")
	}

	if aerr = validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	v, aerr := b.newVnic(*in.CreateVnicDetails, inst.CompartmentID, inst.AvailabilityDomain, false)
	if aerr != nil {
		return nil, "", aerr
	}

	a := b.newVnicAttachment(inst, v, in.DisplayName, nic)

	return a.view(), a.Internal.Etag, nil
}

func (b *InMemoryBackend) newVnicAttachment(inst *Instance, v *Vnic, name string, nic int) *VnicAttachment {
	a := &VnicAttachment{
		Base:               b.newBase(kindVnicAttach, inst.CompartmentID, name, TagsIn{}),
		AvailabilityDomain: inst.AvailabilityDomain, InstanceID: inst.ID, SubnetID: v.SubnetID, VnicID: v.ID,
		NicIndex: nic, VlanTag: nic,
	}
	b.moveTo(a, statAttaching, statAttached)
	b.add(a)

	return a
}

// DetachVnic detaches and deletes a secondary VNIC.
func (b *InMemoryBackend) DetachVnic(id, ifMatch string) *APIError {
	b.mu.Lock("DetachVnic")
	defer b.mu.Unlock()

	a, aerr := get[*VnicAttachment](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(a) {
		return nil
	}

	if aerr = checkEtag(a, ifMatch); aerr != nil {
		return aerr
	}

	if v, ok := b.res[a.VnicID].(*Vnic); ok && v.IsPrimary {
		return errConflict("The primary VNIC of an instance cannot be detached")
	}

	b.detachVnicAttachment(a)

	return nil
}

// UpdateVnicIn is the UpdateVnic request body.
type UpdateVnicIn struct {
	HostnameLabel       *string   `json:"hostnameLabel"`
	SkipSourceDestCheck *bool     `json:"skipSourceDestCheck"`
	NsgIDs              *[]string `json:"nsgIds"`
	UpdateIn
}

// UpdateVnic changes a VNIC.
func (b *InMemoryBackend) UpdateVnic(id, ifMatch string, in UpdateVnicIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(v *Vnic) *APIError {
		s, _ := get[*Subnet](b, v.SubnetID)
		prim := b.primaryIP(v.ID)

		if in.HostnameLabel != nil {
			if aerr := b.checkHostname(s, *in.HostnameLabel, prim.ID); aerr != nil {
				return aerr
			}
		}

		if in.NsgIDs != nil {
			nsgs, aerr := b.checkNsgs(*in.NsgIDs, s.VcnID)
			if aerr != nil {
				return aerr
			}

			v.NsgIDs = nsgs
		}

		if in.HostnameLabel != nil {
			v.HostnameLabel, prim.HostnameLabel = *in.HostnameLabel, *in.HostnameLabel
		}

		if in.SkipSourceDestCheck != nil {
			v.SkipSourceDestCheck = *in.SkipSourceDestCheck
		}

		return nil
	})
}

func (b *InMemoryBackend) primaryIP(vnicID string) *PrivateIP {
	return liveOf(b, func(x *PrivateIP) bool { return x.VnicID == vnicID && x.IsPrimary })[0]
}

// CreatePrivateIPIn is the CreatePrivateIp request body.
type CreatePrivateIPIn struct {
	TagsIn
	DisplayName   string `json:"displayName"`
	HostnameLabel string `json:"hostnameLabel"`
	IPAddress     string `json:"ipAddress"`
	Lifetime      string `json:"lifetime"`
	SubnetID      string `json:"subnetId"`
	VnicID        string `json:"vnicId"`
	VlanID        string `json:"vlanId"`
}

// CreatePrivateIP adds a secondary private IP to a VNIC, or reserves one in a subnet.
func (b *InMemoryBackend) CreatePrivateIP(in CreatePrivateIPIn) (resource, string, *APIError) {
	b.mu.Lock("CreatePrivateIP")
	defer b.mu.Unlock()

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if in.VlanID != "" || (in.VnicID == "" && in.SubnetID == "") {
		return nil, "", errMissing("vnicId or subnetId is required")
	}

	vnic, s, aerr := b.privateIPTarget(in)
	if aerr != nil {
		return nil, "", aerr
	}

	comp, ad := s.CompartmentID, ""
	if vnic != nil {
		comp, ad = vnic.CompartmentID, vnic.AvailabilityDomain
	}

	life, aerr := privateLifetime(in.Lifetime, vnic != nil)
	if aerr != nil {
		return nil, "", aerr
	}

	if vnic != nil && len(liveOf(b, func(x *PrivateIP) bool { return x.VnicID == vnic.ID })) >= maxIPsPerVnic {
		return nil, "", errLimit("A VNIC can have at most 32 private IP addresses")
	}

	if aerr = b.checkHostname(s, in.HostnameLabel, ""); aerr != nil {
		return nil, "", aerr
	}

	addr, aerr := b.freeIP(s, in.IPAddress)
	if aerr != nil {
		return nil, "", aerr
	}

	p := &PrivateIP{
		Base:               b.newBase(kindPrivateIP, comp, in.DisplayName, in.TagsIn),
		AvailabilityDomain: ad, HostnameLabel: in.HostnameLabel, IPAddress: addr, IPState: statAvailable,
		Lifetime: life, SubnetID: s.ID,
	}
	if vnic != nil {
		p.VnicID, p.IPState = vnic.ID, statAssigned
	}

	b.ready(p)
	b.add(p)

	return p.view(), p.Internal.Etag, nil
}

func (b *InMemoryBackend) privateIPTarget(in CreatePrivateIPIn) (*Vnic, *Subnet, *APIError) {
	if in.VnicID == "" {
		s, aerr := getLive[*Subnet](b, in.SubnetID)

		return nil, s, aerr
	}

	v, aerr := getLive[*Vnic](b, in.VnicID)
	if aerr != nil {
		return nil, nil, aerr
	}

	if in.SubnetID != "" && in.SubnetID != v.SubnetID {
		return nil, nil, errParam("subnetId does not match the subnet of the VNIC")
	}

	s, aerr := getLive[*Subnet](b, v.SubnetID)

	return v, s, aerr
}

func privateLifetime(l string, onVnic bool) (string, *APIError) {
	switch {
	case l == "":
		if onVnic {
			return lifetimeEphemeral, nil
		}

		return lifetimeReserved, nil
	case l != lifetimeEphemeral && l != lifetimeReserved:
		return "", errParam("lifetime must be EPHEMERAL or RESERVED")
	case !onVnic && l == lifetimeEphemeral:
		return "", errParam("A private IP created without a VNIC must be RESERVED")
	}

	return l, nil
}

// UpdatePrivateIPIn is the UpdatePrivateIp request body.
type UpdatePrivateIPIn struct {
	HostnameLabel *string `json:"hostnameLabel"`
	VnicID        *string `json:"vnicId"`
	Lifetime      *string `json:"lifetime"`
	UpdateIn
}

// UpdatePrivateIP changes a private IP, or moves it to another VNIC of the same subnet.
func (b *InMemoryBackend) UpdatePrivateIP(id, ifMatch string, in UpdatePrivateIPIn) (resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in.UpdateIn, func(p *PrivateIP) *APIError {
		s, _ := get[*Subnet](b, p.SubnetID)

		if in.HostnameLabel != nil && !p.IsPrimary {
			if aerr := b.checkHostname(s, *in.HostnameLabel, p.ID); aerr != nil {
				return aerr
			}

			p.HostnameLabel = *in.HostnameLabel
		}

		if in.Lifetime != nil && !p.IsPrimary {
			l, aerr := privateLifetime(*in.Lifetime, p.VnicID != "")
			if aerr != nil {
				return aerr
			}

			p.Lifetime = l
		}

		if in.VnicID != nil {
			return b.movePrivateIP(p, *in.VnicID)
		}

		return nil
	})
}

func (b *InMemoryBackend) movePrivateIP(p *PrivateIP, vnicID string) *APIError {
	if p.IsPrimary {
		return errConflict("The primary private IP of a VNIC cannot be moved")
	}

	if vnicID == "" {
		if p.Lifetime != lifetimeReserved {
			return errParam("Only a RESERVED private IP can be unassigned")
		}

		b.releasePublicOf(p)
		p.VnicID, p.IPState = "", statAvailable

		return nil
	}

	v, aerr := getLive[*Vnic](b, vnicID)
	if aerr != nil {
		return aerr
	}

	if v.SubnetID != p.SubnetID {
		return errParam("The VNIC is in a different subnet than the private IP")
	}

	b.releasePublicOf(p)
	p.VnicID, p.IPState, p.AvailabilityDomain = v.ID, statAssigned, v.AvailabilityDomain

	return nil
}

func (b *InMemoryBackend) releasePublicOf(p *PrivateIP) {
	if ip := b.publicOf(p.ID); ip != nil {
		b.unassignPublic(ip)
	}
}

// DeletePrivateIP deletes a secondary private IP.
func (b *InMemoryBackend) DeletePrivateIP(id, ifMatch string) *APIError {
	b.mu.Lock("DeletePrivateIP")
	defer b.mu.Unlock()

	p, aerr := get[*PrivateIP](b, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return aerr
	}

	if p.IsPrimary {
		return errConflict("The primary private IP of a VNIC cannot be deleted")
	}

	if rt := firstID(liveOf(b, routesTo(id))); rt != "" {
		return errConflict("The private IP " + id + " is the target of a rule in route table " + rt)
	}

	b.dropPrivateIP(p)

	return nil
}

// PrivateIPListOpts are the ListPrivateIps filters.
type PrivateIPListOpts struct {
	IPAddress, SubnetID, VnicID, IPState, Lifetime, Page string
	Limit                                                int
}

func (o PrivateIPListOpts) matches(x *PrivateIP) bool {
	return (o.VnicID == "" || x.VnicID == o.VnicID) && (o.SubnetID == "" || x.SubnetID == o.SubnetID) &&
		(o.IPAddress == "" || x.IPAddress == o.IPAddress) && (o.IPState == "" || x.IPState == o.IPState) &&
		(o.Lifetime == "" || x.Lifetime == o.Lifetime)
}

func (b *InMemoryBackend) checkPrivateIPQuery(o PrivateIPListOpts) *APIError {
	if o.VnicID == "" && o.SubnetID == "" {
		return errMissing("vnicId or subnetId is required")
	}

	if o.IPAddress != "" && o.SubnetID == "" {
		return errMissing("subnetId is required with ipAddress")
	}

	for _, id := range []string{o.VnicID, o.SubnetID} {
		if _, ok := b.res[id]; id != "" && !ok {
			return errNotFound(id)
		}
	}

	return nil
}

// ListPrivateIPs lists private IPs by VNIC, or by subnet and address.
func (b *InMemoryBackend) ListPrivateIPs(o PrivateIPListOpts) (page.Page[resource], *APIError) {
	b.mu.RLock("ListPrivateIPs")
	defer b.mu.RUnlock()

	if aerr := b.checkPrivateIPQuery(o); aerr != nil {
		return page.Page[resource]{}, aerr
	}

	all := liveOf(b, o.matches)
	sortRes(all, ListOpts{Order: "ASC"})

	pg := page.New(all, o.Page, o.Limit, defaultListLimit)
	out := make([]resource, 0, len(pg.Data))

	for _, p := range pg.Data {
		out = append(out, p.view())
	}

	return page.Page[resource]{Data: out, Next: pg.Next}, nil
}

// ListNsgVnics lists the VNICs that are members of a network security group.
func (b *InMemoryBackend) ListNsgVnics(id string, o ListOpts) (page.Page[NsgVnic], *APIError) {
	b.mu.RLock("ListNsgVnics")
	defer b.mu.RUnlock()

	if _, aerr := get[*NetworkSecurityGroup](b, id); aerr != nil {
		return page.Page[NsgVnic]{}, aerr
	}

	var out []NsgVnic

	for _, v := range liveOf(b, func(x *Vnic) bool { return slices.Contains(x.NsgIDs, id) }) {
		n := NsgVnic{VnicID: v.ID, TimeAssociated: v.TimeCreated}

		for _, a := range liveOf(b, func(x *VnicAttachment) bool { return x.VnicID == v.ID }) {
			n.ResourceID = a.InstanceID
		}

		out = append(out, n)
	}

	slices.SortFunc(out, func(a, c NsgVnic) int { return strings.Compare(a.VnicID, c.VnicID) })

	return page.New(out, o.Page, o.Limit, defaultListLimit), nil
}
