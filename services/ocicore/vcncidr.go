package ocicore

import (
	"net/http"
	"net/netip"
	"slices"
)

const (
	statUpdating = "UPDATING"

	entityVcn    = "vcn"
	entitySubnet = "subnet"

	opAddVcnCidr       = "ADD_VCN_CIDR"
	opModifyVcnCidr    = "MODIFY_VCN_CIDR"
	opRemoveVcnCidr    = "REMOVE_VCN_CIDR"
	opAddVcnIpv6       = "ADD_VCN_IPV6_CIDR"
	opRemoveVcnIpv6    = "REMOVE_VCN_IPV6_CIDR"
	opAddSubnetCidr    = "ADD_SUBNET_IPV4_CIDR"
	opModifySubnetCidr = "MODIFY_SUBNET_IPV4_CIDR"
	opRemoveSubnetCidr = "REMOVE_SUBNET_IPV4_CIDR"
	opAddSubnetIpv6    = "ADD_SUBNET_IPV6_CIDR"
	opRemoveSubnetIpv6 = "REMOVE_SUBNET_IPV6_CIDR"

	maxSubnetCidrs = 6
)

func within(outer, inner netip.Prefix) bool {
	return outer.Bits() <= inner.Bits() && outer.Contains(inner.Addr())
}

// vcnForUpdate resolves a VCN for a CIDR change: it must be AVAILABLE and match the etag.
func (b *InMemoryBackend) vcnForUpdate(id, ifMatch string) (*Vcn, *APIError) {
	v, aerr := getLive[*Vcn](b, id)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = checkEtag(v, ifMatch); aerr != nil {
		return nil, aerr
	}

	if v.LifecycleState != statAvailable {
		return nil, errState("The VCN " + id + " is " + v.LifecycleState)
	}

	return v, nil
}

// vcnWritable refuses changes to subnets, LPGs and route tables while a CIDR update is running.
func (b *InMemoryBackend) vcnWritable(vcnID string) *APIError {
	if v, ok := b.res[vcnID].(*Vcn); ok && v.LifecycleState == statUpdating {
		return errState("The VCN " + vcnID + " is " + statUpdating + ": subnets, LPGs and route tables cannot change")
	}

	return nil
}

func (b *InMemoryBackend) beginVcnUpdate(v *Vcn, op string) string {
	b.moveTo(v, statUpdating, statAvailable)
	b.refreshPeers(v)

	return b.newWorkRequest(op, v.CompartmentID, []WorkResource{{
		ActionType: wrActionUpdated, EntityType: entityVcn, Identifier: v.ID, EntityURI: apiBase + "/vcns/" + v.ID,
	}})
}

// refreshPeers copies the VCN's CIDR blocks into the peer side of every connected local peering gateway.
func (b *InMemoryBackend) refreshPeers(v *Vcn) {
	for _, l := range liveOf(b, inVcn[*LocalPeeringGateway](v.ID)) {
		peer, ok := b.res[l.PeerID].(*LocalPeeringGateway)
		if !ok || l.PeeringStatus != peerPeered {
			continue
		}

		peer.PeerAdvertisedCidr, peer.PeerAdvertisedDetails = v.CidrBlock, slices.Clone(v.CidrBlocks)
		b.bump(peer)
	}
}

// peeredCidrs lists the CIDR blocks of the VCNs peered through the VCN's local peering gateways.
func (b *InMemoryBackend) peeredCidrs(vcnID string) []string {
	var out []string

	for _, l := range liveOf(b, inVcn[*LocalPeeringGateway](vcnID)) {
		if l.PeeringStatus == peerPeered {
			out = append(out, l.PeerAdvertisedDetails...)
		}
	}

	return out
}

func (b *InMemoryBackend) newVcnCidr(v *Vcn, cidr string, ignore string) (netip.Prefix, *APIError) {
	p, aerr := parseV4(cidr, "VCN CIDR block")
	if aerr != nil {
		return p, aerr
	}

	if reservedV4(p) {
		return p, errParam("VCN CIDR block " + cidr + " overlaps a reserved range")
	}

	for _, c := range v.CidrBlocks {
		if c != ignore && netip.MustParsePrefix(c).Overlaps(p) {
			return p, errParam("VCN CIDR block " + cidr + " overlaps CIDR block " + c + " of the VCN")
		}
	}

	if cidrsOverlap([]string{cidr}, b.peeredCidrs(v.ID)) {
		return p, errParam("VCN CIDR block " + cidr + " overlaps a CIDR block of a peered VCN")
	}

	return p, nil
}

// subnetsIn lists the live subnets of the VCN that have an IPv4 block inside the prefix.
func (b *InMemoryBackend) subnetsIn(vcnID string, p netip.Prefix) []*Subnet {
	return liveOf(b, func(s *Subnet) bool {
		return s.VcnID == vcnID && slices.ContainsFunc(s.Ipv4CidrBlocks, func(c string) bool {
			return within(p, netip.MustParsePrefix(c))
		})
	})
}

// AddVcnCidr adds an IPv4 CIDR block to a VCN and returns the work request id.
func (b *InMemoryBackend) AddVcnCidr(id, ifMatch, cidr string) (string, *APIError) {
	b.mu.Lock("AddVcnCidr")
	defer b.mu.Unlock()

	v, aerr := b.vcnForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if cidr == "" {
		return "", errMissing("cidrBlock is required")
	}

	if _, aerr = b.newVcnCidr(v, cidr, ""); aerr != nil {
		return "", aerr
	}

	if len(v.CidrBlocks) >= maxVcnCidrs {
		return "", errLimit("A VCN can have at most 16 IPv4 CIDR blocks")
	}

	v.CidrBlocks = append(v.CidrBlocks, cidr)

	return b.beginVcnUpdate(v, opAddVcnCidr), nil
}

func (b *InMemoryBackend) checkModifyUsage(v *Vcn, orig, next netip.Prefix) *APIError {
	for _, s := range b.subnetsIn(v.ID, orig) {
		for _, c := range s.Ipv4CidrBlocks {
			sp := netip.MustParsePrefix(c)
			if within(orig, sp) && !within(next, sp) {
				return errParam("Subnet " + s.ID + " would be outside the new CIDR block " + next.String())
			}
		}
	}

	for _, rt := range liveOf(b, inVcn[*RouteTable](v.ID)) {
		for _, r := range rt.RouteRules {
			dp, ok := parseAnyCidr(r.Destination)
			if ok && dp.Addr().Is4() && within(orig, dp) && !within(next, dp) {
				return errParam("Route rule destination " + r.Destination + " of " + rt.ID +
					" would be outside the new CIDR block " + next.String())
			}
		}
	}

	return nil
}

// ModifyVcnCidr replaces one IPv4 CIDR block of a VCN and returns the work request id.
func (b *InMemoryBackend) ModifyVcnCidr(id, ifMatch, original, updated string) (string, *APIError) {
	b.mu.Lock("ModifyVcnCidr")
	defer b.mu.Unlock()

	v, aerr := b.vcnForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if original == "" || updated == "" {
		return "", errMissing("originalCidrBlock and newCidrBlock are required")
	}

	i := slices.Index(v.CidrBlocks, original)
	if i < 0 {
		return "", errParam("originalCidrBlock " + original + " is not a CIDR block of the VCN")
	}

	next, aerr := b.newVcnCidr(v, updated, original)
	if aerr != nil {
		return "", aerr
	}

	if original == updated {
		return "", errParam("newCidrBlock must differ from originalCidrBlock")
	}

	if aerr = b.checkModifyUsage(v, netip.MustParsePrefix(original), next); aerr != nil {
		return "", aerr
	}

	v.CidrBlocks[i] = updated
	if i == 0 {
		v.CidrBlock = updated
	}

	return b.beginVcnUpdate(v, opModifyVcnCidr), nil
}

// RemoveVcnCidr removes an IPv4 CIDR block that no subnet uses and returns the work request id.
func (b *InMemoryBackend) RemoveVcnCidr(id, ifMatch, cidr string) (string, *APIError) {
	b.mu.Lock("RemoveVcnCidr")
	defer b.mu.Unlock()

	v, aerr := b.vcnForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	i := slices.Index(v.CidrBlocks, cidr)
	if i < 0 {
		return "", errParam("cidrBlock " + cidr + " is not a CIDR block of the VCN")
	}

	if len(v.CidrBlocks) == 1 {
		return "", errParam("A VCN must keep at least one IPv4 CIDR block")
	}

	if used := b.subnetsIn(v.ID, netip.MustParsePrefix(cidr)); len(used) > 0 {
		return "", errConflict(
			"The CIDR block " + cidr + " cannot be removed: subnet " + used[0].ID + " uses its addresses",
		)
	}

	v.CidrBlocks = slices.Delete(slices.Clone(v.CidrBlocks), i, i+1)
	v.CidrBlock = v.CidrBlocks[0]

	return b.beginVcnUpdate(v, opRemoveVcnCidr), nil
}

// AddVcnIpv6In is the AddIpv6VcnCidr request body.
type AddVcnIpv6In struct {
	IsOracleGuaAllocationEnabled *bool          `json:"isOracleGuaAllocationEnabled"`
	Byoipv6CidrDetail            map[string]any `json:"byoipv6CidrDetail"`
	Ipv6PrivateCidrBlock         string         `json:"ipv6PrivateCidrBlock"`
}

// AddIpv6VcnCidr adds an Oracle-allocated /56 or a private ULA prefix to a VCN.
func (b *InMemoryBackend) AddIpv6VcnCidr(id, ifMatch string, in AddVcnIpv6In) (string, *APIError) {
	b.mu.Lock("AddIpv6VcnCidr")
	defer b.mu.Unlock()

	v, aerr := b.vcnForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if in.Byoipv6CidrDetail != nil {
		return "", errParam("byoipv6CidrDetail is not supported: BYOIP ranges are not modelled")
	}

	if in.Ipv6PrivateCidrBlock != "" {
		if aerr = validUlaBlocks([]string{in.Ipv6PrivateCidrBlock}); aerr != nil {
			return "", aerr
		}

		if slices.Contains(v.Ipv6PrivateCidrs, in.Ipv6PrivateCidrBlock) {
			return "", errConflict("The IPv6 prefix " + in.Ipv6PrivateCidrBlock + " is already on the VCN")
		}

		v.Ipv6PrivateCidrs = append(v.Ipv6PrivateCidrs, in.Ipv6PrivateCidrBlock)

		return b.beginVcnUpdate(v, opAddVcnIpv6), nil
	}

	if in.IsOracleGuaAllocationEnabled != nil && !*in.IsOracleGuaAllocationEnabled {
		return "", errParam("ipv6PrivateCidrBlock is required when isOracleGuaAllocationEnabled is false")
	}

	if len(v.Ipv6CidrBlocks) > 0 {
		return "", errConflict("The VCN " + id + " already has an Oracle-allocated IPv6 prefix")
	}

	b.ipv6Seq++
	v.Ipv6CidrBlocks = []string{v6Block(b.ipv6Seq)}
	b.addIpv6DefaultRules(v)

	return b.beginVcnUpdate(v, opAddVcnIpv6), nil
}

// addIpv6DefaultRules gives the default security list the IPv6 rules a VCN created with IPv6 would have.
func (b *InMemoryBackend) addIpv6DefaultRules(v *Vcn) {
	sl, ok := b.res[v.DefaultSecurityList].(*SecurityList)
	if !ok || !live(sl) {
		return
	}

	if !slices.ContainsFunc(sl.IngressRules, func(r IngressRule) bool { return r.Source == anyV6 }) {
		for _, r := range defaultIngress(nil, true) {
			if r.Source == anyV6 {
				sl.IngressRules = append(sl.IngressRules, r)
			}
		}
	}

	if !slices.ContainsFunc(sl.EgressRules, func(r EgressRule) bool { return r.Destination == anyV6 }) {
		sl.EgressRules = append(sl.EgressRules, defaultEgress(true)[1])
	}

	b.bump(sl)
}

// RemoveIpv6VcnCidr removes an IPv6 prefix that no subnet uses.
func (b *InMemoryBackend) RemoveIpv6VcnCidr(id, ifMatch, cidr string) (string, *APIError) {
	b.mu.Lock("RemoveIpv6VcnCidr")
	defer b.mu.Unlock()

	v, aerr := b.vcnForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	gua, priv := slices.Index(v.Ipv6CidrBlocks, cidr), slices.Index(v.Ipv6PrivateCidrs, cidr)
	if gua < 0 && priv < 0 {
		return "", errParam("ipv6CidrBlock " + cidr + " is not an IPv6 prefix of the VCN")
	}

	p := netip.MustParsePrefix(cidr)

	for _, s := range liveOf(b, inVcn[*Subnet](v.ID)) {
		if slices.ContainsFunc(s.Ipv6CidrBlocks, func(c string) bool { return within(p, netip.MustParsePrefix(c)) }) {
			return "", errConflict("The IPv6 prefix " + cidr + " cannot be removed: subnet " + s.ID + " uses it")
		}
	}

	if gua >= 0 {
		v.Ipv6CidrBlocks = slices.Delete(slices.Clone(v.Ipv6CidrBlocks), gua, gua+1)
	} else {
		v.Ipv6PrivateCidrs = slices.Delete(slices.Clone(v.Ipv6PrivateCidrs), priv, priv+1)
	}

	return b.beginVcnUpdate(v, opRemoveVcnIpv6), nil
}

func (q *req) acceptCidrChange(wr string, etag string) error {
	if etag != "" {
		q.header("etag", etag)
	}

	return q.acceptWorkRequest(wr)
}

func cidrEntries() []entry {
	vcn, sn := idPath("/vcns", "vcnId"), idPath("/subnets", "subnetId")

	return []entry{
		{(*Handler).addVcnCidr, http.MethodPost, vcn + "/actions/addCidr", "AddVcnCidr"},
		{(*Handler).modifyVcnCidr, http.MethodPost, vcn + "/actions/modifyCidr", "ModifyVcnCidr"},
		{(*Handler).removeVcnCidr, http.MethodPost, vcn + "/actions/removeCidr", "RemoveVcnCidr"},
		{(*Handler).addIpv6VcnCidr, http.MethodPost, vcn + "/actions/addIpv6Cidr", "AddIpv6VcnCidr"},
		{(*Handler).removeIpv6VcnCidr, http.MethodPost, vcn + "/actions/removeIpv6Cidr", "RemoveIpv6VcnCidr"},
		{(*Handler).addIpv4SubnetCidr, http.MethodPost, sn + "/actions/addIpv4Cidr", "AddIpv4SubnetCidr"},
		{(*Handler).modifyIpv4SubnetCidr, http.MethodPost, sn + "/actions/modifyIpv4Cidr", "ModifyIpv4SubnetCidr"},
		{(*Handler).removeIpv4SubnetCidr, http.MethodPost, sn + "/actions/removeIpv4Cidr", "RemoveIpv4SubnetCidr"},
		{(*Handler).addIpv6SubnetCidr, http.MethodPost, sn + "/actions/addIpv6Cidr", "AddIpv6SubnetCidr"},
		{(*Handler).removeIpv6SubnetCidr, http.MethodPost, sn + "/actions/removeIpv6Cidr", "RemoveIpv6SubnetCidr"},
	}
}

func (h *Handler) addVcnCidr(q *req) error {
	var in struct {
		CidrBlock string `json:"cidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.AddVcnCidr(q.param("vcnId"), q.ifMatch(), in.CidrBlock)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func (h *Handler) modifyVcnCidr(q *req) error {
	var in struct {
		OriginalCidrBlock string `json:"originalCidrBlock"`
		NewCidrBlock      string `json:"newCidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.ModifyVcnCidr(q.param("vcnId"), q.ifMatch(), in.OriginalCidrBlock, in.NewCidrBlock)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func (h *Handler) removeVcnCidr(q *req) error {
	var in struct {
		CidrBlock string `json:"cidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.RemoveVcnCidr(q.param("vcnId"), q.ifMatch(), in.CidrBlock)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func (h *Handler) addIpv6VcnCidr(q *req) error {
	var in AddVcnIpv6In
	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	wr, aerr := h.Backend.AddIpv6VcnCidr(q.param("vcnId"), q.ifMatch(), in)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func (h *Handler) removeIpv6VcnCidr(q *req) error {
	var in struct {
		Ipv6CidrBlock string `json:"ipv6CidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.RemoveIpv6VcnCidr(q.param("vcnId"), q.ifMatch(), in.Ipv6CidrBlock)
	if aerr != nil {
		return aerr
	}

	return q.acceptCidrChange(wr, "")
}
