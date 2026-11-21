package gcpcompute

import (
	"fmt"
	"net/netip"
	"slices"
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collICAttachments  = "interconnectAttachments"
	iaEncNone          = "NONE"
	iaEncIPsec         = "IPSEC"
	iaDedicated        = "DEDICATED"
	iaL2Dedicated      = "L2_DEDICATED"
	iaPartner          = "PARTNER"
	iaPartnerProvider  = "PARTNER_PROVIDER"
	iaPendingCustomer  = "PENDING_CUSTOMER"
	iaOSUnprovisioned  = "OS_UNPROVISIONED"
	iaDefaultBandwidth = "BPS_10G"
	iaPartnerBandwidth = "BPS_1G"
	iaDefaultMTU       = 1440
	iaMinVlan          = 2
	iaMaxVlan          = 4093
	iaMaxCandidates    = 16
	iaMaxPrefixLen     = 29
	iaMaxIPv6Prefix    = 126
	iaDefaultSubnetLen = 29
	iaLinkLocal        = "169.254.0.0/16"
	iaMetadataIP       = "169.254.169.254"
)

var iaBandwidths = []string{ //nolint:gochecknoglobals // read-only lookup table
	"BPS_50M", "BPS_100M", "BPS_200M", "BPS_300M", "BPS_400M", "BPS_500M", "BPS_1G", "BPS_2G", "BPS_5G",
	"BPS_10G", "BPS_20G", "BPS_50G", "BPS_100G", "BPS_400G",
}

var iaMTUs = []int64{1440, 1460, 1500, 8896} //nolint:gochecknoglobals // read-only lookup table

var iaDomains = []string{ //nolint:gochecknoglobals // read-only lookup table
	"AVAILABILITY_DOMAIN_ANY", "AVAILABILITY_DOMAIN_1", "AVAILABILITY_DOMAIN_2",
}

var iaPatchDrop = []string{ //nolint:gochecknoglobals // read-only lookup table
	"id", jsonKind, jsonSelfLink, jsonCreated, jsonName, jsonLabels, jsonLabelFP, jsonRegion, "state",
	"operationalStatus", jsonParams, "pairingKey", "cloudRouterIpAddress", "customerRouterIpAddress",
	"cloudRouterIpv6Address", "customerRouterIpv6Address", "privateInterconnectInfo",
}

func icAttachmentSpec() lbSpec[computeapi.InterconnectAttachment] {
	return lbSpec[computeapi.InterconnectAttachment]{
		coll: collICAttachments, kind: "compute#interconnectAttachment", noun: "interconnect_attachment",
		m: func(b *InMemoryBackend) map[string]*computeapi.InterconnectAttachment { return b.icAttachments },
		meta: func(v *computeapi.InterconnectAttachment) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep: prepICAttachment,
		keep: func(old, next *computeapi.InterconnectAttachment) {
			next.State, next.OperationalStatus, next.PairingKey = old.State, old.OperationalStatus, old.PairingKey
			next.CloudRouterIpAddress, next.CustomerRouterIpAddress = old.CloudRouterIpAddress, old.CustomerRouterIpAddress
			next.CloudRouterIpv6Address = old.CloudRouterIpv6Address
			next.CustomerRouterIpv6Address = old.CustomerRouterIpv6Address
			next.PrivateInterconnectInfo = old.PrivateInterconnectInfo
		},
		refs: func(v *computeapi.InterconnectAttachment) []string {
			var out []string
			if v.Interconnect != "" {
				out = append(out, v.Interconnect)
			}

			if v.Router != "" {
				out = append(out, v.Router)
			}

			return out
		},
	}
}

func prepICAttachment(
	b *InMemoryBackend, c lbCtx, in *computeapi.InterconnectAttachment, raw map[string]any,
) *APIError {
	fresh := in.State == ""
	in.Params = nil

	if fresh {
		if err := b.prepICAttachmentCreate(c, in, raw); err != nil {
			return err
		}
	}

	if err := b.prepICAttachmentCommon(c, in); err != nil {
		return err
	}

	in.State, in.OperationalStatus = icActive, icOSActive

	if in.Type == iaPartner && !in.AdminEnabled {
		in.State, in.OperationalStatus = iaPendingCustomer, iaOSUnprovisioned
	}

	return nil
}

func (b *InMemoryBackend) prepICAttachmentCreate(
	c lbCtx, in *computeapi.InterconnectAttachment, raw map[string]any,
) *APIError {
	if in.Type == "" {
		in.Type = iaDedicated
		if in.Interconnect == "" {
			in.Type = iaPartner
		}
	}

	if _, set := raw["adminEnabled"]; !set {
		in.AdminEnabled = true
	}

	if in.Encryption == "" {
		in.Encryption = iaEncNone
	}

	if in.Mtu == 0 {
		in.Mtu = iaDefaultMTU
	}

	if in.StackType == "" {
		in.StackType = stackIPv4Only
	}

	if err := b.prepICType(c, in); err != nil {
		return err
	}

	if err := b.prepICIPsec(c, in); err != nil {
		return err
	}

	if err := b.allocICAddresses(in); err != nil {
		return err
	}

	in.CandidateSubnets, in.SubnetLength = nil, 0
	in.CandidateCloudRouterIpAddress, in.CandidateCustomerRouterIpAddress = "", ""
	in.CandidateCloudRouterIpv6Address, in.CandidateCustomerRouterIpv6Address = "", ""

	return nil
}

func (b *InMemoryBackend) prepICType(c lbCtx, in *computeapi.InterconnectAttachment) *APIError {
	switch in.Type {
	case iaDedicated, iaL2Dedicated:
		return b.prepICDedicated(c, in)
	case iaPartner:
		return prepICPartner(c, in)
	case iaPartnerProvider:
		return errField("resource.type", in.Type,
			"Partner provider attachments are created by Google Cloud partners and are not supported here.")
	default:
		return errField("resource.type", in.Type, "Must be DEDICATED, L2_DEDICATED or PARTNER.")
	}
}

func (b *InMemoryBackend) prepICDedicated(c lbCtx, in *computeapi.InterconnectAttachment) *APIError {
	if in.Type == iaL2Dedicated && in.L2Forwarding == nil {
		return errRequired("resource.l2Forwarding")
	}

	if err := b.resolveICInterconnect(c, in); err != nil {
		return err
	}

	if in.Bandwidth == "" {
		in.Bandwidth = iaDefaultBandwidth
	}

	if in.VlanTag8021q == 0 {
		vlan, err := b.freeICVlan(in.Interconnect)
		if err != nil {
			return err
		}

		in.VlanTag8021q = vlan
	}

	if in.VlanTag8021q < iaMinVlan || in.VlanTag8021q > iaMaxVlan {
		return errField("resource.vlanTag8021q", strconv.FormatInt(in.VlanTag8021q, 10), "Must be 2-4093.")
	}

	for _, other := range b.icAttachments {
		if other.Interconnect == in.Interconnect && other.VlanTag8021q == in.VlanTag8021q {
			return errField("resource.vlanTag8021q", strconv.FormatInt(in.VlanTag8021q, 10),
				"The VLAN tag is already used by "+trimURL(other.SelfLink)+".")
		}
	}

	in.PrivateInterconnectInfo = &computeapi.InterconnectAttachmentPrivateInfo{Tag8021q: in.VlanTag8021q}

	return nil
}

func prepICPartner(c lbCtx, in *computeapi.InterconnectAttachment) *APIError {
	if in.Interconnect != "" {
		return errField("resource.interconnect", in.Interconnect, "Not valid for PARTNER attachments.")
	}

	if in.EdgeAvailabilityDomain == "" {
		in.EdgeAvailabilityDomain = iaDomains[0]
	}

	if !inList(iaDomains, in.EdgeAvailabilityDomain) {
		return errField("resource.edgeAvailabilityDomain", in.EdgeAvailabilityDomain, "")
	}

	domain := map[string]string{iaDomains[0]: "any", iaDomains[1]: "1", iaDomains[2]: "2"}[in.EdgeAvailabilityDomain]
	in.PairingKey = fmt.Sprintf("%016x/%s/%s", in.Id, c.loc, domain)
	in.Bandwidth = iaPartnerBandwidth
	in.VlanTag8021q = 0

	return nil
}

func (b *InMemoryBackend) resolveICInterconnect(c lbCtx, in *computeapi.InterconnectAttachment) *APIError {
	if in.Interconnect == "" {
		return errRequired("resource.interconnect")
	}

	r, err := refGlobal(in.Interconnect, collInterconnects, c.project)
	if err != nil {
		return errField("resource.interconnect", in.Interconnect, "The URL is malformed.")
	}

	ic, ok := b.interconnects[key(r.project, scopeGlobal, r.name)]
	if !ok {
		return errNotFound(relPath(r.project, scopeGlobal, "", collInterconnects, r.name))
	}

	if ic.InterconnectType == icPartner {
		return errField("resource.interconnect", in.Interconnect, "Not a Dedicated Interconnect.")
	}

	if ic.State != icActive {
		return errNotReady(trimURL(ic.SelfLink), "The interconnect is not active.")
	}

	in.Interconnect = ic.SelfLink

	return nil
}

func (b *InMemoryBackend) freeICVlan(interconnect string) (int64, *APIError) {
	used := map[int64]bool{}

	for _, a := range b.icAttachments {
		if a.Interconnect == interconnect {
			used[a.VlanTag8021q] = true
		}
	}

	for v := int64(iaMinVlan); v <= iaMaxVlan; v++ {
		if !used[v] {
			return v, nil
		}
	}

	return 0, errInvalid("Every VLAN tag of the interconnect is in use.")
}

func (b *InMemoryBackend) prepICIPsec(c lbCtx, in *computeapi.InterconnectAttachment) *APIError {
	if !inList([]string{iaEncNone, iaEncIPsec}, in.Encryption) {
		return errField("resource.encryption", in.Encryption, "Must be NONE or IPSEC.")
	}

	if in.Encryption == iaEncNone {
		if len(in.IpsecInternalAddresses) > 0 {
			return errField("resource.ipsecInternalAddresses", "", "Only valid when encryption is IPSEC.")
		}

		return nil
	}

	for i, raw := range in.IpsecInternalAddresses {
		r, err := refLocated(raw, collAddresses, scopeRegion, c.project, c.loc)
		if err != nil || r.loc != c.loc {
			return errField("resource.ipsecInternalAddresses", raw, "Must be a regional address in "+c.loc+".")
		}

		a, aerr := b.getAddressLocked(r.project, scopeRegion, r.loc, r.name)
		if aerr != nil {
			return aerr
		}

		if a.AddressType != addrInternal {
			return errField("resource.ipsecInternalAddresses", raw, "Must be a regional internal address.")
		}

		in.IpsecInternalAddresses[i] = a.SelfLink
	}

	return nil
}

func (b *InMemoryBackend) prepICAttachmentCommon(c lbCtx, in *computeapi.InterconnectAttachment) *APIError {
	if in.Bandwidth != "" && !inList(iaBandwidths, in.Bandwidth) {
		return errField("resource.bandwidth", in.Bandwidth, "")
	}

	if !slices.Contains(iaMTUs, in.Mtu) {
		return errField("resource.mtu", strconv.FormatInt(in.Mtu, 10), "Must be 1440, 1460, 1500 or 8896.")
	}

	if in.StackType != stackIPv4Only && in.StackType != stackIPv4IPv6 {
		return errField("resource.stackType", in.StackType, "Must be IPV4_ONLY or IPV4_IPV6.")
	}

	if in.Router != "" {
		r, ok := parseRef(in.Router, "routers", c.project, c.loc)
		if !ok || r.loc != c.loc {
			return errField("resource.router", in.Router, "The router must be in "+c.loc+".")
		}

		rt, found := b.routers[key(r.project, c.loc, r.name)]
		if !found {
			return errNotFound(relPath(r.project, scopeRegion, c.loc, "routers", r.name))
		}

		in.Router = rt.Router.SelfLink
	}

	if err := validLabels(in.Labels); err != nil {
		return err
	}

	if in.LabelFingerprint == "" {
		in.LabelFingerprint = labelFP(in.Labels)
	}

	return nil
}

func parsePair(
	cloud, customer string,
	maxLen int,
	field string,
	v6 bool,
) (netip.Prefix, netip.Prefix, bool, *APIError) {
	if cloud == "" && customer == "" {
		return netip.Prefix{}, netip.Prefix{}, false, nil
	}

	if cloud == "" || customer == "" {
		return netip.Prefix{}, netip.Prefix{}, false, errField(field, "",
			"Both the cloud router and the customer router address must be set, or neither.")
	}

	cp, err1 := netip.ParsePrefix(cloud)
	rp, err2 := netip.ParsePrefix(customer)

	if err1 != nil || err2 != nil || cp.Addr().Is4() == v6 || rp.Addr().Is4() == v6 {
		return cp, rp, false, errField(field, cloud, "Must be an address with a prefix length.")
	}

	if cp.Bits() != rp.Bits() || cp.Bits() > maxLen {
		return cp, rp, false, errField(field, cloud,
			"Both prefix lengths must be equal and at most "+strconv.Itoa(maxLen)+".")
	}

	return cp, rp, true, nil
}

func (b *InMemoryBackend) allocICAddresses(in *computeapi.InterconnectAttachment) *APIError {
	if _, r, ok, err := parsePair(in.CandidateCloudRouterIpv6Address, in.CandidateCustomerRouterIpv6Address,
		iaMaxIPv6Prefix, "resource.candidateCloudRouterIpv6Address", true); err != nil {
		return err
	} else if ok {
		in.CloudRouterIpv6Address, in.CustomerRouterIpv6Address = in.CandidateCloudRouterIpv6Address, r.String()
	}

	cp, rp, ok, err := parsePair(in.CandidateCloudRouterIpAddress, in.CandidateCustomerRouterIpAddress,
		31, "resource.candidateCloudRouterIpAddress", false) //nolint:mnd // documented maximum prefix length
	if err != nil {
		return err
	}

	if ok {
		in.CloudRouterIpAddress, in.CustomerRouterIpAddress = cp.String(), rp.String()

		return nil
	}

	size := in.SubnetLength
	if size == 0 {
		size = iaDefaultSubnetLen
	}

	if size != iaDefaultSubnetLen && size != iaDefaultSubnetLen+1 {
		return errField("resource.subnetLength", strconv.FormatInt(size, 10), "Must be 29 or 30.")
	}

	cands, cerr := candidatePrefixes(in.CandidateSubnets)
	if cerr != nil {
		return cerr
	}

	block, found := b.freeICBlock(cands, int(size))
	if !found {
		return errInvalid("No unused /%d is available in the requested link-local space.", size)
	}

	in.CloudRouterIpAddress = netip.PrefixFrom(block.Addr().Next(), block.Bits()).String()
	in.CustomerRouterIpAddress = netip.PrefixFrom(block.Addr().Next().Next(), block.Bits()).String()

	return nil
}

func candidatePrefixes(raw []string) ([]netip.Prefix, *APIError) {
	if len(raw) > iaMaxCandidates {
		return nil, errField("resource.candidateSubnets", "", "At most 16 candidate subnets are allowed.")
	}

	if len(raw) == 0 {
		return []netip.Prefix{netip.MustParsePrefix(iaLinkLocal)}, nil
	}

	linkLocal := netip.MustParsePrefix(iaLinkLocal)
	out := make([]netip.Prefix, 0, len(raw))

	for _, s := range raw {
		p, err := netip.ParsePrefix(s)
		if err != nil || !p.Addr().Is4() || p.Masked() != p || !linkLocal.Contains(p.Addr()) ||
			p.Bits() < linkLocal.Bits() || p.Bits() > iaMaxPrefixLen {
			return nil, errField("resource.candidateSubnets", s,
				"Must be a prefix within 169.254.0.0/16 that is /29 or shorter.")
		}

		out = append(out, p)
	}

	return out, nil
}

// freeICBlock returns the lowest unused block of the given prefix size inside the candidates.
func (b *InMemoryBackend) freeICBlock(cands []netip.Prefix, size int) (netip.Prefix, bool) {
	var taken []netip.Prefix

	for _, a := range b.icAttachments {
		if p, err := netip.ParsePrefix(a.CloudRouterIpAddress); err == nil {
			taken = append(taken, p.Masked())
		}
	}

	meta := netip.MustParseAddr(iaMetadataIP)

	for _, cand := range cands {
		for blk := netip.PrefixFrom(cand.Addr(), size); cand.Contains(blk.Addr()); {
			if !blk.Contains(meta) &&
				!slices.ContainsFunc(taken, func(t netip.Prefix) bool { return t.Overlaps(blk) }) {
				return blk, true
			}

			next := lastAddr(blk).Next()
			if !next.IsValid() {
				break
			}

			blk = netip.PrefixFrom(next, size)
		}
	}

	return netip.Prefix{}, false
}

func (b *InMemoryBackend) patchICAttachment(c lbCtx, name string, raw map[string]any) (*computeapi.Operation, error) {
	return lbMutate(b, icAttachmentSpec(), c, name, "patch", nil,
		func(cur *computeapi.InterconnectAttachment) (*computeapi.InterconnectAttachment, *APIError) {
			next, err := mergeInto(cur, maskedBody(raw, "", iaPatchDrop))
			if err != nil {
				return nil, err
			}

			for field, same := range map[string]bool{
				"type":                   next.Type == cur.Type,
				"interconnect":           next.Interconnect == cur.Interconnect,
				"vlanTag8021q":           next.VlanTag8021q == cur.VlanTag8021q,
				"encryption":             next.Encryption == cur.Encryption,
				"ipsecInternalAddresses": slices.Equal(next.IpsecInternalAddresses, cur.IpsecInternalAddresses),
				"edgeAvailabilityDomain": next.EdgeAvailabilityDomain == cur.EdgeAvailabilityDomain,
			} {
				if !same {
					return nil, errField("resource."+field, "", "Field is not updatable.")
				}
			}

			icAttachmentSpec().keep(cur, next)

			return next, nil
		})
}

func (h *Handler) registerICAttachmentOps() {
	r := []string{scopeRegion}

	registerLB(h, icAttachmentSpec(), lbOpts[computeapi.InterconnectAttachment]{
		scopes: r, agg: regionScopes(), noInsert: true,
	})
	regLabelSetter(h, icAttachmentSpec(), r, func(v *computeapi.InterconnectAttachment) (*map[string]string, *string) {
		return &v.Labels, &v.LabelFingerprint
	})
	h.reg(collICAttachments, scopeRegion, verbInsert, func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[computeapi.InterconnectAttachment](rq)
		if err != nil {
			return nil, err
		}

		return opResult(lbInsertMode(h.Backend, icAttachmentSpec(), ctxOf(rq), in, raw,
			rq.q.Get("validateOnly") == "true"))
	})
	h.reg(collICAttachments, scopeRegion, verbPatch, func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.InterconnectAttachment](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.patchICAttachment(ctxOf(rq), rq.rt.name, raw))
	})
}
