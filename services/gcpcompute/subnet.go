package gcpcompute

import (
	"net/netip"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const maxSecondaryRanges = 30

func subnetTarget(project, region string, s *computeapi.Subnetwork) target {
	return target{project: project, scope: scopeRegion, loc: region, collection: "subnetworks", name: s.Name, id: s.Id}
}

func (b *InMemoryBackend) getSubnetLocked(project, region, name string) (*computeapi.Subnetwork, *APIError) {
	s, ok := b.subnets[key(project, region, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeRegion, region, "subnetworks", name))
	}

	return s, nil
}

func (b *InMemoryBackend) resolveSubnetLocked(project, region, raw string) (*computeapi.Subnetwork, *APIError) {
	r, err := refLocated(raw, "subnetworks", scopeRegion, project, region)
	if err != nil {
		return nil, errField("subnetwork", raw, "The URL is malformed.")
	}

	return b.getSubnetLocked(r.project, r.loc, r.name)
}

// networkRangesLocked returns every primary and secondary range of the network's subnets except skip.
func (b *InMemoryBackend) networkRangesLocked(network, skip string) []netip.Prefix {
	var out []netip.Prefix

	for _, s := range b.subnets {
		if s.Network != network || s.SelfLink == skip {
			continue
		}

		if p, err := netip.ParsePrefix(s.IpCidrRange); err == nil {
			out = append(out, p)
		}

		for _, sec := range s.SecondaryIpRanges {
			if p, err := netip.ParsePrefix(sec.IpCidrRange); err == nil {
				out = append(out, p)
			}
		}
	}

	if n := b.networkBySelfLinkLocked(network); n != nil {
		for _, peer := range b.activePeersLocked(n) {
			for p := range b.networkRangesWithOwnerLocked(peer) {
				out = append(out, p)
			}
		}
	}

	return out
}

func validSecondaries(
	secs []*computeapi.SubnetworkSecondaryRange,
	taken []netip.Prefix,
	existing int,
) *APIError {
	if existing+len(secs) > maxSecondaryRanges {
		return errInvalid("Too many secondary ranges; at most %d are allowed.", maxSecondaryRanges)
	}

	names := map[string]bool{}

	for i, sec := range secs {
		field := "resource.secondaryIpRanges[" + int64Str(int64(i)) + "]"

		if err := validName(field+".rangeName", sec.RangeName); err != nil {
			return err
		}

		if names[sec.RangeName] {
			return errField(field+".rangeName", sec.RangeName, "Duplicate secondary range name.")
		}

		names[sec.RangeName] = true

		p, err := parseSubnetCIDR(field+".ipCidrRange", sec.IpCidrRange)
		if err != nil {
			return err
		}

		if overlaps(p, taken...) {
			return errField(
				field+".ipCidrRange",
				sec.IpCidrRange,
				"Range overlaps with an existing range in the network.",
			)
		}

		taken = append(taken, p)
	}

	return nil
}

func (b *InMemoryBackend) insertSubnet(
	project, region string,
	in *computeapi.Subnetwork,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertSubnet")
	defer b.mu.Unlock()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if _, ok := findRegion(region); !ok {
		return nil, errNotFound(scopePath(project, scopeRegion, region))
	}

	if in.Network == "" {
		return nil, errRequired(collNetwork)
	}

	n, err := b.resolveNetworkLocked(project, in.Network)
	if err != nil {
		return nil, err
	}

	if n.AutoCreateSubnetworks {
		return nil, errInvalid(
			"Invalid value for field 'resource.network': '%s'. Subnetworks cannot be created in an auto mode network.",
			in.Network,
		)
	}

	if _, dup := b.subnets[key(project, region, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeRegion, region, "subnetworks", in.Name))
	}

	p, err := parseSubnetCIDR("resource.ipCidrRange", in.IpCidrRange)
	if err != nil {
		return nil, err
	}

	taken := b.networkRangesLocked(n.SelfLink, "")
	if overlaps(p, taken...) {
		return nil, errField(
			"resource.ipCidrRange",
			in.IpCidrRange,
			"Range overlaps with an existing subnetwork in the network.",
		)
	}

	if apiErr := validSecondaries(in.SecondaryIpRanges, append(taken, p), 0); apiErr != nil {
		return nil, apiErr
	}

	s := newSubnet(project, region, in.Name, n.SelfLink, p, b.stamp())
	s.Description, s.PrivateIpGoogleAccess, s.EnableFlowLogs = in.Description, in.PrivateIpGoogleAccess, in.EnableFlowLogs
	s.SecondaryIpRanges = in.SecondaryIpRanges

	if in.LogConfig != nil {
		s.LogConfig, s.EnableFlowLogs = in.LogConfig, in.LogConfig.Enable || in.EnableFlowLogs
	}

	b.subnets[key(project, region, in.Name)] = s
	b.syncSubnetRoutesLocked(project, n)

	return b.newOpLocked("insert", subnetTarget(project, region, s), b.timing.Done), nil
}

func (b *InMemoryBackend) getSubnet(project, region, name string) (*computeapi.Subnetwork, error) {
	b.mu.RLock("GetSubnet")
	defer b.mu.RUnlock()

	s, err := b.getSubnetLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	return viewSubnet(s), nil
}

func viewSubnet(s *computeapi.Subnetwork) *computeapi.Subnetwork {
	out := clone(s)
	out.ForceSendFields = []string{"PrivateIpGoogleAccess", "EnableFlowLogs"}

	return out
}

func (b *InMemoryBackend) listSubnets(project, scope, loc string) []entry {
	b.mu.RLock("ListSubnets")
	defer b.mu.RUnlock()

	var out []entry

	for k, s := range b.subnets {
		region := lastSegment(s.Region)
		if !strings.HasPrefix(k, project+"/") || (scope == scopeRegion && region != loc) {
			continue
		}

		out = append(out, entry{scope: scopeRegion + "/" + region, item: viewSubnet(s)})
	}

	return out
}

func (b *InMemoryBackend) subnetUserLocked(s *computeapi.Subnetwork) string {
	for _, r := range b.instances {
		for _, nic := range r.Inst.NetworkInterfaces {
			if nic.Subnetwork == s.SelfLink {
				return trimURL(r.Inst.SelfLink)
			}
		}
	}

	for _, a := range b.addresses {
		if a.Subnetwork == s.SelfLink {
			return trimURL(a.SelfLink)
		}
	}

	for _, r := range b.routers {
		for _, nat := range r.Router.Nats {
			for _, sn := range nat.Subnetworks {
				if sn.Name == s.SelfLink {
					return trimURL(r.Router.SelfLink)
				}
			}
		}
	}

	return ""
}

func (b *InMemoryBackend) deleteSubnet(project, region, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteSubnet")
	defer b.mu.Unlock()

	s, err := b.getSubnetLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	if by := b.subnetUserLocked(s); by != "" {
		return nil, errInUse("subnetwork", relPath(project, scopeRegion, region, "subnetworks", name), by)
	}

	delete(b.subnets, key(project, region, name))
	b.syncNetworkRoutesBySelfLinkLocked(project, s.Network)

	return b.newOpLocked("delete", subnetTarget(project, region, s), b.timing.Done), nil
}

func (b *InMemoryBackend) expandSubnet(project, region, name, cidr string) (*computeapi.Operation, error) {
	b.mu.Lock("ExpandSubnet")
	defer b.mu.Unlock()

	s, err := b.getSubnetLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	next, err := parseSubnetCIDR("ipCidrRange", cidr)
	if err != nil {
		return nil, err
	}

	cur := netip.MustParsePrefix(s.IpCidrRange)
	if next.Bits() >= cur.Bits() || !next.Contains(cur.Addr()) {
		return nil, errField(
			"ipCidrRange",
			cidr,
			"New range must be a strictly larger range that contains the existing range "+s.IpCidrRange+".",
		)
	}

	if overlaps(next, b.networkRangesLocked(s.Network, s.SelfLink)...) {
		return nil, errField("ipCidrRange", cidr, "Range overlaps with an existing range in the network.")
	}

	s.IpCidrRange, s.GatewayAddress, s.Fingerprint = next.String(), gateway(next).String(), newFingerprint()
	b.syncNetworkRoutesBySelfLinkLocked(project, s.Network)

	return b.newOpLocked("expandIpCidrRange", subnetTarget(project, region, s), b.timing.Done), nil
}

func (b *InMemoryBackend) setSubnetPrivateGoogleAccess(
	project, region, name string,
	enabled bool,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetPrivateIpGoogleAccess")
	defer b.mu.Unlock()

	s, err := b.getSubnetLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	s.PrivateIpGoogleAccess, s.Fingerprint = enabled, newFingerprint()

	return b.newOpLocked("setPrivateIpGoogleAccess", subnetTarget(project, region, s), b.timing.Done), nil
}

var subnetPatchable = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
	"name": true, collNetwork: true, "ipCidrRange": true, "secondaryIpRanges": true, "privateIpGoogleAccess": true,
	"enableFlowLogs": true, "logConfig": true, "description": true, "fingerprint": true, "region": true,
	"kind": true, "id": true, "selfLink": true, "creationTimestamp": true, "gatewayAddress": true, "purpose": true,
	"stackType": true, "privateIpv6GoogleAccess": true,
}

func (b *InMemoryBackend) patchSubnet(
	project, region, name string,
	in *computeapi.Subnetwork,
	raw map[string]any,
) (*computeapi.Operation, error) {
	b.mu.Lock("PatchSubnet")
	defer b.mu.Unlock()

	s, err := b.getSubnetLocked(project, region, name)
	if err != nil {
		return nil, err
	}

	for k := range raw {
		if !subnetPatchable[k] {
			return nil, errField("resource."+k, "", "Field is not updatable.")
		}
	}

	if in.Fingerprint != "" && in.Fingerprint != s.Fingerprint {
		return nil, errPrecondition("Supplied fingerprint does not match current fingerprint.")
	}

	if in.IpCidrRange != "" && in.IpCidrRange != s.IpCidrRange {
		return nil, errField(
			"resource.ipCidrRange",
			in.IpCidrRange,
			"Use expandIpCidrRange to change the primary range.",
		)
	}

	if err = b.mergeSecondariesLocked(s, in); err != nil {
		return nil, err
	}

	if _, ok := raw["privateIpGoogleAccess"]; ok {
		s.PrivateIpGoogleAccess = in.PrivateIpGoogleAccess
	}

	if _, ok := raw["description"]; ok {
		s.Description = in.Description
	}

	if in.LogConfig != nil {
		s.LogConfig, s.EnableFlowLogs = in.LogConfig, in.LogConfig.Enable
	} else if _, ok := raw["enableFlowLogs"]; ok {
		s.EnableFlowLogs = in.EnableFlowLogs
	}

	s.Fingerprint = newFingerprint()
	b.syncNetworkRoutesBySelfLinkLocked(project, s.Network)

	return b.newOpLocked("patch", subnetTarget(project, region, s), b.timing.Done), nil
}

func (b *InMemoryBackend) mergeSecondariesLocked(s, in *computeapi.Subnetwork) *APIError {
	if len(in.SecondaryIpRanges) == 0 {
		return nil
	}

	have := map[string]string{}
	for _, sec := range s.SecondaryIpRanges {
		have[sec.RangeName] = sec.IpCidrRange
	}

	var added []*computeapi.SubnetworkSecondaryRange

	for _, sec := range in.SecondaryIpRanges {
		if cidr, ok := have[sec.RangeName]; ok {
			if cidr != sec.IpCidrRange {
				return errField(
					"resource.secondaryIpRanges",
					sec.RangeName,
					"An existing secondary range cannot be changed.",
				)
			}

			continue
		}

		added = append(added, sec)
	}

	taken := b.networkRangesLocked(s.Network, s.SelfLink)
	taken = append(taken, netip.MustParsePrefix(s.IpCidrRange))

	for _, sec := range s.SecondaryIpRanges {
		taken = append(taken, netip.MustParsePrefix(sec.IpCidrRange))
	}

	if err := validSecondaries(added, taken, len(s.SecondaryIpRanges)); err != nil {
		return err
	}

	s.SecondaryIpRanges = append(s.SecondaryIpRanges, added...)

	return nil
}
