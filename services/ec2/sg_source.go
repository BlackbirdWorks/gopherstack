package ec2

import (
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// SecurityGroupSource exposes live security-group state of every region to the sgfilter enforcer.
func (h *Handler) SecurityGroupSource() sgfilter.Source {
	return &sgSource{backends: h.memBackends}
}

type sgSource struct {
	backends func() []*InMemoryBackend
}

// SecurityGroups implements sgfilter.Source.
func (s *sgSource) SecurityGroups(ids []string) []sgfilter.Group {
	out := make([]sgfilter.Group, 0, len(ids))

	for _, b := range s.backends() {
		out = append(out, b.enforcementGroups(ids)...)
	}

	return out
}

// GroupsOfAddr implements sgfilter.Source.
func (s *sgSource) GroupsOfAddr(addr netip.Addr) []string {
	bks := s.backends()
	out := make([]string, 0, len(bks))

	for _, b := range bks {
		out = append(out, b.groupsOfAddr(addr)...)
	}

	return out
}

func (b *InMemoryBackend) enforcementGroups(ids []string) []sgfilter.Group {
	b.mu.RLock("enforcementGroups")
	defer b.mu.RUnlock()

	var out []sgfilter.Group

	for _, id := range ids {
		sg, ok := b.securityGroups.Get(id)
		if !ok {
			continue
		}

		out = append(out, sgfilter.Group{
			ID:      sg.ID,
			Ingress: b.enforcementRulesLocked(sg.IngressRules),
			Egress:  b.enforcementRulesLocked(sg.EgressRules),
		})
	}

	return out
}

func (b *InMemoryBackend) enforcementRulesLocked(rules []SecurityGroupRule) []sgfilter.Rule {
	out := make([]sgfilter.Rule, 0, len(rules))

	for _, r := range rules {
		out = append(out, sgfilter.Rule{
			Protocol: r.Protocol,
			FromPort: r.FromPort,
			ToPort:   r.ToPort,
			CIDRs:    b.ruleCIDRsLocked(r),
			Groups:   sourceGroups(r),
		})
	}

	return out
}

func sourceGroups(r SecurityGroupRule) []string {
	if r.SourceGroupID == "" {
		return nil
	}

	return []string{r.SourceGroupID}
}

func (b *InMemoryBackend) ruleCIDRsLocked(r SecurityGroupRule) []netip.Prefix {
	var out []netip.Prefix

	for _, c := range []string{r.IPRange, r.IPv6Range} {
		if p, err := netip.ParsePrefix(c); err == nil {
			out = append(out, p)
		}
	}

	if r.PrefixListID == "" {
		return out
	}

	if pl, ok := b.managedPrefixLists.Get(r.PrefixListID); ok {
		for _, e := range pl.Entries {
			if p, err := netip.ParsePrefix(strings.TrimSpace(e.Cidr)); err == nil {
				out = append(out, p)
			}
		}
	}

	return out
}

func (b *InMemoryBackend) groupsOfAddr(addr netip.Addr) []string {
	b.mu.RLock("groupsOfAddr")
	defer b.mu.RUnlock()

	var out []string

	for _, eni := range b.networkInterfaces.All() {
		if eniOwnsAddr(eni, addr) {
			out = append(out, eni.SecurityGroupIDs...)
		}
	}

	for _, inst := range b.instances.All() {
		if p, err := netip.ParseAddr(inst.PrivateIP); err == nil && p == addr {
			out = append(out, inst.SecurityGroups...)
		}
	}

	slices.Sort(out)

	return slices.Compact(out)
}

func eniOwnsAddr(eni *NetworkInterface, addr netip.Addr) bool {
	for _, s := range append([]string{eni.PrivateIP}, eni.SecondaryPrivateIPs...) {
		if p, err := netip.ParseAddr(s); err == nil && p == addr {
			return true
		}
	}

	return false
}
