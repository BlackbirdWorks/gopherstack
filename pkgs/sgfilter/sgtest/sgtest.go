// Package sgtest provides an in-memory sgfilter.Source for service tests.
package sgtest

import (
	"net"
	"net/netip"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// Source serves security groups that tests replace at will, so rule edits can be applied between connections.
type Source struct {
	groups  atomic.Pointer[map[string]sgfilter.Group]
	members atomic.Pointer[map[netip.Addr][]string]
	// DefaultID is returned for every VPC; empty means no default group.
	DefaultID string
}

// New returns a Source holding groups.
func New(groups ...sgfilter.Group) *Source {
	s := &Source{}
	s.Set(groups...)

	return s
}

// Set replaces every group.
func (s *Source) Set(groups ...sgfilter.Group) {
	m := make(map[string]sgfilter.Group, len(groups))
	for _, g := range groups {
		m[g.ID] = g
	}

	s.groups.Store(&m)
}

// SetMembers declares which groups the interface owning each address belongs to.
func (s *Source) SetMembers(m map[netip.Addr][]string) { s.members.Store(&m) }

// SecurityGroups implements sgfilter.Source.
func (s *Source) SecurityGroups(ids []string) []sgfilter.Group {
	var out []sgfilter.Group

	for _, id := range ids {
		if g, ok := (*s.groups.Load())[id]; ok {
			out = append(out, g)
		}
	}

	return out
}

// GroupsOfAddr implements sgfilter.Source.
func (s *Source) GroupsOfAddr(a netip.Addr) []string {
	if m := s.members.Load(); m != nil {
		return (*m)[a]
	}

	return nil
}

// DefaultGroupID implements sgfilter.DefaultGroupSource.
func (s *Source) DefaultGroupID(string) string { return s.DefaultID }

// Enforcer returns an enforcer reading from s.
func (s *Source) Enforcer() *sgfilter.Enforcer {
	e := sgfilter.New()
	e.SetSource(s)

	return e
}

// OpenTo is group id admitting TCP port from cidr.
func OpenTo(id string, port int, cidr string) sgfilter.Group {
	return sgfilter.Group{ID: id, Ingress: []sgfilter.Rule{{
		Protocol: sgfilter.ProtoTCP, FromPort: port, ToPort: port, CIDRs: []netip.Prefix{netip.MustParsePrefix(cidr)},
	}}}
}

// Loopback is a client address on 127.0.0.1.
func Loopback() net.Addr { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1")} }
