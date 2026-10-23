package rds

import (
	"net"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// SetSecurityGroupEnforcer makes docker-backed database endpoints refuse clients their VPC security groups
// do not admit; nil leaves them open.
func (b *InMemoryBackend) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	b.mu.Lock("SetSecurityGroupEnforcer")
	defer b.mu.Unlock()

	b.sgEnforcer = e
}

// SetSecurityGroupEnforcer wires the enforcer into the home backend, every region backend and later ones.
func (h *Handler) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	h.mu.Lock()
	h.sgEnforcer = e
	backends := make([]*InMemoryBackend, 0, 1+len(h.backends))
	backends = append(backends, h.Backend)

	for _, b := range h.backends {
		backends = append(backends, b)
	}
	h.mu.Unlock()

	for _, b := range backends {
		b.SetSecurityGroupEnforcer(e)
	}
}

// admitClient reports whether the unit's security groups admit a client connecting from remote. Group IDs
// are read per connection, so rule and membership edits apply to the next connection.
func (b *InMemoryBackend) admitClient(lb *liveDB, remote net.Addr) bool {
	b.mu.RLock("rdsAdmitClient")
	enf := b.sgEnforcer
	if enf == nil {
		b.mu.RUnlock()

		return true
	}

	ids, port := b.unitSecurityGroupsLocked(lb)
	b.mu.RUnlock()

	return enf.AdmitIngress(ids, sgfilter.ProtoTCP, port, remote)
}

func (b *InMemoryBackend) unitSecurityGroupsLocked(lb *liveDB) ([]string, int) {
	port := cmpPort(int(lb.dbPort.Load()), engineContainerPort(lb.kind))

	if strings.HasPrefix(lb.key, unitInstancePrefix) {
		if inst, ok := b.instances.Get(normalizeID(lb.resourceID)); ok {
			return membershipIDs(inst.VpcSecurityGroups), port
		}

		return nil, port
	}

	if c, ok := b.clusters.Get(normalizeID(lb.resourceID)); ok {
		return membershipIDs(c.VpcSecurityGroups), port
	}

	return nil, port
}

// recordedPortLocked is the database port on the unit's API record, before the engine rewrites it to the
// host port clients dial.
func (b *InMemoryBackend) recordedPortLocked(lb *liveDB) int {
	if strings.HasPrefix(lb.key, unitInstancePrefix) {
		if inst, ok := b.instances.Get(normalizeID(lb.resourceID)); ok {
			return inst.Port
		}

		return 0
	}

	if c, ok := b.clusters.Get(normalizeID(lb.resourceID)); ok {
		return c.Port
	}

	return 0
}

func membershipIDs(ms []VpcSecurityGroupMembership) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.VpcSecurityGroupID)
	}

	return out
}

func cmpPort(configured, fallback int) int {
	if configured > 0 {
		return configured
	}

	return fallback
}

// SecurityGroupEnforcer returns the installed enforcer, nil when security groups are not enforced.
func (b *InMemoryBackend) SecurityGroupEnforcer() *sgfilter.Enforcer {
	b.mu.RLock("SecurityGroupEnforcer")
	defer b.mu.RUnlock()

	return b.sgEnforcer
}
