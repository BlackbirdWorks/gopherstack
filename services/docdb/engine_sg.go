package docdb

import (
	"net"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// SetSecurityGroupEnforcer makes docker clusters refuse clients their VPC security groups do not admit.
func (b *InMemoryBackend) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	b.mu.Lock("SetSecurityGroupEnforcer")
	defer b.mu.Unlock()

	b.sgEnforcer = e
}

// SetSecurityGroupEnforcer forwards to the backend.
func (h *Handler) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	h.Backend.SetSecurityGroupEnforcer(e)
}

func (b *InMemoryBackend) enforcing() bool {
	b.mu.RLock("docdbEnforcing")
	defer b.mu.RUnlock()

	return b.sgEnforcer != nil
}

// clusterAdmit re-reads the cluster's groups on every connection so edits apply to the next one; nil when
// security groups are not enforced.
func (b *InMemoryBackend) clusterAdmit(lb *liveCluster) sgfilter.Admitter {
	b.mu.RLock("docdbClusterAdmit")
	enf := b.sgEnforcer
	b.mu.RUnlock()

	if enf == nil {
		return nil
	}

	return func(remote net.Addr) bool {
		ids, port := b.clusterGroups(lb)

		return enf.AdmitIngress(ids, sgfilter.ProtoTCP, port, remote)
	}
}

func (b *InMemoryBackend) clusterGroups(lb *liveCluster) ([]string, int) {
	b.mu.RLock("docdbClusterGroups")
	defer b.mu.RUnlock()

	c, ok := b.clusterGet(lb.region, lb.id)
	if !ok {
		return nil, lb.svcPort
	}

	return append([]string(nil), c.VpcSecurityGroupIDs...), lb.svcPort
}
