package neptune

import (
	"context"
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// SetSecurityGroupEnforcer makes docker clusters refuse clients their VPC security groups do not admit.
func (b *InMemoryBackend) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	b.mu.Lock("SetSecurityGroupEnforcer")
	defer b.mu.Unlock()

	b.sgEnforcer = e
}

// SetSecurityGroupEnforcer forwards to the backend when it supports enforcement.
func (h *Handler) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.SetSecurityGroupEnforcer(e)
	}
}

func (b *InMemoryBackend) enforcing() bool {
	b.mu.RLock("neptuneEnforcing")
	defer b.mu.RUnlock()

	return b.sgEnforcer != nil
}

// clusterAdmit re-reads the cluster's groups on every connection; nil when security groups are not enforced.
func (b *InMemoryBackend) clusterAdmit(lb *liveCluster) sgfilter.Admitter {
	b.mu.RLock("neptuneClusterAdmit")
	enf := b.sgEnforcer
	b.mu.RUnlock()

	if enf == nil {
		return nil
	}

	return func(remote net.Addr) bool {
		return enf.AdmitIngress(b.clusterGroups(lb), sgfilter.ProtoTCP, lb.svcPort, remote)
	}
}

func (b *InMemoryBackend) clusterGroups(lb *liveCluster) []string {
	b.mu.RLock("neptuneClusterGroups")
	defer b.mu.RUnlock()

	c, ok := b.clusterGet(lb.region, lb.id)
	if !ok {
		return nil
	}

	return append([]string(nil), c.VpcSecurityGroupIDs...)
}

// startFronts opens the listener clients dial: the openCypher front when enabled, else a bare relay used
// only to enforce security groups. Both are nil when inner is 0.
func (b *InMemoryBackend) startFronts(lb *liveCluster, port, inner int) (*cypherFront, *sgfilter.Proxy, error) {
	if inner == 0 {
		return nil, nil, nil
	}

	e := b.engine
	upstream := net.JoinHostPort(container.LoopbackHost, strconv.Itoa(inner))
	admit := b.clusterAdmit(lb)

	if e.cfg.OpenCypher {
		front, err := startCypherFront(e.cfg.Host, port, upstream, lb.graph, admit, b.clusterAuthFn(lb))

		return front, nil, err
	}

	relay, err := sgfilter.Front(context.Background(),
		net.JoinHostPort(container.BindHostFor(e.cfg.Host), strconv.Itoa(port)), upstream, admit)

	return nil, relay, err
}

func closeFronts(front *cypherFront, relay *sgfilter.Proxy) {
	if front != nil {
		front.Close()
	}

	if relay != nil {
		relay.Close()
	}
}
