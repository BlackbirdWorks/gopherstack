package mq

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// SetSecurityGroupEnforcer makes docker brokers refuse clients their security groups do not admit.
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

// brokerAdmit re-reads the broker's groups on every connection; nil when security groups are not enforced.
func (b *InMemoryBackend) brokerAdmit(brokerID string, port int) sgfilter.Admitter {
	b.mu.RLock("mqBrokerAdmit")
	enf := b.sgEnforcer
	b.mu.RUnlock()

	if enf == nil {
		return nil
	}

	return func(remote net.Addr) bool {
		return enf.AdmitIngress(b.brokerGroups(brokerID), sgfilter.ProtoTCP, port, remote)
	}
}

func (b *InMemoryBackend) brokerGroups(brokerID string) []string {
	b.mu.RLock("mqBrokerGroups")
	defer b.mu.RUnlock()

	br, ok := b.brokers.Get(brokerID)
	if !ok {
		return nil
	}

	return append([]string(nil), br.SecurityGroups...)
}

func newLiveNode(ports []int, maps []portMap, relayed bool) *liveNode {
	n := &liveNode{ports: ports, hostPorts: make(map[string]int, len(maps))}
	for i, pm := range maps {
		n.hostPorts[pm.name] = ports[i]
	}

	if relayed {
		n.inner = make(map[string]int, len(maps))
		for i, pm := range maps {
			n.inner[pm.name] = ports[len(maps)+i]
		}
	}

	return n
}

// acquireNodePorts reserves the advertised ports and, when security groups are enforced, a loopback port
// per advertised one for the container to publish on.
func (e *brokerEngine) acquireNodePorts(name string, lb *liveBroker, maps []portMap) ([]int, error) {
	ports, err := e.acquirePorts(name, maps)
	if err != nil || e.admit == nil || e.admit(lb.id, 0) == nil {
		return ports, err
	}

	inner, err := e.acquirePorts(name+"-in", maps)
	if err != nil {
		e.releasePorts(ports)

		return nil, err
	}

	return append(ports, inner...), nil
}

// startRelays fronts each advertised port with a security-group-checked relay to its loopback port.
func (e *brokerEngine) startRelays(n *liveNode, lb *liveBroker, maps []portMap) error {
	if n.inner == nil {
		return nil
	}

	bind := container.BindHostFor(e.cfg.Host)

	for _, pm := range maps {
		listen := net.JoinHostPort(bind, strconv.Itoa(n.hostPorts[pm.name]))
		upstream := net.JoinHostPort(container.LoopbackHost, strconv.Itoa(n.inner[pm.name]))

		relay, err := sgfilter.Front(context.Background(), listen, upstream, e.admit(lb.id, pm.container))
		if err != nil {
			return fmt.Errorf("relay %s: %w", pm.name, err)
		}

		n.relays = append(n.relays, relay)
	}

	return nil
}

func (e *brokerEngine) removeNode(n *liveNode) {
	for _, r := range n.relays {
		r.Close()
	}

	e.remove(n.containerID, n.ports)
}

// probeAddr is where the server itself reaches a node, bypassing the security-group relay.
func (e *brokerEngine) probeAddr(n *liveNode, name string) string {
	if p, ok := n.inner[name]; ok {
		return net.JoinHostPort(container.LoopbackHost, strconv.Itoa(p))
	}

	return e.nodeAddr(n, name)
}
