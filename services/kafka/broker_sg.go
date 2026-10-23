package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

const (
	relaySuffix   = "_PUB"
	iamClientPort = 9098
)

// SetSecurityGroupEnforcer makes docker brokers refuse clients the cluster's security groups do not admit.
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

// clusterAdmit re-reads the cluster's groups on every connection; nil when security groups are not enforced.
func (b *InMemoryBackend) clusterAdmit(clusterArn string, port int) sgfilter.Admitter {
	b.mu.RLock("kafkaClusterAdmit")
	enf := b.sgEnforcer
	b.mu.RUnlock()

	if enf == nil {
		return nil
	}

	return func(remote net.Addr) bool {
		return enf.AdmitIngress(b.clusterGroups(clusterArn), sgfilter.ProtoTCP, port, remote)
	}
}

func (b *InMemoryBackend) clusterGroups(clusterArn string) []string {
	b.mu.RLock("kafkaClusterGroups")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil
	}

	return append([]string(nil), c.BrokerNodeGroupInfo.SecurityGroups...)
}

func (e *brokerEngine) admitFor(clusterArn string, port int) sgfilter.Admitter {
	if e.admit == nil {
		return nil
	}

	return e.admit(clusterArn, port)
}

// clientPort is the host port clients dial for a listener: its security-group relay when one fronts it.
func (r *brokerRes) clientPort(name string) int {
	if p := r.ports[name+relaySuffix]; p != 0 {
		return p
	}

	return r.ports[name]
}

// relayedListeners maps the public listeners to the AWS port their security-group rules match.
func relayedListeners() []relayedListener {
	return []relayedListener{{listenerPlaintext, 9092}, {listenerTLS, 9094}, {listenerScram, 9096}}
}

type relayedListener struct {
	name string
	port int
}

func (p brokerPlan) hasListener(name string) bool {
	switch name {
	case listenerPlaintext:
		return p.plaintext
	case listenerTLS:
		return p.tls
	case listenerScram:
		return p.scram
	default:
		return false
	}
}

// startRelays fronts each public listener with a security-group-checked relay when enforcement is on; the
// container then publishes the listener on loopback only.
func (e *brokerEngine) startRelays(res *brokerRes, plan brokerPlan, clusterArn string) error {
	for _, l := range relayedListeners() {
		if !plan.hasListener(l.name) {
			continue
		}

		admit := e.admitFor(clusterArn, l.port)
		if admit == nil {
			continue
		}

		pub, err := e.acquirePort(clusterArn + "-" + l.name + relaySuffix)
		if err != nil {
			return fmt.Errorf("relay port: %w", err)
		}

		res.ports[l.name+relaySuffix] = pub

		bind := net.JoinHostPort(container.BindHostFor(e.cfg.Host), strconv.Itoa(pub))
		upstream := net.JoinHostPort(brokerDefaultHost, strconv.Itoa(res.ports[l.name]))

		relay, err := sgfilter.Front(context.Background(), bind, upstream, admit)
		if err != nil {
			return fmt.Errorf("relay %s: %w", l.name, err)
		}

		res.relays = append(res.relays, relay)
	}

	return nil
}
