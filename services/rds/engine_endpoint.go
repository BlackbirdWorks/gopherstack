package rds

import (
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// endpointListener is the host port of one custom cluster endpoint; it relays to the cluster's database while
// the endpoint still selects at least one available member.
type endpointListener struct {
	ln    net.Listener
	be    *InMemoryBackend
	lb    *liveDB
	id    string
	conns sync.Map
	wg    sync.WaitGroup
	port  int
	once  sync.Once
}

func (l *endpointListener) close() {
	l.once.Do(func() {
		_ = l.ln.Close()

		l.conns.Range(func(k, _ any) bool {
			if c, ok := k.(net.Conn); ok {
				_ = c.Close()
			}

			return true
		})

		l.be.engine.releasePort(l.port)
	})
}

// endpointListenerLocked returns the live listener for ep, if its cluster's database is up.
func (b *InMemoryBackend) endpointListenerLocked(ep *DBClusterEndpoint) *endpointListener {
	lb := b.clusterUnitLocked(ep.DBClusterIdentifier)
	if lb == nil {
		return nil
	}

	v, ok := lb.eps.Load(ep.DBClusterEndpointIdentifier)
	if !ok {
		return nil
	}

	l, _ := v.(*endpointListener)

	return l
}

// endpointViewLocked copies ep for a response, advertising the real host:port when a listener serves it.
func (b *InMemoryBackend) endpointViewLocked(ep *DBClusterEndpoint) *DBClusterEndpoint {
	cp := *ep
	cp.StaticMembers = slices.Clone(ep.StaticMembers)
	cp.ExcludedMembers = slices.Clone(ep.ExcludedMembers)

	if l := b.endpointListenerLocked(ep); l != nil {
		cp.Endpoint = net.JoinHostPort(b.engine.cfg.Host, strconv.Itoa(l.port))
	}

	return &cp
}

// syncClusterEndpointsLocked opens a listener for each custom endpoint of the ready cluster unit lb.
func (b *InMemoryBackend) syncClusterEndpointsLocked(lb *liveDB) {
	if lb == nil || !lb.ready || b.engine == nil || !strings.HasPrefix(lb.key, unitClusterPrefix) {
		return
	}

	for _, ep := range b.clusterEndpoints.All() {
		if !idEqual(ep.DBClusterIdentifier, lb.resourceID) {
			continue
		}

		if _, open := lb.eps.Load(ep.DBClusterEndpointIdentifier); open {
			continue
		}

		if err := b.openEndpointListenerLocked(lb, ep.DBClusterEndpointIdentifier); err != nil {
			logger.Load(context.Background()).Warn("rds: cluster endpoint listener failed",
				"endpoint", ep.DBClusterEndpointIdentifier, "error", err)
		}
	}
}

func (b *InMemoryBackend) openEndpointListenerLocked(lb *liveDB, id string) error {
	e := b.engine

	port, err := e.acquirePort()
	if err != nil {
		return err
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		net.JoinHostPort(container.BindHostFor(e.cfg.Host), strconv.Itoa(port)))
	if err != nil {
		e.releasePort(port)

		return err //nolint:wrapcheck // net error is self-describing
	}

	l := &endpointListener{be: b, lb: lb, ln: ln, port: port, id: id}
	lb.eps.Store(id, l)

	select {
	case <-lb.gone:
		l.close()

		return nil
	default:
	}

	l.wg.Go(l.acceptLoop)

	return nil
}

func (b *InMemoryBackend) closeEndpointListenerLocked(ep *DBClusterEndpoint) {
	if l := b.endpointListenerLocked(ep); l != nil {
		l.lb.eps.Delete(l.id)
		l.close()
	}
}

func (l *endpointListener) acceptLoop() {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return
		}

		l.conns.Store(conn, struct{}{})
		l.wg.Go(func() {
			defer l.conns.Delete(conn)

			l.serve(conn)
		})
	}
}

// upstream is the address clients of the cluster itself use; "" when the endpoint selects no available member.
func (l *endpointListener) upstream() string {
	l.be.mu.RLock("rdsEndpointTarget")
	defer l.be.mu.RUnlock()

	ep, ok := l.be.clusterEndpoints.Get(l.id)
	if !ok || len(l.be.endpointMembersLocked(ep)) == 0 {
		return ""
	}

	return net.JoinHostPort(l.be.engine.cfg.Host, strconv.Itoa(l.lb.advertisedPort()))
}

func (l *endpointListener) serve(client net.Conn) {
	defer func() { _ = client.Close() }()

	if !l.be.admitClient(l.lb, client.RemoteAddr()) {
		return
	}

	addr := l.upstream()
	if addr == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), proxyUpstreamTimeout)
	up, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)

	cancel()

	if err != nil {
		return
	}

	defer func() { _ = up.Close() }()

	pipeConns(client, up)
}

// endpointMembersLocked lists the available member instances the endpoint currently routes to.
func (b *InMemoryBackend) endpointMembersLocked(ep *DBClusterEndpoint) []string {
	c, ok := b.clusters.Get(normalizeID(ep.DBClusterIdentifier))
	if !ok {
		return nil
	}

	var out []string

	for _, m := range c.DBClusterMembers {
		if !endpointSelects(ep, m) {
			continue
		}

		if inst, found := b.instances.Get(normalizeID(m.DBInstanceIdentifier)); found &&
			inst.DBInstanceStatus == instanceStatusAvailable {
			out = append(out, m.DBInstanceIdentifier)
		}
	}

	return out
}

func endpointSelects(ep *DBClusterEndpoint, m DBClusterMember) bool {
	named := func(list []string) bool {
		return slices.ContainsFunc(list, func(id string) bool { return idEqual(id, m.DBInstanceIdentifier) })
	}

	switch {
	case len(ep.StaticMembers) > 0:
		return named(ep.StaticMembers)
	case named(ep.ExcludedMembers):
		return false
	case ep.EndpointType == clusterEndpointReader:
		return !m.IsClusterWriter
	case ep.EndpointType == clusterEndpointWriter:
		return m.IsClusterWriter
	default:
		return true
	}
}
