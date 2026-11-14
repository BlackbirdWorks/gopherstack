package ociloadbalancer

import (
	"net"
	"net/http"
	"net/netip"
	"slices"
)

func checkIPMaxConns(r Rule) *APIError {
	if r.DefaultMaxConnections != nil && *r.DefaultMaxConnections < 1 {
		return errParam("defaultMaxConnections must be at least 1")
	}

	if r.DefaultMaxConnections == nil && len(r.IPMaxConnections) == 0 {
		return errMissing(actIPMaxConns + " needs defaultMaxConnections or ipMaxConnections")
	}

	for _, e := range r.IPMaxConnections {
		if len(e.IPAddresses) == 0 || e.MaxConnections < 1 {
			return errParam("ipMaxConnections entries need ipAddresses and a maxConnections of at least 1")
		}

		for _, a := range e.IPAddresses {
			if !validSource(a) {
				return errParam("ipMaxConnections address " + a + " is not an IP address or CIDR")
			}
		}
	}

	return nil
}

// ipLimit returns the connection cap for ip on a port from the IP_BASED_MAX_CONNECTIONS rules of its listeners.
func ipLimit(lb *LoadBalancer, port int, ip netip.Addr) (int, bool) {
	for _, l := range listenersOn(lb, port) {
		for _, rs := range ruleSetsOf(lb, l) {
			for _, r := range rs.Items {
				if limit, ok := ruleLimit(r, ip); ok {
					return limit, true
				}
			}
		}
	}

	return 0, false
}

// ruleLimit is a matching ipMaxConnections entry's cap, else defaultMaxConnections.
func ruleLimit(r Rule, ip netip.Addr) (int, bool) {
	if r.Action != actIPMaxConns {
		return 0, false
	}

	for _, e := range r.IPMaxConnections {
		if slices.ContainsFunc(e.IPAddresses, func(a string) bool { return sourceMatches(a, ip) }) {
			return e.MaxConnections, true
		}
	}

	if r.DefaultMaxConnections != nil {
		return *r.DefaultMaxConnections, true
	}

	return 0, false
}

// connState enforces IP_BASED_MAX_CONNECTIONS: a connection over its source address's cap is closed on accept.
func (d *dataPlane) connState(p *portListener, c net.Conn, st http.ConnState) {
	switch st {
	case http.StateNew:
		d.admit(p, c)
	case http.StateClosed, http.StateHijacked:
		d.release(p, c)
	case http.StateActive, http.StateIdle:
	}
}

func (d *dataPlane) admit(p *portListener, c net.Conn) {
	ap, ok := addrPort(c.RemoteAddr())
	if !ok {
		return
	}

	lb := d.b.activeLB(p.spec.lbID)
	if lb == nil {
		return
	}

	d.b.mu.RLock("admit")
	limit, limited := ipLimit(lb, p.spec.port, ap.Addr())
	d.b.mu.RUnlock()

	if !limited {
		return
	}

	key := p.spec.key() + "|" + ap.Addr().String()
	n := d.counter("ip|" + key)

	if int(n.Add(1)) > limit {
		n.Add(-1)
		_ = c.Close()

		return
	}

	d.mu.Lock("admit")
	p.held[c] = key
	d.mu.Unlock()
}

func (d *dataPlane) release(p *portListener, c net.Conn) {
	d.mu.Lock("release")
	key, ok := p.held[c]
	delete(p.held, c)
	d.mu.Unlock()

	if ok {
		d.counter("ip|" + key).Add(-1)
	}
}
