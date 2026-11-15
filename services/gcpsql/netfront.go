package gcpsql

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	frontDialTimeout = 5 * time.Second
	pipes            = 2
	bitsPerByte      = 8
)

// frontDoor is the address clients of an instance connect to. It admits peers by the instance's authorized
// networks and forwards to whichever engine currently serves the instance, so the address survives a failover.
type frontDoor struct {
	ln     net.Listener
	allow  func(net.IP) bool
	target func() (string, int)
	mu     *lockmetrics.RWMutex
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup
	port   int
}

func newFrontDoor(ctx context.Context, allow func(net.IP) bool, target func() (string, int)) (*frontDoor, error) {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", net.JoinHostPort("127.0.0.1", "0"))
	if err != nil {
		return nil, err //nolint:wrapcheck // caller logs it
	}

	f := &frontDoor{
		ln: ln, allow: allow, target: target, mu: lockmetrics.New("gcpsql.front"), conns: map[net.Conn]struct{}{},
	}

	if a, ok := ln.Addr().(*net.TCPAddr); ok {
		f.port = a.Port
	}

	f.wg.Add(1)

	go f.serve(context.WithoutCancel(ctx))

	return f, nil
}

func (f *frontDoor) serve(ctx context.Context) {
	defer f.wg.Done()

	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}

		f.wg.Add(1)

		go f.handle(ctx, c)
	}
}

func (f *frontDoor) track(c net.Conn, add bool) {
	f.mu.Lock("track")
	defer f.mu.Unlock()

	if add {
		f.conns[c] = struct{}{}
	} else {
		delete(f.conns, c)
	}
}

func peerIP(c net.Conn) net.IP {
	if a, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		return a.IP
	}

	return nil
}

func (f *frontDoor) handle(ctx context.Context, c net.Conn) {
	defer f.wg.Done()
	defer c.Close()

	if !f.allow(peerIP(c)) {
		logger.Load(ctx).DebugContext(ctx, "gcpsql: connection refused, peer is not an authorized network",
			"peer", c.RemoteAddr().String())

		return
	}

	host, port := f.target()
	if port == 0 {
		return
	}

	d := net.Dialer{Timeout: frontDialTimeout}

	up, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return
	}
	defer up.Close()

	f.track(c, true)
	f.track(up, true)

	defer f.track(c, false)
	defer f.track(up, false)

	done := make(chan struct{}, pipes)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		_ = dst.Close()
		_ = src.Close()
		done <- struct{}{}
	}

	go pipe(up, c)
	go pipe(c, up)

	<-done
	<-done
}

// Close stops accepting, drops every open connection and waits for the copiers to finish.
func (f *frontDoor) Close() {
	_ = f.ln.Close()

	f.mu.Lock("Close")
	for c := range f.conns {
		_ = c.Close()
	}
	f.mu.Unlock()

	f.wg.Wait()
}

// authorizedNets parses settings.ipConfiguration.authorizedNetworks; a bare address is a /32.
func authorizedNets(settings map[string]any) ([]*net.IPNet, error) {
	list, _ := asMap(settings["ipConfiguration"])["authorizedNetworks"].([]any)
	out := make([]*net.IPNet, 0, len(list))

	for _, it := range list {
		v := settingString(asMap(it), "value")

		if ip := net.ParseIP(v); ip != nil {
			bits := bitsPerByte * net.IPv6len
			if ip.To4() != nil {
				bits = bitsPerByte * net.IPv4len
			}

			out = append(out, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})

			continue
		}

		_, n, err := net.ParseCIDR(v)
		if err != nil {
			return nil, errInvalid("Invalid authorized network %q: not an IP address or CIDR range.", v)
		}

		out = append(out, n)
	}

	return out, nil
}

// admits reports whether a peer may connect: with ipv4Enabled false nobody may; with no authorized network
// configured every peer is admitted (the Auth Proxy and connectors that real instances rely on are not emulated).
func admits(settings map[string]any, ip net.IP) bool {
	cfg := asMap(settings["ipConfiguration"])
	if v, ok := cfg["ipv4Enabled"].(bool); ok && !v {
		return false
	}

	nets, err := authorizedNets(settings)
	if err != nil || len(nets) == 0 {
		return true
	}

	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}

	return false
}

// openFrontLocked starts the front door of rec once its engine runs; a failure leaves clients on the engine's own
// port.
func (b *InMemoryBackend) openFrontLocked(ctx context.Context, rec *instRec) {
	if rec.eng == nil || rec.front != nil {
		return
	}

	allow := func(ip net.IP) bool {
		b.mu.RLock("front.allow")
		defer b.mu.RUnlock()

		return admits(rec.Inst.Settings, ip)
	}
	target := func() (string, int) {
		b.mu.RLock("front.target")
		eng := rec.eng
		b.mu.RUnlock()

		if eng == nil {
			return "", 0
		}

		return eng.Endpoint()
	}

	f, err := newFrontDoor(ctx, allow, target)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "gcpsql: no front door, clients use the engine port",
			"instance", rec.Inst.Name, "error", err)

		return
	}

	rec.front = f
}

type endpointer struct {
	Engine
	host string
	port int
}

func (e endpointer) Endpoint() (string, int) { return e.host, e.port }

// clientEndpoint is the engine as clients see it: the front door's address when there is one.
func (b *InMemoryBackend) clientEndpoint(rec *instRec) Engine {
	if rec.eng == nil || rec.front == nil {
		return rec.eng
	}

	return endpointer{Engine: rec.eng, host: "127.0.0.1", port: rec.front.port}
}

// closeFront stops the front door of rec.
func closeFront(f *frontDoor) {
	if f != nil {
		f.Close()
	}
}
