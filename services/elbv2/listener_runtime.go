package elbv2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// ListenerModeReal binds a real local socket per listener.
	ListenerModeReal = "real"
	// ListenerModeRouted keeps the path/host-routed data plane.
	ListenerModeRouted = "routed"

	defaultListenerHost = "127.0.0.1"
	// privilegedPortLimit: below this the requested port is never tried directly.
	privilegedPortLimit = 1024
	bindAttempts        = 16
)

var errNoHandler = errors.New("no handler attached to backend")

// RealListenerConfig opts a backend into binding a real socket per listener.
type RealListenerConfig struct {
	// Ports supplies host ports when the requested one is privileged or taken; nil uses ephemeral ports.
	Ports *portalloc.Allocator
	// Host is the bind address, default 127.0.0.1.
	Host string
}

// ListenerEndpoint is where a bound listener is reachable on the host.
type ListenerEndpoint struct {
	ListenerArn     string `json:"listenerArn"`
	LoadBalancerArn string `json:"loadBalancerArn"`
	Protocol        string `json:"protocol"`
	Address         string `json:"address"`
	Port            int32  `json:"port"`
	HostPort        int    `json:"hostPort"`
}

type realListeners struct {
	cfg     *RealListenerConfig
	handler *Handler
	bound   map[string]*boundListener
	closed  bool
}

type boundListener struct {
	ctx      context.Context
	cancel   context.CancelFunc
	tcp      net.Listener
	udp      net.PacketConn
	srv      httpServer
	sig      string
	endpoint ListenerEndpoint
	poolPort int
	wg       sync.WaitGroup
}

type httpServer interface{ Close() error }

func (bl *boundListener) stop(alloc *portalloc.Allocator) {
	bl.cancel()

	if bl.srv != nil {
		_ = bl.srv.Close()
	}

	if bl.tcp != nil {
		_ = bl.tcp.Close()
	}

	if bl.udp != nil {
		_ = bl.udp.Close()
	}

	bl.wg.Wait()
	portalloc.ReleaseTo(alloc, bl.poolPort)
}

// EnableRealListeners switches the backend to real-listener mode. Call before serving traffic.
func (b *InMemoryBackend) EnableRealListeners(cfg RealListenerConfig) {
	b.mu.Lock("EnableRealListeners")
	defer b.mu.Unlock()

	if cfg.Host == "" {
		cfg.Host = defaultListenerHost
	}

	b.rl.cfg = &cfg
}

// ListenerEndpoints returns the host endpoint of every bound listener.
func (b *InMemoryBackend) ListenerEndpoints() []ListenerEndpoint {
	b.mu.RLock("ListenerEndpoints")
	defer b.mu.RUnlock()

	out := make([]ListenerEndpoint, 0, len(b.rl.bound))
	for _, bl := range b.rl.bound {
		out = append(out, bl.endpoint)
	}

	return out
}

// ListenerEndpoint returns the host endpoint of one bound listener.
func (b *InMemoryBackend) ListenerEndpoint(listenerArn string) (ListenerEndpoint, bool) {
	b.mu.RLock("ListenerEndpoint")
	defer b.mu.RUnlock()

	bl, ok := b.rl.bound[listenerArn]
	if !ok {
		return ListenerEndpoint{}, false
	}

	return bl.endpoint, true
}

func (b *InMemoryBackend) attachHandler(h *Handler) {
	b.mu.Lock("attachHandler")
	defer b.mu.Unlock()

	b.rl.handler = h
}

func isRealProtocol(proto string) bool {
	switch proto {
	case protoHTTP, protoHTTPS, protoTCP, protoTLS, protoUDP, protoTCPUDP:
		return true
	}

	return false
}

func listenerSig(l *Listener) string {
	return l.Protocol + "|" + strconv.Itoa(int(l.Port))
}

// syncListeners reconciles bound sockets with the listener table. Callers must not hold b.mu.
func (b *InMemoryBackend) syncListeners() {
	b.mu.Lock("syncListeners")

	cfg := b.rl.cfg
	if cfg == nil || b.rl.closed {
		b.mu.Unlock()

		return
	}

	want := make(map[string]*Listener)

	for _, l := range b.listeners.All() {
		if isRealProtocol(l.Protocol) {
			want[l.ListenerArn] = l
		}
	}

	var stale []*boundListener

	for arn, bl := range b.rl.bound {
		if l, ok := want[arn]; !ok || bl.sig != listenerSig(l) {
			stale = append(stale, bl)

			delete(b.rl.bound, arn)
		}
	}

	b.mu.Unlock()

	for _, bl := range stale {
		bl.stop(cfg.Ports)
	}

	b.bindMissing(cfg)
}

func (b *InMemoryBackend) bindMissing(cfg *RealListenerConfig) {
	b.mu.Lock("bindMissing")
	defer b.mu.Unlock()

	if b.rl.closed {
		return
	}

	for _, l := range b.listeners.All() {
		if !isRealProtocol(l.Protocol) {
			continue
		}

		if _, ok := b.rl.bound[l.ListenerArn]; ok {
			continue
		}

		bl, err := b.bindListener(cfg, l)
		if err != nil {
			logger.Load(context.Background()).Warn("elbv2: cannot bind listener",
				"listener", l.ListenerArn, "error", err)

			continue
		}

		b.rl.bound[l.ListenerArn] = bl
	}
}

func (b *InMemoryBackend) closeRealListeners() {
	b.mu.Lock("closeRealListeners")

	b.rl.closed = true
	cfg := b.rl.cfg
	all := make([]*boundListener, 0, len(b.rl.bound))

	for arn, bl := range b.rl.bound {
		all = append(all, bl)

		delete(b.rl.bound, arn)
	}

	b.mu.Unlock()

	var alloc *portalloc.Allocator
	if cfg != nil {
		alloc = cfg.Ports
	}

	for _, bl := range all {
		bl.stop(alloc)
	}
}

type socketPair struct {
	tcp      net.Listener
	udp      net.PacketConn
	port     int
	poolPort int
}

func listenOn(host string, port int, wantTCP, wantUDP bool) (net.Listener, net.PacketConn, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	var (
		tcp net.Listener
		udp net.PacketConn
		err error
		lc  net.ListenConfig
	)

	if wantTCP {
		tcp, err = lc.Listen(context.Background(), "tcp", addr)
		if err != nil {
			return nil, nil, fmt.Errorf("listen tcp %s: %w", addr, err)
		}
	}

	if wantUDP {
		udp, err = lc.ListenPacket(context.Background(), "udp", addr)
		if err != nil {
			if tcp != nil {
				_ = tcp.Close()
			}

			return nil, nil, fmt.Errorf("listen udp %s: %w", addr, err)
		}
	}

	return tcp, udp, nil
}

func tcpPort(tcp net.Listener, udp net.PacketConn) int {
	if tcp != nil {
		if a, ok := tcp.Addr().(*net.TCPAddr); ok {
			return a.Port
		}
	}

	if a, ok := udp.LocalAddr().(*net.UDPAddr); ok {
		return a.Port
	}

	return 0
}

// bindSockets tries the requested port first (unless privileged) and falls back to the port pool.
func bindSockets(cfg *RealListenerConfig, l *Listener, label string) (socketPair, error) {
	wantTCP := l.Protocol != protoUDP
	wantUDP := l.Protocol == protoUDP || l.Protocol == protoTCPUDP

	if l.Port >= privilegedPortLimit {
		tcp, udp, err := listenOn(cfg.Host, int(l.Port), wantTCP, wantUDP)
		if err == nil {
			return socketPair{tcp: tcp, udp: udp, port: tcpPort(tcp, udp)}, nil
		}
	}

	var lastErr error

	for range bindAttempts {
		p, err := portalloc.AcquireFrom(cfg.Ports, label)
		if err != nil {
			return socketPair{}, fmt.Errorf("acquire host port: %w", err)
		}

		tcp, udp, err := listenOn(cfg.Host, p, wantTCP, wantUDP)
		if err == nil {
			return socketPair{tcp: tcp, udp: udp, port: tcpPort(tcp, udp), poolPort: p}, nil
		}

		portalloc.ReleaseTo(cfg.Ports, p)

		lastErr = err
	}

	return socketPair{}, lastErr
}

// bindListener binds sockets for l and starts serving. Callers must hold b.mu.
func (b *InMemoryBackend) bindListener(cfg *RealListenerConfig, l *Listener) (*boundListener, error) {
	h := b.rl.handler
	if h == nil {
		return nil, errNoHandler
	}

	sp, err := bindSockets(cfg, l, "elbv2-listener:"+l.ListenerArn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	bl := &boundListener{
		ctx: ctx, cancel: cancel, tcp: sp.tcp, udp: sp.udp, poolPort: sp.poolPort, sig: listenerSig(l),
		endpoint: ListenerEndpoint{
			ListenerArn: l.ListenerArn, LoadBalancerArn: l.LoadBalancerArn, Protocol: l.Protocol,
			Port: l.Port, HostPort: sp.port, Address: net.JoinHostPort(cfg.Host, strconv.Itoa(sp.port)),
		},
	}

	switch l.Protocol {
	case protoHTTP, protoHTTPS:
		b.serveHTTPListener(bl, h, l.ListenerArn, l.Protocol == protoHTTPS)
	default:
		b.serveL4Listener(bl, h, l.ListenerArn)
	}

	logger.Load(context.Background()).Info("elbv2: listener bound",
		"listener", l.ListenerArn, "protocol", l.Protocol, "port", l.Port, "address", bl.endpoint.Address)

	return bl, nil
}

// listenerView returns copies of the listener and its load balancer.
func (b *InMemoryBackend) listenerView(arn string) (*Listener, *LoadBalancer, bool) {
	b.mu.RLock("listenerView")
	defer b.mu.RUnlock()

	l, ok := b.listeners.Get(arn)
	if !ok {
		return nil, nil, false
	}

	lb, ok := b.loadBalancers.Get(l.LoadBalancerArn)
	if !ok {
		return nil, nil, false
	}

	lc, lbc := *l, *lb

	return &lc, &lbc, true
}

// CreateListener creates a listener and binds its socket in real-listener mode.
func (b *InMemoryBackend) CreateListener(input CreateListenerInput) (*Listener, error) {
	l, err := b.createListener(input)
	b.syncListeners()

	return l, err
}

// DeleteListener deletes a listener and closes its socket.
func (b *InMemoryBackend) DeleteListener(listenerArn string) error {
	err := b.deleteListener(listenerArn)
	b.syncListeners()

	return err
}

// ModifyListener updates a listener and rebinds its socket when protocol or port changed.
func (b *InMemoryBackend) ModifyListener(input ModifyListenerInput) (*Listener, error) {
	l, err := b.modifyListener(input)
	b.syncListeners()

	return l, err
}

// DeleteLoadBalancer deletes a load balancer and closes its listeners' sockets.
func (b *InMemoryBackend) DeleteLoadBalancer(lbArn string) error {
	err := b.deleteLoadBalancer(lbArn)
	b.syncListeners()

	return err
}

// Restore loads a snapshot and rebinds sockets for the restored listeners.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	err := b.restore(ctx, data)
	b.syncListeners()

	return err
}
