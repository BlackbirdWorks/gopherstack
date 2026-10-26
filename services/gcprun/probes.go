package gcprun

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

const (
	defaultProbePeriod    = 10 * time.Second
	defaultProbeTimeout   = time.Second
	defaultProbeThreshold = 3
	maxStartupPeriod      = 600
	maxLivenessPeriod     = 3600
	tcpSettle             = 150 * time.Millisecond
	httpOKFloor           = 200
	httpOKCeil            = 400
)

var (
	errProbeStatus  = errors.New("unexpected HTTP status")
	errNotServing   = errors.New("gRPC health check reports not serving")
	errProbeFailure = errors.New("probe failed")
)

// ProbeKind selects how a Prober checks a container.
type ProbeKind string

// Probe kinds, matching the Probe.probe_type oneof.
const (
	ProbeHTTP ProbeKind = "http"
	ProbeTCP  ProbeKind = "tcp"
	ProbeGRPC ProbeKind = "grpc"
)

// ProbeTarget describes one probe attempt. Port is the host port that reaches ContainerPort in the instance.
type ProbeTarget struct {
	Headers       http.Header
	Kind          ProbeKind
	Container     string
	Path          string
	Service       string
	Port          int
	ContainerPort int
	Timeout       time.Duration
}

// Prober runs a single probe attempt; a nil error means the probe passed.
type Prober interface {
	Probe(ctx context.Context, t ProbeTarget) error
}

// SetProber replaces the probe executor (tests inject one); nil restores the network prober.
func (b *InMemoryBackend) SetProber(p Prober) {
	b.mu.Lock("SetProber")
	defer b.mu.Unlock()

	if p == nil {
		p = &netProber{transport: b.transport}
	}

	b.prober = p
}

func (b *InMemoryBackend) probeExec() Prober {
	b.mu.RLock("probeExec")
	defer b.mu.RUnlock()

	return b.prober
}

type netProber struct{ transport *http.Transport }

func (p *netProber) Probe(ctx context.Context, t ProbeTarget) error {
	pctx, cancel := context.WithTimeout(ctx, t.Timeout)
	defer cancel()

	addr := net.JoinHostPort(container.LoopbackHost, strconv.Itoa(t.Port))

	switch t.Kind {
	case ProbeTCP:
		var d net.Dialer

		conn, err := d.DialContext(pctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("tcp connect: %w", err)
		}

		defer conn.Close()

		return stillOpen(conn)
	case ProbeHTTP:
		return p.http(pctx, addr, t)
	case ProbeGRPC:
		return grpcHealth(pctx, addr, t.Service)
	}

	return fmt.Errorf("%w: unknown kind %q", errProbeFailure, t.Kind)
}

// stillOpen tells a listening container from a forwarder that accepts and then drops the connection when nothing
// listens behind the published port: a server that is up keeps the connection open and silent.
func stillOpen(conn net.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(tcpSettle)); err != nil {
		return fmt.Errorf("tcp deadline: %w", err)
	}

	var one [1]byte

	_, err := conn.Read(one[:])

	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return nil
	}

	return fmt.Errorf("%w: connection closed by the peer", errProbeFailure)
}

func (p *netProber) http(ctx context.Context, addr string, t ProbeTarget) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+t.Path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("User-Agent", gcpserve.ProbeUserAgent)

	for k, vs := range t.Headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	client := &http.Client{
		Transport:     p.transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http get: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode < httpOKFloor || resp.StatusCode >= httpOKCeil {
		return fmt.Errorf("%w: %d", errProbeStatus, resp.StatusCode)
	}

	return nil
}

func grpcHealth(ctx context.Context, addr, svc string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc dial: %w", err)
	}

	defer conn.Close()

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: svc})
	if err != nil {
		return fmt.Errorf("grpc health check: %w", err)
	}

	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return errNotServing
	}

	return nil
}

// probeSpec is a Probe with its documented defaults applied and its action resolved.
type probeSpec struct {
	target       ProbeTarget
	initialDelay time.Duration
	period       time.Duration
	failures     int
}

func secondsOr(n int32, def time.Duration) time.Duration {
	if n <= 0 {
		return def
	}

	return time.Duration(n) * time.Second
}

// resolveProbe applies the defaults (period 10s, timeout 1s, 3 failures) and picks the probe's port.
func resolveProbe(c *runpb.Container, p *runpb.Probe) (probeSpec, bool) {
	if p == nil {
		return probeSpec{}, false
	}

	defPort := int32(0)
	if len(c.GetPorts()) > 0 {
		defPort = c.GetPorts()[0].GetContainerPort()
	}

	spec := probeSpec{
		initialDelay: time.Duration(p.GetInitialDelaySeconds()) * time.Second,
		period:       secondsOr(p.GetPeriodSeconds(), defaultProbePeriod),
		failures:     int(p.GetFailureThreshold()),
		target: ProbeTarget{
			Container: c.GetName(),
			Timeout:   secondsOr(p.GetTimeoutSeconds(), defaultProbeTimeout),
		},
	}

	if spec.failures <= 0 {
		spec.failures = defaultProbeThreshold
	}

	port := defPort

	switch a := p.GetProbeType().(type) {
	case *runpb.Probe_HttpGet:
		spec.target.Kind, spec.target.Path, spec.target.Headers = ProbeHTTP, a.HttpGet.GetPath(), http.Header{}
		if spec.target.Path == "" {
			spec.target.Path = "/"
		}

		for _, h := range a.HttpGet.GetHttpHeaders() {
			spec.target.Headers.Add(h.GetName(), h.GetValue())
		}

		if a.HttpGet.GetPort() != 0 {
			port = a.HttpGet.GetPort()
		}
	case *runpb.Probe_TcpSocket:
		spec.target.Kind = ProbeTCP
		if a.TcpSocket.GetPort() != 0 {
			port = a.TcpSocket.GetPort()
		}
	case *runpb.Probe_Grpc:
		spec.target.Kind, spec.target.Service = ProbeGRPC, a.Grpc.GetService()
		if a.Grpc.GetPort() != 0 {
			port = a.Grpc.GetPort()
		}
	default:
		return probeSpec{}, false
	}

	spec.target.ContainerPort = int(port)

	return spec, true
}

func validateProbe(c *runpb.Container, p *runpb.Probe, kind string) error {
	if p == nil {
		return nil
	}

	spec, ok := resolveProbe(c, p)
	if !ok {
		return errInvalid("Container %q: %s probe needs one of httpGet, tcpSocket or grpc.", c.GetName(), kind)
	}

	if spec.target.ContainerPort < 1 || spec.target.ContainerPort > maxPort {
		return errInvalid("Container %q: %s probe needs a port from 1 to %d.", c.GetName(), kind, maxPort)
	}

	maxPeriod := time.Duration(maxStartupPeriod) * time.Second
	if kind == "liveness" {
		maxPeriod = time.Duration(maxLivenessPeriod) * time.Second

		if spec.target.Kind == ProbeTCP {
			return errInvalid("Container %q: liveness probes support httpGet and grpc only.", c.GetName())
		}
	}

	if spec.period > maxPeriod || spec.initialDelay > maxPeriod || p.GetInitialDelaySeconds() < 0 {
		return errInvalid("Container %q: %s probe delays and period must not exceed %s.", c.GetName(), kind, maxPeriod)
	}

	if spec.target.Timeout > spec.period {
		return errInvalid("Container %q: %s probe timeout_seconds must not exceed period_seconds.", c.GetName(), kind)
	}

	return nil
}

// sleep waits d on the backend clock; false means ctx ended first.
func (b *InMemoryBackend) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	t := b.clk().NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C():
		return true
	case <-ctx.Done():
		return false
	}
}

func (b *InMemoryBackend) probeOnce(ctx context.Context, spec probeSpec, ports map[int]int) error {
	t := spec.target
	t.Port = ports[t.ContainerPort]

	return b.probeExec().Probe(ctx, t)
}

// runStartupProbe polls until the probe passes; failure_threshold consecutive failures fail the start.
func (b *InMemoryBackend) runStartupProbe(ctx context.Context, spec probeSpec, ports map[int]int) error {
	if !b.sleep(ctx, spec.initialDelay) {
		return fmt.Errorf("startup probe: %w", ctx.Err())
	}

	var last error

	for fails := 0; fails < spec.failures; {
		if last = b.probeOnce(ctx, spec, ports); last == nil {
			return nil
		}

		fails++

		if fails >= spec.failures || !b.sleep(ctx, spec.period) {
			break
		}
	}

	return fmt.Errorf("startup probe of container %q failed %d times: %w", spec.target.Container, spec.failures, last)
}

// monitorLiveness probes a started container until ctx ends; failure_threshold consecutive failures kill the instance.
func (b *InMemoryBackend) monitorLiveness(ctx context.Context, spec probeSpec, in *inst, rev string) {
	if !b.sleep(ctx, spec.initialDelay) {
		return
	}

	fails := 0

	for {
		if b.probeOnce(ctx, spec, in.ports) == nil {
			fails = 0
		} else if ctx.Err() == nil {
			fails++
		}

		if ctx.Err() != nil {
			return
		}

		if fails >= spec.failures {
			b.killInstance(rev, in)

			return
		}

		if !b.sleep(ctx, spec.period) {
			return
		}
	}
}
