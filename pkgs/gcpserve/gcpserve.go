// Package gcpserve holds the pieces GCP emulators share to serve user workloads from containers.
package gcpserve

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	pingTimeout       = 5 * time.Second
	probeTimeout      = 2 * time.Second
	readyPollInterval = 25 * time.Millisecond
)

// ProbeUserAgent identifies readiness probes sent to workload containers.
const ProbeUserAgent = "gopherstack-readiness-probe"

// Runner is the slice of the container runtime that served workloads need.
type Runner interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// PortPool hands out host ports for published containers.
type PortPool interface {
	Acquire(label string) (int, error)
	Release(port int) error
}

// LazyRuntime connects to the container runtime on first use and retries while it is unreachable.
type LazyRuntime struct {
	log *slog.Logger
	rt  container.Runtime
	mu  *lockmetrics.RWMutex
}

// NewLazyRuntime returns a LazyRuntime whose lock is named lockName.
func NewLazyRuntime(log *slog.Logger, lockName string) *LazyRuntime {
	return &LazyRuntime{log: log, mu: lockmetrics.New(lockName)}
}

// Get returns the connected runtime, dialing it if needed.
func (l *LazyRuntime) Get() (Runner, error) {
	l.mu.Lock("Get")
	defer l.mu.Unlock()

	if l.rt != nil {
		return l.rt, nil
	}

	rt, err := container.NewRuntime(container.Config{Logger: l.log})
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	if err = rt.Ping(ctx); err != nil {
		_ = rt.Close()

		return nil, fmt.Errorf("ping: %w", err)
	}

	l.rt = rt

	return rt, nil
}

// Close releases the runtime client.
func (l *LazyRuntime) Close() error {
	l.mu.Lock("Close")
	defer l.mu.Unlock()

	if c, ok := l.rt.(io.Closer); ok {
		l.rt = nil

		return c.Close()
	}

	return nil
}

// WaitReady blocks until the loopback port accepts TCP and answers an HTTP request (any status), or ctx ends.
// The TCP check alone is not enough: a published container port accepts connections before the app listens.
func WaitReady(ctx context.Context, port int) error {
	addr := net.JoinHostPort(container.LoopbackHost, strconv.Itoa(port))
	tick := time.NewTicker(readyPollInterval)

	defer tick.Stop()

	tr := httpclient.NewTransport()
	tr.DisableKeepAlives = true

	defer tr.CloseIdleConnections()

	probe := &http.Client{Transport: tr, Timeout: probeTimeout}

	var d net.Dialer

	for {
		if conn, err := d.DialContext(ctx, "tcp", addr); err == nil {
			_ = conn.Close()

			if httpAnswers(ctx, probe, "http://"+addr+"/") {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("container did not accept connections and answer HTTP on its port: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func httpAnswers(ctx context.Context, c *http.Client, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}

	req.Header.Set("User-Agent", ProbeUserAgent)

	resp, err := c.Do(req)
	if err != nil {
		return false
	}

	_ = resp.Body.Close()

	return true
}

// AcquirePort takes a host port from pool, or a free loopback port when pool is nil.
func AcquirePort(ctx context.Context, pool PortPool, label string) (int, error) {
	if pool != nil {
		return pool.Acquire(label)
	}

	var lc net.ListenConfig

	l, err := lc.Listen(ctx, "tcp", container.LoopbackHost+":0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}
	defer l.Close()

	addr, _ := l.Addr().(*net.TCPAddr)

	return addr.Port, nil
}

// ReleasePort returns port to pool; a nil pool or zero port is a no-op.
func ReleasePort(pool PortPool, port int) {
	if pool != nil && port > 0 {
		_ = pool.Release(port)
	}
}

// Proxy reverse-proxies to the container published on the loopback port, preserving the inbound Host.
func Proxy(port int, rt http.RoundTripper) *httputil.ReverseProxy {
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort(container.LoopbackHost, strconv.Itoa(port))}

	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
		},
		Transport: rt,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			code := http.StatusBadGateway
			if errors.Is(r.Context().Err(), context.DeadlineExceeded) {
				code = http.StatusGatewayTimeout
			}

			http.Error(w, "container unreachable: "+err.Error(), code)
		},
	}
}
