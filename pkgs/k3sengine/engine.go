package k3sengine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	defaultHost   = "127.0.0.1"
	startTimeout  = 5 * time.Minute
	probeInterval = time.Second
	probeTimeout  = 5 * time.Second
	stopTimeout   = 30 * time.Second
	minTokenLen   = 16
)

var (
	// ErrInvalidConfig is returned when the engine host or token is unusable.
	ErrInvalidConfig = errors.New("invalid k3s cluster engine configuration")
	// ErrDefaultTokenExposed is returned when the public default token would be exposed off-host.
	ErrDefaultTokenExposed = errors.New("a cluster token must be set when the cluster host is not loopback")
	// ErrSuperseded is returned by Launch when the caller's record callback declined the new container.
	ErrSuperseded = errors.New("cluster was superseded while starting")
	// ErrStopUnsupported is returned by Stop and Start when the runtime cannot keep a stopped container.
	ErrStopUnsupported = errors.New("container runtime cannot stop and restart a container in place")

	tokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)
	hostPattern  = regexp.MustCompile(`^[A-Za-z0-9.:-]+$`)
)

// Runtime is the slice of container.Runtime the engine needs.
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// Stoppable is implemented by runtimes that can stop a container and start it again with its data and ports.
type Stoppable interface {
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
}

// Config configures an Engine. Prefix names containers and port labels ("eks", "gke").
type Config struct {
	Runtime      Runtime
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Probe        func(ctx context.Context, addr, token string) ([]byte, error)
	Prefix       string
	Host         string
	Token        string
	StartTimeout time.Duration
}

// Engine starts and stops single-node k3s containers.
type Engine struct {
	cfg Config
}

// NormalizeAccess validates host and token and fills their defaults.
func NormalizeAccess(host, token string) (string, string, error) {
	if host == "" {
		host = defaultHost
	}

	defaulted := token == ""
	if defaulted {
		token = DefaultToken
	}

	if !hostPattern.MatchString(host) || !tokenPattern.MatchString(token) || len(token) < minTokenLen {
		return "", "", fmt.Errorf("%w: host must match %s and token %s (min %d chars)",
			ErrInvalidConfig, hostPattern, tokenPattern, minTokenLen)
	}

	if defaulted && container.BindHostFor(host) != container.LoopbackHost {
		return "", "", ErrDefaultTokenExposed
	}

	return host, token, nil
}

// New validates cfg and returns an Engine with defaults applied.
func New(cfg Config) (*Engine, error) {
	var err error

	if cfg.Host, cfg.Token, err = NormalizeAccess(cfg.Host, cfg.Token); err != nil {
		return nil, err
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = startTimeout
	}

	return &Engine{cfg: cfg}, nil
}

// Host is the host clients reach the API server on.
func (e *Engine) Host() string { return e.cfg.Host }

// Token is the static cluster-admin bearer token.
func (e *Engine) Token() string { return e.cfg.Token }

// Runtime is the container runtime the engine starts nodes on.
func (e *Engine) Runtime() Runtime { return e.cfg.Runtime }

// Addr is the host:port of an API server published on port.
func (e *Engine) Addr(port int) string { return net.JoinHostPort(e.cfg.Host, strconv.Itoa(port)) }

// Log returns the configured logger, or the context's.
func (e *Engine) Log(ctx context.Context) *slog.Logger {
	if e.cfg.Logger != nil {
		return e.cfg.Logger
	}

	return logger.Load(ctx)
}

// AcquirePort takes a host port labelled for name.
func (e *Engine) AcquirePort(name string) (int, error) {
	return portalloc.AcquireFrom(e.cfg.Ports, e.cfg.Prefix+"-"+name)
}

// ReleasePort returns a port taken by AcquirePort.
func (e *Engine) ReleasePort(port int) { portalloc.ReleaseTo(e.cfg.Ports, port) }

// ReleaseAll returns an API port and its load-balancer host ports.
func (e *Engine) ReleaseAll(port int, lbHost []int) {
	e.ReleasePort(port)

	for _, p := range lbHost {
		e.ReleasePort(p)
	}
}

// Remove stops and removes a container, then releases ports.
func (e *Engine) Remove(containerID string, ports ...int) {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()

	if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
		e.Log(ctx).WarnContext(ctx, e.cfg.Prefix+": cluster container removal failed",
			"container", containerID, "error", err)
	}

	for _, p := range ports {
		e.ReleasePort(p)
	}
}

// Stop stops the container but keeps it, so its filesystem (the cluster state) and published ports survive.
func (e *Engine) Stop(ctx context.Context, containerID string) error {
	rt, ok := e.cfg.Runtime.(Stoppable)
	if !ok {
		return ErrStopUnsupported
	}

	return rt.StopContainer(ctx, containerID)
}

// Start restarts a container kept by Stop and waits for its API server on addr; it returns the serving CA.
func (e *Engine) Start(ctx context.Context, containerID, addr string, ca []byte) ([]byte, error) {
	rt, ok := e.cfg.Runtime.(Stoppable)
	if !ok {
		return nil, ErrStopUnsupported
	}

	if err := rt.StartContainer(ctx, containerID); err != nil {
		return nil, fmt.Errorf("start cluster container: %w", err)
	}

	return e.AwaitReady(ctx, addr, ca)
}

// Close closes the runtime when it supports it.
func (e *Engine) Close() {
	if c, ok := e.cfg.Runtime.(io.Closer); ok {
		_ = c.Close()
	}
}

// Node describes one k3s server container.
type Node struct {
	Name      string
	Image     string
	JoinToken string
	Mounts    []string
	LBPorts   []int
}

// Instance is a started k3s container.
type Instance struct {
	CA          *CA
	ContainerID string
	LBHost      []int
	Port        int
}

func (e *Engine) spec(n Node, port int, lbHost []int, ca *CA) container.Spec {
	bind := container.BindHostFor(e.cfg.Host)
	ports := make([]string, 0, 1+len(n.LBPorts))
	ports = append(ports, container.PortSpec(bind, strconv.Itoa(port), strconv.Itoa(APIPort)))

	for i, p := range n.LBPorts {
		ports = append(ports, container.PortSpec(bind, strconv.Itoa(lbHost[i]), strconv.Itoa(p)))
	}

	return container.Spec{
		Name:       "gopherstack-" + e.cfg.Prefix + "-" + n.Name + "-" + uuid.NewString()[:8],
		Labels:     container.ServiceLabels("k3s-" + e.cfg.Prefix),
		Image:      n.Image,
		Entrypoint: []string{"/bin/sh", "-c", Script(len(n.LBPorts) > 0)},
		Env: []string{
			envAdminAuth + "=" + e.cfg.Token, envHost + "=" + e.cfg.Host, "K3S_TOKEN=" + n.JoinToken,
			envCACert + "=" + string(ca.CertPEM), envCAKey + "=" + string(ca.KeyPEM),
		},
		Ports:      ports,
		Mounts:     n.Mounts,
		Tmpfs:      []string{"/run", "/var/run"},
		Privileged: true,
	}
}

// Launch starts the container for n. record runs once the container exists and returns false when the caller no
// longer wants it, in which case the container is removed and ErrSuperseded returned.
func (e *Engine) Launch(ctx context.Context, n Node, record func(*Instance) bool) (*Instance, error) {
	ca, err := NewCA("gopherstack-" + e.cfg.Prefix + "-" + n.Name + "-ca")
	if err != nil {
		return nil, err
	}

	var (
		port   int
		lbHost []int
	)

	id, err := container.CreateAndStartRetry(ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			p, perr := e.AcquirePort(n.Name)
			if perr != nil {
				return container.Spec{}, perr
			}

			port = p
			lbHost = nil

			for range n.LBPorts {
				hp, lerr := e.AcquirePort(n.Name)
				if lerr != nil {
					e.ReleaseAll(port, lbHost)

					return container.Spec{}, lerr
				}

				lbHost = append(lbHost, hp)
			}

			return e.spec(n, p, lbHost, ca), nil
		},
		func() { e.ReleaseAll(port, lbHost) })
	if err != nil {
		return nil, err
	}

	inst := &Instance{CA: ca, ContainerID: id, Port: port, LBHost: lbHost}
	if record != nil && !record(inst) {
		e.Remove(id, append([]int{port}, lbHost...)...)

		return nil, ErrSuperseded
	}

	return inst, nil
}

func (e *Engine) probe(ctx context.Context, addr string, ca []byte) ([]byte, error) {
	if e.cfg.Probe != nil {
		return e.cfg.Probe(ctx, addr, e.cfg.Token)
	}

	return ca, Probe(ctx, addr, e.cfg.Token, ca)
}

// AwaitReady polls the API server until it answers or the start timeout passes; it returns the serving CA.
func (e *Engine) AwaitReady(ctx context.Context, addr string, ca []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()

	for {
		pctx, pcancel := context.WithTimeout(ctx, probeTimeout)
		got, err := e.probe(pctx, addr, ca)

		pcancel()

		if err == nil {
			return got, nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("API server not ready: %w", err)
		case <-ticker.C:
		}
	}
}
