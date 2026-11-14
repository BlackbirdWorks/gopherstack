package redisdocker

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	defaultHost         = "127.0.0.1"
	defaultStartTimeout = 3 * time.Minute
	defaultPollInterval = 250 * time.Millisecond
	stopTimeout         = 30 * time.Second
	adminPasswordBytes  = 16
	containerNameBytes  = 4
	busPort             = 16379
	mountDir            = "/gs"
	dirMode             = 0o755
)

// ErrNoCA is returned when TLS is requested but the engine has no certificate authority.
var ErrNoCA = errors.New("redis group needs a TLS certificate authority")

// Config configures an Engine.
type Config struct {
	Runtime Runtime
	Ports   *portalloc.Allocator
	// CA signs node certificates; TLS groups cannot launch without it.
	CA *devtls.CA
	// Dial opens a control connection; defaults to go-redis.
	Dial Dialer
	// FetchRDB reads a node's dataset; defaults to a SYNC handshake.
	FetchRDB func(ctx context.Context, t Target) ([]byte, error)
	// Host is the address clients are told to use; defaults to 127.0.0.1.
	Host string
	// Label names the port reservations, e.g. "memorydb".
	Label string
	// TempDir holds per-group mount directories; defaults to the OS temp dir.
	TempDir      string
	StartTimeout time.Duration
	PollInterval time.Duration
}

// Engine launches groups of Redis containers.
type Engine struct {
	cfg Config
}

// New returns an Engine; zero Config fields take their defaults.
func New(cfg Config) *Engine {
	if cfg.Host == "" {
		cfg.Host = defaultHost
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = defaultStartTimeout
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if cfg.Dial == nil {
		cfg.Dial = dialRedis
	}

	if cfg.FetchRDB == nil {
		cfg.FetchRDB = fetchRDB
	}

	if cfg.Label == "" {
		cfg.Label = "redis"
	}

	return &Engine{cfg: cfg}
}

// Host is the address nodes are advertised on.
func (e *Engine) Host() string { return e.cfg.Host }

// CAPEM returns the PEM of the CA that signs node certificates, or nil without one.
func (e *Engine) CAPEM() []byte {
	if e.cfg.CA == nil {
		return nil
	}

	return e.cfg.CA.CertPEM()
}

type node struct {
	containerID string
	ip          string
	port        int
	shard       int
	replica     int
}

func (n *node) primary() bool { return n.replica == 0 }

// Group is a launched set of containers.
type Group struct {
	engine   *Engine
	admin    string
	dir      string
	aclUsers []string
	nodes    []*node
	spec     Spec
	mu       sync.Mutex
	closed   bool
}

// Endpoint is a client-reachable node address.
type Endpoint struct {
	Host string
	Port int
}

// Addr formats the endpoint as host:port.
func (e Endpoint) Addr() string { return net.JoinHostPort(e.Host, strconv.Itoa(e.Port)) }

// Endpoints lists each shard's nodes, primary first.
func (g *Group) Endpoints() [][]Endpoint {
	out := make([][]Endpoint, len(g.spec.Shards))

	for _, n := range g.nodes {
		out[n.shard] = append(out[n.shard], Endpoint{Host: g.engine.cfg.Host, Port: n.port})
	}

	return out
}

// ClusterMode reports whether the group runs Redis Cluster.
func (g *Group) ClusterMode() bool { return g.spec.ClusterMode }

// TLS reports whether nodes serve TLS.
func (g *Group) TLS() bool { return g.spec.TLS }

func randomHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// Launch starts the containers of spec, wires replication or the cluster, and returns once the group serves traffic.
func (e *Engine) Launch(ctx context.Context, spec Spec) (*Group, error) {
	spec = spec.withDefaultSlots()
	if err := spec.validate(); err != nil {
		return nil, err
	}

	if spec.TLS && e.cfg.CA == nil {
		return nil, ErrNoCA
	}

	g := &Group{engine: e, spec: spec, admin: randomHex(adminPasswordBytes)}

	lctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	if err := g.start(lctx); err != nil {
		g.Close()

		return nil, err
	}

	return g, nil
}

func (g *Group) start(ctx context.Context) error {
	dir, err := os.MkdirTemp(g.engine.cfg.TempDir, "gopherstack-redis-")
	if err != nil {
		return fmt.Errorf("group directory: %w", err)
	}

	g.dir = dir

	if err = os.Chmod(dir, dirMode); err != nil {
		return fmt.Errorf("group directory mode: %w", err)
	}

	if err = g.writeFiles(); err != nil {
		return err
	}

	for si, sh := range g.spec.Shards {
		for r := 0; r <= sh.Replicas; r++ {
			if err = g.startNode(ctx, &node{shard: si, replica: r}); err != nil {
				return err
			}
		}
	}

	if err = g.awaitNodes(ctx); err != nil {
		return err
	}

	return g.wire(ctx)
}

func (g *Group) startNode(ctx context.Context, n *node) error {
	cfg := g.engine.cfg

	id, err := container.CreateAndStartRetry(ctx, cfg.Runtime,
		func() (container.Spec, error) {
			port, perr := portalloc.AcquireFrom(cfg.Ports, cfg.Label)
			if perr != nil {
				return container.Spec{}, fmt.Errorf("allocate redis port: %w", perr)
			}

			n.port = port

			return g.containerSpec(n), nil
		},
		func() { portalloc.ReleaseTo(cfg.Ports, n.port); n.port = 0 })
	if err != nil {
		return fmt.Errorf("start redis node: %w", err)
	}

	n.containerID = id

	g.mu.Lock()
	g.nodes = append(g.nodes, n)
	g.mu.Unlock()

	return nil
}

// Close removes the containers, frees their ports and deletes the mount directory. It is idempotent.
func (g *Group) Close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()

		return
	}

	g.closed = true
	nodes := append([]*node(nil), g.nodes...)
	g.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()

	for _, n := range nodes {
		if err := g.engine.cfg.Runtime.StopAndRemove(ctx, n.containerID); err != nil {
			logger.Load(ctx).WarnContext(ctx, "redis group: container removal failed",
				"container", n.containerID, "error", err)
		}

		portalloc.ReleaseTo(g.engine.cfg.Ports, n.port)
	}

	if g.dir != "" {
		_ = os.RemoveAll(g.dir)
	}
}

func (g *Group) tlsConfig() *tls.Config {
	if !g.spec.TLS {
		return nil
	}

	return &tls.Config{RootCAs: g.engine.cfg.CA.Pool(), ServerName: defaultHost, MinVersion: tls.VersionTLS12}
}

func (g *Group) target(n *node) Target {
	return Target{
		Addr:     net.JoinHostPort(defaultHost, strconv.Itoa(n.port)),
		Username: AdminUser,
		Password: g.admin,
		TLS:      g.tlsConfig(),
	}
}

func (g *Group) dial(n *node) Admin { return g.engine.cfg.Dial(g.target(n)) }
