package bedrockagentcore

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// EngineAuto runs containers when a container runtime is reachable.
	EngineAuto = "auto"
	// EngineNone never starts containers; invocations that need one fail.
	EngineNone = "none"

	defaultHost         = container.LoopbackHost
	defaultStartTimeout = 3 * time.Minute
	defaultPollInterval = 250 * time.Millisecond
	defaultStopTimeout  = 30 * time.Second
	defaultIdleTimeout  = 15 * time.Minute
	defaultMaxLifetime  = 8 * time.Hour
	defaultPythonImage  = "python:3.12-slim"
	maxErrorBody        = 4 << 10
)

// ContainerRuntime is the slice of container.Runtime the engine uses.
type ContainerRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// HTTPDoer is the slice of *http.Client used to reach agent containers.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// EngineConfig configures container-backed runtimes and sandboxes.
type EngineConfig struct {
	Runtime ContainerRuntime
	// Ports hands out host ports for runtime containers; nil picks free OS ports.
	Ports *portalloc.Allocator
	// HTTP reaches agent containers; nil uses an isolated client.
	HTTP HTTPDoer
	// Host is the address published container ports are reached on; default 127.0.0.1.
	Host string
	// EndpointURL is the gopherstack URL as seen from containers (AWS_ENDPOINT_URL).
	EndpointURL string
	// RegistryHost, when set, replaces the host of ECR image URIs so images are pulled from the emulated registry.
	RegistryHost string
	// PythonImage is the code interpreter sandbox image.
	PythonImage string
	// StartTimeout bounds boot until a runtime container answers its health check.
	StartTimeout time.Duration
	PollInterval time.Duration
	StopTimeout  time.Duration
	// IdleTimeout applies to runtimes that set no idleRuntimeSessionTimeout.
	IdleTimeout time.Duration
}

type engine struct {
	runtimes  map[string]*runtimeSession
	sandboxes map[string]*sandbox
	mu        *lockmetrics.RWMutex
	now       func() time.Time
	cfg       EngineConfig
	owns      bool
}

func withEngineDefaults(cfg EngineConfig) EngineConfig {
	if cfg.Host == "" {
		cfg.Host = defaultHost
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = defaultStartTimeout
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = defaultStopTimeout
	}

	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}

	if cfg.PythonImage == "" {
		cfg.PythonImage = defaultPythonImage
	}

	if cfg.HTTP == nil {
		hc := httpclient.New(0)
		hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		cfg.HTTP = hc
	}

	return cfg
}

func newEngine(now func() time.Time) *engine {
	return &engine{
		cfg: withEngineDefaults(EngineConfig{}), runtimes: map[string]*runtimeSession{},
		sandboxes: map[string]*sandbox{}, mu: lockmetrics.New(serviceName + "-engine"), now: now,
	}
}

// EnableEngine switches the backend to container-backed runtimes and sandboxes. Call it before serving.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	b.engine.cfg = withEngineDefaults(cfg)
	b.engine.owns = true
}

// EngineEnabled reports whether containers can be started.
func (b *InMemoryBackend) EngineEnabled() bool { return b.engine.cfg.Runtime != nil }

var errNoRuntime = internal(
	"no container runtime is available: start a Docker-compatible daemon and keep --agentcore-engine=auto",
)

func (b *InMemoryBackend) containerEngine() (*engine, error) {
	if b.engine.cfg.Runtime == nil {
		return nil, errNoRuntime
	}

	return b.engine, nil
}

// Close stops every runtime container and sandbox.
func (b *InMemoryBackend) Close() {
	e := b.engine
	e.stopAll()

	if h, ok := e.cfg.HTTP.(interface{ CloseIdleConnections() }); ok {
		h.CloseIdleConnections()
	}

	if c, ok := e.cfg.Runtime.(io.Closer); ok && e.owns && e.cfg.Runtime != nil {
		_ = c.Close()
	}
}

// ReapIdle stops runtime sessions idle past their timeout and terminates expired tool sessions.
func (b *InMemoryBackend) ReapIdle() {
	keys := b.expireSessions()

	e := b.engine

	for _, k := range keys {
		e.releaseSandbox(k)
	}

	e.reapRuntimes()
}

func (e *engine) stopContainer(id string) {
	if id == "" || e.cfg.Runtime == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.StopTimeout)
	defer cancel()

	if err := e.cfg.Runtime.StopAndRemove(ctx, id); err != nil {
		logger.Load(ctx).WarnContext(ctx, "bedrockagentcore: container removal failed", "container", id, "error", err)
	}
}

func (e *engine) stopAll() {
	if e == nil {
		return
	}

	e.mu.Lock("stopAll")
	rts, sbs := e.runtimes, e.sandboxes
	e.runtimes, e.sandboxes = map[string]*runtimeSession{}, map[string]*sandbox{}
	e.mu.Unlock()

	for _, rs := range rts {
		rs.cancel()
		<-rs.ready
		e.teardownRuntime(rs)
	}

	for _, sb := range sbs {
		e.teardownSandbox(sb)
	}
}

func (e *engine) acquirePort(label string) (int, error) {
	return portalloc.AcquireFrom(e.cfg.Ports, label)
}

func (e *engine) hostAddr(port int) string {
	return e.cfg.Host + ":" + strconv.Itoa(port)
}

func removeAll(dir string) {
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}
