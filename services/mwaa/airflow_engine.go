package mwaa

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
)

const (
	// EngineDocker is the engine mode that runs a real Airflow container per environment.
	EngineDocker = "docker"

	airflowImageRepo    = "apache/airflow:"
	airflowDefaultHost  = "127.0.0.1"
	airflowWebPort      = "8080"
	airflowStartTimeout = 15 * time.Minute
	airflowPollInterval = 2 * time.Second
	airflowSyncInterval = 30 * time.Second
	airflowStopTimeout  = 2 * time.Minute
	airflowHTTPTimeout  = 30 * time.Second
	airflowTokenTTL     = 60 * time.Second
	airflowDirMode      = 0o777
	airflowFileMode     = 0o644
	airflowDefaultKey   = "test"
	airflowMaxBody      = 8 << 20
)

var (
	errAirflowTimeout   = errors.New("airflow webserver did not become healthy")
	errAirflowS3Missing = errors.New("emulated S3 is not wired")
	errAirflowBucketARN = errors.New("source bucket ARN is malformed")
	errAirflowNoEngine  = errors.New("environment is not backed by an Airflow container")
	errAirflowToken     = errors.New("token is invalid or expired")
)

// EngineConfig exposes the MWAA engine mode (stub or docker) and the endpoint containers use to reach gopherstack.
type EngineConfig interface {
	GetMWAAEngine() string
	GetMWAAEndpointURL() string
}

// AirflowRuntime is the slice of container.Runtime the Airflow engine needs.
type AirflowRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// AirflowHTTP is the slice of *http.Client used to reach the Airflow webserver.
type AirflowHTTP interface {
	Do(req *http.Request) (*http.Response, error)
}

// AirflowObjects reads DAG sources from the emulated S3.
type AirflowObjects interface {
	List(ctx context.Context, bucket, prefix string) ([]string, error)
	Get(ctx context.Context, bucket, key, version string) ([]byte, error)
}

// AirflowConfig configures docker-backed MWAA environments.
type AirflowConfig struct {
	Runtime AirflowRuntime
	// Ports hands out host ports for each webserver; nil picks a free OS port.
	Ports *portalloc.Allocator
	// HTTP calls the Airflow webserver; nil uses an isolated client.
	HTTP AirflowHTTP
	// Objects reads DAG sources; nil uses the emulated S3 backend wired through SetAppConfig.
	Objects AirflowObjects
	// Host is the address webserver ports are reached on; default 127.0.0.1.
	Host string
	// EndpointURL is the gopherstack URL as seen from inside containers (AWS_ENDPOINT_URL).
	EndpointURL string
	// Image overrides the apache/airflow:<version> image.
	Image string
	// StartTimeout bounds container boot until the webserver is healthy.
	StartTimeout time.Duration
	StopTimeout  time.Duration
	PollInterval time.Duration
	// SyncInterval is how often DAGs are re-read from S3 while AVAILABLE.
	SyncInterval time.Duration
	// TokenTTL is the CLI and web token lifetime.
	TokenTTL time.Duration
}

type airflowToken struct {
	expires time.Time
	key     string
	kind    string
}

type airflowEnv struct {
	ctx         context.Context //nolint:containedctx // env-scoped context cancelled on delete or restart
	cancel      context.CancelFunc
	done        chan struct{}
	containerID string
	base        string
	stageDir    string
	version     string
	adminPass   string
	port        int
}

type airflowEngine struct {
	envs   map[string]*airflowEnv
	tokens *safemap.Map[string, airflowToken]
	cfg    AirflowConfig
	wg     sync.WaitGroup
	owns   bool
}

// EnableAirflow switches the backend to docker mode: every environment runs a real Airflow container.
func (b *InMemoryBackend) EnableAirflow(cfg AirflowConfig) {
	cfg = withAirflowDefaults(cfg)

	if cfg.HTTP == nil {
		hc := httpclient.New(airflowHTTPTimeout)
		hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		cfg.HTTP = hc
	}

	if cfg.Objects == nil {
		cfg.Objects = s3Objects{b: b}
	}

	b.mu.Lock("EnableAirflow")
	defer b.mu.Unlock()

	b.airflow = &airflowEngine{
		cfg:    cfg,
		envs:   make(map[string]*airflowEnv),
		tokens: safemap.New[string, airflowToken]("mwaa-tokens"),
		owns:   true,
	}
	b.reconcileAirflowLocked()
}

func withAirflowDefaults(cfg AirflowConfig) AirflowConfig {
	if cfg.Host == "" {
		cfg.Host = airflowDefaultHost
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = airflowStartTimeout
	}

	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = airflowStopTimeout
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = airflowPollInterval
	}

	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = airflowSyncInterval
	}

	if cfg.TokenTTL <= 0 {
		cfg.TokenTTL = airflowTokenTTL
	}

	return cfg
}

// AirflowEnabled reports whether the docker engine is active.
func (b *InMemoryBackend) AirflowEnabled() bool {
	b.mu.RLock("AirflowEnabled")
	defer b.mu.RUnlock()

	return b.airflow != nil
}

// stopAirflowEnvs cancels every environment and waits for containers to be removed.
func (b *InMemoryBackend) stopAirflowEnvs() *airflowEngine {
	b.mu.Lock("stopAirflowEnvs")
	e := b.airflow

	if e == nil {
		b.mu.Unlock()

		return nil
	}

	for key, ae := range e.envs {
		ae.cancel()
		delete(e.envs, key)
	}

	b.mu.Unlock()

	e.wg.Wait()

	return e
}

// Close stops every Airflow container and waits for in-flight launches.
func (b *InMemoryBackend) Close() {
	e := b.stopAirflowEnvs()
	if e == nil {
		return
	}

	if h, ok := e.cfg.HTTP.(interface{ CloseIdleConnections() }); ok {
		h.CloseIdleConnections()
	}

	if c, ok := e.cfg.Runtime.(io.Closer); ok && e.owns {
		_ = c.Close()
	}
}

func (b *InMemoryBackend) airflowManagedLocked(key string) bool {
	if b.airflow == nil {
		return false
	}

	_, ok := b.airflow.envs[key]

	return ok
}

func (e *airflowEngine) acquirePort(name string) (int, error) {
	if e.cfg.Ports != nil {
		return e.cfg.Ports.Acquire("mwaa-" + name) //nolint:wrapcheck // allocator error is self-describing
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}

	defer func() { _ = l.Close() }()

	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}

	return addr.Port, nil
}

func (e *airflowEngine) baseURL(port int) string {
	return "http://" + net.JoinHostPort(e.cfg.Host, strconv.Itoa(port))
}

// teardown removes the container and staged files; it runs only after the env's goroutine stopped using them.
func (e *airflowEngine) teardown(ae *airflowEnv) {
	if ae.containerID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), e.cfg.StopTimeout)
		defer cancel()

		if err := e.cfg.Runtime.StopAndRemove(ctx, ae.containerID); err != nil {
			logger.Load(ctx).WarnContext(ctx, "mwaa: airflow container removal failed",
				"container", ae.containerID, "error", err)
		}
	}

	if ae.stageDir != "" {
		_ = os.RemoveAll(ae.stageDir)
	}

	if e.cfg.Ports != nil && ae.port > 0 {
		_ = e.cfg.Ports.Release(ae.port)
	}
}

// airflowImage maps an MWAA AirflowVersion (2.10.3) to its image (apache/airflow:2.10.3).
func airflowImage(version string) string {
	return airflowImageRepo + strings.TrimSpace(version)
}

// pollUntil runs fn immediately and then every interval until it reports done, errors or ctx ends.
func pollUntil(ctx context.Context, interval time.Duration, fn func(context.Context) (bool, error)) error {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		done, err := fn(ctx)
		if err != nil {
			return err
		}

		if done {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errAirflowTimeout, ctx.Err())
		case <-t.C:
		}
	}
}
