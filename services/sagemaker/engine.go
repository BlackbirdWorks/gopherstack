package sagemaker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Engine modes for SageMaker.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

const (
	defaultStartupTimeout = 5 * time.Minute
	defaultPingInterval   = 250 * time.Millisecond
	defaultInvokeTimeout  = 60 * time.Second
	engineCleanupTimeout  = 30 * time.Second
	logDrainLimit         = 5 * time.Second
	modelContainerPort    = "8080"
	loopbackHost          = "127.0.0.1"
	dirMode               = 0o755
	fileMode              = 0o644
)

var (
	// ErrEngineUnavailable means no container runtime backs the SageMaker engine.
	ErrEngineUnavailable = errors.New("sagemaker container engine is unavailable")
	// ErrEndpointNotRunning means the endpoint has no live model container.
	ErrEndpointNotRunning = errors.New("endpoint has no running model container")
)

// EngineRuntime is the slice of the container runtime the engine needs. Runtimes that also
// implement container.ExitWaiter and container.LogStreamer enable job execution and logs.
type EngineRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// ObjectStore reads and writes S3 objects in the emulator.
type ObjectStore interface {
	GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error)
	PutObjectBytes(ctx context.Context, bucket, key string, data []byte) error
	ListObjectKeys(ctx context.Context, bucket, prefix string) ([]string, error)
}

// LogSink delivers container output to CloudWatch Logs in the emulator.
type LogSink interface {
	EnsureLogGroupAndStream(ctx context.Context, region, group, stream string) error
	PutLogLines(ctx context.Context, region, group, stream string, lines []string) error
}

// Notifier publishes to SNS topics in the emulator.
type Notifier interface {
	Publish(ctx context.Context, region, topicARN, message string) error
}

// EngineServices are the emulator backends the engine reads from and writes to.
type EngineServices struct {
	Store    ObjectStore
	Logs     LogSink
	Notifier Notifier
}

// EngineConfig configures container-backed endpoints and jobs.
type EngineConfig struct {
	// Runtime is nil when no container runtime could be reached; workloads then fail with Reason.
	Runtime EngineRuntime
	// Reason explains why Runtime is nil; it becomes the FailureReason of every failed workload.
	Reason string
	// WorkDir holds staging directories; default is under the OS temp dir.
	WorkDir string
	// StartupTimeout bounds the /ping health check of a model container; default five minutes.
	StartupTimeout time.Duration
	// PingInterval is the delay between /ping attempts; default 250ms.
	PingInterval time.Duration
	// InvokeTimeout bounds one /invocations request; default 60s.
	InvokeTimeout time.Duration
}

type engineState struct {
	services    EngineServices
	mu          *lockmetrics.RWMutex
	http        *http.Client
	deployments map[string]*endpointDeployment
	cancels     map[string]context.CancelFunc
	cfg         EngineConfig
}

func newEngineState(cfg EngineConfig) *engineState {
	if cfg.WorkDir == "" {
		cfg.WorkDir = filepath.Join(os.TempDir(), "gopherstack-sagemaker")
	}

	if cfg.StartupTimeout <= 0 {
		cfg.StartupTimeout = defaultStartupTimeout
	}

	if cfg.PingInterval <= 0 {
		cfg.PingInterval = defaultPingInterval
	}

	if cfg.InvokeTimeout <= 0 {
		cfg.InvokeTimeout = defaultInvokeTimeout
	}

	return &engineState{
		mu:          lockmetrics.New("sagemaker.engine"),
		cfg:         cfg,
		http:        httpclient.New(0),
		deployments: make(map[string]*endpointDeployment),
		cancels:     make(map[string]context.CancelFunc),
	}
}

func (e *engineState) svc() EngineServices {
	e.mu.RLock("svc")
	defer e.mu.RUnlock()

	return e.services
}

// unavailable reports why workloads cannot run, or nil when a runtime is present.
func (e *engineState) unavailable() error {
	if e.cfg.Runtime != nil {
		return nil
	}

	reason := e.cfg.Reason
	if reason == "" {
		reason = "no container runtime is available"
	}

	return fmt.Errorf("%w: %s", ErrEngineUnavailable, reason)
}

// track registers a cancel func under key so StopXxx can interrupt the running workload.
func (e *engineState) track(key string, cancel context.CancelFunc) {
	e.mu.Lock("track")
	defer e.mu.Unlock()

	e.cancels[key] = cancel
}

func (e *engineState) untrack(key string) {
	e.mu.Lock("untrack")
	defer e.mu.Unlock()

	delete(e.cancels, key)
}

// interrupt cancels the workload registered under key and reports whether one was running.
func (e *engineState) interrupt(key string) bool {
	e.mu.Lock("interrupt")
	cancel, ok := e.cancels[key]
	delete(e.cancels, key)
	e.mu.Unlock()

	if ok {
		cancel()
	}

	return ok
}

// stopAll tears down every running workload and endpoint deployment.
func (e *engineState) stopAll() {
	e.mu.Lock("stopAll")
	cancels := e.cancels
	deps := e.deployments
	e.cancels = make(map[string]context.CancelFunc)
	e.deployments = make(map[string]*endpointDeployment)
	e.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}

	for _, d := range deps {
		d.stop()
	}

	e.http.CloseIdleConnections()
}

func engineKey(kind, region, name string) string {
	return kind + "/" + region + "/" + name
}

// EnableEngine switches the backend to container mode: endpoints and jobs run real containers.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	b.mu.Lock("EnableEngine")
	prev := b.engine
	b.engine = newEngineState(cfg)
	b.mu.Unlock()

	if prev != nil {
		prev.stopAll()
	}
}

// SetEngineServices wires the emulator backends used by the engine; a no-op without an engine.
func (b *InMemoryBackend) SetEngineServices(s EngineServices) {
	eng := b.engineOrNil()
	if eng == nil {
		return
	}

	eng.mu.Lock("SetEngineServices")
	eng.services = s
	eng.mu.Unlock()
}

// EngineEnabled reports whether endpoints and jobs run in containers.
func (b *InMemoryBackend) EngineEnabled() bool { return b.engineOrNil() != nil }

// SetEngineServices wires the emulator backends used by the engine.
func (h *Handler) SetEngineServices(s EngineServices) { h.Backend.SetEngineServices(s) }

func (b *InMemoryBackend) engineOrNil() *engineState {
	b.mu.RLock("engineOrNil")
	defer b.mu.RUnlock()

	return b.engine
}

// jobContext derives the context a background workload runs under: it carries the region and is
// cancelled by Reset and Shutdown.
func (b *InMemoryBackend) jobContext(region string) (context.Context, context.CancelFunc) {
	b.mu.RLock("jobContext")
	defer b.mu.RUnlock()

	return b.jobContextLocked(region)
}

// jobContextLocked is jobContext for callers that already hold b.mu.
func (b *InMemoryBackend) jobContextLocked(region string) (context.Context, context.CancelFunc) {
	return context.WithCancel(context.WithValue(b.lifecycleCtx, regionContextKey{}, region))
}

// goEngine runs fn on the backend's wait group so Shutdown drains it.
func (b *InMemoryBackend) goEngine(fn func()) { b.wg.Go(fn) }

func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), engineCleanupTimeout)
}
