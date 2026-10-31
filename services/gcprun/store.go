package gcprun

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const defaultPageSize = 100

// Runner is the slice of the container runtime that Cloud Run workloads need.
type Runner interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// PortPool hands out host ports for published containers.
type PortPool interface {
	Acquire(label string) (int, error)
	Release(port int) error
}

type svcRec struct {
	pb      *runpb.Service
	failure string
	revSeq  int
	pending int
}

type revRec struct {
	pb       *runpb.Revision
	pool     *gcpscale.Pool[*inst]
	startErr string
	seq      int
}

type execRec struct {
	pb     *runpb.Execution
	cancel context.CancelFunc
	done   chan struct{}
}

type taskRec struct {
	pb    *runpb.Task
	state taskState
}

// InMemoryBackend holds all Cloud Run state under one lock.
type InMemoryBackend struct {
	ports          PortPool
	prober         Prober
	runCtx         context.Context
	secrets        SecretResolver
	services       map[string]*svcRec
	runtime        func() (Runner, error)
	transport      *http.Transport
	ops            *gcplro.Store
	logs           *gcpserve.LogCapture
	wg             *sync.WaitGroup
	monWG          *sync.WaitGroup
	mu             *lockmetrics.RWMutex
	clock          atomic.Pointer[clockBox]
	iam            *gcpiampolicy.Store
	onClose        func()
	revisions      map[string]*revRec
	jobs           map[string]*runpb.Job
	executions     map[string]*execRec
	tasks          map[string]*taskRec
	deleted        *tombstones
	idleTimeout    time.Duration
	startupTimeout time.Duration
	ingressPort    int
	rr             atomic.Uint64
	instSeq        atomic.Uint64
}

// NewInMemoryBackend creates an empty backend with no container runtime.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:             lockmetrics.New("gcprun"),
		runCtx:         context.Background(),
		ops:            gcplro.New("gcprun"),
		logs:           gcpserve.NewLogCapture("gcprun.logs"),
		wg:             &sync.WaitGroup{},
		monWG:          &sync.WaitGroup{},
		idleTimeout:    DefaultIdleTimeout,
		transport:      httpclient.NewTransport(),
		startupTimeout: DefaultStartupTimeout,
	}
	b.clock.Store(&clockBox{Clock: realClock{}})
	b.prober = &netProber{transport: b.transport}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.services = make(map[string]*svcRec)
	b.revisions = make(map[string]*revRec)
	b.jobs = make(map[string]*runpb.Job)
	b.executions = make(map[string]*execRec)
	b.tasks = make(map[string]*taskRec)
	b.deleted = newTombstones()
	b.iam = gcpiampolicy.NewStore()
}

// Reset stops every workload and clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.cancelAllLocked()
	revs := b.revisions
	b.resetMaps()
	b.mu.Unlock()

	b.ops.Reset()

	for _, r := range revs {
		b.teardown(r)
	}

	b.wg.Wait()
	b.monWG.Wait()
	b.logs.Wait()
}

// SetRuntime wires the container runtime source; nil disables workloads.
func (b *InMemoryBackend) SetRuntime(fn func() (Runner, error)) {
	b.mu.Lock("SetRuntime")
	defer b.mu.Unlock()

	b.runtime = fn
}

// SetOnClose registers a hook run by Close after every container is stopped.
func (b *InMemoryBackend) SetOnClose(fn func()) {
	b.mu.Lock("SetOnClose")
	defer b.mu.Unlock()

	b.onClose = fn
}

// SetPortPool wires the host port source for published service containers.
func (b *InMemoryBackend) SetPortPool(p PortPool) {
	b.mu.Lock("SetPortPool")
	defer b.mu.Unlock()

	b.ports = p
}

// SetRunContext sets the context workloads run under; cancel it to abort them.
func (b *InMemoryBackend) SetRunContext(ctx context.Context) {
	b.mu.Lock("SetRunContext")
	defer b.mu.Unlock()

	b.runCtx = ctx
}

// SetStartupTimeout bounds how long a revision may take to accept connections.
func (b *InMemoryBackend) SetStartupTimeout(d time.Duration) {
	b.mu.Lock("SetStartupTimeout")
	defer b.mu.Unlock()

	if d > 0 {
		b.startupTimeout = d
	}
}

// SetIngressPort records the bound ingress port used to build service URIs.
func (b *InMemoryBackend) SetIngressPort(p int) {
	b.mu.Lock("SetIngressPort")
	defer b.mu.Unlock()

	b.ingressPort = p
}

// Drain blocks until every background deployment and execution has finished.
func (b *InMemoryBackend) Drain() { b.wg.Wait() }

// Close stops all containers and releases idle connections.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	b.cancelAllLocked()
	revs := make([]*revRec, 0, len(b.revisions))

	for _, r := range b.revisions {
		revs = append(revs, r)
	}
	b.mu.Unlock()

	b.wg.Wait()

	for _, r := range revs {
		b.teardown(r)
	}

	b.monWG.Wait()
	b.logs.Wait()
	b.transport.CloseIdleConnections()

	b.mu.RLock("Close.hook")
	hook := b.onClose
	b.mu.RUnlock()

	if hook != nil {
		hook()
	}
}

func (b *InMemoryBackend) cancelAllLocked() {
	for _, e := range b.executions {
		if e.cancel != nil {
			e.cancel()
		}
	}
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.clk().Now()) }

func newUID() string { return uuid.NewString() }

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)

	return out
}

func (b *InMemoryBackend) acquirePort() (int, error) {
	b.mu.RLock("acquirePort")
	pool := b.ports
	b.mu.RUnlock()

	if pool == nil {
		return portalloc.AcquireEphemeral()
	}

	return pool.Acquire("gcprun")
}

func (b *InMemoryBackend) releasePort(port int) {
	b.mu.RLock("releasePort")
	pool := b.ports
	b.mu.RUnlock()

	switch {
	case port <= 0:
	case pool == nil:
		portalloc.ReleaseEphemeral(port)
	default:
		_ = pool.Release(port)
	}
}

func (b *InMemoryBackend) runner() (Runner, error) {
	b.mu.RLock("runner")
	fn := b.runtime
	b.mu.RUnlock()

	if fn == nil {
		return nil, errNoRuntime
	}

	r, err := fn()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errNoRuntime, err)
	}

	return r, nil
}
