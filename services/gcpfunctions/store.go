package gcpfunctions

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const defaultPageSize = 100

// Runner is the slice of the container runtime that functions need.
type Runner = gcpserve.Runner

// PortPool hands out host ports for published containers.
type PortPool = gcpserve.PortPool

// EventBus is the Pub/Sub surface event-triggered functions subscribe through.
type EventBus interface {
	GetTopic(name string) (*pubsubpb.Topic, error)
	CreateSubscription(in *pubsubpb.Subscription) (*pubsubpb.Subscription, error)
	DeleteSubscription(name string) error
}

// ObjectReader reads source archives from an object store (the GCS emulator).
type ObjectReader interface {
	ReadObject(bucket, object string) ([]byte, error)
}

type fnRec struct {
	pb       *functionspb.Function
	pool     *gcpscale.Pool[box]
	deployed chan struct{}
	failure  string
	subName  string
	zip      []byte
	revSeq   int
	epoch    int
	pending  int
}

type uploadRec struct {
	data       []byte
	generation int64
}

// InMemoryBackend holds all Cloud Functions state under one lock.
type InMemoryBackend struct {
	now            func() time.Time
	mu             *lockmetrics.RWMutex
	runCtx         context.Context //nolint:containedctx // workload lifetime, set by the worker
	runtime        func() (Runner, error)
	onClose        func()
	ports          PortPool
	events         EventBus
	objects        ObjectReader
	transport      *http.Transport
	ops            *gcplro.Store
	logs           *gcpserve.LogCapture
	secrets        gcpserve.SecretResolver
	buckets        BucketLookup
	images         func(runtime, image string) string
	clock          gcpscale.Clock
	evCtx          context.Context //nolint:containedctx // cancels event redelivery on Reset and Close
	evCancel       context.CancelFunc
	evWG           *sync.WaitGroup
	wg             *sync.WaitGroup
	functions      map[string]*fnRec
	uploads        map[string]*uploadRec
	startupTimeout time.Duration
	idleTimeout    time.Duration
	retryMin       time.Duration
	retryMax       time.Duration
	retention      time.Duration
	ingressPort    int
	lastGen        int64
}

// NewInMemoryBackend creates an empty backend with no container runtime.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:             lockmetrics.New("gcpfunctions"),
		now:            time.Now,
		runCtx:         context.Background(),
		ops:            gcplro.New("gcpfunctions"),
		logs:           gcpserve.NewLogCapture("gcpfunctions.logs"),
		wg:             &sync.WaitGroup{},
		transport:      httpclient.NewTransport(),
		startupTimeout: DefaultStartupTimeout,
		clock:          gcpscale.RealClock{},
		idleTimeout:    gcpscale.DefaultIdleTimeout,
		retryMin:       DefaultRetryMin,
		retryMax:       DefaultRetryMax,
		retention:      DefaultRetention,
		evWG:           &sync.WaitGroup{},
	}
	b.evCtx, b.evCancel = context.WithCancel(context.Background())
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.functions = make(map[string]*fnRec)
	b.uploads = make(map[string]*uploadRec)
}

// Reset stops every function and clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	pools := b.takeAllLocked()
	b.resetMaps()
	b.mu.Unlock()

	b.stopEvents()
	b.ops.Reset()
	b.wg.Wait()
	closeAll(pools)
	b.logs.Wait()
}

// SetRuntime wires the container runtime source; nil disables functions.
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

// SetPortPool wires the host port source for published function containers.
func (b *InMemoryBackend) SetPortPool(p PortPool) {
	b.mu.Lock("SetPortPool")
	defer b.mu.Unlock()

	b.ports = p
}

// SetEventBus wires the Pub/Sub emulator that event triggers subscribe through.
func (b *InMemoryBackend) SetEventBus(e EventBus) {
	b.mu.Lock("SetEventBus")
	defer b.mu.Unlock()

	b.events = e
}

// SetObjectReader wires the object store that storage_source archives are read from.
func (b *InMemoryBackend) SetObjectReader(o ObjectReader) {
	b.mu.Lock("SetObjectReader")
	defer b.mu.Unlock()

	b.objects = o
}

// SetRunContext sets the context containers run under; cancel it to abort starts in flight.
func (b *InMemoryBackend) SetRunContext(ctx context.Context) {
	b.mu.Lock("SetRunContext")
	defer b.mu.Unlock()

	b.runCtx = ctx
}

// SetStartupTimeout bounds how long a function container may take to accept connections.
func (b *InMemoryBackend) SetStartupTimeout(d time.Duration) {
	b.mu.Lock("SetStartupTimeout")
	defer b.mu.Unlock()

	if d > 0 {
		b.startupTimeout = d
	}
}

// SetIngressPort records the bound ingress port used to build URLs.
func (b *InMemoryBackend) SetIngressPort(p int) {
	b.mu.Lock("SetIngressPort")
	defer b.mu.Unlock()

	b.ingressPort = p
}

// Drain blocks until every background deployment and event delivery has finished.
func (b *InMemoryBackend) Drain() {
	b.wg.Wait()

	b.mu.RLock("Drain")
	wg := b.evWG
	b.mu.RUnlock()

	if wg != nil {
		wg.Wait()
	}
}

// Close stops all containers and releases idle connections.
func (b *InMemoryBackend) Close() {
	b.wg.Wait()

	b.mu.Lock("Close")
	pools := b.takeAllLocked()
	hook := b.onClose
	b.mu.Unlock()

	b.stopEvents()
	closeAll(pools)
	b.logs.Wait()
	b.transport.CloseIdleConnections()

	if hook != nil {
		hook()
	}
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func newUID() string { return uuid.NewString() }

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)

	return out
}

func (b *InMemoryBackend) nextGenLocked() int64 {
	g := b.now().UnixMicro()
	if g <= b.lastGen {
		g = b.lastGen + 1
	}

	b.lastGen = g

	return g
}

func (b *InMemoryBackend) ingressBase() (string, error) {
	b.mu.RLock("ingressBase")
	defer b.mu.RUnlock()

	if b.ingressPort <= 0 {
		return "", errPrecondition("The HTTP ingress is not running; set --gcp-functions-ingress-port.")
	}

	return fmt.Sprintf("http://localhost:%d", b.ingressPort), nil
}
