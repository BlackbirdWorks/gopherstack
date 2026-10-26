package gcpworkflows

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

const (
	httpTimeout  = 30 * time.Minute
	revisionTail = 3
)

type workflowRec struct {
	progs map[string]*wfinterp.Program
	revs  []*workflowspb.Workflow
}

type execRec struct {
	cancel context.CancelFunc
	pb     *executionspb.Execution
}

type callback struct {
	hit    chan map[string]any
	method string
	exec   string
}

// Endpoints resolves the base URL (scheme and host:port) of another emulator's REST API for connectors.
type Endpoints interface {
	BaseURL(service string) string
}

// InMemoryBackend holds all Workflows state under one lock.
type InMemoryBackend struct {
	runCtx    context.Context
	endpoints Endpoints
	logs      gcpserve.LogSink
	clock     wfinterp.Clock
	stop      context.CancelFunc
	ops       *gcplro.Store
	engine    *wfinterp.Engine
	client    *http.Client
	workflows map[string]*workflowRec
	execs     map[string]*execRec
	callbacks map[string]*callback
	mu        *lockmetrics.RWMutex
	wg        *sync.WaitGroup
	pushBase  string
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	ctx, stop := context.WithCancel(context.Background())

	b := &InMemoryBackend{
		runCtx:    ctx,
		stop:      stop,
		mu:        lockmetrics.New("gcpworkflows"),
		wg:        &sync.WaitGroup{},
		clock:     wfinterp.RealClock{},
		ops:       gcplro.New("gcpworkflows"),
		engine:    wfinterp.NewEngine(),
		client:    newHTTPClient(),
		workflows: make(map[string]*workflowRec),
		execs:     make(map[string]*execRec),
		callbacks: make(map[string]*callback),
	}
	b.engine.KnownConnector = isConnector

	return b
}

func newHTTPClient() *http.Client {
	tr := httpclient.NewTransport()
	tr.DialContext = loopbackDialer(tr.DialContext)

	return &http.Client{Timeout: httpTimeout, Transport: tr}
}

// SetClock replaces the clock sys.sleep, retries and timestamps use; tests pass a fake.
func (b *InMemoryBackend) SetClock(c wfinterp.Clock) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = c
}

// SetEndpoints connects googleapis.* connectors to the other emulators' REST APIs.
func (b *InMemoryBackend) SetEndpoints(e Endpoints) {
	b.mu.Lock("SetEndpoints")
	defer b.mu.Unlock()

	b.endpoints = e
}

// SetLogSink sends sys.log output to Cloud Logging; nil disables it.
func (b *InMemoryBackend) SetLogSink(s gcpserve.LogSink) {
	b.mu.Lock("SetLogSink")
	defer b.mu.Unlock()

	b.logs = s
}

// SetPushBase records the base URL (scheme and host:port) callback endpoints are served from.
func (b *InMemoryBackend) SetPushBase(base string) {
	b.mu.Lock("SetPushBase")
	defer b.mu.Unlock()

	b.pushBase = base
}

// CloseIdleConnections releases pooled outbound connections.
func (b *InMemoryBackend) CloseIdleConnections() { b.client.CloseIdleConnections() }

func (b *InMemoryBackend) now() time.Time { return b.clock.Now() }

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)

	return out
}

// stopExecutions cancels every running execution and waits for the runners to exit.
func (b *InMemoryBackend) stopExecutions() {
	b.stop()
	b.wg.Wait()
}

// Close stops all running executions; the backend is unusable afterwards.
func (b *InMemoryBackend) Close() { b.stopExecutions() }

// Reset clears all state and cancels running executions.
func (b *InMemoryBackend) Reset() {
	b.stopExecutions()

	b.mu.Lock("Reset")
	b.workflows = make(map[string]*workflowRec)
	b.execs = make(map[string]*execRec)
	b.callbacks = make(map[string]*callback)
	b.runCtx, b.stop = context.WithCancel(context.Background())
	b.mu.Unlock()

	b.ops.Reset()
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

const (
	localhostSuffix = ".localhost"
	loopbackIP      = "127.0.0.1"
)

// loopbackDialer sends *.localhost hosts (Cloud Run and Functions serving URLs) to the loopback interface.
func loopbackDialer(
	next func(ctx context.Context, network, addr string) (net.Conn, error),
) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if host, port, err := net.SplitHostPort(addr); err == nil && strings.HasSuffix(host, localhostSuffix) {
			addr = net.JoinHostPort(loopbackIP, port)
		}

		return next(ctx, network, addr)
	}
}
