package gcpscheduler

import (
	"context"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultPageSize = 500
	maxPageSize     = 500
)

// Publisher is the slice of the Pub/Sub emulator that PubsubTarget jobs publish into.
type Publisher interface {
	Publish(req *pubsubpb.PublishRequest) ([]string, error)
}

// jobRec is one job with its (persisted) retry state and non-persisted in-flight count.
type jobRec struct {
	pb        *schedulerpb.Job
	loc       *time.Location
	sched     *cronSchedule
	retryAt   time.Time
	execStart time.Time
	fireAt    time.Time
	failures  int32
	running   int
}

// InMemoryBackend holds all jobs under one lock.
type InMemoryBackend struct {
	now       func() time.Time
	mu        *lockmetrics.RWMutex
	runCtx    context.Context //nolint:containedctx // dispatcher lifetime, set by the worker
	client    *http.Client
	publisher Publisher
	jobs      map[string]*jobRec
	wg        *sync.WaitGroup
	appEngine string
	jobSeq    uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:     lockmetrics.New("gcpscheduler"),
		now:    time.Now,
		runCtx: context.Background(),
		client: newDispatchClient(),
		wg:     &sync.WaitGroup{},
	}
	b.resetMaps()

	return b
}

func newDispatchClient() *http.Client {
	c := httpclient.New(0)
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	return c
}

func (b *InMemoryBackend) resetMaps() {
	b.jobs = make(map[string]*jobRec)
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetMaps()
	b.jobSeq = 0
}

// SetAppEngineBaseURL sets where AppEngineHttpTarget jobs are dispatched; empty disables them.
func (b *InMemoryBackend) SetAppEngineBaseURL(u string) {
	b.mu.Lock("SetAppEngineBaseURL")
	defer b.mu.Unlock()

	b.appEngine = u
}

// SetPublisher wires the Pub/Sub emulator that PubsubTarget jobs publish into; nil disables them.
func (b *InMemoryBackend) SetPublisher(p Publisher) {
	b.mu.Lock("SetPublisher")
	defer b.mu.Unlock()

	b.publisher = p
}

// SetRunContext sets the context in-flight dispatches run under; cancel it to abort them.
func (b *InMemoryBackend) SetRunContext(ctx context.Context) {
	b.mu.Lock("SetRunContext")
	defer b.mu.Unlock()

	b.runCtx = ctx
}

// Drain blocks until every in-flight dispatch has finished.
func (b *InMemoryBackend) Drain() { b.wg.Wait() }
