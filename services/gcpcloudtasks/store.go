package gcpcloudtasks

import (
	"context"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultPageSize   = 25
	maxPageSize       = 1000
	taskTombstoneTTL  = time.Hour
	queueTombstoneTTL = 7 * 24 * time.Hour
)

// queueRec is one queue with its tasks and (non-persisted) dispatcher state.
type queueRec struct {
	pb       *cloudtaskspb.Queue
	tasks    map[string]*taskRec
	lastFill time.Time
	tokens   float64
	inflight int
}

// taskRec wraps a task; pb.ScheduleTime doubles as the next dispatch time.
type taskRec struct {
	pb      *cloudtaskspb.Task
	running int
}

// InMemoryBackend holds queues, tasks, tombstones, IAM policies and CMEK configs under one lock.
type InMemoryBackend struct {
	now        func() time.Time
	mu         *lockmetrics.RWMutex
	runCtx     context.Context //nolint:containedctx // dispatcher lifetime, set by the worker
	client     *http.Client
	queues     map[string]*queueRec
	tombstones map[string]time.Time
	policies   map[string]*iampb.Policy
	cmek       map[string]*cloudtaskspb.CmekConfig
	wg         *sync.WaitGroup
	ops        *gcplro.Store
	appEngine  string
	etagSeq    uint64
	taskSeq    uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:     lockmetrics.New("gcpcloudtasks"),
		now:    time.Now,
		runCtx: context.Background(),
		client: newDispatchClient(),
		wg:     &sync.WaitGroup{},
		ops:    gcplro.New("gcpcloudtasks"),
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
	b.queues = make(map[string]*queueRec)
	b.tombstones = make(map[string]time.Time)
	b.policies = make(map[string]*iampb.Policy)
	b.cmek = make(map[string]*cloudtaskspb.CmekConfig)
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetMaps()
	b.ops.Reset()
	b.etagSeq = 0
	b.taskSeq = 0
}

// SetAppEngineBaseURL sets where AppEngineHttpRequest tasks are dispatched; empty disables them.
func (b *InMemoryBackend) SetAppEngineBaseURL(u string) {
	b.mu.Lock("SetAppEngineBaseURL")
	defer b.mu.Unlock()

	b.appEngine = u
}

// SetRunContext sets the context in-flight dispatches run under; cancel it to abort them.
func (b *InMemoryBackend) SetRunContext(ctx context.Context) {
	b.mu.Lock("SetRunContext")
	defer b.mu.Unlock()

	b.runCtx = ctx
}

// Drain blocks until every in-flight dispatch has finished.
func (b *InMemoryBackend) Drain() { b.wg.Wait() }

func pageSizeOf(n int32) (int, error) {
	if n < 0 {
		return 0, errInvalid("page_size must not be negative.")
	}

	return min(int(n), maxPageSize), nil
}
