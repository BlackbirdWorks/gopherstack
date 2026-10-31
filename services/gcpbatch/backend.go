package gcpbatch

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultListPage = 100
	apiVersion      = "v1"
	verbDelete      = "delete"
	verbCancel      = "cancel"
	defaultStart    = 2 * time.Second
)

// Runtime is the slice of the container runtime a task needs. The runtime must also implement
// container.ExitWaiter; it should implement container.LogStreamer (Cloud Logging output).
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// SecretResolver reads a Secret Manager secret version ("projects/p/secrets/s/versions/v").
type SecretResolver interface {
	AccessSecret(versionName string) ([]byte, error)
}

// Services are the emulator backends a job reads from and writes to; absent ones stay nil.
type Services struct {
	Secrets SecretResolver
	Logs    gcpserve.LogSink
}

// Timing sets how long a state-only job takes on the backend clock. Every task runs for TaskRun and at most
// parallelism tasks run at once.
type Timing struct {
	Schedule time.Duration
	Start    time.Duration
	TaskRun  time.Duration
	Cancel   time.Duration
	Delete   time.Duration
}

// DefaultTiming returns the durations used unless a test overrides them.
func DefaultTiming() Timing {
	return Timing{
		Schedule: time.Second, Start: defaultStart, TaskRun: defaultStart,
		Cancel: time.Second, Delete: time.Second,
	}
}

// timerFunc starts a timer; the channel fires after d and stop cancels it.
type timerFunc func(d time.Duration) (<-chan time.Time, func() bool)

func realTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)

	return t.C, t.Stop
}

// InMemoryBackend holds all Batch state under one lock.
type InMemoryBackend struct {
	clock    atomic.Pointer[func() time.Time]
	timer    atomic.Pointer[timerFunc]
	mu       *lockmetrics.RWMutex
	jobs     map[string]*jobRec
	ops      map[string]*opRec
	requests map[string]string
	cancels  map[string]context.CancelFunc
	wg       *sync.WaitGroup
	rt       Runtime
	ctx      context.Context //nolint:containedctx // parent of every job runner
	svc      Services
	script   string
	timing   Timing
}

// NewInMemoryBackend creates an empty state-only backend on the wall clock.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:       lockmetrics.New("gcpbatch"),
		jobs:     map[string]*jobRec{},
		ops:      map[string]*opRec{},
		requests: map[string]string{},
		cancels:  map[string]context.CancelFunc{},
		wg:       &sync.WaitGroup{},
		ctx:      context.Background(),
		script:   DefaultScriptImage,
		timing:   DefaultTiming(),
	}
	b.SetClock(time.Now)
	b.SetTimer(realTimer)

	return b
}

// SetClock replaces the clock lifecycle states and timestamps derive from.
func (b *InMemoryBackend) SetClock(now func() time.Time) { b.clock.Store(&now) }

// SetTimer replaces the timer behind max_run_duration and runnable timeouts.
func (b *InMemoryBackend) SetTimer(t func(d time.Duration) (<-chan time.Time, func() bool)) {
	f := timerFunc(t)
	b.timer.Store(&f)
}

func (b *InMemoryBackend) now() time.Time { return (*b.clock.Load())().UTC() }

func (b *InMemoryBackend) startTimer(d time.Duration) (<-chan time.Time, func() bool) {
	return (*b.timer.Load())(d)
}

// SetTiming replaces the state-only durations.
func (b *InMemoryBackend) SetTiming(t Timing) {
	b.mu.Lock("SetTiming")
	defer b.mu.Unlock()

	b.timing = t
}

// SetContext sets the parent context of job runners; cancelling it stops them.
func (b *InMemoryBackend) SetContext(ctx context.Context) {
	b.mu.Lock("SetContext")
	defer b.mu.Unlock()

	b.ctx = ctx
}

// SetRuntime turns the container engine on: new jobs run their runnables as containers.
func (b *InMemoryBackend) SetRuntime(rt Runtime, scriptImage string) {
	b.mu.Lock("SetRuntime")
	defer b.mu.Unlock()

	b.rt = rt
	if scriptImage != "" {
		b.script = scriptImage
	}
}

// SetServices connects Secret Manager and Cloud Logging.
func (b *InMemoryBackend) SetServices(s Services) {
	b.mu.Lock("SetServices")
	defer b.mu.Unlock()

	b.svc = s
}

// WaitIdle blocks until every job runner has finished.
func (b *InMemoryBackend) WaitIdle() { b.wg.Wait() }

// Close cancels every running job and waits for the runners to stop.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")

	for _, c := range b.cancels {
		c()
	}

	b.mu.Unlock()
	b.wg.Wait()
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)

	return out
}

// Reset stops every runner and clears all state.
func (b *InMemoryBackend) Reset() {
	b.Close()

	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.jobs, b.ops = map[string]*jobRec{}, map[string]*opRec{}
	b.requests, b.cancels = map[string]string{}, map[string]context.CancelFunc{}
}
