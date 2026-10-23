package gcpcloudbuild

import (
	"context"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxBuilds       = 10000
	maxTriggers     = 600
	maxWorkerPools  = 100
	maxSteps        = 1000
	maxListEntries  = 100
	defaultPageSize = 100
)

type buildRec struct {
	pb     *cloudbuildpb.Build
	orig   *cloudbuildpb.Build
	ref    ref
	opName string
}

// InMemoryBackend holds builds, triggers and worker pools under one lock and runs builds as containers.
type InMemoryBackend struct {
	now      func() time.Time
	after    timerFunc
	ctx      context.Context //nolint:containedctx // lifetime of the build goroutines
	mu       *lockmetrics.RWMutex
	builds   map[string]*buildRec
	triggers map[string]*cloudbuildpb.BuildTrigger
	pools    map[string]*cloudbuildpb.WorkerPool
	cancels  map[string]context.CancelFunc
	ops      *gcplro.Store
	wg       *sync.WaitGroup
	rt       Runtime
	svc      Services
	workDir  string
	noEngine string
}

// NewInMemoryBackend creates an empty backend with no container runtime.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		now: time.Now, after: realTimer, ctx: context.Background(), mu: lockmetrics.New("gcpcloudbuild"),
		builds: map[string]*buildRec{}, triggers: map[string]*cloudbuildpb.BuildTrigger{},
		pools: map[string]*cloudbuildpb.WorkerPool{}, cancels: map[string]context.CancelFunc{},
		ops: gcplro.New("gcpcloudbuild"), wg: &sync.WaitGroup{}, workDir: os.TempDir(),
		noEngine: reasonNoRuntime,
	}
}

// SetContext bounds the lifetime of build goroutines.
func (b *InMemoryBackend) SetContext(ctx context.Context) {
	b.mu.Lock("SetContext")
	defer b.mu.Unlock()

	b.ctx = ctx
}

// SetRuntime enables execution of build steps on rt.
func (b *InMemoryBackend) SetRuntime(rt Runtime) {
	b.mu.Lock("SetRuntime")
	defer b.mu.Unlock()

	b.rt = rt
}

// DisableEngine records why builds cannot run; builds then fail with this status detail.
func (b *InMemoryBackend) DisableEngine(reason string) {
	b.mu.Lock("DisableEngine")
	defer b.mu.Unlock()

	b.rt, b.noEngine = nil, reason
}

// SetServices wires the Cloud Storage, Pub/Sub, Logging, Secret Manager and Artifact Registry seams.
func (b *InMemoryBackend) SetServices(s Services) {
	b.mu.Lock("SetServices")
	defer b.mu.Unlock()

	b.svc = s
}

// SetWorkDir sets the host directory build workspaces are created under.
func (b *InMemoryBackend) SetWorkDir(dir string) {
	b.mu.Lock("SetWorkDir")
	defer b.mu.Unlock()

	b.workDir = dir
}

// SetClock replaces the wall clock and the timeout timer, for tests.
func (b *InMemoryBackend) SetClock(now func() time.Time, after timerFunc) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now, b.after = now, after
}

// Wait blocks until every running build goroutine has exited.
func (b *InMemoryBackend) Wait() { b.wg.Wait() }

// Close cancels running builds and waits for them to exit.
func (b *InMemoryBackend) Close() {
	b.mu.RLock("Close")

	for _, cancel := range b.cancels {
		cancel()
	}

	b.mu.RUnlock()
	b.wg.Wait()
}

// Reset cancels running builds and drops all state.
func (b *InMemoryBackend) Reset() {
	b.Close()

	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.builds = map[string]*buildRec{}
	b.triggers = map[string]*cloudbuildpb.BuildTrigger{}
	b.pools = map[string]*cloudbuildpb.WorkerPool{}
	b.ops.Reset()
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func cloneBuild(pb *cloudbuildpb.Build) *cloudbuildpb.Build {
	out, _ := proto.Clone(pb).(*cloudbuildpb.Build)

	return out
}

func cloneTrigger(pb *cloudbuildpb.BuildTrigger) *cloudbuildpb.BuildTrigger {
	out, _ := proto.Clone(pb).(*cloudbuildpb.BuildTrigger)

	return out
}

func clonePool(pb *cloudbuildpb.WorkerPool) *cloudbuildpb.WorkerPool {
	out, _ := proto.Clone(pb).(*cloudbuildpb.WorkerPool)

	return out
}

func terminal(s cloudbuildpb.Build_Status) bool {
	switch s {
	case cloudbuildpb.Build_SUCCESS, cloudbuildpb.Build_FAILURE, cloudbuildpb.Build_INTERNAL_ERROR,
		cloudbuildpb.Build_TIMEOUT, cloudbuildpb.Build_CANCELLED, cloudbuildpb.Build_EXPIRED:
		return true
	default:
		return false
	}
}
