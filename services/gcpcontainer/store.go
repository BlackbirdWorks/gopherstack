package gcpcontainer

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defCreate = 600 * time.Millisecond
	defMutate = 300 * time.Millisecond
	defDelete = 400 * time.Millisecond
)

// Timing controls how long operations take on the backend clock.
type Timing struct {
	Create time.Duration
	Mutate time.Duration
	Delete time.Duration
}

// DefaultTiming returns the operation durations used unless a test overrides them.
func DefaultTiming() Timing {
	return Timing{Create: defCreate, Mutate: defMutate, Delete: defDelete}
}

// Compute resolves the networks and machine types of the Compute Engine emulator, so clusters can be checked
// against them.
type Compute interface {
	NetworkExists(project, network string) bool
	SubnetworkExists(project, region, subnetwork string) bool
	SecondaryRange(project, region, subnetwork, rangeName string) (string, bool)
	MachineTypeExists(machineType string) bool
}

type poolRec struct {
	Pool        *containerpb.NodePool
	PrevVersion string
	DeleteOp    string
	Size        int32
}

type rotationRec struct {
	OldEndpoint string
	Credentials bool
}

type clusterRec struct {
	Project  string
	Cluster  *containerpb.Cluster
	Rotation *rotationRec
	Failure  string
	DeleteOp string
	Pools    []*poolRec
}

type opKind int

const (
	kindCreate opKind = iota
	kindDelete
	kindClusterOp
	kindPoolOp
)

type opRec struct {
	Op       *containerpb.Operation
	EndAt    time.Time
	Project  string
	Location string
	Cluster  string
	Pool     string
	Failure  string
	Kind     opKind
	Manual   bool
	Finished bool
}

// InMemoryBackend holds all Kubernetes Engine state under one lock.
type InMemoryBackend struct {
	now      func() time.Time
	compute  Compute
	engine   *clusterEngine
	mu       *lockmetrics.RWMutex
	wg       *sync.WaitGroup
	runCtx   context.Context //nolint:containedctx // engine goroutines outlive any single request
	stop     context.CancelFunc
	clusters map[string]*clusterRec
	ops      map[string]*opRec
	timing   Timing
	seq      uint64
}

// NewInMemoryBackend creates an empty backend on the wall clock.
func NewInMemoryBackend() *InMemoryBackend {
	ctx, stop := context.WithCancel(context.Background())

	return &InMemoryBackend{
		now: time.Now, mu: lockmetrics.New("gcpcontainer"), wg: &sync.WaitGroup{}, runCtx: ctx, stop: stop,
		clusters: map[string]*clusterRec{}, ops: map[string]*opRec{}, timing: DefaultTiming(),
	}
}

// SetClock replaces the clock statuses and operations are derived from; tests inject a fake clock here.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetTiming replaces the operation durations.
func (b *InMemoryBackend) SetTiming(t Timing) {
	b.mu.Lock("SetTiming")
	defer b.mu.Unlock()

	b.timing = t
}

// SetCompute connects clusters to the Compute Engine emulator's networks and machine types; nil disconnects.
func (b *InMemoryBackend) SetCompute(c Compute) {
	b.mu.Lock("SetCompute")
	defer b.mu.Unlock()

	b.compute = c
}

func newRunCtx() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)

	return out
}

// Close stops engine goroutines and releases cluster containers.
func (b *InMemoryBackend) Close() {
	b.stop()
	b.wg.Wait()
	b.closeEngine()
}

// Reset clears all state and tears down engine clusters.
func (b *InMemoryBackend) Reset() {
	b.stop()
	b.wg.Wait()

	b.mu.Lock("Reset")
	lcs := b.detachAllLocked()
	b.clusters = map[string]*clusterRec{}
	b.ops = map[string]*opRec{}
	b.runCtx, b.stop = newRunCtx()
	e := b.engine
	b.mu.Unlock()

	if e != nil {
		e.reap(lcs)
	}
}
