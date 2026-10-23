package gcpredis

import (
	"sync"
	"time"

	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

const maxInstances = 128

// ObjectStore reads and writes objects in the GCS emulator, for ImportInstance and ExportInstance.
type ObjectStore interface {
	ReadObject(bucket, object string) ([]byte, error)
	WriteObject(bucket, object string, data []byte) error
}

type instRec struct {
	pb         *redispb.Instance
	engine     *redisengine.Engine
	authString string
	exports    int
}

// InMemoryBackend holds all Memorystore for Redis state under one lock.
type InMemoryBackend struct {
	now       func() time.Time
	mu        *lockmetrics.RWMutex
	alloc     *portalloc.Allocator
	objects   ObjectStore
	ops       *gcplro.Store
	wg        *sync.WaitGroup
	instances map[string]*instRec
	gate      func()
}

// NewInMemoryBackend creates an empty backend that serves instances on ephemeral loopback ports.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:        lockmetrics.New("gcpredis"),
		now:       time.Now,
		ops:       gcplro.New("gcpredis"),
		wg:        &sync.WaitGroup{},
		instances: make(map[string]*instRec),
	}
}

// SetPortPool makes instances take their Redis ports from alloc.
func (b *InMemoryBackend) SetPortPool(alloc *portalloc.Allocator) {
	b.mu.Lock("SetPortPool")
	defer b.mu.Unlock()

	b.alloc = alloc
}

// SetObjectStore connects ImportInstance and ExportInstance to an object store.
func (b *InMemoryBackend) SetObjectStore(o ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	b.objects = o
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func cloneInstance(pb *redispb.Instance) *redispb.Instance {
	out, _ := proto.Clone(pb).(*redispb.Instance)

	return out
}

// Reset stops every instance and clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	engines := b.takeEnginesLocked()
	b.instances = make(map[string]*instRec)
	b.mu.Unlock()

	b.wg.Wait()
	b.ops.Reset()
	closeAll(engines)
}

// Close stops every instance's Redis server and waits for background work.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	engines := b.takeEnginesLocked()
	b.mu.Unlock()

	b.wg.Wait()
	closeAll(engines)
}

func (b *InMemoryBackend) takeEnginesLocked() []*redisengine.Engine {
	out := make([]*redisengine.Engine, 0, len(b.instances))

	for _, r := range b.instances {
		if r.engine != nil {
			out = append(out, r.engine)
			r.engine = nil
		}
	}

	return out
}

func closeAll(engines []*redisengine.Engine) {
	for _, e := range engines {
		e.Close()
	}
}
