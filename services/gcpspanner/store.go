package gcpspanner

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxInstances = 64
	maxDatabases = 100
)

type dbRec struct {
	pb  *databasepb.Database
	ddl []string
}

type instRec struct {
	pb     *instancepb.Instance
	policy *iampb.Policy
	dbs    map[string]*dbRec
}

// InMemoryBackend holds all Cloud Spanner admin state under one lock. The data plane lives in the emulator.
type InMemoryBackend struct {
	now       func() time.Time
	mu        *lockmetrics.RWMutex
	ops       *gcplro.Store
	wg        *sync.WaitGroup
	instances map[string]*instRec
	emu       *emulator
	etagSeq   uint64
}

// NewInMemoryBackend creates an empty backend with no data plane; see SetLauncher.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("gcpspanner"),
		now:       time.Now,
		ops:       gcplro.New("gcpspanner"),
		wg:        &sync.WaitGroup{},
		instances: make(map[string]*instRec),
	}
	b.emu = newEmulator(b, nil)

	return b
}

// SetLauncher connects the data plane to an emulator launcher; nil leaves the backend metadata only.
func (b *InMemoryBackend) SetLauncher(l launcher) { b.emu.setLauncher(l) }

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func cloneInstance(pb *instancepb.Instance) *instancepb.Instance {
	out, _ := proto.Clone(pb).(*instancepb.Instance)

	return out
}

func cloneDatabase(pb *databasepb.Database) *databasepb.Database {
	out, _ := proto.Clone(pb).(*databasepb.Database)

	return out
}

// Reset stops the emulator and clears all state.
func (b *InMemoryBackend) Reset() {
	b.wg.Wait()
	b.emu.close(context.Background())

	b.mu.Lock("Reset")
	b.instances = make(map[string]*instRec)
	b.etagSeq = 0
	b.mu.Unlock()

	b.ops.Reset()
}

// Close stops the emulator and waits for background work.
func (b *InMemoryBackend) Close() {
	b.wg.Wait()
	b.emu.shutdown(context.Background())
}
