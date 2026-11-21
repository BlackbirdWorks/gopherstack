package gcpbigtable

import (
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type cell struct {
	val []byte
	ts  int64
}

// family maps column qualifier to its cells, newest first.
type family map[string][]cell

type rowRec struct {
	fams map[string]family
}

type viewRec struct {
	pb     *adminpb.AuthorizedView
	policy *iampb.Policy
}

type tableRec struct {
	pb      *adminpb.Table
	policy  *iampb.Policy
	samples *sampleCache
	rows    map[string]*rowRec
	views   map[string]*viewRec
	bundles map[string]*adminpb.SchemaBundle
	splits  []string
	keys    []string
	changes []changeRec
	keyGen  uint64
	changeN uint64
	dirty   bool
}

type backupRec struct {
	pb     *adminpb.Backup
	policy *iampb.Policy
	table  *tableRec
}

type instRec struct {
	pb       *adminpb.Instance
	policy   *iampb.Policy
	clusters map[string]*adminpb.Cluster
	profiles map[string]*adminpb.AppProfile
	tables   map[string]*tableRec
	logical  map[string]*adminpb.LogicalView
	matviews map[string]*adminpb.MaterializedView
	deleted  map[string]*deletedTable
	backups  map[string]*backupRec
	tokens   map[string]map[string]bool
}

func newInstRec(pb *adminpb.Instance) *instRec {
	return &instRec{
		pb:       pb,
		clusters: make(map[string]*adminpb.Cluster),
		profiles: make(map[string]*adminpb.AppProfile),
		tables:   make(map[string]*tableRec),
		logical:  make(map[string]*adminpb.LogicalView),
		matviews: make(map[string]*adminpb.MaterializedView),
		deleted:  make(map[string]*deletedTable),
		backups:  make(map[string]*backupRec),
		tokens:   make(map[string]map[string]bool),
	}
}

func newTableRec(pb *adminpb.Table) *tableRec {
	return &tableRec{
		pb: pb, rows: make(map[string]*rowRec),
		views: make(map[string]*viewRec), bundles: make(map[string]*adminpb.SchemaBundle),
	}
}

// InMemoryBackend holds all Cloud Bigtable state under one lock.
type InMemoryBackend struct {
	now        func() time.Time
	mu         *lockmetrics.RWMutex
	ops        *gcplro.Store
	instances  map[string]*instRec
	keys       cmek.Checker
	notify     chan struct{}
	deletedCap int64
	seq        uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:         lockmetrics.New("gcpbigtable"),
		now:        time.Now,
		deletedCap: maxDeletedBytes,
		ops:        gcplro.New("gcpbigtable"),
		instances:  make(map[string]*instRec),
		notify:     make(chan struct{}),
	}
	b.ops.SetClock(func() time.Time { return b.now() })

	return b
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func (b *InMemoryBackend) nextSeq() uint64 {
	b.seq++

	return b.seq
}

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)

	return out
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.instances = make(map[string]*instRec)
	b.seq = 0
	b.wakeStreams()
	b.mu.Unlock()

	b.ops.Reset()
}

func (b *InMemoryBackend) clock() time.Time {
	b.mu.RLock("clock")
	defer b.mu.RUnlock()

	return b.now()
}
