package gcplogging

import (
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultRetentionDays  = 30
	requiredRetentionDays = 400
	maxRetentionDays      = 3650
	maxEntriesPerBucket   = 50000
	undeleteWindow        = 7 * 24 * time.Hour

	defaultSinkFilter = `NOT LOG_ID("cloudaudit.googleapis.com/activity") AND ` +
		`NOT LOG_ID("externalaudit.googleapis.com/activity") AND ` +
		`NOT LOG_ID("cloudaudit.googleapis.com/system_event") AND ` +
		`NOT LOG_ID("externalaudit.googleapis.com/system_event") AND ` +
		`NOT LOG_ID("cloudaudit.googleapis.com/access_transparency") AND ` +
		`NOT LOG_ID("externalaudit.googleapis.com/access_transparency")`
	requiredSinkFilter = `LOG_ID("cloudaudit.googleapis.com/activity") OR ` +
		`LOG_ID("externalaudit.googleapis.com/activity") OR ` +
		`LOG_ID("cloudaudit.googleapis.com/system_event") OR ` +
		`LOG_ID("externalaudit.googleapis.com/system_event") OR ` +
		`LOG_ID("cloudaudit.googleapis.com/access_transparency") OR ` +
		`LOG_ID("externalaudit.googleapis.com/access_transparency")`
)

type bucketData struct {
	deleteRequested time.Time
	pb              *loggingpb.LogBucket
	keys            map[string]struct{}
	entries         []*loggingpb.LogEntry
}

func newBucketData(pb *loggingpb.LogBucket) *bucketData {
	return &bucketData{pb: pb, keys: make(map[string]struct{})}
}

// InMemoryBackend holds all Cloud Logging state under one lock.
type InMemoryBackend struct {
	now        func() time.Time
	mu         *lockmetrics.RWMutex
	exportMu   *lockmetrics.RWMutex
	pub        Publisher
	objs       ObjectStore
	tables     TableStore
	ops        *gcplro.Store
	buckets    map[string]*bucketData
	views      map[string]*loggingpb.LogView
	sinks      map[string]*loggingpb.LogSink
	exclusions map[string]*loggingpb.LogExclusion
	metrics    map[string]*metricRec
	settings   map[string]*loggingpb.Settings
	cmek       map[string]*loggingpb.CmekSettings
	seeded     map[string]bool
	filters    map[string]*filter
	tails      *tailHub
	seq        uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:       lockmetrics.New("gcplogging"),
		exportMu: lockmetrics.New("gcplogging.export"),
		now:      time.Now,
		ops:      gcplro.New("gcplogging"),
		tails:    newTailHub(),
	}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.buckets = make(map[string]*bucketData)
	b.views = make(map[string]*loggingpb.LogView)
	b.sinks = make(map[string]*loggingpb.LogSink)
	b.exclusions = make(map[string]*loggingpb.LogExclusion)
	b.metrics = make(map[string]*metricRec)
	b.settings = make(map[string]*loggingpb.Settings)
	b.cmek = make(map[string]*loggingpb.CmekSettings)
	b.seeded = make(map[string]bool)
	b.filters = make(map[string]*filter)
}

// Reset clears all state; exporter wiring is kept.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.resetMaps()
	b.seq = 0
	b.mu.Unlock()
	b.ops.Reset()
}

// SetPublisher wires Pub/Sub sink destinations.
func (b *InMemoryBackend) SetPublisher(p Publisher) {
	b.mu.Lock("SetPublisher")
	b.pub = p
	b.mu.Unlock()
}

// SetObjectStore wires Cloud Storage sink destinations.
func (b *InMemoryBackend) SetObjectStore(s ObjectStore) {
	b.mu.Lock("SetObjectStore")
	b.objs = s
	b.mu.Unlock()
}

// SetTableStore wires BigQuery sink destinations.
func (b *InMemoryBackend) SetTableStore(s TableStore) {
	b.mu.Lock("SetTableStore")
	b.tables = s
	b.mu.Unlock()
}

func (b *InMemoryBackend) nextSeq() uint64 {
	b.seq++

	return b.seq
}

func (b *InMemoryBackend) isSeeded(parent string) bool {
	b.mu.RLock("isSeeded")
	defer b.mu.RUnlock()

	return b.seeded[parent]
}

// seed creates the built-in _Default and _Required buckets, views and sinks of a parent on first use.
func (b *InMemoryBackend) seed(parent string) {
	if b.isSeeded(parent) {
		return
	}

	b.mu.Lock("seed")
	defer b.mu.Unlock()

	b.seedLocked(parent)
}

func (b *InMemoryBackend) seedLocked(parent string) {
	if b.seeded[parent] {
		return
	}

	b.seeded[parent] = true
	now := timestamppb.New(b.now())

	for _, def := range []struct {
		id, desc, filter string
		retention        int32
		locked           bool
	}{
		{defaultBucketID, "Default bucket", defaultSinkFilter, defaultRetentionDays, false},
		{requiredBucketID, "Audit bucket", requiredSinkFilter, requiredRetentionDays, true},
	} {
		name := bucketName(parent, globalLocation, def.id)
		b.buckets[name] = newBucketData(&loggingpb.LogBucket{
			Name: name, Description: def.desc, RetentionDays: def.retention, Locked: def.locked,
			LifecycleState: loggingpb.LifecycleState_ACTIVE, CreateTime: now, UpdateTime: now,
		})
		b.putAllLogsView(name, now)
		b.sinks[sinkName(parent, def.id)] = &loggingpb.LogSink{
			Name: def.id, Destination: "logging.googleapis.com/" + name, Filter: def.filter,
			CreateTime: now, UpdateTime: now,
		}
	}
}

func (b *InMemoryBackend) putAllLogsView(bucket string, now *timestamppb.Timestamp) {
	name := bucket + "/views/" + allLogsViewID
	b.views[name] = &loggingpb.LogView{
		Name: name, Description: "Contains all logs ingested into the bucket", CreateTime: now, UpdateTime: now,
	}
}

// compiled returns the cached compiled form of a stored filter; callers hold the write lock.
func (b *InMemoryBackend) compiled(src string) *filter {
	if f, ok := b.filters[src]; ok {
		return f
	}

	f, err := compileFilter(src)
	if err != nil {
		return &filter{root: &notNode{kid: &andNode{}}}
	}

	if len(b.filters) >= maxFilterCache {
		b.filters = make(map[string]*filter)
	}

	b.filters[src] = f

	return f
}

const maxFilterCache = 1024
