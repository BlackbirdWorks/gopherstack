// Package rum emulates Amazon CloudWatch RUM: app monitors, the PutRumEvents data plane, raw event
// queries, metric destinations and definitions, resource policies and tags.
package rum

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName      = "rum"
	defaultRegion    = "us-east-1"
	maxAppMonitors   = 10
	maxTags          = 50
	maxEventAge      = 30 * 24 * time.Hour
	maxEventsPerApp  = 20000
	defaultPageSize  = 100
	maxPageSize      = 100
	maxPolicyBytes   = 4096
	maxMetricBatch   = 200
	maxMetricsPerDst = 2000
)

// LogEvent is one record written to a CloudWatch Logs stream.
type LogEvent struct {
	Message   string
	Timestamp int64
}

// LogSink delivers RUM events to CloudWatch Logs when an app monitor has CwLogEnabled.
type LogSink interface {
	EnsureLogGroup(ctx context.Context, region, group string) error
	PutLogEvents(ctx context.Context, region, group, stream string, events []LogEvent) error
}

// InMemoryBackend is the RUM state; one coarse lock guards every table.
type InMemoryBackend struct {
	monitors     *store.Table[AppMonitor]
	events       *store.Table[Event]
	eventsByApp  *store.Index[Event]
	destinations *store.Table[Destination]
	definitions  *store.Table[MetricDef]
	defsByApp    *store.Index[MetricDef]
	registry     *store.Registry
	mu           *lockmetrics.RWMutex
	now          func() time.Time
	logs         LogSink
	metrics      cwmetric.Emitter
	accountID    string
	region       string
	seq          int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.region }

// SetLogSink wires CloudWatch Logs delivery for app monitors with CwLogEnabled.
func (b *InMemoryBackend) SetLogSink(s LogSink) {
	b.mu.Lock("SetLogSink")
	defer b.mu.Unlock()

	b.logs = s
}

// SetMetricEmitter wires CloudWatch metric delivery for default and custom metrics.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) {
	b.mu.Lock("SetMetricEmitter")
	defer b.mu.Unlock()

	b.metrics = e
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.seq = 0
}

type scope struct{ account, region string }

func (b *InMemoryBackend) scopeOf(ctx context.Context) scope {
	sc := scope{account: awsmeta.Account(ctx), region: awsmeta.Region(ctx)}
	if sc.account == "" {
		sc.account = b.accountID
	}

	if sc.region == "" {
		sc.region = b.region
	}

	return sc
}

func (b *InMemoryBackend) nextSeqLocked() int64 {
	b.seq++

	return b.seq
}

func monitorKey(v *AppMonitor) string { return v.ID }
func eventKey(v *Event) string        { return v.Key }
func destinationKey(v *Destination) string {
	return v.AppID + "|" + v.Destination + "|" + v.DestinationARN
}
func definitionKey(v *MetricDef) string { return v.ID }

func registerAllTables(b *InMemoryBackend) {
	b.monitors = store.Register(b.registry, "appMonitors", store.New(monitorKey))
	b.events = store.Register(b.registry, "events", store.New(eventKey))
	b.eventsByApp = b.events.AddIndex("byApp", func(v *Event) string { return v.AppID })
	b.destinations = store.Register(b.registry, "destinations", store.New(destinationKey))
	b.definitions = store.Register(b.registry, "metricDefinitions", store.New(definitionKey))
	b.defsByApp = b.definitions.AddIndex("byApp", func(v *MetricDef) string { return v.AppID })
}
