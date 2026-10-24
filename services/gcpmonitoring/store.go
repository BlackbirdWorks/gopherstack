package gcpmonitoring

import (
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxPointsPerSeries = 5000
	maxSeriesTotal     = 100000
	maxDeliveryLog     = 1000
	httpTimeout        = 30 * time.Second
)

// Publisher publishes an alert notification to a Pub/Sub topic (projects/P/topics/T).
type Publisher interface {
	Publish(topic string, data []byte, attrs map[string]string) error
}

// seriesRec is one time series: a metric and resource label combination with its points in ascending end-time order.
type seriesRec struct {
	metric   *metricpb.Metric
	resource *monitoredres.MonitoredResource
	metadata *monitoredres.MonitoredResourceMetadata
	project  string
	unit     string
	points   []*monitoringpb.Point
	kind     metricpb.MetricDescriptor_MetricKind
	vtype    metricpb.MetricDescriptor_ValueType
}

type channelRec struct {
	codeAt    time.Time
	pb        *monitoringpb.NotificationChannel
	sensitive map[string]string
	code      string
}

type uptimeRec struct {
	pb      *monitoringpb.UptimeCheckConfig
	lastRun time.Time
}

// InMemoryBackend holds all Cloud Monitoring state under one lock.
type InMemoryBackend struct {
	pub          Publisher
	logs         LogMetricSource
	policies     map[string]*monitoringpb.AlertPolicy
	mu           *lockmetrics.RWMutex
	httpc        *http.Client
	insecure     *http.Client
	descriptors  map[string]*metricpb.MetricDescriptor
	series       map[string]*seriesRec
	now          func() time.Time
	channels     map[string]*channelRec
	uptime       map[string]*uptimeRec
	groups       map[string]*monitoringpb.Group
	snoozes      map[string]*monitoringpb.Snooze
	services     map[string]*monitoringpb.Service
	slos         map[string]*monitoringpb.ServiceLevelObjective
	incidents    map[string]*Incident
	deliveries   []Delivery
	insecureOnce sync.Once
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:    lockmetrics.New("gcpmonitoring"),
		now:   time.Now,
		httpc: httpclient.New(httpTimeout),
	}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.descriptors = make(map[string]*metricpb.MetricDescriptor)
	b.series = make(map[string]*seriesRec)
	b.policies = make(map[string]*monitoringpb.AlertPolicy)
	b.channels = make(map[string]*channelRec)
	b.uptime = make(map[string]*uptimeRec)
	b.groups = make(map[string]*monitoringpb.Group)
	b.snoozes = make(map[string]*monitoringpb.Snooze)
	b.services = make(map[string]*monitoringpb.Service)
	b.slos = make(map[string]*monitoringpb.ServiceLevelObjective)
	b.incidents = make(map[string]*Incident)
	b.deliveries = nil
}

// Reset clears all state; seam wiring is kept.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.resetMaps()
	b.mu.Unlock()
}

// SetPublisher wires Pub/Sub notification channels.
func (b *InMemoryBackend) SetPublisher(p Publisher) {
	b.mu.Lock("SetPublisher")
	b.pub = p
	b.mu.Unlock()
}

// SetLogMetricSource wires log-based metrics from Cloud Logging.
func (b *InMemoryBackend) SetLogMetricSource(s LogMetricSource) {
	b.mu.Lock("SetLogMetricSource")
	b.logs = s
	b.mu.Unlock()
}

// SetClock replaces the backend clock; alert evaluation, uptime checks and timestamp validation use it.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}
