package gcpmonitoring

import (
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api"
	"google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxMetricLabels     = 30
	maxLabelValueLen    = 1024
	maxMetricTypeLen    = 200
	uptimeCheckPassed   = "monitoring.googleapis.com/uptime_check/check_passed"
	uptimeCheckLatency  = "monitoring.googleapis.com/uptime_check/request_latency"
	labelCheckID        = "check_id"
	labelCheckerLoc     = "checker_location"
	prefixCustom        = "custom.googleapis.com/"
	prefixExternal      = "external.googleapis.com/"
	prefixWorkload      = "workload.googleapis.com/"
	prefixPrometheus    = "prometheus.googleapis.com/"
	unitOne             = "1"
	unitMillis          = "ms"
	unitSeconds         = "s"
	displayRequestCount = "Request count"
)

var (
	labelKeyRe   = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,99}$`)
	metricTypeRe = regexp.MustCompile(`^[a-zA-Z0-9_./\-]+$`)
)

type builtinDef struct {
	typ, display, unit, desc string
	labels                   []string
	resources                []string
	kind                     metricpb.MetricDescriptor_MetricKind
	vtype                    metricpb.MetricDescriptor_ValueType
}

//nolint:gochecknoglobals // immutable catalog of built-in metric descriptors
var builtinDefs = []builtinDef{
	{"compute.googleapis.com/instance/cpu/utilization", "CPU utilization", "10^2.%",
		"Fractional utilization of allocated CPU on this instance.", []string{"instance_name"},
		[]string{typeGCEInstance}, kindGauge, vtDouble},
	{"compute.googleapis.com/instance/uptime", "Uptime", unitSeconds,
		"Elapsed time since the VM started, in seconds.", []string{"instance_name"},
		[]string{typeGCEInstance}, kindDelta, vtDouble},
	{"run.googleapis.com/request_count", displayRequestCount, unitOne,
		"Number of requests reaching the revision.", []string{labelResponseCode, "response_code_class"},
		[]string{typeCloudRunRevision}, kindDelta, vtInt64},
	{"run.googleapis.com/request_latencies", "Request latencies", unitMillis,
		"Distribution of request latencies in milliseconds.", []string{labelResponseCode, "response_code_class"},
		[]string{typeCloudRunRevision}, kindDelta, vtDist},
	{"cloudfunctions.googleapis.com/function/execution_count", "Executions", unitOne,
		"Number of function executions.", []string{"status"}, []string{"cloud_function"}, kindDelta, vtInt64},
	{"pubsub.googleapis.com/topic/send_message_operation_count", "Publish message operations", unitOne,
		"Cumulative count of publish message operations.", []string{labelResponseCode},
		[]string{"pubsub_topic"}, kindDelta, vtInt64},
	{"pubsub.googleapis.com/subscription/num_undelivered_messages", "Unacked messages", unitOne,
		"Number of unacknowledged messages in a subscription.", nil,
		[]string{"pubsub_subscription"}, kindGauge, vtInt64},
	{"storage.googleapis.com/api/request_count", displayRequestCount, unitOne,
		"Delta count of API calls, grouped by the API method name and response code.",
		[]string{"method", labelResponseCode}, []string{"gcs_bucket"}, kindDelta, vtInt64},
	{"serviceruntime.googleapis.com/api/request_count", displayRequestCount, unitOne,
		"Delta count of API calls, grouped by response code.", []string{labelResponseCode},
		[]string{"consumed_api"}, kindDelta, vtInt64},
	{"logging.googleapis.com/log_entry_count", "Log entries", unitOne,
		"Number of log entries ingested.", []string{"log", fldSeverity},
		[]string{typeGlobal, typeGCEInstance, typeCloudRunRevision}, kindDelta, vtInt64},
	{uptimeCheckPassed, "Check passed", unitOne, "Whether the uptime check succeeded.",
		[]string{labelCheckID, labelCheckerLoc}, []string{typeUptimeURL}, kindGauge, vtBool},
	{uptimeCheckLatency, "Request latency", unitMillis, "Latency of the uptime check request in milliseconds.",
		[]string{labelCheckID, labelCheckerLoc}, []string{typeUptimeURL}, kindGauge, vtDist},
}

func descriptorName(project, typ string) string {
	return projectScope + project + "/" + collMetricDescriptors + "/" + typ
}

func cloneDescriptor(d *metricpb.MetricDescriptor) *metricpb.MetricDescriptor {
	out, _ := proto.Clone(d).(*metricpb.MetricDescriptor)

	return out
}

func builtinDescriptor(project string, d builtinDef) *metricpb.MetricDescriptor {
	out := &metricpb.MetricDescriptor{
		Name: descriptorName(project, d.typ), Type: d.typ, DisplayName: d.display, Description: d.desc,
		MetricKind: d.kind, ValueType: d.vtype, Unit: d.unit, LaunchStage: api.LaunchStage_GA,
		MonitoredResourceTypes: d.resources,
	}

	for _, l := range d.labels {
		out.Labels = append(out.Labels, &label.LabelDescriptor{Key: l, ValueType: label.LabelDescriptor_STRING})
	}

	return out
}

func builtinOf(typ string) (builtinDef, bool) {
	i := slices.IndexFunc(builtinDefs, func(d builtinDef) bool { return d.typ == typ })
	if i < 0 {
		return builtinDef{}, false
	}

	return builtinDefs[i], true
}

func writable(typ string) bool {
	return strings.HasPrefix(typ, prefixCustom) || strings.HasPrefix(typ, prefixExternal) ||
		strings.HasPrefix(typ, prefixWorkload) || strings.HasPrefix(typ, prefixPrometheus)
}

// lookupDescriptorLocked resolves a metric type to its descriptor: custom, built-in or log-based.
func (b *InMemoryBackend) lookupDescriptorLocked(
	project, typ string, logs []LogMetric,
) (*metricpb.MetricDescriptor, bool) {
	if d, ok := b.descriptors[descriptorName(project, typ)]; ok {
		return d, true
	}

	if d, ok := builtinOf(typ); ok {
		return builtinDescriptor(project, d), true
	}

	if short, ok := strings.CutPrefix(typ, userMetricPrefix); ok {
		for _, lm := range logs {
			if lm.Name == short {
				return logDescriptor(project, lm), true
			}
		}
	}

	return nil, false
}

func validateLabels(labels []*label.LabelDescriptor) error {
	if len(labels) > maxMetricLabels {
		return errInvalid("A metric can have at most %d labels.", maxMetricLabels)
	}

	seen := map[string]struct{}{}

	for _, l := range labels {
		if !labelKeyRe.MatchString(l.GetKey()) {
			return errInvalid("Invalid label key [%s].", l.GetKey())
		}

		if _, dup := seen[l.GetKey()]; dup {
			return errInvalid("Duplicate label key [%s].", l.GetKey())
		}

		seen[l.GetKey()] = struct{}{}
	}

	return nil
}

func validateKindType(kind metricpb.MetricDescriptor_MetricKind, vt metricpb.MetricDescriptor_ValueType) error {
	switch vt {
	case metricpb.MetricDescriptor_VALUE_TYPE_UNSPECIFIED, metricpb.MetricDescriptor_MONEY:
		return errInvalid("Field metric_descriptor.value_type had an invalid value: %s is not supported.", vt)
	case vtBool, vtString:
		if kind != kindGauge {
			return errInvalid("Value type %s requires metric kind GAUGE.", vt)
		}
	case vtInt64, vtDouble, vtDist:
	}

	return nil
}

func validateDescriptor(in *metricpb.MetricDescriptor) (*metricpb.MetricDescriptor, error) {
	typ := in.GetType()
	if typ == "" || len(typ) > maxMetricTypeLen || !metricTypeRe.MatchString(typ) {
		return nil, errInvalid("Field metric_descriptor.type had an invalid value of %q: invalid metric type.", typ)
	}

	if !writable(typ) {
		return nil, errInvalid("Field metric_descriptor.type had an invalid value of %q: "+
			"metric type must start with custom.googleapis.com/, external.googleapis.com/, "+
			"workload.googleapis.com/ or prometheus.googleapis.com/.", typ)
	}

	out := cloneDescriptor(in)
	if out.GetMetricKind() == metricpb.MetricDescriptor_METRIC_KIND_UNSPECIFIED {
		out.MetricKind = kindGauge
	}

	if err := validateKindType(out.GetMetricKind(), out.GetValueType()); err != nil {
		return nil, err
	}

	if err := validateLabels(out.GetLabels()); err != nil {
		return nil, err
	}

	out.LaunchStage = api.LaunchStage_GA
	out.MonitoredResourceTypes = nil

	return out, nil
}

// CreateMetricDescriptor stores a custom metric descriptor; an identical re-create returns the existing one.
func (b *InMemoryBackend) CreateMetricDescriptor(
	req *monitoringpb.CreateMetricDescriptorRequest,
) (*metricpb.MetricDescriptor, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, err
	}

	if req.GetMetricDescriptor() == nil {
		return nil, errInvalid("metric_descriptor is required.")
	}

	d, err := validateDescriptor(req.GetMetricDescriptor())
	if err != nil {
		return nil, err
	}

	d.Name = descriptorName(project, d.GetType())

	b.mu.Lock("CreateMetricDescriptor")
	defer b.mu.Unlock()

	if cur, ok := b.descriptors[d.GetName()]; ok {
		if proto.Equal(cur, d) {
			return cloneDescriptor(cur), nil
		}

		return nil, errExists("Metric descriptor %s already exists with a different definition.", d.GetType())
	}

	b.descriptors[d.GetName()] = d

	return cloneDescriptor(d), nil
}

// GetMetricDescriptor returns a custom, built-in or log-based descriptor.
func (b *InMemoryBackend) GetMetricDescriptor(name string) (*metricpb.MetricDescriptor, error) {
	project, typ, err := splitName(name, collMetricDescriptors)
	if err != nil {
		return nil, err
	}

	logs := b.logMetrics(project)

	b.mu.RLock("GetMetricDescriptor")
	defer b.mu.RUnlock()

	d, ok := b.lookupDescriptorLocked(project, typ, logs)
	if !ok {
		return nil, errNotFound("Metric descriptor %s does not exist.", name)
	}

	return cloneDescriptor(d), nil
}

// DeleteMetricDescriptor removes a custom descriptor and the series written with it.
func (b *InMemoryBackend) DeleteMetricDescriptor(name string) error {
	project, typ, err := splitName(name, collMetricDescriptors)
	if err != nil {
		return err
	}

	b.mu.Lock("DeleteMetricDescriptor")
	defer b.mu.Unlock()

	if _, ok := b.descriptors[descriptorName(project, typ)]; !ok {
		if _, builtin := builtinOf(typ); builtin || strings.HasPrefix(typ, userMetricPrefix) {
			return errInvalid("Metric descriptor %s is not a custom metric and cannot be deleted.", typ)
		}

		return errNotFound("Metric descriptor %s does not exist.", name)
	}

	delete(b.descriptors, descriptorName(project, typ))

	for key, s := range b.series {
		if s.project == project && s.metric.GetType() == typ {
			delete(b.series, key)
		}
	}

	return nil
}

// ListMetricDescriptors pages every descriptor of the project, narrowed by a metric.type filter.
func (b *InMemoryBackend) ListMetricDescriptors(
	req *monitoringpb.ListMetricDescriptorsRequest,
) ([]*metricpb.MetricDescriptor, string, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	f, err := parseFilter(req.GetFilter())
	if err != nil {
		return nil, "", err
	}

	logs := b.logMetrics(project)

	b.mu.RLock("ListMetricDescriptors")

	all := make([]*metricpb.MetricDescriptor, 0, len(builtinDefs)+len(b.descriptors)+len(logs))

	for _, d := range builtinDefs {
		all = append(all, builtinDescriptor(project, d))
	}

	for _, lm := range logs {
		all = append(all, logDescriptor(project, lm))
	}

	prefix := descriptorName(project, "")

	for name, d := range b.descriptors {
		if strings.HasPrefix(name, prefix) {
			all = append(all, cloneDescriptor(d))
		}
	}

	b.mu.RUnlock()

	all = slices.DeleteFunc(all, func(d *metricpb.MetricDescriptor) bool {
		return !f.matches(mapEnv{pathMetricType: {d.GetType()}, "project": {project}})
	})

	slices.SortFunc(all, func(a, c *metricpb.MetricDescriptor) int { return strings.Compare(a.GetType(), c.GetType()) })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, nil
}
