package gcpmonitoring

import (
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	distpb "google.golang.org/genproto/googleapis/api/distribution"
	"google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	maxSeriesPerRequest = 200
	maxPointAge         = 25 * time.Hour
	maxFutureSkew       = 5 * time.Minute
	pointRetention      = 42 * 24 * time.Hour
)

type writeMode int

const (
	modeUser writeMode = iota
	modeService
	modeSystem
)

type writeCtx struct {
	now     time.Time
	seen    map[string]struct{}
	project string
	mode    writeMode
}

func fieldErr(i int, path, val, msg string) error {
	return errInvalid("Field timeSeries[%d].%s had an invalid value of %q: %s", i, path, val, msg)
}

func seriesKey(project string, m *metricpb.Metric, r *monitoredres.MonitoredResource) string {
	var sb strings.Builder

	sb.WriteString(project)
	sb.WriteByte(0)
	sb.WriteString(m.GetType())
	writeLabels(&sb, m.GetLabels())
	sb.WriteByte(0)
	sb.WriteString(r.GetType())
	writeLabels(&sb, r.GetLabels())

	return sb.String()
}

func writeLabels(sb *strings.Builder, labels map[string]string) {
	keys := sortedKeys(labels)
	for _, k := range keys {
		sb.WriteByte(0)
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(labels[k])
	}
}

// CreateTimeSeries writes one point per series as a user; valid series are stored even when others fail.
func (b *InMemoryBackend) CreateTimeSeries(req *monitoringpb.CreateTimeSeriesRequest) error {
	return b.createTimeSeries(req, modeUser)
}

// CreateServiceTimeSeries writes to built-in service metrics as well as custom ones.
func (b *InMemoryBackend) CreateServiceTimeSeries(req *monitoringpb.CreateTimeSeriesRequest) error {
	return b.createTimeSeries(req, modeService)
}

func (b *InMemoryBackend) createTimeSeries(req *monitoringpb.CreateTimeSeriesRequest, mode writeMode) error {
	project, err := projectOf(req.GetName())
	if err != nil {
		return err
	}

	n := len(req.GetTimeSeries())
	if n == 0 {
		return errInvalid("Field timeSeries had an invalid value: at least one time series is required.")
	}

	if n > maxSeriesPerRequest {
		return errInvalid("Field timeSeries had an invalid value: at most %d time series per request.",
			maxSeriesPerRequest)
	}

	b.mu.Lock("CreateTimeSeries")
	defer b.mu.Unlock()

	w := &writeCtx{project: project, now: b.now(), mode: mode, seen: make(map[string]struct{}, n)}
	summary := &monitoringpb.CreateTimeSeriesSummary{TotalPointCount: int32(n)}

	var first error

	for i, ts := range req.GetTimeSeries() {
		if err = b.writeOneLocked(w, i, ts); err != nil {
			if first == nil {
				first = err
			}

			summary.Errors = append(summary.Errors, summaryError(err))

			continue
		}

		summary.SuccessPointCount++
	}

	if first == nil {
		return nil
	}

	return withSummary(first, summary)
}

func summaryError(err error) *monitoringpb.CreateTimeSeriesSummary_Error {
	st, _ := grpcstatus.FromError(err)

	return &monitoringpb.CreateTimeSeriesSummary_Error{Status: st.Proto(), PointCount: 1}
}

func withSummary(err error, summary *monitoringpb.CreateTimeSeriesSummary) error {
	st, _ := grpcstatus.FromError(err)

	withDetails, derr := st.WithDetails(summary)
	if derr != nil {
		return err
	}

	return withDetails.Err()
}

func (b *InMemoryBackend) writeOneLocked(w *writeCtx, i int, ts *monitoringpb.TimeSeries) error {
	if err := checkMetric(w, i, ts.GetMetric()); err != nil {
		return err
	}

	res, err := normalizeResource(w, i, ts.GetResource())
	if err != nil {
		return err
	}

	if len(ts.GetPoints()) != 1 {
		return errInvalid("Field timeSeries[%d].points had an invalid value: "+
			"Only one point can be written per TimeSeries per request.", i)
	}

	key := seriesKey(w.project, ts.GetMetric(), res)
	if _, dup := w.seen[key]; dup {
		return errInvalid("Field timeSeries[%d] had an invalid value: Duplicate TimeSeries encountered. "+
			"Only one point can be written per TimeSeries per request.", i)
	}

	w.seen[key] = struct{}{}
	pt := ts.GetPoints()[0]

	kind, vt, err := b.resolveTypesLocked(w, i, ts, pt)
	if err != nil {
		return err
	}

	if err = checkPoint(w, i, kind, vt, pt); err != nil {
		return err
	}

	rec, err := b.seriesForWriteLocked(key, w.project, ts, res, kind, vt)
	if err != nil {
		return err
	}

	if err = checkOrder(i, rec, pt); err != nil {
		return err
	}

	b.ensureDescriptorLocked(w.project, ts, kind, vt)
	rec.points = append(rec.points, clonePoint(pt))
	trimPoints(rec, w.now)

	return nil
}

func checkMetric(w *writeCtx, i int, m *metricpb.Metric) error {
	typ := m.GetType()
	if typ == "" || len(typ) > maxMetricTypeLen || !metricTypeRe.MatchString(typ) {
		return fieldErr(i, pathMetricType, typ, "invalid metric type.")
	}

	if w.mode == modeUser && !writable(typ) {
		return fieldErr(i, pathMetricType, typ, "The metric type is not writable by users; "+
			"custom metrics must start with custom.googleapis.com/ or external.googleapis.com/.")
	}

	if len(m.GetLabels()) > maxMetricLabels {
		return fieldErr(i, scopeMetricLabels, typ, "too many labels.")
	}

	for k, v := range m.GetLabels() {
		if !labelKeyRe.MatchString(k) {
			return fieldErr(i, scopeMetricLabels, k, "invalid label key.")
		}

		if len(v) > maxLabelValueLen {
			return fieldErr(i, "metric.labels."+k, "", "label value is too long.")
		}
	}

	return nil
}

func normalizeResource(w *writeCtx, i int, r *monitoredres.MonitoredResource) (*monitoredres.MonitoredResource, error) {
	def, ok := catalogDef(r.GetType())
	if !ok {
		return nil, fieldErr(i, pathResourceTyp, r.GetType(), "Unrecognized resource type.")
	}

	labels := make(map[string]string, len(def.labels))
	for k, v := range r.GetLabels() {
		if !containsStr(def.labels, k) {
			return nil, fieldErr(i, scopeResourceLabels, k, "Unrecognized resource label.")
		}

		labels[k] = v
	}

	if _, has := labels[labelProject]; !has && containsStr(def.labels, labelProject) {
		labels[labelProject] = w.project
	}

	for _, l := range def.labels {
		if _, has := labels[l]; !has {
			return nil, fieldErr(i, scopeResourceLabels, l, "Missing required resource label.")
		}
	}

	return &monitoredres.MonitoredResource{Type: def.typ, Labels: labels}, nil
}

// resolveTypesLocked determines the metric kind and value type from the request and any existing descriptor.
func (b *InMemoryBackend) resolveTypesLocked(
	w *writeCtx, i int, ts *monitoringpb.TimeSeries, pt *monitoringpb.Point,
) (metricpb.MetricDescriptor_MetricKind, metricpb.MetricDescriptor_ValueType, error) {
	kind, vt := ts.GetMetricKind(), ts.GetValueType()

	if d, ok := b.lookupDescriptorLocked(w.project, ts.GetMetric().GetType(), nil); ok {
		if kind == metricpb.MetricDescriptor_METRIC_KIND_UNSPECIFIED {
			kind = d.GetMetricKind()
		}

		if vt == metricpb.MetricDescriptor_VALUE_TYPE_UNSPECIFIED {
			vt = d.GetValueType()
		}

		if kind != d.GetMetricKind() {
			return 0, 0, fieldErr(i, "metric_kind", kind.String(),
				"Metric kind does not match the metric descriptor ("+d.GetMetricKind().String()+").")
		}

		if vt != d.GetValueType() {
			return 0, 0, fieldErr(i, "value_type", vt.String(),
				"Value type does not match the metric descriptor ("+d.GetValueType().String()+").")
		}
	}

	if kind == metricpb.MetricDescriptor_METRIC_KIND_UNSPECIFIED {
		kind = kindGauge
	}

	if vt == metricpb.MetricDescriptor_VALUE_TYPE_UNSPECIFIED {
		vt = valueTypeOf(pt.GetValue())
	}

	if err := validateKindType(kind, vt); err != nil {
		return 0, 0, fieldErr(i, "value_type", vt.String(), "value type is not valid for the metric kind.")
	}

	return kind, vt, nil
}

func checkPoint(
	w *writeCtx, i int, kind metricpb.MetricDescriptor_MetricKind,
	vt metricpb.MetricDescriptor_ValueType, pt *monitoringpb.Point,
) error {
	if valueTypeOf(pt.GetValue()) != vt {
		return errInvalid("Field timeSeries[%d].points[0].value had an invalid value: "+
			"Value type for metric must be %s, but is %s.", i, vt, valueTypeOf(pt.GetValue()))
	}

	if vt == vtDist {
		if err := checkDistribution(i, pt.GetValue().GetDistributionValue()); err != nil {
			return err
		}
	}

	return checkInterval(w, i, kind, pt.GetInterval())
}

func checkInterval(w *writeCtx, i int, kind metricpb.MetricDescriptor_MetricKind, iv *monitoringpb.TimeInterval) error {
	if iv.GetEndTime() == nil {
		return errInvalid("Field timeSeries[%d].points[0].interval.end_time had an invalid value: "+
			"Interval end time is required.", i)
	}

	end := iv.GetEndTime().AsTime()
	if w.mode != modeSystem && end.Before(w.now.Add(-maxPointAge)) {
		return errInvalid("Field timeSeries[%d].points[0].interval.end_time had an invalid value of %q: "+
			"Timestamp is too old.", i, end.Format(time.RFC3339))
	}

	if end.After(w.now.Add(maxFutureSkew)) {
		return errInvalid("Field timeSeries[%d].points[0].interval.end_time had an invalid value of %q: "+
			"Timestamp is in the future.", i, end.Format(time.RFC3339))
	}

	if kind == kindGauge {
		if iv.GetStartTime() != nil && !iv.GetStartTime().AsTime().Equal(end) {
			return errInvalid("Field timeSeries[%d].points[0].interval.start_time had an invalid value: "+
				"The start time must be equal to the end time for a GAUGE metric.", i)
		}

		return nil
	}

	if iv.GetStartTime() == nil || !iv.GetStartTime().AsTime().Before(end) {
		return errInvalid("Field timeSeries[%d].points[0].interval.start_time had an invalid value: "+
			"The start time must be before the end time for a %s metric.", i, kind)
	}

	return nil
}

func checkOrder(i int, rec *seriesRec, pt *monitoringpb.Point) error {
	if len(rec.points) == 0 {
		return nil
	}

	prev := rec.points[len(rec.points)-1]
	if !endOf(pt).After(endOf(prev)) {
		return errInvalid("Field timeSeries[%d].points[0].interval.end_time had an invalid value: "+
			"Points must be written in order. One or more of the points specified had an older "+
			"end time than the most recent point.", i)
	}

	if rec.kind == kindCumulative && !startOf(pt).Equal(startOf(prev)) && startOf(pt).Before(endOf(prev)) {
		return errInvalid("Field timeSeries[%d].points[0].interval.start_time had an invalid value: "+
			"Cumulative start time must stay the same or be later than the previous end time.", i)
	}

	return nil
}

func (b *InMemoryBackend) seriesForWriteLocked(
	key, project string, ts *monitoringpb.TimeSeries, res *monitoredres.MonitoredResource,
	kind metricpb.MetricDescriptor_MetricKind, vt metricpb.MetricDescriptor_ValueType,
) (*seriesRec, error) {
	if rec, ok := b.series[key]; ok {
		return rec, nil
	}

	if len(b.series) >= maxSeriesTotal {
		return nil, errInvalid("Too many time series; the emulator holds at most %d.", maxSeriesTotal)
	}

	m, _ := proto.Clone(ts.GetMetric()).(*metricpb.Metric)
	rec := &seriesRec{
		project: project, metric: m, resource: res, kind: kind, vtype: vt, unit: ts.GetUnit(),
	}

	if ts.GetMetadata() != nil {
		rec.metadata, _ = proto.Clone(ts.GetMetadata()).(*monitoredres.MonitoredResourceMetadata)
	}

	b.series[key] = rec

	return rec, nil
}

// ensureDescriptorLocked auto-creates the descriptor of a custom metric and extends its labels.
func (b *InMemoryBackend) ensureDescriptorLocked(
	project string, ts *monitoringpb.TimeSeries,
	kind metricpb.MetricDescriptor_MetricKind, vt metricpb.MetricDescriptor_ValueType,
) {
	typ := ts.GetMetric().GetType()
	name := descriptorName(project, typ)

	d, ok := b.descriptors[name]
	if !ok {
		if _, builtin := builtinOf(typ); builtin || !writable(typ) {
			return
		}

		d = &metricpb.MetricDescriptor{
			Name: name, Type: typ, MetricKind: kind, ValueType: vt, Unit: ts.GetUnit(),
		}
		b.descriptors[name] = d
	}

	have := map[string]struct{}{}
	for _, l := range d.GetLabels() {
		have[l.GetKey()] = struct{}{}
	}

	keys := sortedKeys(ts.GetMetric().GetLabels())
	sort.Strings(keys)

	for _, k := range keys {
		if _, dup := have[k]; !dup {
			d.Labels = append(d.Labels, &label.LabelDescriptor{Key: k, ValueType: label.LabelDescriptor_STRING})
		}
	}
}

func trimPoints(rec *seriesRec, now time.Time) {
	cutoff := now.Add(-pointRetention)
	drop := 0

	for drop < len(rec.points)-1 && endOf(rec.points[drop]).Before(cutoff) {
		drop++
	}

	if over := len(rec.points) - maxPointsPerSeries; over > drop {
		drop = over
	}

	if drop > 0 {
		rec.points = append([]*monitoringpb.Point(nil), rec.points[drop:]...)
	}
}

// checkDistribution verifies bucket counts agree with the bucket options and the sample count.
func checkDistribution(i int, d *distpb.Distribution) error {
	if d == nil {
		return errInvalid("Field timeSeries[%d].points[0].value had an invalid value: distribution is required.", i)
	}

	var total int64
	for _, c := range d.GetBucketCounts() {
		if c < 0 {
			return errInvalid("Field timeSeries[%d].points[0].value.distribution_value.bucket_counts "+
				"had an invalid value: counts must not be negative.", i)
		}

		total += c
	}

	if want := bucketCount(d); want >= 0 && len(d.GetBucketCounts()) != want {
		return errInvalid("Field timeSeries[%d].points[0].value.distribution_value.bucket_counts "+
			"had an invalid value: expected %d buckets, got %d.", i, want, len(d.GetBucketCounts()))
	}

	if len(d.GetBucketCounts()) > 0 && total != d.GetCount() {
		return errInvalid("Field timeSeries[%d].points[0].value.distribution_value.count "+
			"had an invalid value: count must equal the sum of bucket_counts.", i)
	}

	return nil
}

func clonePoint(p *monitoringpb.Point) *monitoringpb.Point {
	out, _ := proto.Clone(p).(*monitoringpb.Point)

	return out
}
