package gcpmonitoring

import (
	"slices"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	pathMetricType  = "metric.type"
	pathResourceTyp = "resource.type"
	prefixMetricLbl = "metric.labels."
	prefixResLbl    = "resource.labels."
	prefixSysLbl    = "metadata.system_labels."
	prefixUserLbl   = "metadata.user_labels."
	pathGroupID     = "group.id"
	pathProject     = "project"
)

// query describes one read of the time series store.
type query struct {
	win     window
	filter  *filter
	agg     *monitoringpb.Aggregation
	sec     *monitoringpb.Aggregation
	project string
	extra   []*seriesRec
}

// seriesEnv resolves filter fields of a series.
type seriesEnv struct {
	rec    *seriesRec
	groups func(*seriesRec) []string
}

func (e seriesEnv) values(path string) []string {
	switch {
	case path == pathMetricType:
		return []string{e.rec.metric.GetType()}
	case path == pathResourceTyp:
		return []string{e.rec.resource.GetType()}
	case path == pathProject:
		return []string{e.rec.project}
	case path == pathGroupID:
		return e.groups(e.rec)
	case strings.HasPrefix(path, prefixMetricLbl):
		return one(e.rec.metric.GetLabels(), strings.TrimPrefix(path, prefixMetricLbl))
	case strings.HasPrefix(path, prefixResLbl):
		return one(e.rec.resource.GetLabels(), strings.TrimPrefix(path, prefixResLbl))
	case strings.HasPrefix(path, prefixUserLbl):
		return one(e.rec.metadata.GetUserLabels(), strings.TrimPrefix(path, prefixUserLbl))
	case strings.HasPrefix(path, prefixSysLbl):
		return systemLabel(e.rec.metadata, strings.TrimPrefix(path, prefixSysLbl))
	}

	return nil
}

func one(m map[string]string, key string) []string {
	if v, ok := m[key]; ok {
		return []string{v}
	}

	return nil
}

func systemLabel(md *monitoredres.MonitoredResourceMetadata, key string) []string {
	if v, ok := md.GetSystemLabels().GetFields()[key]; ok {
		return []string{v.GetStringValue()}
	}

	return nil
}

// ListTimeSeries returns the series matching the filter with aggregation applied, newest point first.
func (b *InMemoryBackend) ListTimeSeries(
	req *monitoringpb.ListTimeSeriesRequest,
) (*monitoringpb.ListTimeSeriesResponse, error) {
	q, err := b.prepareQuery(req)
	if err != nil {
		return nil, err
	}

	if mayMatchLogMetrics(req.GetFilter()) {
		q.extra = b.logSeries(q.project, b.logMetrics(q.project), b.currentTime(), 0)
	}

	b.mu.RLock("ListTimeSeries")
	series, err := b.selectLocked(q)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, err
	}

	out := make([]*monitoringpb.TimeSeries, len(series))
	for i, s := range series {
		out[i] = s.toProto(req.GetView() != monitoringpb.ListTimeSeriesRequest_HEADERS)
	}

	pg := page.New(out, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)
	resp := &monitoringpb.ListTimeSeriesResponse{TimeSeries: pg.Data, NextPageToken: pg.Next}

	if len(series) > 0 {
		resp.Unit = series[0].unit
	}

	return resp, nil
}

func (b *InMemoryBackend) currentTime() time.Time {
	b.mu.RLock("now")
	defer b.mu.RUnlock()

	return b.now()
}

func (b *InMemoryBackend) prepareQuery(req *monitoringpb.ListTimeSeriesRequest) (query, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return query{}, err
	}

	if req.GetFilter() == "" {
		return query{}, errInvalid(
			"Field filter had an invalid value: a filter that specifies metric.type is required.",
		)
	}

	f, err := parseFilter(req.GetFilter())
	if err != nil {
		return query{}, err
	}

	if _, ok := f.referenced()[pathMetricType]; !ok {
		return query{}, errInvalid("The supplied filter does not specify a valid combination of metric and " +
			"monitored resource descriptors. The filter must specify metric.type.")
	}

	win, err := windowOf(req.GetInterval())
	if err != nil {
		return query{}, err
	}

	if err = validateAggregation(req.GetAggregation(), "aggregation"); err != nil {
		return query{}, err
	}

	if err = validateAggregation(req.GetSecondaryAggregation(), "secondary_aggregation"); err != nil {
		return query{}, err
	}

	return query{
		project: project, filter: f, win: win,
		agg: req.GetAggregation(), sec: req.GetSecondaryAggregation(),
	}, nil
}

func windowOf(iv *monitoringpb.TimeInterval) (window, error) {
	if iv == nil || iv.GetEndTime() == nil {
		return window{}, errInvalid("Field interval.end_time had an invalid value: the interval end time is required.")
	}

	end := iv.GetEndTime().AsTime()
	start := end

	if iv.GetStartTime() != nil {
		start = iv.GetStartTime().AsTime()
	}

	if start.After(end) {
		return window{}, errInvalid("Field interval.start_time had an invalid value: " +
			"the start time must not be later than the end time.")
	}

	return window{start: start, end: end}, nil
}

// selectLocked runs filter, interval selection and both aggregation stages; callers hold a read lock.
func (b *InMemoryBackend) selectLocked(q query) ([]*aSeries, error) {
	env := b.groupResolverLocked(q.project)
	aligning := q.agg.GetPerSeriesAligner() != alignNone

	var picked []*aSeries

	visit := func(rec *seriesRec) error {
		if rec.project != q.project || !q.filter.matches(seriesEnv{rec: rec, groups: env}) {
			return nil
		}

		s := rec.view(q.win, aligning)
		if len(s.pts) == 0 {
			return nil
		}

		if aligning {
			var err error
			if s, err = alignSeries(s, q.agg, q.win); err != nil {
				return err
			}
		}

		picked = append(picked, s)

		return nil
	}

	for _, rec := range b.series {
		if err := visit(rec); err != nil {
			return nil, err
		}
	}

	for _, rec := range q.extra {
		if err := visit(rec); err != nil {
			return nil, err
		}
	}

	return finishSeries(picked, q)
}

func finishSeries(picked []*aSeries, q query) ([]*aSeries, error) {
	out, err := reduceSeries(picked, q.agg)
	if err != nil {
		return nil, err
	}

	if q.sec.GetPerSeriesAligner() != alignNone {
		out, err = secondPass(out, q)
		if err != nil {
			return nil, err
		}
	}

	out = slices.DeleteFunc(out, func(s *aSeries) bool { return len(s.pts) == 0 })
	sort.SliceStable(out, func(i, j int) bool { return seriesID(out[i]) < seriesID(out[j]) })

	return out, nil
}

func secondPass(in []*aSeries, q query) ([]*aSeries, error) {
	out := make([]*aSeries, 0, len(in))

	for _, s := range in {
		a, err := alignSeries(s, q.sec, q.win)
		if err != nil {
			return nil, err
		}

		out = append(out, a)
	}

	return reduceSeries(out, q.sec)
}

func seriesID(s *aSeries) string {
	var sb strings.Builder

	sb.WriteString(s.metric.GetType())
	writeLabels(&sb, s.metric.GetLabels())
	sb.WriteByte(0)
	sb.WriteString(s.resource.GetType())
	writeLabels(&sb, s.resource.GetLabels())

	return sb.String()
}

// view returns the series restricted to the window; when aligning it keeps every earlier point for delta baselines.
func (r *seriesRec) view(w window, upToEnd bool) *aSeries {
	s := &aSeries{
		metric: r.metric, resource: r.resource, metadata: r.metadata, unit: r.unit, kind: r.kind, vtype: r.vtype,
	}

	for _, p := range r.points {
		t := endOf(p)

		switch {
		case upToEnd && !t.After(w.end):
			s.pts = append(s.pts, p)
		case !upToEnd && w.contains(t):
			s.pts = append(s.pts, p)
		}
	}

	return s
}

func (s *aSeries) toProto(withPoints bool) *monitoringpb.TimeSeries {
	ts := &monitoringpb.TimeSeries{
		Metric: s.metric, Resource: s.resource, Metadata: s.metadata, MetricKind: s.kind, ValueType: s.vtype,
		Unit: s.unit,
	}

	if withPoints {
		for i := range slices.Backward(s.pts) {
			ts.Points = append(ts.Points, s.pts[i])
		}
	}

	out, _ := proto.Clone(ts).(*monitoringpb.TimeSeries)

	return out
}
