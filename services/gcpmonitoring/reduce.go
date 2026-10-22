package gcpmonitoring

import (
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
)

// groupField validates a group_by_fields entry and returns its (scope, key).
func groupField(f string) ([2]string, bool) {
	switch {
	case f == pathMetricType || f == pathResourceTyp:
		return [2]string{f, ""}, true
	case strings.HasPrefix(f, "metric.labels.") && len(f) > len("metric.labels."):
		return [2]string{scopeMetricLabels, strings.TrimPrefix(f, "metric.labels.")}, true
	case strings.HasPrefix(f, "resource.labels.") && len(f) > len("resource.labels."):
		return [2]string{scopeResourceLabels, strings.TrimPrefix(f, "resource.labels.")}, true
	}

	return [2]string{}, false
}

func fieldValue(s *aSeries, f string) string {
	sc, _ := groupField(f)

	switch sc[0] {
	case pathMetricType:
		return s.metric.GetType()
	case pathResourceTyp:
		return s.resource.GetType()
	case scopeMetricLabels:
		return s.metric.GetLabels()[sc[1]]
	default:
		return s.resource.GetLabels()[sc[1]]
	}
}

func groupKey(s *aSeries, fields []string) string {
	parts := make([]string, len(fields))
	for i, f := range fields {
		parts[i] = fieldValue(s, f)
	}

	return strings.Join(parts, "\x00")
}

// reduceSeries combines aligned series that share the group_by_fields values into one series per group.
func reduceSeries(in []*aSeries, agg *monitoringpb.Aggregation) ([]*aSeries, error) {
	r := agg.GetCrossSeriesReducer()
	if r == reduceNone {
		return in, nil
	}

	groups := map[string][]*aSeries{}

	var order []string

	for _, s := range in {
		if !reducerApplies(r, s.vtype) {
			return nil, errInvalid("Field aggregation.cross_series_reducer had an invalid value of %q: "+
				"The reducer cannot be applied to metrics with value type %s.", r, s.vtype)
		}

		k := groupKey(s, agg.GetGroupByFields())
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}

		groups[k] = append(groups[k], s)
	}

	out := make([]*aSeries, 0, len(order))

	for _, k := range order {
		out = append(out, reduceGroup(groups[k], agg))
	}

	return out, nil
}

func reduceGroup(members []*aSeries, agg *monitoringpb.Aggregation) *aSeries {
	first := members[0]
	vt := reducedType(agg.GetCrossSeriesReducer(), first.vtype)
	out := &aSeries{
		metric: &metricpb.Metric{Type: first.metric.GetType(), Labels: map[string]string{}},
		resource: &monitoredres.MonitoredResource{
			Type: commonResourceType(members), Labels: map[string]string{},
		},
		unit: first.unit, kind: first.kind, vtype: vt,
	}

	for _, f := range agg.GetGroupByFields() {
		sc, _ := groupField(f)

		switch sc[0] {
		case scopeMetricLabels:
			out.metric.Labels[sc[1]] = fieldValue(first, f)
		case scopeResourceLabels:
			out.resource.Labels[sc[1]] = fieldValue(first, f)
		}
	}

	byTime := map[int64][]*monitoringpb.TypedValue{}
	starts := map[int64]time.Time{}

	for _, m := range members {
		for _, p := range m.pts {
			t := endOf(p).UnixNano()
			byTime[t] = append(byTime[t], p.GetValue())
			starts[t] = startOf(p)
		}
	}

	times := make([]int64, 0, len(byTime))
	for t := range byTime {
		times = append(times, t)
	}

	slices.Sort(times)

	for _, t := range times {
		if v, ok := reduceValues(agg.GetCrossSeriesReducer(), byTime[t], first.vtype); ok {
			out.pts = append(out.pts, pointAt(starts[t], time.Unix(0, t).UTC(), v))
		}
	}

	return out
}

func commonResourceType(members []*aSeries) string {
	t := members[0].resource.GetType()
	for _, m := range members[1:] {
		if m.resource.GetType() != t {
			return ""
		}
	}

	return t
}

func reducedType(r reducer, vt metricpb.MetricDescriptor_ValueType) metricpb.MetricDescriptor_ValueType {
	switch r {
	case reduceMean, reduceStddev, reduceFracTrue:
		return vtDouble
	case reduceCount, reduceCountTrue, reduceCountFalse:
		return vtInt64
	default:
		return vt
	}
}
