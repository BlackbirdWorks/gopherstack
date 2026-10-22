package gcpmonitoring

import (
	"math"
	"sort"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
)

// aSeries is a time series being aggregated; points are in ascending end-time order.
type aSeries struct {
	metric   *metricpb.Metric
	resource *monitoredres.MonitoredResource
	metadata *monitoredres.MonitoredResourceMetadata
	unit     string
	pts      []*monitoringpb.Point
	kind     metricpb.MetricDescriptor_MetricKind
	vtype    metricpb.MetricDescriptor_ValueType
}

type window struct {
	start, end time.Time
}

func (w window) contains(t time.Time) bool {
	if w.start.Equal(w.end) {
		return t.Equal(w.end)
	}

	return t.After(w.start) && !t.After(w.end)
}

func bucketsFor(w window, period time.Duration) int {
	span := w.end.Sub(w.start)
	if span <= 0 {
		return 1
	}

	return min(int((span+period-1)/period), maxAlignBuckets)
}

// alignSeries applies the per-series aligner over fixed-width buckets ending at the interval end.
func alignSeries(s *aSeries, agg *monitoringpb.Aggregation, w window) (*aSeries, error) {
	a := agg.GetPerSeriesAligner()
	if !alignerApplies(a, s.kind, s.vtype) {
		return nil, errInvalid("Field aggregation.per_series_aligner had an invalid value of %q: "+
			"The aligner cannot be applied to metrics with kind %s and value type %s.", a, s.kind, s.vtype)
	}

	k, vt := outputTypes(a, s.kind, s.vtype)
	out := &aSeries{metric: s.metric, resource: s.resource, metadata: s.metadata, unit: s.unit, kind: k, vtype: vt}
	period := agg.GetAlignmentPeriod().AsDuration()
	n := bucketsFor(w, period)

	for i := n - 1; i >= 0; i-- {
		bEnd := w.end.Add(-time.Duration(i) * period)
		bStart := bEnd.Add(-period)

		lo := sort.Search(len(s.pts), func(j int) bool { return endOf(s.pts[j]).After(bStart) })
		hi := sort.Search(len(s.pts), func(j int) bool { return endOf(s.pts[j]).After(bEnd) })

		v, ok := alignBucket(a, s, lo, hi, bEnd, period)
		if !ok {
			continue
		}

		start := bEnd

		switch k {
		case kindDelta:
			start = bStart
		case kindCumulative:
			if hi > 0 {
				start = startOf(s.pts[hi-1])
			}
		case kindGauge, metricpb.MetricDescriptor_METRIC_KIND_UNSPECIFIED:
		}

		out.pts = append(out.pts, pointAt(start, bEnd, v))
	}

	return out, nil
}

func alignBucket(
	a aligner, s *aSeries, lo, hi int, bEnd time.Time, period time.Duration,
) (*monitoringpb.TypedValue, bool) {
	in := s.pts[lo:hi]

	if a == alignNextOlder {
		return nextOlder(s, lo, hi, bEnd)
	}

	if a == alignDelta || a == alignRate {
		return deltaOrRate(a, s, lo, hi, period)
	}

	if len(in) == 0 {
		return nil, false
	}

	if p, isP := percentileOf(a); isP {
		return percentileOfBucket(in, p)
	}

	if s.vtype == vtDist {
		return distMean(in)
	}

	return reduceValues(reductionOf(a), pointValues(in), s.vtype)
}

func nextOlder(s *aSeries, lo, hi int, bEnd time.Time) (*monitoringpb.TypedValue, bool) {
	if hi > lo {
		return s.pts[hi-1].GetValue(), true
	}

	if lo > 0 && bEnd.Sub(endOf(s.pts[lo-1])) <= staleness {
		return s.pts[lo-1].GetValue(), true
	}

	return nil, false
}

// deltaOrRate computes the change over the bucket, and divides by the period for a rate.
func deltaOrRate(a aligner, s *aSeries, lo, hi int, period time.Duration) (*monitoringpb.TypedValue, bool) {
	if hi <= lo {
		return nil, false
	}

	var delta float64

	if s.kind == kindDelta {
		for _, p := range s.pts[lo:hi] {
			f := number(p.GetValue())
			delta += f
		}
	} else {
		last := number(s.pts[hi-1].GetValue())
		base := 0.0

		if lo > 0 && startOf(s.pts[lo-1]).Equal(startOf(s.pts[hi-1])) {
			base = number(s.pts[lo-1].GetValue())
		}

		delta = math.Max(last-base, 0)
	}

	if a == alignRate {
		return doubleValue(delta / period.Seconds()), true
	}

	return numberOf(s.vtype, delta), true
}

func percentileOfBucket(in []*monitoringpb.Point, p float64) (*monitoringpb.TypedValue, bool) {
	d := in[len(in)-1].GetValue().GetDistributionValue()

	v, ok := percentile(d, p)
	if !ok {
		return nil, false
	}

	return doubleValue(v), true
}

func distMean(in []*monitoringpb.Point) (*monitoringpb.TypedValue, bool) {
	var count, sum float64

	for _, p := range in {
		d := p.GetValue().GetDistributionValue()
		count += float64(d.GetCount())
		sum += d.GetMean() * float64(d.GetCount())
	}

	if count == 0 {
		return nil, false
	}

	return doubleValue(sum / count), true
}

func pointValues(in []*monitoringpb.Point) []*monitoringpb.TypedValue {
	out := make([]*monitoringpb.TypedValue, len(in))
	for i, p := range in {
		out[i] = p.GetValue()
	}

	return out
}

// reductionOf maps a per-series aligner to the equivalent cross-series reduction.
func reductionOf(a aligner) reducer {
	switch a {
	case alignMin:
		return reduceMin
	case alignMax:
		return reduceMax
	case alignMean:
		return reduceMean
	case alignSum:
		return reduceSum
	case alignStddev:
		return reduceStddev
	case alignCountTrue:
		return reduceCountTrue
	case alignCountFalse:
		return reduceCountFalse
	case alignFracTrue:
		return reduceFracTrue
	default:
		return reduceCount
	}
}

// reduceValues folds a set of values into one according to the reducer.
func reduceValues(
	r reducer, vals []*monitoringpb.TypedValue, vt metricpb.MetricDescriptor_ValueType,
) (*monitoringpb.TypedValue, bool) {
	if len(vals) == 0 {
		return nil, false
	}

	nums := make([]float64, len(vals))

	var trues int64

	for i, v := range vals {
		nums[i] = number(v)

		if nums[i] != 0 {
			trues++
		}
	}

	total := int64(len(vals))

	switch r {
	case reduceCount:
		return intValue(total), true
	case reduceCountTrue:
		return intValue(trues), true
	case reduceCountFalse:
		return intValue(total - trues), true
	case reduceFracTrue:
		return doubleValue(float64(trues) / float64(total)), true
	case reduceMean:
		return doubleValue(sum(nums) / float64(total)), true
	case reduceStddev:
		return doubleValue(stddev(nums)), true
	case reduceMin:
		return numberOf(vt, slicesMin(nums)), true
	case reduceMax:
		return numberOf(vt, slicesMax(nums)), true
	case reduceSum:
		return numberOf(vt, sum(nums)), true
	default:
		return nil, false
	}
}

// minStddevSamples is the fewest values with a sample deviation.
const minStddevSamples = 2

func sum(xs []float64) float64 {
	var t float64
	for _, x := range xs {
		t += x
	}

	return t
}

func slicesMin(xs []float64) float64 { return slicesFold(xs, math.Min) }
func slicesMax(xs []float64) float64 { return slicesFold(xs, math.Max) }

func slicesFold(xs []float64, f func(a, b float64) float64) float64 {
	m := xs[0]
	for _, x := range xs[1:] {
		m = f(m, x)
	}

	return m
}

// stddev is the sample standard deviation; a single sample has none.
func stddev(xs []float64) float64 {
	if len(xs) < minStddevSamples {
		return 0
	}

	mean := sum(xs) / float64(len(xs))

	var ss float64
	for _, x := range xs {
		ss += (x - mean) * (x - mean)
	}

	return math.Sqrt(ss / float64(len(xs)-1))
}
