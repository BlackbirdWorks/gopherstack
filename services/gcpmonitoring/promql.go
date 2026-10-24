package gcpmonitoring

import (
	"maps"
	"math"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	distpb "google.golang.org/genproto/googleapis/api/distribution"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	promMetricPrefix = "prometheus.googleapis.com/"
	labelMonitoredRs = "monitored_resource"
	labelLE          = "le"
	suffixSum        = "_sum"
	suffixCount      = "_count"
	suffixBucket     = "_bucket"
)

var promNameRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// promMetricName maps a Cloud Monitoring metric type to its Managed Service for Prometheus name:
// prometheus.googleapis.com/NAME/TYPE is NAME; DOMAIN/PATH becomes DOMAIN:PATH with dots and slashes as underscores.
func promMetricName(typ string) string {
	if rest, ok := strings.CutPrefix(typ, promMetricPrefix); ok {
		if i := strings.LastIndex(rest, "/"); i > 0 {
			return rest[:i]
		}

		return rest
	}

	domain, path, ok := strings.Cut(typ, "/")
	if !ok {
		return promNameRe.ReplaceAllString(typ, "_")
	}

	return promNameRe.ReplaceAllString(domain, "_") + ":" + promNameRe.ReplaceAllString(path, "_")
}

func promLabels(rec *seriesRec, name string) promql.Labels {
	l := promql.Labels{}

	maps.Copy(l, rec.resource.GetLabels())

	if t := rec.resource.GetType(); t != "" {
		l[labelMonitoredRs] = t
	}

	maps.Copy(l, rec.metric.GetLabels())

	l[promql.NameLabel] = name

	return l
}

func formatLE(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}

	return strconv.FormatFloat(v, 'f', -1, 64)
}

// distState accumulates a distribution series so DELTA points read as cumulative counters.
type distState struct {
	buckets []int64
	count   int64
	sum     float64
}

func (s *distState) add(d *distpb.Distribution, accumulate bool) {
	if !accumulate {
		s.buckets, s.count, s.sum = nil, 0, 0
	}

	if len(s.buckets) < len(d.GetBucketCounts()) {
		s.buckets = append(s.buckets, make([]int64, len(d.GetBucketCounts())-len(s.buckets))...)
	}

	for i, c := range d.GetBucketCounts() {
		s.buckets[i] += c
	}

	s.count += d.GetCount()
	s.sum += d.GetMean() * float64(d.GetCount())
}

func inWindow(t, minT, maxT int64) bool { return t >= minT && t <= maxT }

// promSeriesOf converts one stored series to Prometheus series; distributions expand to _sum, _count and _bucket.
func promSeriesOf(rec *seriesRec, minT, maxT int64) []promql.Series {
	base := promMetricName(rec.metric.GetType())
	acc := rec.kind == kindDelta

	if rec.vtype == vtDist {
		return promDistSeries(rec, base, acc, minT, maxT)
	}

	if rec.vtype == vtString {
		return nil
	}

	s := promql.Series{Labels: promLabels(rec, base)}
	run := 0.0

	for _, p := range rec.points {
		v := number(p.GetValue())
		if acc {
			run += v
			v = run
		}

		if t := endOf(p).UnixMilli(); inWindow(t, minT, maxT) {
			s.Samples = append(s.Samples, promql.Sample{T: t, V: v})
		}
	}

	return []promql.Series{s}
}

func promDistSeries(rec *seriesRec, base string, acc bool, minT, maxT int64) []promql.Series {
	sum := promql.Series{Labels: promLabels(rec, base+suffixSum)}
	cnt := promql.Series{Labels: promLabels(rec, base+suffixCount)}
	buckets := map[int]*promql.Series{}

	var (
		st    distState
		bound []float64
	)

	for _, p := range rec.points {
		d := p.GetValue().GetDistributionValue()
		st.add(d, acc)

		t := endOf(p).UnixMilli()
		if !inWindow(t, minT, maxT) {
			continue
		}

		sum.Samples = append(sum.Samples, promql.Sample{T: t, V: st.sum})
		cnt.Samples = append(cnt.Samples, promql.Sample{T: t, V: float64(st.count)})

		bound = bound[:0]
		for i := range d.GetBucketCounts() {
			_, upper := bucketBounds(d, i)
			if i == len(d.GetBucketCounts())-1 {
				upper = math.Inf(1)
			}

			bound = append(bound, upper)
		}

		running := int64(0)

		for i := range st.buckets {
			running += st.buckets[i]

			if i >= len(bound) {
				break
			}

			b, ok := buckets[i]
			if !ok {
				l := promLabels(rec, base+suffixBucket)
				l[labelLE] = formatLE(bound[i])
				b = &promql.Series{Labels: l}
				buckets[i] = b
			}

			b.Samples = append(b.Samples, promql.Sample{T: t, V: float64(running)})
		}
	}

	out := []promql.Series{sum, cnt}
	for i := range len(buckets) {
		if b, ok := buckets[i]; ok {
			out = append(out, *b)
		}
	}

	return out
}

// promStorage serves the stored series of one project to the PromQL engine.
type promStorage struct {
	series []*seriesRec
}

func (s promStorage) Select(ms []*promql.Matcher, minT, maxT int64) []promql.Series {
	var out []promql.Series

	for _, rec := range s.series {
		for _, ps := range promSeriesOf(rec, minT, maxT) {
			if promql.MatchesAll(ps.Labels, ms) && len(ps.Samples) > 0 {
				out = append(out, ps)
			}
		}
	}

	return out
}

// promStorageFor snapshots the project's series, including log-based metrics, for a query.
func (b *InMemoryBackend) promStorageFor(project string) promStorage {
	extra := b.logSeries(project, b.logMetrics(project), b.currentTime(), 0)

	b.mu.RLock("promStorageFor")
	defer b.mu.RUnlock()

	var recs []*seriesRec

	for _, rec := range b.series {
		if rec.project == project {
			recs = append(recs, copySeriesRec(rec))
		}
	}

	return promStorage{series: append(recs, extra...)}
}

func copySeriesRec(r *seriesRec) *seriesRec {
	out := *r
	out.points = append([]*monitoringpb.Point(nil), r.points...)

	return &out
}
