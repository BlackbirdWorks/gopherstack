package gcpmonitoring

import (
	"cmp"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	distpb "google.golang.org/genproto/googleapis/api/distribution"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

type promExemplar struct {
	Labels    promql.Labels `json:"labels"`
	Value     string        `json:"value"`
	Timestamp float64       `json:"timestamp"`
}

type promExemplarSeries struct {
	SeriesLabels promql.Labels  `json:"seriesLabels"`
	Exemplars    []promExemplar `json:"exemplars"`
}

// exemplarLabels rebuilds the exemplar labels from its attachments: a SpanContext gives project_id, trace_id and
// span_id, DroppedLabels give the rest.
func exemplarLabels(ex *distpb.Distribution_Exemplar) promql.Labels {
	out := promql.Labels{}

	for _, a := range ex.GetAttachments() {
		switch {
		case a.MessageIs(&monitoringpb.DroppedLabels{}):
			dl := &monitoringpb.DroppedLabels{}
			if a.UnmarshalTo(dl) == nil {
				maps.Copy(out, dl.GetLabel())
			}
		case a.MessageIs(&monitoringpb.SpanContext{}):
			sc := &monitoringpb.SpanContext{}
			if a.UnmarshalTo(sc) == nil {
				spanLabels(sc.GetSpanName(), out)
			}
		}
	}

	return out
}

// spanLabels splits projects/PROJECT/traces/TRACE/spans/SPAN.
func spanLabels(name string, out promql.Labels) {
	p := strings.Split(name, "/")

	const parts = 6
	if len(p) != parts || p[0] != "projects" || p[2] != "traces" || p[4] != "spans" {
		return
	}

	out["project_id"], out["trace_id"], out["span_id"] = p[1], p[3], p[5]
}

// exemplarBucket returns the le bound of the bucket holding v.
func exemplarBucket(d *distpb.Distribution, v float64) float64 {
	n := len(d.GetBucketCounts())

	for i := range n - 1 {
		if _, upper := bucketBounds(d, i); v < upper {
			return upper
		}
	}

	return math.Inf(1)
}

func exemplarTime(p *monitoringpb.Point, ex *distpb.Distribution_Exemplar) time.Time {
	if ex.GetTimestamp() != nil {
		return ex.GetTimestamp().AsTime()
	}

	return endOf(p)
}

func (s promStorage) exemplars(selectors [][]*promql.Matcher, minT, maxT int64) []promExemplarSeries {
	byKey := map[string]*promExemplarSeries{}

	for _, rec := range s.series {
		if rec.vtype != vtDist {
			continue
		}

		base := promMetricName(rec.metric.GetType()) + suffixBucket

		for _, p := range rec.points {
			d := p.GetValue().GetDistributionValue()

			for _, ex := range d.GetExemplars() {
				ts := exemplarTime(p, ex)
				if !inWindow(ts.UnixMilli(), minT, maxT) {
					continue
				}

				labels := promLabels(rec, base)
				labels[labelLE] = formatLE(exemplarBucket(d, ex.GetValue()))

				if matchesAnySelector(labels, selectors) {
					addExemplar(byKey, labels, ex, ts)
				}
			}
		}
	}

	return sortedExemplarSeries(byKey)
}

func addExemplar(
	byKey map[string]*promExemplarSeries, labels promql.Labels, ex *distpb.Distribution_Exemplar, ts time.Time,
) {
	key := promql.Fingerprint(labels)

	e, ok := byKey[key]
	if !ok {
		e = &promExemplarSeries{SeriesLabels: labels}
		byKey[key] = e
	}

	e.Exemplars = append(e.Exemplars, promExemplar{
		Labels: exemplarLabels(ex), Value: formatPromValue(ex.GetValue()),
		Timestamp: float64(ts.UnixMilli()) / msPerSecond,
	})
}

func sortedExemplarSeries(byKey map[string]*promExemplarSeries) []promExemplarSeries {
	out := make([]promExemplarSeries, 0, len(byKey))

	for _, e := range byKey {
		slices.SortStableFunc(e.Exemplars, func(a, c promExemplar) int { return cmp.Compare(a.Timestamp, c.Timestamp) })
		out = append(out, *e)
	}

	slices.SortFunc(out, func(a, c promExemplarSeries) int {
		return strings.Compare(promql.Fingerprint(a.SeriesLabels), promql.Fingerprint(c.SeriesLabels))
	})

	return out
}

func matchesAnySelector(l promql.Labels, selectors [][]*promql.Matcher) bool {
	for _, ms := range selectors {
		if promql.MatchesAll(l, ms) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) promExemplarsHandler(w http.ResponseWriter, project string, vals url.Values) {
	q := vals.Get("query")
	if q == "" {
		promError(w, http.StatusBadRequest, errTypeBadData, "invalid parameter \"query\": query is required")

		return
	}

	expr, err := promql.Parse(q)
	if err != nil {
		promError(w, http.StatusBadRequest, errTypeBadData, "invalid parameter \"query\": "+err.Error())

		return
	}

	start, err := parsePromTime(vals.Get("start"), time.Unix(0, 0).UTC())
	if err != nil {
		promError(w, http.StatusBadRequest, errTypeBadData, "invalid parameter \"start\": "+err.Error())

		return
	}

	end, err := parsePromTime(vals.Get("end"), b.currentTime())
	if err != nil {
		promError(w, http.StatusBadRequest, errTypeBadData, "invalid parameter \"end\": "+err.Error())

		return
	}

	out := b.promStorageFor(project).exemplars(promql.Selectors(expr), start.UnixMilli(), end.UnixMilli())
	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: out})
}
