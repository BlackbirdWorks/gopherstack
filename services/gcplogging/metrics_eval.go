package gcplogging

import (
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/genproto/googleapis/api/distribution"
)

var (
	extractRe       = regexp.MustCompile(`^EXTRACT\(\s*([^)]+?)\s*\)$`)
	regexpExtractRe = regexp.MustCompile(`^REGEXP_EXTRACT\(\s*([^,\s]+)\s*,\s*"(.*)"\s*\)$`)
)

// extractor reads a string from an entry: EXTRACT(field) or REGEXP_EXTRACT(field, "re").
type extractor struct {
	re   *regexp.Regexp
	path []string
}

type metricCompiled struct {
	filter *filter
	value  *extractor
	labels map[string]*extractor
	bounds []float64
}

// seriesRec is one label combination of a metric: a count, or distribution statistics.
type seriesRec struct {
	labels  map[string]string
	buckets []int64
	count   int64
	sum     float64
	mean    float64
	ssd     float64
}

type metricRec struct {
	pb       *loggingpb.LogMetric
	compiled *metricCompiled
	series   map[string]*seriesRec
}

// MetricPoint is the accumulated value of one label combination of a log-based metric.
type MetricPoint struct {
	Labels       map[string]string
	BucketCounts []int64
	Count        int64
	Sum          float64
	SumSqDev     float64
}

func parseExtractor(src string) (*extractor, error) {
	if m := extractRe.FindStringSubmatch(src); m != nil {
		return &extractor{path: splitPath(m[1])}, nil
	}

	if m := regexpExtractRe.FindStringSubmatch(src); m != nil {
		if len(m[2]) > maxRegexLen {
			return nil, errInvalid("Extractor regular expression is too long.")
		}

		re, err := regexp.Compile(m[2])
		if err != nil {
			return nil, errInvalid("Invalid extractor regular expression %q.", m[2])
		}

		return &extractor{path: splitPath(m[1]), re: re}, nil
	}

	return nil, errInvalid("Invalid extractor [%s]: use EXTRACT(field) or REGEXP_EXTRACT(field, \"regex\").", src)
}

func (x *extractor) value(d *entryDoc) (string, bool) {
	vals := lookup(d.doc(), x.path)
	if len(vals) == 0 {
		return "", false
	}

	s := scalarString(vals[0])
	if x.re == nil {
		return s, true
	}

	m := x.re.FindStringSubmatch(s)
	switch {
	case m == nil:
		return "", false
	case len(m) > 1:
		return m[1], true
	}

	return m[0], true
}

func compileMetric(m *loggingpb.LogMetric) (*metricCompiled, error) {
	f, err := compileFilter(m.GetFilter())
	if err != nil {
		return nil, err
	}

	c := &metricCompiled{filter: f, labels: map[string]*extractor{}}

	for k, src := range m.GetLabelExtractors() {
		x, xerr := parseExtractor(src)
		if xerr != nil {
			return nil, xerr
		}

		c.labels[k] = x
	}

	if m.GetValueExtractor() == "" {
		return c, nil
	}

	if c.value, err = parseExtractor(m.GetValueExtractor()); err != nil {
		return nil, err
	}

	c.bounds, err = bucketBounds(m.GetBucketOptions())

	return c, err
}

// bucketBounds returns the finite bucket boundaries of the bucket options.
func bucketBounds(o *distribution.Distribution_BucketOptions) ([]float64, error) {
	switch t := o.GetOptions().(type) {
	case *distribution.Distribution_BucketOptions_LinearBuckets:
		return linearBounds(t.LinearBuckets)
	case *distribution.Distribution_BucketOptions_ExponentialBuckets:
		return exponentialBounds(t.ExponentialBuckets)
	case *distribution.Distribution_BucketOptions_ExplicitBuckets:
		return explicitBounds(t.ExplicitBuckets.GetBounds())
	}

	return nil, errInvalid("Distribution metrics need bucket_options.")
}

func linearBounds(l *distribution.Distribution_BucketOptions_Linear) ([]float64, error) {
	n := int(l.GetNumFiniteBuckets())
	if n < 1 || n > maxFiniteBuckets || l.GetWidth() <= 0 {
		return nil, errInvalid("Invalid linear bucket options.")
	}

	out := make([]float64, n+1)
	for i := range out {
		out[i] = l.GetOffset() + float64(i)*l.GetWidth()
	}

	return out, nil
}

func exponentialBounds(e *distribution.Distribution_BucketOptions_Exponential) ([]float64, error) {
	n := int(e.GetNumFiniteBuckets())
	if n < 1 || n > maxFiniteBuckets || e.GetGrowthFactor() <= 1 || e.GetScale() <= 0 {
		return nil, errInvalid("Invalid exponential bucket options.")
	}

	out := make([]float64, n+1)
	for i := range out {
		out[i] = e.GetScale() * math.Pow(e.GetGrowthFactor(), float64(i))
	}

	return out, nil
}

func explicitBounds(bounds []float64) ([]float64, error) {
	if len(bounds) < 1 || len(bounds) > maxFiniteBuckets || !sort.Float64sAreSorted(bounds) {
		return nil, errInvalid("Invalid explicit bucket options.")
	}

	return append([]float64(nil), bounds...), nil
}

func (b *InMemoryBackend) updateMetricsLocked(parent string, doc *entryDoc) {
	prefix := parent + "/metrics/"

	for key, rec := range b.metrics {
		if !strings.HasPrefix(key, prefix) || rec.pb.GetDisabled() || !rec.compiled.filter.matches(doc) {
			continue
		}

		rec.record(doc)
	}
}

func (r *metricRec) record(doc *entryDoc) {
	labels := map[string]string{}

	for k, x := range r.compiled.labels {
		labels[k], _ = x.value(doc)
	}

	var (
		val float64
		hit bool
	)

	if r.compiled.value != nil {
		s, ok := r.compiled.value.value(doc)
		if !ok {
			return
		}

		var err error
		if val, err = strconv.ParseFloat(strings.TrimSpace(s), 64); err != nil {
			return
		}

		hit = true
	}

	s := r.seriesFor(labels)
	s.count++

	if hit {
		s.observe(val, r.compiled.bounds)
	}
}

func (r *metricRec) seriesFor(labels map[string]string) *seriesRec {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var sb strings.Builder

	for _, k := range keys {
		sb.WriteString(k + "\x00" + labels[k] + "\x00")
	}

	s, ok := r.series[sb.String()]
	if !ok {
		s = &seriesRec{labels: labels}
		if r.compiled.value != nil {
			s.buckets = make([]int64, len(r.compiled.bounds)+1)
		}

		r.series[sb.String()] = s
	}

	return s
}

// observe adds one sample using Welford's update for the mean and sum of squared deviations.
func (s *seriesRec) observe(v float64, bounds []float64) {
	delta := v - s.mean
	s.mean += delta / float64(s.count)
	s.ssd += delta * (v - s.mean)
	s.sum += v
	s.buckets[sort.SearchFloat64s(bounds, math.Nextafter(v, math.Inf(1)))]++
}

// MetricPoints returns the accumulated series of a log-based metric ([PARENT]/metrics/[ID]).
func (b *InMemoryBackend) MetricPoints(name string) ([]MetricPoint, error) {
	if _, _, err := metricKey(name); err != nil {
		return nil, err
	}

	b.mu.RLock("MetricPoints")
	defer b.mu.RUnlock()

	rec, ok := b.metrics[name]
	if !ok {
		return nil, errNotFound("Metric %s does not exist.", name)
	}

	out := make([]MetricPoint, 0, len(rec.series))
	for _, s := range rec.series {
		out = append(
			out,
			MetricPoint{
				Labels: s.labels, Count: s.count, Sum: s.sum, SumSqDev: s.ssd,
				BucketCounts: append([]int64(nil), s.buckets...),
			},
		)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })

	return out, nil
}
