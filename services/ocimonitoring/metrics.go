package ocimonitoring

import (
	"fmt"
	"math"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxStreamsPerPost  = 50
	maxDimensions      = 20
	maxDimKey          = 256
	maxDimValue        = 512
	maxMetadata        = 256
	maxPastAge         = 2 * time.Hour
	maxFutureSkew      = 10 * time.Minute
	defaultRange       = 3 * time.Hour
	defaultResolution  = time.Minute
	maxReturnedPoints  = 100000
	atomicBatch        = "ATOMIC"
	nonAtomicBatch     = "NON_ATOMIC"
	groupByNamespace   = "namespace"
	groupByName        = "name"
	groupByResourceGrp = "resourceGroup"
)

var (
	namespaceRe     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
	metricNameRe    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._$-]*$`)
	resourceGroupRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._$-]*$`)
)

// DatapointIn is one posted datapoint.
type DatapointIn struct {
	Timestamp *time.Time `json:"timestamp"`
	Value     *float64   `json:"value"`
	Count     *int       `json:"count,omitempty"`
}

// MetricDataIn is one posted metric stream.
type MetricDataIn struct {
	Dimensions    map[string]string `json:"dimensions"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	ResourceGroup *string           `json:"resourceGroup,omitempty"`
	Namespace     string            `json:"namespace"`
	CompartmentID string            `json:"compartmentId"`
	Name          string            `json:"name"`
	Datapoints    []DatapointIn     `json:"datapoints"`
}

// PostMetricDataIn is the PostMetricData request body.
type PostMetricDataIn struct {
	BatchAtomicity string         `json:"batchAtomicity"`
	MetricData     []MetricDataIn `json:"metricData"`
}

// FailedMetric describes a rejected metric stream.
type FailedMetric struct {
	Message    string       `json:"message"`
	MetricData MetricDataIn `json:"metricData"`
}

// PostMetricDataOut is the PostMetricData response body.
type PostMetricDataOut struct {
	FailedMetrics      []FailedMetric `json:"failedMetrics,omitempty"`
	FailedMetricsCount int            `json:"failedMetricsCount"`
}

func printableASCII(s string) bool {
	for _, r := range s {
		if r <= ' ' || r > '~' {
			return false
		}
	}

	return s != ""
}

// validateMetric checks one posted stream and returns the reason it is rejected, or "".
func validateMetric(m MetricDataIn, now time.Time) string {
	if msg := validateIdentity(m); msg != "" {
		return msg
	}

	if msg := validateDimensions(m); msg != "" {
		return msg
	}

	for _, d := range m.Datapoints {
		if msg := validateDatapoint(d, now); msg != "" {
			return msg
		}
	}

	return ""
}

func validateIdentity(m MetricDataIn) string {
	switch {
	case !namespaceRe.MatchString(m.Namespace):
		return "namespace must start with a letter and contain only letters, digits and underscores"
	case strings.HasPrefix(strs.Fold(m.Namespace), "oci_") || strings.HasPrefix(strs.Fold(m.Namespace), "oracle_"):
		return "namespaces starting with oci_ or oracle_ are reserved"
	case m.CompartmentID == "":
		return "compartmentId is required"
	case !metricNameRe.MatchString(m.Name):
		return "name must start with a letter and contain only letters, digits, dots, underscores, hyphens " +
			"and dollar signs"
	case len(m.Datapoints) == 0:
		return "at least one datapoint is required"
	case m.ResourceGroup != nil && !resourceGroupRe.MatchString(*m.ResourceGroup):
		return "resourceGroup must start with a letter and contain only letters, digits, periods, underscores, " +
			"hyphens and dollar signs"
	}

	return ""
}

func validateDimensions(m MetricDataIn) string {
	if len(m.Dimensions) < 1 || len(m.Dimensions) > maxDimensions {
		return "a metric needs between 1 and 20 dimensions"
	}

	for k, v := range m.Dimensions {
		if !printableASCII(k) || len(k) > maxDimKey {
			return "dimension keys must be printable ASCII without spaces, up to 256 characters"
		}

		if v == "" || len(v) > maxDimValue {
			return "dimension values must be 1 to 512 characters"
		}
	}

	for k, v := range m.Metadata {
		if k == "" || len(k) > maxMetadata || len(v) > maxMetadata {
			return "metadata keys and values are limited to 256 characters"
		}
	}

	return ""
}

func validateDatapoint(d DatapointIn, now time.Time) string {
	switch {
	case d.Timestamp == nil || d.Value == nil:
		return "every datapoint needs a timestamp and a value"
	case d.Timestamp.Before(now.Add(-maxPastAge)) || !d.Timestamp.Before(now.Add(maxFutureSkew)):
		return "datapoint timestamps must be less than two hours in the past and less than ten minutes in the future"
	case d.Count != nil && *d.Count <= 0:
		return "datapoint count must be greater than zero"
	case math.IsNaN(*d.Value) || math.IsInf(*d.Value, 0):
		return "datapoint values must be finite numbers"
	}

	return ""
}

func streamKey(namespace, compartment, name, rg string, dims map[string]string) string {
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	var sb strings.Builder

	sb.WriteString(namespace + "\x00" + compartment + "\x00" + name + "\x00" + rg)

	for _, k := range keys {
		sb.WriteString("\x00" + k + "=" + dims[k])
	}

	return sb.String()
}

// PostMetricData stores the datapoints of every valid stream.
func (b *InMemoryBackend) PostMetricData(in PostMetricDataIn) (PostMetricDataOut, error) {
	atomicity := strings.ToUpper(in.BatchAtomicity)

	switch atomicity {
	case "":
		atomicity = nonAtomicBatch
	case atomicBatch, nonAtomicBatch:
	default:
		return PostMetricDataOut{}, errParam("batchAtomicity must be ATOMIC or NON_ATOMIC")
	}

	if len(in.MetricData) == 0 {
		return PostMetricDataOut{}, errMissing("metricData is required")
	}

	b.mu.Lock("PostMetricData")
	defer b.mu.Unlock()

	now := b.nowUTC()
	unique := map[string]struct{}{}
	valid := make([]MetricDataIn, 0, len(in.MetricData))

	var out PostMetricDataOut

	for _, m := range in.MetricData {
		rg := ""
		if m.ResourceGroup != nil {
			rg = *m.ResourceGroup
		}

		unique[streamKey(m.Namespace, m.CompartmentID, m.Name, rg, m.Dimensions)] = struct{}{}

		if msg := validateMetric(m, now); msg != "" {
			out.FailedMetrics = append(out.FailedMetrics, FailedMetric{MetricData: m, Message: msg})

			continue
		}

		valid = append(valid, m)
	}

	if len(unique) > maxStreamsPerPost {
		return PostMetricDataOut{}, errParam("a request can carry at most 50 unique metric streams")
	}

	out.FailedMetricsCount = len(out.FailedMetrics)

	if out.FailedMetricsCount > 0 && (atomicity == atomicBatch || len(valid) == 0) {
		return PostMetricDataOut{}, errParam(out.FailedMetrics[0].Message)
	}

	for _, m := range valid {
		b.store(m)
	}

	return out, nil
}

func (b *InMemoryBackend) store(m MetricDataIn) {
	rg := ""
	if m.ResourceGroup != nil {
		rg = *m.ResourceGroup
	}

	key := streamKey(m.Namespace, m.CompartmentID, m.Name, rg, m.Dimensions)

	s, ok := b.streams[key]
	if !ok {
		s = &Stream{
			Namespace: m.Namespace, CompartmentID: m.CompartmentID, Name: m.Name, ResourceGroup: rg,
			Dimensions: m.Dimensions,
		}
		b.streams[key] = s
	}

	if m.Metadata != nil {
		s.Metadata = m.Metadata
	}

	for _, d := range m.Datapoints {
		n := 1
		if d.Count != nil {
			n = *d.Count
		}

		s.Points = append(s.Points, Point{T: d.Timestamp.UTC(), V: *d.Value, N: n})
	}

	sort.SliceStable(s.Points, func(i, j int) bool { return s.Points[i].T.Before(s.Points[j].T) })
}

// group is a set of streams aggregated together.
type group struct {
	dims     map[string]string
	metadata map[string]string
	points   []Point
}

// collectGroups selects the streams a query covers and merges them per group; the caller holds the lock.
func (b *InMemoryBackend) collectGroups(q *MQL, namespace, compartment, rg string) []*group {
	byKey := map[string]*group{}

	for _, s := range b.streams {
		if s.Namespace != namespace || s.CompartmentID != compartment || s.Name != q.Metric ||
			s.ResourceGroup != rg || !q.matches(s.Dimensions) {
			continue
		}

		dims := s.Dimensions

		if q.Grouped {
			dims = map[string]string{}

			for _, d := range q.GroupBy {
				if v, ok := s.Dimensions[d]; ok {
					dims[d] = v
				}
			}
		}

		key := streamKey("", "", "", "", dims)

		g, ok := byKey[key]
		if !ok {
			g = &group{dims: dims}
			if !q.Grouped {
				g.metadata = s.Metadata
			}

			byKey[key] = g
		}

		g.points = append(g.points, s.Points...)
	}

	out := make([]*group, 0, len(byKey))

	for _, g := range byKey {
		sort.SliceStable(g.points, func(i, j int) bool { return g.points[i].T.Before(g.points[j].T) })
		out = append(out, g)
	}

	slices.SortFunc(out, func(a, c *group) int {
		return compareStr(streamKey("", "", "", "", a.dims), streamKey("", "", "", "", c.dims))
	})

	return out
}

// windowStat applies a statistic to the (time ordered) points of one window.
func windowStat(q *MQL, pts []Point) (float64, bool) {
	if q.Stat == statAbsent {
		return b2f(len(pts) == 0), true
	}

	if len(pts) == 0 {
		return 0, false
	}

	var (
		sum, cnt float64
		lo, hi   = pts[0].V, pts[0].V
	)

	for _, p := range pts {
		sum += p.V * float64(p.N)
		cnt += float64(p.N)
		lo, hi = math.Min(lo, p.V), math.Max(hi, p.V)
	}

	first, last := pts[0], pts[len(pts)-1]

	switch q.Stat {
	case statSum:
		return sum, true
	case statCount:
		return cnt, true
	case statMax:
		return hi, true
	case statMin:
		return lo, true
	case statFirst:
		return first.V, true
	case statLast:
		return last.V, true
	case statIncrement:
		return last.V - first.V, true
	case statRate:
		secs := last.T.Sub(first.T).Seconds()
		if secs <= 0 {
			return 0, false
		}

		return (last.V - first.V) / secs, true
	case statPercent:
		return percentile(pts, q.Quantile), true
	}

	return sum / cnt, true
}

func b2f(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

// percentile uses the nearest-rank method over the counted values.
func percentile(pts []Point, p float64) float64 {
	sorted := slices.Clone(pts)
	slices.SortFunc(sorted, func(a, c Point) int {
		switch {
		case a.V < c.V:
			return -1
		case a.V > c.V:
			return 1
		}

		return 0
	})

	total := 0
	for _, pt := range sorted {
		total += pt.N
	}

	rank := int(math.Ceil(p * float64(total)))
	seen := 0

	for _, pt := range sorted {
		seen += pt.N
		if seen >= rank {
			return pt.V
		}
	}

	return sorted[len(sorted)-1].V
}

// pointsIn returns the points with from <= T < to.
func pointsIn(pts []Point, from, to time.Time) []Point {
	lo := sort.Search(len(pts), func(i int) bool { return !pts[i].T.Before(from) })
	hi := sort.Search(len(pts), func(i int) bool { return !pts[i].T.Before(to) })

	return pts[lo:hi]
}

// SummarizeIn is the SummarizeMetricsData request body.
type SummarizeIn struct {
	StartTime     *time.Time `json:"startTime"`
	EndTime       *time.Time `json:"endTime"`
	ResourceGroup *string    `json:"resourceGroup"`
	Namespace     string     `json:"namespace"`
	Query         string     `json:"query"`
	Resolution    string     `json:"resolution"`
}

type aggregatedWire struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// MetricDataWire is one aggregated series of a SummarizeMetricsData response.
type MetricDataWire struct {
	Dimensions           map[string]string `json:"dimensions"`
	Metadata             map[string]string `json:"metadata,omitempty"`
	Namespace            string            `json:"namespace"`
	CompartmentID        string            `json:"compartmentId"`
	Name                 string            `json:"name"`
	ResourceGroup        string            `json:"resourceGroup,omitempty"`
	Resolution           string            `json:"resolution"`
	AggregatedDatapoints []aggregatedWire  `json:"aggregatedDatapoints"`
}

// SummarizeMetricsData aggregates stored datapoints with the MQL statistic of the query.
func (b *InMemoryBackend) SummarizeMetricsData(compartment string, in SummarizeIn) ([]MetricDataWire, error) {
	if compartment == "" {
		return nil, errMissing("compartmentId is required")
	}

	if in.Namespace == "" || in.Query == "" {
		return nil, errMissing("namespace and query are required")
	}

	q, err := ParseMQL(in.Query)
	if err != nil {
		return nil, err
	}

	if q.Pred != nil {
		return nil, errParam("a threshold condition is only valid in an alarm query")
	}

	resolution := defaultResolution

	if in.Resolution != "" {
		var aerr *APIError
		if resolution, aerr = parseInterval(in.Resolution); aerr != nil {
			return nil, aerr
		}
	}

	if resolution > q.Interval {
		return nil, errParam("resolution must be equal to or less than the query interval")
	}

	b.mu.RLock("SummarizeMetricsData")
	defer b.mu.RUnlock()

	start, end, aerr := summaryRange(in, b.nowUTC())
	if aerr != nil {
		return nil, aerr
	}

	groups := b.collectGroups(q, in.Namespace, compartment, rgOf(in.ResourceGroup))
	if int(end.Sub(start)/resolution)*len(groups) > maxReturnedPoints {
		return nil, errParam("the query would return more than 100000 data points; narrow the time range")
	}

	out := make([]MetricDataWire, 0, len(groups))

	for _, g := range groups {
		w := MetricDataWire{
			Dimensions: g.dims, Metadata: g.metadata, Namespace: in.Namespace, CompartmentID: compartment,
			Name: q.Metric, ResourceGroup: rgOf(in.ResourceGroup), Resolution: resolutionString(in.Resolution),
			AggregatedDatapoints: aggregate(q, g.points, start, end, resolution),
		}

		if len(w.AggregatedDatapoints) > 0 {
			out = append(out, w)
		}
	}

	return out, nil
}

// summaryRange resolves the requested time range against the defaults.
func summaryRange(in SummarizeIn, now time.Time) (time.Time, time.Time, *APIError) {
	start, end := now.Add(-defaultRange), now

	if in.StartTime != nil {
		start = in.StartTime.UTC()
	}

	if in.EndTime != nil {
		end = in.EndTime.UTC()
	}

	if !start.Before(end) {
		return start, end, errParam("startTime must be before endTime")
	}

	return start, end, nil
}

// aggregate evaluates the statistic over the windows that start at every resolution step of [start, end).
func aggregate(q *MQL, points []Point, start, end time.Time, resolution time.Duration) []aggregatedWire {
	out := []aggregatedWire{}
	clipped := pointsIn(points, start, end)
	first := start.Truncate(resolution)

	if first.Before(start) {
		first = first.Add(resolution)
	}

	for ts := first; ts.Before(end); ts = ts.Add(resolution) {
		if v, ok := windowStat(q, pointsIn(clipped, ts, ts.Add(q.Interval))); ok {
			out = append(out, aggregatedWire{Timestamp: ts, Value: v})
		}
	}

	return out
}

func resolutionString(r string) string {
	if r == "" {
		return "1m"
	}

	return r
}

func rgOf(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

// ListMetricsIn is the ListMetrics request body.
type ListMetricsIn struct {
	DimensionFilters map[string]string `json:"dimensionFilters"`
	Name             *string           `json:"name"`
	Namespace        *string           `json:"namespace"`
	ResourceGroup    *string           `json:"resourceGroup"`
	SortBy           string            `json:"sortBy"`
	SortOrder        string            `json:"sortOrder"`
	GroupBy          []string          `json:"groupBy"`
}

// MetricWire is one ListMetrics item.
type MetricWire struct {
	Dimensions    map[string]string `json:"dimensions,omitempty"`
	Name          string            `json:"name,omitempty"`
	Namespace     string            `json:"namespace,omitempty"`
	ResourceGroup string            `json:"resourceGroup,omitempty"`
	CompartmentID string            `json:"compartmentId"`
}

func wildcardMatch(pattern *string, v string) bool {
	if pattern == nil {
		return true
	}

	parts := strings.Split(*pattern, "*")
	for i := range parts {
		parts[i] = regexp.QuoteMeta(parts[i])
	}

	return regexp.MustCompile("^" + strings.Join(parts, ".*") + "$").MatchString(v)
}

// ListMetrics lists the metric definitions of a compartment.
func (b *InMemoryBackend) ListMetrics(
	compartment string, in ListMetricsIn, pageToken string, limit int,
) (page.Page[MetricWire], error) {
	if compartment == "" {
		return page.Page[MetricWire]{}, errMissing("compartmentId is required")
	}

	for _, g := range in.GroupBy {
		if g != groupByNamespace && g != groupByName && g != groupByResourceGrp {
			return page.Page[MetricWire]{}, errParam("groupBy supports namespace, name and resourceGroup")
		}
	}

	b.mu.RLock("ListMetrics")
	defer b.mu.RUnlock()

	seen := map[string]MetricWire{}

	for _, s := range b.streams {
		if s.CompartmentID != compartment || !in.selects(s) {
			continue
		}

		w := metricDefinition(s, in.GroupBy)
		seen[fmt.Sprintf("%s|%s|%s|%s", w.Namespace, w.Name, w.ResourceGroup, streamKey("", "", "", "", w.Dimensions))] = w
	}

	all := make([]MetricWire, 0, len(seen))
	for _, w := range seen {
		all = append(all, w)
	}

	return paginate(
		all,
		ListOpts{SortBy: strings.ToUpper(in.SortBy), Order: in.SortOrder, Page: pageToken, Limit: limit},
		compareMetrics,
		"NAMESPACE",
	), nil
}

// selects reports whether a stream passes the name, namespace, resource group and dimension filters.
func (in ListMetricsIn) selects(s *Stream) bool {
	if !wildcardMatch(in.Name, s.Name) || !wildcardMatch(in.Namespace, s.Namespace) ||
		(in.ResourceGroup != nil && *in.ResourceGroup != s.ResourceGroup) {
		return false
	}

	return len(in.GroupBy) > 0 || hasDims(s.Dimensions, in.DimensionFilters)
}

// metricDefinition is the stream's definition, reduced to the grouped fields when groupBy is used.
func metricDefinition(s *Stream, groupBy []string) MetricWire {
	if len(groupBy) == 0 {
		return MetricWire{
			CompartmentID: s.CompartmentID, Name: s.Name, Namespace: s.Namespace, ResourceGroup: s.ResourceGroup,
			Dimensions: s.Dimensions,
		}
	}

	w := MetricWire{CompartmentID: s.CompartmentID}

	for _, g := range groupBy {
		switch g {
		case groupByNamespace:
			w.Namespace = s.Namespace
		case groupByName:
			w.Name = s.Name
		default:
			w.ResourceGroup = s.ResourceGroup
		}
	}

	return w
}

func compareMetrics(a, c MetricWire, by string) int {
	switch by {
	case "NAME":
		return compareStr(a.Name, c.Name)
	case "RESOURCEGROUP":
		return compareStr(a.ResourceGroup, c.ResourceGroup)
	}

	return compareStr(a.Namespace+"\x00"+a.Name+"\x00"+streamKey("", "", "", "", a.Dimensions),
		c.Namespace+"\x00"+c.Name+"\x00"+streamKey("", "", "", "", c.Dimensions))
}

func hasDims(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}

	return true
}

func (h *Handler) postMetricData(q *req) error {
	var in PostMetricDataIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, err := h.Backend.PostMetricData(in)

	return reply(q, out, "", err)
}

func (h *Handler) summarizeMetricsData(q *req) error {
	var in SummarizeIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, err := h.Backend.SummarizeMetricsData(q.query("compartmentId"), in)
	if err != nil {
		return err
	}

	if out == nil {
		out = []MetricDataWire{}
	}

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) listMetrics(q *req) error {
	var in ListMetricsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	limit := 0

	if raw := q.query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return errParam("limit must be an integer")
		}

		limit = n
	}

	pg, err := h.Backend.ListMetrics(q.query("compartmentId"), in, q.query("page"), limit)

	return listReply(q, pg, err)
}
