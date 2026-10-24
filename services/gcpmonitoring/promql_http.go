package gcpmonitoring

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	promPathMarker  = "/location/global/prometheus/api/v1/"
	promPathPrefix  = "/v1/projects/"
	msPerSecond     = 1000.0
	maxPromBody     = 1 << 20
	statusSuccess   = "success"
	errTypeBadData  = "bad_data"
	errTypeExec     = "execution"
	errTypeTimeout  = "timeout"
	errTypeNotFound = "not_found"
)

var (
	errBadParam  = errors.New("invalid value")
	promLabelRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	promProjectR = regexp.MustCompile(`^/v1/projects/([^/]+)/location/global/prometheus/api/v1/(.+)$`)
)

// IsPrometheusPath reports whether the URL path addresses the Prometheus-compatible query API.
func IsPrometheusPath(path string) bool {
	return strings.HasPrefix(path, promPathPrefix) && strings.Contains(path, promPathMarker)
}

// PrometheusHandler serves the Managed Service for Prometheus query API over the stored metrics:
// /v1/projects/P/location/global/prometheus/api/v1/{query,query_range,series,labels,label/NAME/values,metadata}.
func (b *InMemoryBackend) PrometheusHandler() http.Handler {
	return http.HandlerFunc(b.servePrometheus)
}

type promPoint [2]any

type promSeriesJSON struct {
	Metric promql.Labels `json:"metric"`
	Value  *promPoint    `json:"value,omitempty"`
	Values []promPoint   `json:"values,omitempty"`
}

type promData struct {
	Result     any    `json:"result"`
	ResultType string `json:"resultType"`
}

type promResponse struct {
	Data      any    `json:"data,omitempty"`
	Status    string `json:"status"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

func writePromJSON(w http.ResponseWriter, code int, body promResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func promError(w http.ResponseWriter, code int, errType, msg string) {
	writePromJSON(w, code, promResponse{Status: "error", ErrorType: errType, Error: msg})
}

func formatPromValue(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}

	return strconv.FormatFloat(v, 'f', -1, 64)
}

func promPointOf(t int64, v float64) promPoint {
	return promPoint{float64(t) / msPerSecond, formatPromValue(v)}
}

func promResult(v promql.Value) promData {
	d := promData{ResultType: promql.ResultType(v)}

	switch x := v.(type) {
	case promql.Scalar:
		d.Result = promPointOf(x.T, x.V)
	case promql.StringValue:
		d.Result = promPoint{float64(x.T) / msPerSecond, x.V}
	case promql.Vector:
		out := make([]promSeriesJSON, 0, len(x))

		for _, s := range x {
			p := promPointOf(s.T, s.V)
			out = append(out, promSeriesJSON{Metric: s.Metric, Value: &p})
		}

		d.Result = out
	case promql.Matrix:
		out := make([]promSeriesJSON, 0, len(x))

		for _, s := range x {
			vals := make([]promPoint, 0, len(s.Samples))
			for _, p := range s.Samples {
				vals = append(vals, promPointOf(p.T, p.V))
			}

			out = append(out, promSeriesJSON{Metric: s.Labels, Values: vals})
		}

		d.Result = out
	}

	return d
}

func promParams(r *http.Request) (url.Values, error) {
	vals := r.URL.Query()
	if r.Method != http.MethodPost {
		return vals, nil
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxPromBody))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, fmt.Errorf("parse form: %w", err)
	}

	for k, v := range form {
		vals[k] = append(vals[k], v...)
	}

	return vals, nil
}

func parsePromTime(raw string, def time.Time) (time.Time, error) {
	if raw == "" {
		return def, nil
	}

	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		sec, frac := math.Modf(f)

		return time.Unix(int64(sec), int64(frac*1e9)).UTC(), nil //nolint:mnd // fraction to nanoseconds
	}

	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp: %w", raw, errBadParam)
	}

	return t.UTC(), nil
}

func parsePromStep(raw string) (time.Duration, error) {
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}

	d, err := promql.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %q to a valid duration: %w", raw, errBadParam)
	}

	return d, nil
}

func (b *InMemoryBackend) servePrometheus(w http.ResponseWriter, r *http.Request) {
	m := promProjectR.FindStringSubmatch(r.URL.Path)
	if m == nil {
		promError(w, http.StatusNotFound, errTypeNotFound, "unknown Prometheus API path")

		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		promError(w, http.StatusMethodNotAllowed, errTypeBadData, "method not allowed")

		return
	}

	vals, err := promParams(r)
	if err != nil {
		promError(w, http.StatusBadRequest, errTypeBadData, "cannot read request parameters")

		return
	}

	project, op := m[1], m[2]

	switch {
	case op == "query":
		b.promInstant(w, r, project, vals)
	case op == "query_range":
		b.promRange(w, r, project, vals)
	case op == "series":
		b.promSeries(w, project, vals)
	case op == "labels":
		b.promLabelNames(w, project, vals)
	case op == "metadata":
		b.promMetadata(w, project, vals)
	case strings.HasPrefix(op, "label/") && strings.HasSuffix(op, "/values"):
		b.promLabelValues(w, project, strings.TrimSuffix(strings.TrimPrefix(op, "label/"), "/values"), vals)
	default:
		promError(w, http.StatusNotFound, errTypeNotFound, "unknown Prometheus API path")
	}
}

func promQueryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, promql.ErrTimeout):
		promError(w, http.StatusServiceUnavailable, errTypeTimeout, err.Error())
	case errors.Is(err, promql.ErrSyntax):
		promError(w, http.StatusBadRequest, errTypeBadData, err.Error())
	default:
		promError(w, http.StatusUnprocessableEntity, errTypeExec, err.Error())
	}
}

func (b *InMemoryBackend) promInstant(w http.ResponseWriter, r *http.Request, project string, vals url.Values) {
	ts, err := parsePromTime(vals.Get("time"), b.currentTime())
	if err != nil {
		promError(w, http.StatusBadRequest, errTypeBadData, "invalid parameter \"time\": "+err.Error())

		return
	}

	q := vals.Get("query")
	if q == "" {
		promError(w, http.StatusBadRequest, errTypeBadData, "invalid parameter \"query\": query is required")

		return
	}

	v, err := (&promql.Engine{Ctx: r.Context(), Store: b.promStorageFor(project)}).Instant(q, ts)
	if err != nil {
		promQueryError(w, err)

		return
	}

	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: promResult(v)})
}

func (b *InMemoryBackend) promRange(w http.ResponseWriter, r *http.Request, project string, vals url.Values) {
	start, err1 := parsePromTime(vals.Get("start"), time.Time{})
	end, err2 := parsePromTime(vals.Get("end"), b.currentTime())
	step, err3 := parsePromStep(vals.Get("step"))

	for _, p := range []struct {
		err  error
		name string
	}{{err1, "start"}, {err2, "end"}, {err3, "step"}} {
		if p.err != nil {
			promError(w, http.StatusBadRequest, errTypeBadData, "invalid parameter \""+p.name+"\": "+p.err.Error())

			return
		}
	}

	if vals.Get("start") == "" || vals.Get("step") == "" || vals.Get("query") == "" {
		promError(w, http.StatusBadRequest, errTypeBadData, "query, start and step are required")

		return
	}

	mat, err := (&promql.Engine{Ctx: r.Context(), Store: b.promStorageFor(project)}).
		Range(vals.Get("query"), start, end, step)
	if err != nil {
		promQueryError(w, err)

		return
	}

	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: promResult(mat)})
}

func promSelectors(vals url.Values) ([][]*promql.Matcher, error) {
	out := make([][]*promql.Matcher, 0, len(vals["match[]"]))

	for _, raw := range vals["match[]"] {
		e, err := promql.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("match[]: %w", err)
		}

		vs, ok := e.(*promql.VectorSelector)
		if !ok {
			return nil, fmt.Errorf("match[] must be a vector selector: %w", errBadParam)
		}

		out = append(out, vs.Matchers)
	}

	return out, nil
}

func promBounds(vals url.Values) (int64, int64, error) {
	minT, maxT := int64(math.MinInt64), int64(math.MaxInt64)

	if raw := vals.Get("start"); raw != "" {
		t, err := parsePromTime(raw, time.Time{})
		if err != nil {
			return 0, 0, err
		}

		minT = t.UnixMilli()
	}

	if raw := vals.Get("end"); raw != "" {
		t, err := parsePromTime(raw, time.Time{})
		if err != nil {
			return 0, 0, err
		}

		maxT = t.UnixMilli()
	}

	return minT, maxT, nil
}

// promMatching returns the label sets of the series selected by match[] in the start/end window.
func (b *InMemoryBackend) promMatching(
	w http.ResponseWriter, project string, vals url.Values, requireMatch bool,
) ([]promql.Labels, bool) {
	sels, err := promSelectors(vals)
	if err != nil {
		promError(w, http.StatusBadRequest, errTypeBadData, err.Error())

		return nil, false
	}

	if len(sels) == 0 {
		if requireMatch {
			promError(w, http.StatusBadRequest, errTypeBadData, "no match[] parameter provided")

			return nil, false
		}

		all, _ := promql.NewMatcher(promql.MatchRegexp, promql.NameLabel, ".+")
		sels = [][]*promql.Matcher{{all}}
	}

	minT, maxT, err := promBounds(vals)
	if err != nil {
		promError(w, http.StatusBadRequest, errTypeBadData, err.Error())

		return nil, false
	}

	st := b.promStorageFor(project)
	seen := map[string]bool{}

	var out []promql.Labels

	for _, ms := range sels {
		for _, s := range st.Select(ms, minT, maxT) {
			if fp := promql.Fingerprint(s.Labels); !seen[fp] {
				seen[fp] = true
				out = append(out, s.Labels)
			}
		}
	}

	return out, true
}

func (b *InMemoryBackend) promSeries(w http.ResponseWriter, project string, vals url.Values) {
	out, ok := b.promMatching(w, project, vals, true)
	if !ok {
		return
	}

	if out == nil {
		out = []promql.Labels{}
	}

	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: out})
}

func (b *InMemoryBackend) promLabelNames(w http.ResponseWriter, project string, vals url.Values) {
	series, ok := b.promMatching(w, project, vals, false)
	if !ok {
		return
	}

	set := map[string]bool{}

	for _, l := range series {
		for k := range l {
			set[k] = true
		}
	}

	names := make([]string, 0, len(set))
	for k := range set {
		names = append(names, k)
	}

	slices.Sort(names)
	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: names})
}

func (b *InMemoryBackend) promLabelValues(w http.ResponseWriter, project, name string, vals url.Values) {
	if !promLabelRe.MatchString(name) {
		promError(w, http.StatusBadRequest, errTypeBadData, "invalid label name: "+name)

		return
	}

	series, ok := b.promMatching(w, project, vals, false)
	if !ok {
		return
	}

	set := map[string]bool{}

	for _, l := range series {
		if v, has := l[name]; has {
			set[v] = true
		}
	}

	values := make([]string, 0, len(set))
	for v := range set {
		values = append(values, v)
	}

	slices.Sort(values)
	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: values})
}

type promMeta struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

func promTypeOf(rec *seriesRec) string {
	switch {
	case rec.vtype == vtDist:
		return "histogram"
	case rec.kind == kindGauge:
		return "gauge"
	}

	return "counter"
}

func (b *InMemoryBackend) promMetadata(w http.ResponseWriter, project string, vals url.Values) {
	limit := math.MaxInt
	if raw := vals.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			promError(w, http.StatusBadRequest, errTypeBadData, "invalid parameter \"limit\"")

			return
		}

		limit = n
	}

	want := vals.Get("metric")
	out := map[string][]promMeta{}

	for _, rec := range b.promStorageFor(project).series {
		name := promMetricName(rec.metric.GetType())
		if want != "" && name != want {
			continue
		}

		meta := promMeta{Type: promTypeOf(rec), Unit: rec.unit}
		if !slices.Contains(out[name], meta) {
			out[name] = append(out[name], meta)
		}
	}

	names := make([]string, 0, len(out))
	for n := range out {
		names = append(names, n)
	}

	slices.Sort(names)

	res := map[string][]promMeta{}
	for _, n := range names[:min(limit, len(names))] {
		res[n] = out[n]
	}

	writePromJSON(w, http.StatusOK, promResponse{Status: statusSuccess, Data: res})
}
