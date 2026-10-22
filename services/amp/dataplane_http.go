package amp

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

var errBadParam = errors.New("invalid value")

const (
	statusSuccess = "success"
	opRemoteWrite = "RemoteWrite"
	opQuery       = "Query"
	opQueryRange  = "QueryRange"
	opSeries      = "Series"
	opLabels      = "Labels"
	opLabelValues = "LabelValues"
	opRules       = "Rules"
	opAlerts      = "Alerts"
	opMetadata    = "Metadata"
	opBuildInfo   = "BuildInfo"
	opExemplars   = "QueryExemplars"
	opAMAlerts    = "AlertManagerAlerts"
	opAMPostAlert = "AlertManagerPostAlerts"

	promBase = "/workspaces/{workspaceId}/api/v1/"
	amBase   = "/workspaces/{workspaceId}/alertmanager/api/v2/"
)

func dataRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPost, Path: promBase + "remote_write", Op: opRemoteWrite},
		{Method: http.MethodGet, Path: promBase + "query", Op: opQuery},
		{Method: http.MethodPost, Path: promBase + "query", Op: opQuery},
		{Method: http.MethodGet, Path: promBase + "query_range", Op: opQueryRange},
		{Method: http.MethodPost, Path: promBase + "query_range", Op: opQueryRange},
		{Method: http.MethodGet, Path: promBase + "series", Op: opSeries},
		{Method: http.MethodPost, Path: promBase + "series", Op: opSeries},
		{Method: http.MethodGet, Path: promBase + "labels", Op: opLabels},
		{Method: http.MethodPost, Path: promBase + "labels", Op: opLabels},
		{Method: http.MethodGet, Path: promBase + "label/{name}/values", Op: opLabelValues},
		{Method: http.MethodGet, Path: promBase + "rules", Op: opRules},
		{Method: http.MethodGet, Path: promBase + "alerts", Op: opAlerts},
		{Method: http.MethodGet, Path: promBase + "metadata", Op: opMetadata},
		{Method: http.MethodGet, Path: promBase + "status/buildinfo", Op: opBuildInfo},
		{Method: http.MethodGet, Path: promBase + "query_exemplars", Op: opExemplars},
		{Method: http.MethodPost, Path: promBase + "query_exemplars", Op: opExemplars},
		{Method: http.MethodGet, Path: amBase + "alerts", Op: opAMAlerts},
		{Method: http.MethodPost, Path: amBase + "alerts", Op: opAMPostAlert},
	}
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

func promError(c *echo.Context, status int, errType, msg string) error {
	return c.JSON(status, promResponse{Status: "error", ErrorType: errType, Error: msg})
}

func formatValue(v float64) string {
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

const msPerSecond = 1000.0

func seconds(ms int64) float64 { return float64(ms) / msPerSecond }

func point(t int64, v float64) promPoint { return promPoint{seconds(t), formatValue(v)} }

func resultJSON(v promql.Value) promData {
	d := promData{ResultType: promql.ResultType(v)}

	switch x := v.(type) {
	case promql.Scalar:
		d.Result = point(x.T, x.V)
	case promql.StringValue:
		d.Result = promPoint{seconds(x.T), x.V}
	case promql.Vector:
		out := make([]promSeriesJSON, 0, len(x))

		for _, s := range x {
			p := point(s.T, s.V)
			out = append(out, promSeriesJSON{Metric: s.Metric, Value: &p})
		}

		d.Result = out
	case promql.Matrix:
		d.Result = matrixJSON(x)
	}

	return d
}

func matrixJSON(m promql.Matrix) []promSeriesJSON {
	out := make([]promSeriesJSON, 0, len(m))

	for _, s := range m {
		vals := make([]promPoint, 0, len(s.Samples))
		for _, p := range s.Samples {
			vals = append(vals, point(p.T, p.V))
		}

		out = append(out, promSeriesJSON{Metric: s.Labels, Values: vals})
	}

	return out
}

// promParams merges URL query and form-encoded body parameters.
func promParams(c *echo.Context) (url.Values, error) {
	r := c.Request()
	vals := r.URL.Query()

	if r.Method != http.MethodPost {
		return vals, nil
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return nil, err
	}

	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}

	for k, v := range form {
		vals[k] = append(vals[k], v...)
	}

	return vals, nil
}

func parseTime(raw string, def time.Time) (time.Time, error) {
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

func parseStep(raw string) (time.Duration, error) {
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}

	d, err := promql.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %q to a valid duration: %w", raw, errBadParam)
	}

	return d, nil
}

func (h *Handler) serveData(c *echo.Context, op string, params map[string]string) error {
	wsID := params[keyWorkspaceID]

	if op == opRemoteWrite {
		return h.remoteWrite(c, wsID)
	}

	if op == opAMPostAlert {
		return h.postAlerts(c, wsID)
	}

	vals, err := promParams(c)
	if err != nil {
		return promError(c, http.StatusBadRequest, "bad_data", "cannot read request parameters")
	}

	switch op {
	case opRules, opAlerts, opMetadata, opBuildInfo, opExemplars, opAMAlerts:
		return h.serveStatus(c, op, wsID, vals)
	case opQuery:
		return h.instantQuery(c, wsID, vals)
	case opQueryRange:
		return h.rangeQuery(c, wsID, vals)
	case opSeries:
		return h.seriesQuery(c, wsID, vals)
	case opLabels:
		return h.labelNames(c, wsID, vals)
	default:
		return h.labelValues(c, wsID, params["name"], vals)
	}
}

func (h *Handler) remoteWrite(c *echo.Context, wsID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return restroute.WriteError(
			c,
			http.StatusBadRequest,
			"ValidationException",
			"Unable to read the request body",
			nil,
		)
	}

	series, err := decodeWriteRequest(body)
	if err != nil {
		return restroute.WriteError(c, http.StatusBadRequest, "ValidationException", err.Error(), nil)
	}

	if _, ierr := h.Backend.Ingest(c.Request().Context(), wsID, series); ierr != nil {
		return h.server.WriteErr(c, ierr)
	}

	if ierr := h.Backend.IngestMetadata(c.Request().Context(), wsID, decodeWriteMetadata(body)); ierr != nil {
		return h.server.WriteErr(c, ierr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) instantQuery(c *echo.Context, wsID string, vals url.Values) error {
	ctx := c.Request().Context()

	ts, err := parseTime(vals.Get("time"), h.Backend.now())
	if err != nil {
		return promError(c, http.StatusBadRequest, "bad_data", "invalid parameter \"time\": "+err.Error())
	}

	q := vals.Get("query")
	if q == "" {
		return promError(c, http.StatusBadRequest, "bad_data", "invalid parameter \"query\": query is required")
	}

	var (
		val  promql.Value
		qerr error
	)

	qctx, cancel, terr := queryContext(ctx, vals)
	if terr != nil {
		return promError(c, http.StatusBadRequest, "bad_data", "invalid parameter \"timeout\": "+terr.Error())
	}
	defer cancel()

	if ierr := h.Backend.withStorage(ctx, wsID, func(st promql.Storage) {
		val, qerr = (&promql.Engine{Ctx: qctx, Store: st}).Instant(q, ts)
	}); ierr != nil {
		return h.server.WriteErr(c, ierr)
	}

	if qerr != nil {
		return queryError(c, qerr)
	}

	return c.JSON(http.StatusOK, promResponse{Status: statusSuccess, Data: resultJSON(val)})
}

func queryError(c *echo.Context, err error) error {
	if errors.Is(err, promql.ErrTimeout) {
		return promError(c, http.StatusServiceUnavailable, "timeout", err.Error())
	}

	if errors.Is(err, promql.ErrSyntax) {
		return promError(c, http.StatusBadRequest, "bad_data", err.Error())
	}

	return promError(c, http.StatusUnprocessableEntity, "execution", err.Error())
}

func (h *Handler) rangeQuery(c *echo.Context, wsID string, vals url.Values) error {
	ctx := c.Request().Context()
	now := h.Backend.now()

	start, err1 := parseTime(vals.Get("start"), time.Time{})
	end, err2 := parseTime(vals.Get("end"), now)
	step, err3 := parseStep(vals.Get("step"))

	for name, e := range map[string]error{"start": err1, "end": err2, "step": err3} {
		if e != nil {
			return promError(c, http.StatusBadRequest, "bad_data", "invalid parameter \""+name+"\": "+e.Error())
		}
	}

	if vals.Get("start") == "" || vals.Get("step") == "" || vals.Get("query") == "" {
		return promError(c, http.StatusBadRequest, "bad_data", "query, start and step are required")
	}

	var (
		m    promql.Matrix
		qerr error
	)

	qctx, cancel, terr := queryContext(ctx, vals)
	if terr != nil {
		return promError(c, http.StatusBadRequest, "bad_data", "invalid parameter \"timeout\": "+terr.Error())
	}
	defer cancel()

	if err := h.Backend.withStorage(ctx, wsID, func(st promql.Storage) {
		m, qerr = (&promql.Engine{Ctx: qctx, Store: st}).Range(vals.Get("query"), start, end, step)
	}); err != nil {
		return h.server.WriteErr(c, err)
	}

	if qerr != nil {
		return queryError(c, qerr)
	}

	return c.JSON(http.StatusOK, promResponse{Status: statusSuccess, Data: resultJSON(m)})
}

// matchSelectors parses match[] arguments into matcher sets.
func matchSelectors(vals url.Values) ([][]*promql.Matcher, error) {
	out := make([][]*promql.Matcher, 0, len(vals["match[]"]))

	for _, raw := range vals["match[]"] {
		e, err := promql.Parse(raw)
		if err != nil {
			return nil, err
		}

		vs, ok := e.(*promql.VectorSelector)
		if !ok {
			return nil, fmt.Errorf("match[] must be a vector selector: %w", errBadParam)
		}

		out = append(out, vs.Matchers)
	}

	return out, nil
}

// timeBounds returns the sample window of start/end, defaulting to all of time.
func timeBounds(vals url.Values) (int64, int64, error) {
	minT, maxT := int64(math.MinInt64), int64(math.MaxInt64)

	if raw := vals.Get("start"); raw != "" {
		t, err := parseTime(raw, time.Time{})
		if err != nil {
			return 0, 0, err
		}

		minT = t.UnixMilli()
	}

	if raw := vals.Get("end"); raw != "" {
		t, err := parseTime(raw, time.Time{})
		if err != nil {
			return 0, 0, err
		}

		maxT = t.UnixMilli()
	}

	return minT, maxT, nil
}

// seriesResult is the outcome of resolving a series query; when done is set, err is the already-written response.
type seriesResult struct {
	err    error
	series []promql.Labels
	done   bool
}

// matchingSeries resolves match[] and the time window.
func (h *Handler) matchingSeries(c *echo.Context, wsID string, vals url.Values, requireMatch bool) seriesResult {
	sels, err := matchSelectors(vals)
	if err != nil {
		return seriesResult{done: true, err: promError(c, http.StatusBadRequest, "bad_data", err.Error())}
	}

	if len(sels) == 0 {
		if requireMatch {
			return seriesResult{
				done: true, err: promError(c, http.StatusBadRequest, "bad_data", "no match[] parameter provided"),
			}
		}

		sels = [][]*promql.Matcher{{allNamed()}}
	}

	minT, maxT, err := timeBounds(vals)
	if err != nil {
		return seriesResult{done: true, err: promError(c, http.StatusBadRequest, "bad_data", err.Error())}
	}

	series, err := h.Backend.SeriesLabels(c.Request().Context(), wsID, sels, minT, maxT)
	if err != nil {
		return seriesResult{done: true, err: h.server.WriteErr(c, err)}
	}

	return seriesResult{series: series}
}

// allNamed matches every series that has a metric name.
func allNamed() *promql.Matcher {
	m, _ := promql.NewMatcher(promql.MatchRegexp, promql.NameLabel, ".+")

	return m
}

func (h *Handler) seriesQuery(c *echo.Context, wsID string, vals url.Values) error {
	res := h.matchingSeries(c, wsID, vals, true)
	if res.done {
		return res.err
	}

	out := res.series

	if out == nil {
		out = []promql.Labels{}
	}

	return c.JSON(http.StatusOK, promResponse{Status: statusSuccess, Data: out})
}

func (h *Handler) labelNames(c *echo.Context, wsID string, vals url.Values) error {
	res := h.matchingSeries(c, wsID, vals, false)
	if res.done {
		return res.err
	}

	series := res.series

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

	return c.JSON(http.StatusOK, promResponse{Status: statusSuccess, Data: names})
}

func (h *Handler) labelValues(c *echo.Context, wsID, name string, vals url.Values) error {
	if !labelNameRe.MatchString(name) {
		return promError(c, http.StatusBadRequest, "bad_data", "invalid label name: "+name)
	}

	res := h.matchingSeries(c, wsID, vals, false)
	if res.done {
		return res.err
	}

	series := res.series

	set := map[string]bool{}

	for _, l := range series {
		if v, ok := l[name]; ok {
			set[v] = true
		}
	}

	values := make([]string, 0, len(set))
	for v := range set {
		values = append(values, v)
	}

	slices.Sort(values)

	return c.JSON(http.StatusOK, promResponse{Status: statusSuccess, Data: values})
}
