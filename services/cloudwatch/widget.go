package cloudwatch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image/png"
	"math"
	"strconv"
	"strings"
	"time"
)

// Widget rendering constants.
const (
	widgetDefaultWidth  = 600
	widgetDefaultHeight = 400
	widgetMinDimension  = 1
	widgetMaxDimension  = 2000
	widgetDefaultPeriod = 300
	widgetDefaultStat   = "Average"
	widgetDefaultWindow = 3 * time.Hour
	dayDuration         = 24 * time.Hour

	widgetMarginLeft   = 48
	widgetMarginRight  = 16
	widgetMarginTop    = 28
	widgetMarginBottom = 24
	widgetGridLines    = 4
	fontPixel          = 1
	glyphW             = 3
	glyphH             = 5
	glyphSpace         = 1

	// minSeriesTokens is the minimum leading strings (namespace + metric name) a
	// metrics entry must have; dimStride is the name/value stride for dimensions.
	minSeriesTokens = 2
	dimStride       = 2
)

// widgetSpec is the decoded MetricWidget JSON document.
type widgetSpec struct {
	Title   string              `json:"title"`
	Start   string              `json:"start"`
	End     string              `json:"end"`
	Stat    string              `json:"stat"`
	Metrics [][]json.RawMessage `json:"metrics"`
	Width   int                 `json:"width"`
	Height  int                 `json:"height"`
	Period  int                 `json:"period"`
}

// widgetSeriesQuery describes a single metric line to plot.
type widgetSeriesQuery struct {
	namespace  string
	metricName string
	stat       string
	label      string
	dimensions []Dimension
	period     int
}

// widgetPoint is a single rendered data point.
type widgetPoint struct {
	t time.Time
	v float64
}

// renderMetricWidgetPNG parses a MetricWidget JSON document, resolves its metric
// series from the backend, and renders a genuine PNG line plot at the requested
// dimensions using only the standard library. It never returns a nil image: on
// malformed input or missing data it still emits a correctly-sized PNG.
func renderMetricWidgetPNG(bk *InMemoryBackend, widgetJSON string, now time.Time) ([]byte, error) {
	spec := parseWidgetSpec(widgetJSON)
	width := clampDimension(spec.Width, widgetDefaultWidth)
	height := clampDimension(spec.Height, widgetDefaultHeight)

	start, end := resolveWidgetRange(spec, now)

	var series [][]widgetPoint
	var labels []string
	if bk != nil {
		for _, elem := range spec.Metrics {
			q, ok := parseWidgetSeries(elem, spec)
			if !ok {
				continue
			}
			pts := resolveWidgetSeries(bk, q, start, end)
			series = append(series, pts)
			labels = append(labels, q.label)
		}
	}

	img := drawWidget(width, height, spec.Title, series, labels)

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, fmt.Errorf("encode widget png: %w", err)
	}

	return buf.Bytes(), nil
}

// parseWidgetSpec decodes the MetricWidget JSON, tolerating malformed input by
// returning a zero-value spec (callers fall back to defaults).
func parseWidgetSpec(widgetJSON string) widgetSpec {
	var spec widgetSpec
	if strings.TrimSpace(widgetJSON) == "" {
		return spec
	}
	_ = json.Unmarshal([]byte(widgetJSON), &spec)

	return spec
}

// clampDimension clamps a requested pixel dimension to sane bounds, using def when
// the requested value is zero/unset.
func clampDimension(v, def int) int {
	if v <= 0 {
		v = def
	}
	if v < widgetMinDimension {
		v = widgetMinDimension
	}
	if v > widgetMaxDimension {
		v = widgetMaxDimension
	}

	return v
}

// resolveWidgetRange determines the [start, end) time window for the widget. It
// supports absolute RFC3339 timestamps and the common relative forms used by the
// console ("-PT3H", "-P1D", "PT0H"); anything unrecognised falls back to the
// default trailing window ending at now.
func resolveWidgetRange(spec widgetSpec, now time.Time) (time.Time, time.Time) {
	end := now
	if t, ok := parseWidgetTime(spec.End, now); ok {
		end = t
	}

	start := end.Add(-widgetDefaultWindow)
	if t, ok := parseWidgetTime(spec.Start, end); ok {
		start = t
	}

	if !start.Before(end) {
		start = end.Add(-widgetDefaultWindow)
	}

	return start, end
}

// parseWidgetTime parses an absolute RFC3339 timestamp or a signed ISO-8601-style
// relative offset from base (e.g. "-PT3H", "-P1D"). Returns false when unparseable.
func parseWidgetTime(s string, base time.Time) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}

	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}

	if d, ok := parseRelativeDuration(s); ok {
		return base.Add(d), true
	}

	return time.Time{}, false
}

// parseRelativeDuration parses a signed ISO-8601 duration such as "-PT3H30M",
// "-P1D", or "PT0H" into a time.Duration. Only day/hour/minute/second components
// are honoured (months/years are not fixed-length and are rejected).
func parseRelativeDuration(s string) (time.Duration, bool) {
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}

	if !strings.HasPrefix(s, "P") {
		return 0, false
	}
	s = s[1:]

	datePart, timePart, hasTime := strings.Cut(s, "T")
	if !hasTime {
		datePart, timePart = s, ""
	}

	var total time.Duration
	if d, ok := scanDurationComponent(datePart, 'D', dayDuration); ok {
		total += d
	} else if datePart != "" {
		return 0, false
	}

	for _, comp := range []struct {
		unit  byte
		scale time.Duration
	}{{'H', time.Hour}, {'M', time.Minute}, {'S', time.Second}} {
		before, after, found := strings.Cut(timePart, string(comp.unit))
		if !found {
			continue
		}
		n, err := strconv.ParseFloat(before, 64)
		if err != nil {
			return 0, false
		}
		total += time.Duration(n * float64(comp.scale))
		timePart = after
	}

	if negative {
		total = -total
	}

	return total, true
}

// scanDurationComponent parses a single "<n><unit>" component (e.g. "1D").
func scanDurationComponent(s string, unit byte, scale time.Duration) (time.Duration, bool) {
	before, _, found := strings.Cut(s, string(unit))
	if !found {
		return 0, false
	}
	n, err := strconv.ParseFloat(before, 64)
	if err != nil {
		return 0, false
	}

	return time.Duration(n * float64(scale)), true
}

// parseWidgetSeries decodes one entry of the MetricWidget "metrics" array into a
// series query. The entry is a heterogeneous JSON array: leading strings are the
// namespace, metric name, then dimension name/value pairs, optionally followed by
// an options object ({"stat":...,"period":...,"label":...}). Render-only entries
// (leading object, e.g. metric-math expressions) are skipped.
func parseWidgetSeries(elem []json.RawMessage, spec widgetSpec) (widgetSeriesQuery, bool) {
	q := widgetSeriesQuery{
		stat:   firstNonEmpty(spec.Stat, widgetDefaultStat),
		period: firstPositive(spec.Period, widgetDefaultPeriod),
	}

	var strs []string
	var opts map[string]json.RawMessage

	for _, raw := range elem {
		trimmed := strings.TrimSpace(string(raw))
		if strings.HasPrefix(trimmed, "{") {
			_ = json.Unmarshal(raw, &opts)

			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			strs = append(strs, s)
		}
	}

	if len(strs) < minSeriesTokens {
		return widgetSeriesQuery{}, false
	}

	q.namespace = strs[0]
	q.metricName = strs[1]

	for i := minSeriesTokens; i+1 < len(strs); i += dimStride {
		q.dimensions = append(q.dimensions, Dimension{Name: strs[i], Value: strs[i+1]})
	}

	applyWidgetOptions(&q, opts)

	if q.label == "" {
		q.label = q.metricName
	}

	return q, true
}

// applyWidgetOptions overlays the trailing options object onto a series query.
func applyWidgetOptions(q *widgetSeriesQuery, opts map[string]json.RawMessage) {
	if opts == nil {
		return
	}
	if raw, ok := opts["stat"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			q.stat = s
		}
	}
	if raw, ok := opts["label"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			q.label = s
		}
	}
	if raw, ok := opts["period"]; ok {
		var p int
		if json.Unmarshal(raw, &p) == nil && p > 0 {
			q.period = p
		}
	}
}

// resolveWidgetSeries fetches and aggregates a series' data points from the backend.
func resolveWidgetSeries(
	bk *InMemoryBackend,
	q widgetSeriesQuery,
	start, end time.Time,
) []widgetPoint {
	dps, err := bk.GetMetricStatistics(
		q.namespace, q.metricName, q.dimensions,
		start, end, clampPeriodInt32(q.period),
		[]string{q.stat}, nil,
	)
	if err != nil {
		return nil
	}

	pts := make([]widgetPoint, 0, len(dps))
	for _, dp := range dps {
		pts = append(pts, widgetPoint{t: dp.Timestamp, v: statValue(dp, q.stat)})
	}

	return pts
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func firstPositive(a, b int) int {
	if a > 0 {
		return a
	}

	return b
}

// clampPeriodInt32 converts a period (seconds) to int32, guarding against overflow
// and non-positive values by falling back to the default period.
func clampPeriodInt32(p int) int32 {
	if p < 1 {
		return widgetDefaultPeriod
	}
	if p > math.MaxInt32 {
		return math.MaxInt32
	}

	return int32(p)
}
