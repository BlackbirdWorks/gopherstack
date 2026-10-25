// Package kql implements a subset of the Kusto Query Language for the Azure
// Monitor Logs emulator.
package kql

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

// Type is a KQL column type name as it appears in query results.
type Type string

// Column and expression types.
const (
	TBool     Type = "bool"
	TInt      Type = "int"
	TLong     Type = "long"
	TReal     Type = "real"
	TString   Type = "string"
	TDatetime Type = "datetime"
	TTimespan Type = "timespan"
	TDynamic  Type = "dynamic"
	TGUID     Type = "guid"
)

// Col is a named, typed column.
type Col struct {
	Name string
	Type Type
}

// Rel is a materialised table. Cells hold nil, bool, int64, float64, string,
// time.Time, time.Duration, []any or map[string]any.
type Rel struct {
	Cols []Col
	Rows [][]any
}

const (
	ticksPerSecond    = 10_000_000
	nanosPerTick      = 100
	ticksAtUnixEpoch  = 621355968000000000
	timeFormat7       = "2006-01-02T15:04:05.0000000Z"
	secondsPerMinute  = 60
	minutesPerHour    = 60
	hoursPerDay       = 24
	asciiPrintableMin = 0x20
)

func isNumeric(v any) bool {
	switch v.(type) {
	case int64, float64:
		return true
	}

	return false
}

func isNumType(t Type) bool { return t == TInt || t == TLong || t == TReal }

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case bool:
		if x {
			return 1, true
		}

		return 0, true
	}

	return 0, false
}

func toInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}

		return int64(x), true
	case bool:
		if x {
			return 1, true
		}

		return 0, true
	}

	return 0, false
}

// compareValues orders two values of comparable kinds.
func compareValues(a, b any) (int, bool) {
	switch x := a.(type) {
	case int64:
		if y, ok := b.(int64); ok {
			return cmpOrdered(x, y), true
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y), true
		}
	case time.Time:
		if y, ok := b.(time.Time); ok {
			return x.Compare(y), true
		}
	case time.Duration:
		if y, ok := b.(time.Duration); ok {
			return cmpOrdered(x, y), true
		}
	case bool:
		if y, ok := b.(bool); ok {
			return cmpOrdered(boolInt(x), boolInt(y)), true
		}
	}

	if fa, ok := a.(float64); ok || isNumeric(a) {
		if !ok {
			fa, _ = toFloat(a)
		}

		if fb, okb := toFloat(b); okb && isNumeric(b) {
			return cmpOrdered(fa, fb), true
		}
	}

	return 0, false
}

func cmpOrdered[T int | int64 | float64 | time.Duration](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func boolInt(b bool) int {
	if b {
		return 1
	}

	return 0
}

// valuesEqual is deep equality used by distinct, dcount and group keys.
func valuesEqual(a, b any) bool {
	return groupKey(a) == groupKey(b)
}

func groupKey(v any) string {
	switch x := v.(type) {
	case nil:
		return "n"
	case string:
		return "s" + x
	case int64:
		return "i" + strconv.FormatInt(x, 10)
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1e15 {
			return "i" + strconv.FormatInt(int64(x), 10)
		}

		return "f" + strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return "b" + strconv.FormatBool(x)
	case time.Time:
		return "t" + strconv.FormatInt(x.UnixNano(), 10)
	case time.Duration:
		return "d" + strconv.FormatInt(int64(x), 10)
	}

	raw, _ := json.Marshal(v)

	return "j" + string(raw)
}

// FormatTime renders a datetime the way the query API returns it.
func FormatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func formatTime7(t time.Time) string { return t.UTC().Format(timeFormat7) }

// FormatTimespan renders a timespan as [-][d.]hh:mm:ss[.fffffff].
func FormatTimespan(d time.Duration) string {
	var sb strings.Builder

	if d < 0 {
		sb.WriteByte('-')

		d = -d
	}

	ticks := int64(d) / nanosPerTick
	days := ticks / (ticksPerSecond * secondsPerMinute * minutesPerHour * hoursPerDay)
	ticks -= days * ticksPerSecond * secondsPerMinute * minutesPerHour * hoursPerDay

	if days > 0 {
		sb.WriteString(strconv.FormatInt(days, 10))
		sb.WriteByte('.')
	}

	h := ticks / (ticksPerSecond * secondsPerMinute * minutesPerHour)
	ticks -= h * ticksPerSecond * secondsPerMinute * minutesPerHour
	m := ticks / (ticksPerSecond * secondsPerMinute)
	ticks -= m * ticksPerSecond * secondsPerMinute
	s := ticks / ticksPerSecond
	frac := ticks % ticksPerSecond

	sb.WriteString(pad2(h) + ":" + pad2(m) + ":" + pad2(s))

	if frac > 0 {
		f := strconv.FormatInt(frac, 10)
		sb.WriteString("." + strings.Repeat("0", 7-len(f)) + f)
	}

	return sb.String()
}

func pad2(n int64) string {
	if n < 10 {
		return "0" + strconv.FormatInt(n, 10)
	}

	return strconv.FormatInt(n, 10)
}

// ToString implements KQL tostring().
func ToString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return formatReal(x)
	case time.Time:
		return formatTime7(x)
	case time.Duration:
		return FormatTimespan(x)
	}

	return jsonString(v)
}

func formatReal(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}

	return strconv.FormatFloat(f, 'g', -1, 64)
}

func jsonString(v any) string {
	raw, err := json.Marshal(jsonable(v))
	if err != nil {
		return ""
	}

	return string(raw)
}

// jsonable converts engine values to encoding/json friendly ones.
func jsonable(v any) any {
	switch x := v.(type) {
	case time.Time:
		return FormatTime(x)
	case time.Duration:
		return FormatTimespan(x)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return formatReal(x)
		}
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonable(e)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = jsonable(e)
		}

		return out
	}

	return v
}

// ResultValue converts a cell to its query-API JSON representation.
func ResultValue(t Type, v any) any {
	if v == nil {
		return nil
	}

	switch t { //nolint:exhaustive // other types render as stored
	case TDynamic:
		return jsonString(v)
	case TDatetime:
		if tv, ok := v.(time.Time); ok {
			return FormatTime(tv)
		}
	case TTimespan:
		if d, ok := v.(time.Duration); ok {
			return FormatTimespan(d)
		}
	}

	return jsonable(v)
}

// NormalizeJSON converts decoded JSON (float64 numbers) into engine dynamic values.
func NormalizeJSON(v any) any {
	switch x := v.(type) {
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1<<53 {
			return int64(x)
		}

		return x
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}

		f, _ := x.Float64()

		return f
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = NormalizeJSON(e)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = NormalizeJSON(e)
		}

		return out
	}

	return v
}
