package bqsql

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Canonical stored encodings of temporal values.
const (
	TimestampLayout   = "2006-01-02 15:04:05.000000"
	DateLayout        = "2006-01-02"
	DatetimeLayout    = "2006-01-02T15:04:05.000000"
	TimeLayout        = "15:04:05.000000"
	numericScale      = 9
	microsPerSecond   = 1e6
	nanosPerMicroUnit = 1e3
	nanosPerSecondF   = 1e9
	bitSize           = 64
	decimalBase       = 10
)

// ErrValue is wrapped by every value conversion failure.
var ErrValue = errors.New("invalid value")

func valueErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValue, fmt.Sprintf(format, args...))
}

// ParseOptions tunes ParseValue.
type ParseOptions struct {
	IgnoreUnknown bool
}

// ParseValue converts a decoded JSON value into the stored form for f. Stored scalars are strings, records are
// positional []any and repeated fields are []any; NULL is nil.
func ParseValue(f Field, v any, o ParseOptions) (any, error) {
	if strings.EqualFold(f.Mode, ModeRepeated) {
		return parseRepeated(f, v, o)
	}

	return parseSingle(f, v, o)
}

func parseRepeated(f Field, v any, o ParseOptions) (any, error) {
	if v == nil {
		return []any{}, nil
	}

	arr, ok := v.([]any)
	if !ok {
		return nil, valueErr("field %s is repeated and requires an array", f.Name)
	}

	elem := f
	elem.Mode = ModeNullable
	out := make([]any, len(arr))

	for i, e := range arr {
		if e == nil {
			return nil, valueErr("array field %s contains a null element", f.Name)
		}

		s, err := parseSingle(elem, e, o)
		if err != nil {
			return nil, err
		}

		out[i] = s
	}

	return out, nil
}

func parseSingle(f Field, v any, o ParseOptions) (any, error) {
	if v == nil {
		if strings.EqualFold(f.Mode, ModeRequired) {
			return nil, valueErr("field %s is required", f.Name)
		}

		return nil, nil //nolint:nilnil // NULL is the stored nil
	}

	if k, _ := KindOf(f.Type); k == KindStruct {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, valueErr("field %s is a record and requires an object", f.Name)
		}

		return ParseRecord(f.Fields, obj, o)
	}

	k, _ := KindOf(f.Type)

	s, err := scalarStored(k, v)
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", f.Name, err)
	}

	return s, nil
}

// ParseRecord converts a JSON object into a positional record for fields.
func ParseRecord(fields []Field, obj map[string]any, o ParseOptions) ([]any, error) {
	index := make(map[string]int, len(fields))
	for i, f := range fields {
		index[strings.ToLower(f.Name)] = i
	}

	for k := range obj {
		if _, ok := index[strings.ToLower(k)]; !ok && !o.IgnoreUnknown {
			return nil, valueErr("no such field: %s", k)
		}
	}

	lowered := make(map[string]any, len(obj))
	for k, v := range obj {
		lowered[strings.ToLower(k)] = v
	}

	row := make([]any, len(fields))

	for i, f := range fields {
		cell, err := ParseValue(f, lowered[strings.ToLower(f.Name)], o)
		if err != nil {
			return nil, err
		}

		row[i] = cell
	}

	return row, nil
}

// scalarStored normalises a scalar of kind k into its canonical stored string.
func scalarStored(k Kind, v any) (string, error) {
	switch k {
	case KindInt:
		return storedInt(v)
	case KindFloat:
		return storedFloat(v)
	case KindBool:
		return storedBool(v)
	case KindBytes:
		return storedBytes(v)
	case KindNumeric, KindBigNumeric:
		return storedNumeric(v)
	case KindTimestamp, KindDate, KindTime, KindDatetime:
		return storedTemporal(k, v)
	case KindJSON:
		return storedJSON(v)
	case KindUnknown, KindString, KindGeography, KindStruct, KindArray:
		return storedString(v)
	}

	return storedString(v)
}

func storedString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case json.Number:
		return t.String(), nil
	case bool:
		return strconv.FormatBool(t), nil
	case int64:
		return strconv.FormatInt(t, decimalBase), nil
	case float64:
		return FormatFloat(t), nil
	default:
		return "", valueErr("cannot use %T as STRING", v)
	}
}

func storedJSON(v any) (string, error) {
	if s, ok := v.(string); ok {
		if !json.Valid([]byte(s)) {
			return "", valueErr("invalid JSON text")
		}

		return s, nil
	}

	b, err := json.Marshal(v)
	if err != nil {
		return "", valueErr("cannot encode JSON: %v", err)
	}

	return string(b), nil
}

func numberText(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t), true
	case []byte:
		return strings.TrimSpace(string(t)), true
	case json.Number:
		return t.String(), true
	case int64:
		return strconv.FormatInt(t, decimalBase), true
	case float64:
		return strconv.FormatFloat(t, 'g', -1, bitSize), true
	case int:
		return strconv.Itoa(t), true
	case int32:
		return strconv.FormatInt(int64(t), decimalBase), true
	case uint32:
		return strconv.FormatUint(uint64(t), decimalBase), true
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32), true
	default:
		return "", false
	}
}

func storedInt(v any) (string, error) {
	s, ok := numberText(v)
	if !ok {
		return "", valueErr("cannot use %T as INT64", v)
	}

	if n, err := strconv.ParseInt(s, decimalBase, bitSize); err == nil {
		return strconv.FormatInt(n, decimalBase), nil
	}

	f, err := strconv.ParseFloat(s, bitSize)
	if err != nil || f != math.Trunc(f) || math.Abs(f) >= math.MaxInt64 {
		return "", valueErr("invalid INT64 value %q", s)
	}

	return strconv.FormatInt(int64(f), decimalBase), nil
}

func storedFloat(v any) (string, error) {
	s, ok := numberText(v)
	if !ok {
		return "", valueErr("cannot use %T as FLOAT64", v)
	}

	f, err := parseFloatText(s)
	if err != nil {
		return "", valueErr("invalid FLOAT64 value %q", s)
	}

	return FormatFloat(f), nil
}

func parseFloatText(s string) (float64, error) {
	switch strings.ToLower(s) {
	case "nan":
		return math.NaN(), nil
	case "inf", "+inf", "infinity", "+infinity":
		return math.Inf(1), nil
	case "-inf", "-infinity":
		return math.Inf(-1), nil
	}

	return strconv.ParseFloat(s, bitSize)
}

// FormatFloat renders f as the REST API does: shortest round-trip digits, with ".0" on whole numbers.
func FormatFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}

	s := strconv.FormatFloat(f, 'g', -1, bitSize)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}

	return s
}

func storedBool(v any) (string, error) {
	switch t := v.(type) {
	case bool:
		return strconv.FormatBool(t), nil
	case int64:
		return strconv.FormatBool(t != 0), nil
	case json.Number:
		return strconv.FormatBool(t.String() != "0"), nil
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case litTrueX, "t", "yes", "y", "1":
			return litTrueX, nil
		case litFalseX, "f", "no", "n", "0":
			return litFalseX, nil
		}
	}

	return "", valueErr("cannot use %v as BOOL", v)
}

func storedBytes(v any) (string, error) {
	switch t := v.(type) {
	case string:
		if _, err := base64.StdEncoding.DecodeString(t); err != nil {
			return "", valueErr("BYTES value is not valid base64")
		}

		return t, nil
	case []byte:
		return base64.StdEncoding.EncodeToString(t), nil
	default:
		return "", valueErr("cannot use %T as BYTES", v)
	}
}

func storedNumeric(v any) (string, error) {
	s, ok := numberText(v)
	if !ok {
		return "", valueErr("cannot use %T as NUMERIC", v)
	}

	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return "", valueErr("invalid NUMERIC value %q", s)
	}

	return trimDecimal(r.FloatString(numericScale)), nil
}

// FormatNumeric renders f as a NUMERIC with scale 9 and no trailing zeros.
func FormatNumeric(f float64) string {
	return trimDecimal(strconv.FormatFloat(f, 'f', numericScale, bitSize))
}

func trimDecimal(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}

	s = strings.TrimRight(s, "0")

	return strings.TrimSuffix(s, ".")
}

func storedTemporal(k Kind, v any) (string, error) {
	t, err := ParseTemporal(k, v)
	if err != nil {
		return "", err
	}

	return FormatTemporal(k, t), nil
}

// FormatTemporal renders t in the canonical stored encoding for k.
func FormatTemporal(k Kind, t time.Time) string {
	t = t.UTC()

	switch k {
	case KindDate:
		return t.Format(DateLayout)
	case KindTime:
		return t.Format(TimeLayout)
	case KindDatetime:
		return t.Format(DatetimeLayout)
	case KindUnknown, KindInt, KindFloat, KindBool, KindString, KindBytes, KindTimestamp, KindNumeric,
		KindBigNumeric, KindJSON, KindGeography, KindStruct, KindArray:
		return t.Format(TimestampLayout)
	}

	return t.Format(TimestampLayout)
}

func timestampFormats() []string {
	return []string{
		"2006-01-02 15:04:05.999999999 MST",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999 Z07:00",
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05Z07",
		"2006-01-02 15:04Z07:00",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		DateLayout,
	}
}

// ParseTemporal parses the many spellings BigQuery accepts for TIMESTAMP, DATE, DATETIME and TIME.
func ParseTemporal(k Kind, v any) (time.Time, error) {
	if k == KindTimestamp {
		if t, ok := epochTime(v); ok {
			return t, nil
		}
	}

	s, ok := v.(string)
	if !ok {
		if b, isBytes := v.([]byte); isBytes {
			s, ok = string(b), true
		}
	}

	if !ok {
		return time.Time{}, valueErr("cannot use %T as %s", v, k.RESTName())
	}

	s = strings.TrimSpace(s)

	if k == KindTime {
		return parseClock(s)
	}

	for _, layout := range timestampFormats() {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}

	if k == KindTimestamp {
		if t, isEpoch := epochTime(s); isEpoch {
			return t, nil
		}
	}

	return time.Time{}, valueErr("invalid %s value %q", k.RESTName(), s)
}

func parseClock(s string) (time.Time, error) {
	for _, layout := range []string{"15:04:05.999999999", "15:04:05", "15:04"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, valueErr("invalid TIME value %q", s)
}

func epochTime(v any) (time.Time, bool) {
	var f float64

	switch t := v.(type) {
	case json.Number:
		g, err := t.Float64()
		if err != nil {
			return time.Time{}, false
		}

		f = g
	case float64:
		f = t
	case int64:
		f = float64(t)
	case string:
		if strings.ContainsAny(t, ":- ") && !strings.ContainsAny(t, "eE") {
			return time.Time{}, false
		}

		g, err := strconv.ParseFloat(strings.TrimSpace(t), bitSize)
		if err != nil {
			return time.Time{}, false
		}

		f = g
	default:
		return time.Time{}, false
	}

	sec, frac := math.Modf(f)

	return time.Unix(int64(sec), int64(math.Round(frac*microsPerSecond))*nanosPerMicroUnit).UTC(), true
}

// CellOptions tunes the REST cell encoding.
type CellOptions struct {
	// Int64Timestamp renders TIMESTAMP cells as integer microseconds, as formatOptions.useInt64Timestamp requests.
	Int64Timestamp bool
}

// Cell converts a stored value into the REST "v" encoding for f.
func Cell(f Field, stored any) any { return CellFor(f, stored, CellOptions{}) }

// CellFor converts a stored value into the REST "v" encoding for f under o.
func CellFor(f Field, stored any, o CellOptions) any {
	if strings.EqualFold(f.Mode, ModeRepeated) {
		arr, _ := stored.([]any)
		elem := f
		elem.Mode = ModeNullable
		out := make([]map[string]any, len(arr))

		for i, e := range arr {
			out[i] = map[string]any{"v": CellFor(elem, e, o)}
		}

		return out
	}

	if stored == nil {
		return nil
	}

	k, _ := KindOf(f.Type)
	if k == KindStruct {
		rec, _ := stored.([]any)

		return map[string]any{"f": recordCells(f.Fields, rec, o)}
	}

	s, _ := stored.(string)

	return wireScalar(k, s, o)
}

func recordCells(fields []Field, rec []any, o CellOptions) []map[string]any {
	out := make([]map[string]any, len(fields))

	for i, fl := range fields {
		var v any
		if i < len(rec) {
			v = rec[i]
		}

		out[i] = map[string]any{"v": CellFor(fl, v, o)}
	}

	return out
}

// Row converts a stored row into the REST {"f":[{"v":...}]} encoding.
func Row(fields []Field, row []any) map[string]any { return RowFor(fields, row, CellOptions{}) }

// RowFor converts a stored row into the REST {"f":[{"v":...}]} encoding under o.
func RowFor(fields []Field, row []any, o CellOptions) map[string]any {
	return map[string]any{"f": recordCells(fields, row, o)}
}

func wireScalar(k Kind, s string, o CellOptions) string {
	switch k {
	case KindTimestamp:
		t, err := time.Parse(TimestampLayout, s)
		if err != nil {
			return s
		}

		if o.Int64Timestamp {
			return strconv.FormatInt(t.UnixMicro(), decimalBase)
		}

		return epochSeconds(t)
	case KindDatetime:
		return trimFraction(s)
	case KindTime:
		return trimFraction(s)
	case KindUnknown, KindInt, KindFloat, KindBool, KindString, KindBytes, KindDate, KindNumeric,
		KindBigNumeric, KindJSON, KindGeography, KindStruct, KindArray:
		return s
	}

	return s
}

func trimFraction(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}

	s = strings.TrimRight(s, "0")

	return strings.TrimSuffix(s, ".")
}

func epochSeconds(t time.Time) string {
	secs := float64(t.Unix()) + float64(t.Nanosecond())/nanosPerSecondF
	s := strconv.FormatFloat(secs, 'E', -1, bitSize)

	if i := strings.Index(s, "E"); i >= 0 {
		exp := strings.TrimPrefix(s[i+1:], "+")
		neg := strings.HasPrefix(exp, "-")
		exp = strings.TrimLeft(strings.TrimPrefix(exp, "-"), "0")

		if exp == "" {
			exp = "0"
		}

		if neg {
			exp = "-" + exp
		}

		s = s[:i] + "E" + exp
	}

	if mant, _, found := strings.Cut(s, "E"); found && !strings.Contains(mant, ".") {
		s = strings.Replace(s, "E", ".0E", 1)
	}

	return s
}

// LogicalBytes estimates the logical storage size of a stored value the way BigQuery bills it.
func LogicalBytes(f Field, stored any) int64 {
	if strings.EqualFold(f.Mode, ModeRepeated) {
		arr, _ := stored.([]any)
		elem := f
		elem.Mode = ModeNullable

		var n int64
		for _, e := range arr {
			n += LogicalBytes(elem, e)
		}

		return n
	}

	if stored == nil {
		return 0
	}

	k, _ := KindOf(f.Type)

	switch k {
	case KindStruct:
		rec, _ := stored.([]any)

		var n int64

		for i, fl := range f.Fields {
			if i < len(rec) {
				n += LogicalBytes(fl, rec[i])
			}
		}

		return n
	case KindInt, KindFloat, KindTimestamp, KindDatetime, KindTime:
		return 8 //nolint:mnd // BigQuery logical size
	case KindBool:
		return 1
	case KindDate:
		return 4 //nolint:mnd // BigQuery logical size
	case KindNumeric:
		return 16 //nolint:mnd // BigQuery logical size
	case KindBigNumeric:
		return 32 //nolint:mnd // BigQuery logical size
	case KindUnknown, KindString, KindBytes, KindJSON, KindGeography, KindArray:
		s, _ := stored.(string)

		return int64(len(s)) + 2 //nolint:mnd // BigQuery adds two bytes to variable-length values
	}

	return 0
}
