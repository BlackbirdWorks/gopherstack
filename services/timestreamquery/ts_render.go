package timestreamquery

import (
	"bytes"
	"encoding/binary"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	rowOrderKey  = "\x00row"
	ivBlobLen    = 13
	ivKindOffset = 4
	ivSignFlip   = uint64(1) << 63
	msToNs       = int64(time.Millisecond)
)

func ivBlobMagic() []byte { return []byte{0xF3, 'i', 'v', 0x1D} }

// renderValue formats an engine value as a Datum of the static type t.
func renderValue(v any, t tsType) Datum {
	if v == nil {
		return NullDatum()
	}

	switch t.kind {
	case tyArray, tyRow, tyTimeseries:
		return renderComplexValue(v, t)
	default:
		return renderScalar(v, t.kind)
	}
}

func renderScalar(v any, k tsKind) Datum {
	if b, ok := v.([]byte); ok {
		if kind, n, isIv := decodeIntervalBlob(b); isIv {
			return renderIntervalBlob(kind, n)
		}

		v = string(b)
	}

	switch k {
	case tyBoolean:
		return renderBool(v)
	case tyInteger, tyBigint:
		return renderInt(v)
	case tyDouble:
		return renderDouble(v)
	case tyTimestamp:
		return renderTimestamp(v)
	case tyDate:
		return renderDate(v)
	case tyTime:
		return renderTimeOfDay(v)
	case tyIntervalDS:
		return renderIntervalDS(v)
	default:
		return renderText(v)
	}
}

func decodeIntervalBlob(b []byte) (byte, int64, bool) {
	if len(b) != ivBlobLen || !bytes.HasPrefix(b, ivBlobMagic()) {
		return 0, 0, false
	}

	raw := binary.BigEndian.Uint64(b[ivKindOffset+1:]) ^ ivSignFlip

	return b[ivKindOffset], int64(raw), true //nolint:gosec // offset-binary payload, wrap-around intended
}

func renderIntervalBlob(kind byte, n int64) Datum {
	if kind == 'Y' {
		return ScalarDatum(fmt.Sprintf("%d-%d", n/monthsPerYear, n%monthsPerYear))
	}

	return ScalarDatum(formatInterval(n * msToNs))
}

const monthsPerYear = 12

func renderBool(v any) Datum {
	switch x := v.(type) {
	case int64:
		return ScalarDatum(strconv.FormatBool(x != 0))
	case float64:
		return ScalarDatum(strconv.FormatBool(x != 0))
	case bool:
		return ScalarDatum(strconv.FormatBool(x))
	case string:
		return ScalarDatum(strconv.FormatBool(strings.EqualFold(x, "true") || x == "1"))
	}

	return renderText(v)
}

func renderInt(v any) Datum {
	switch x := v.(type) {
	case int64:
		return ScalarDatum(strconv.FormatInt(x, 10))
	case float64:
		if x != math.Trunc(x) || x >= twoPow63 || x < -twoPow63 {
			return renderDouble(x)
		}

		return ScalarDatum(strconv.FormatInt(int64(x), 10))
	case bool:
		if x {
			return ScalarDatum("1")
		}

		return ScalarDatum("0")
	}

	return renderText(v)
}

func renderDouble(v any) Datum {
	switch x := v.(type) {
	case int64:
		return ScalarDatum(javaDouble(float64(x)))
	case float64:
		return ScalarDatum(javaDouble(x))
	case string:
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return ScalarDatum(javaDouble(f))
		}
	}

	return renderText(v)
}

func renderTimestamp(v any) Datum {
	if s, ok := v.(string); ok {
		if t, parsed := parseTSValue(s); parsed {
			return ScalarDatum(formatTS(t))
		}
	}

	if t, ok := v.(time.Time); ok {
		return ScalarDatum(formatTS(t))
	}

	return renderText(v)
}

func renderDate(v any) Datum {
	if s, ok := v.(string); ok {
		if t, parsed := parseTSValue(s); parsed {
			return ScalarDatum(t.Format(time.DateOnly))
		}
	}

	return renderText(v)
}

func renderTimeOfDay(v any) Datum {
	s, ok := v.(string)
	if !ok {
		return renderText(v)
	}

	for _, l := range []string{layoutClockFrac, layoutClock} {
		if t, err := time.Parse(l, s); err == nil {
			return ScalarDatum(t.Format("15:04:05.000000000"))
		}
	}

	return ScalarDatum(s)
}

func renderIntervalDS(v any) Datum {
	if n, ok := v.(int64); ok {
		return ScalarDatum(formatInterval(n))
	}

	return renderText(v)
}

func renderText(v any) Datum {
	switch x := v.(type) {
	case string:
		return ScalarDatum(x)
	case []byte:
		return ScalarDatum(string(x))
	case int64:
		return ScalarDatum(strconv.FormatInt(x, 10))
	case float64:
		return ScalarDatum(javaDouble(x))
	case bool:
		return ScalarDatum(strconv.FormatBool(x))
	case time.Time:
		return ScalarDatum(formatTS(x))
	}

	return ScalarDatum(fmt.Sprint(v))
}

func decodeJSONText(s string, out any) bool {
	dec := jsontext.NewDecoder(strings.NewReader(s))

	return json.UnmarshalDecode(dec, out, jsonx.V1(), jsonx.UseNumber()) == nil
}

func renderComplexValue(v any, t tsType) Datum {
	s, ok := stringOf(v)
	if !ok {
		return NullDatum()
	}

	switch t.kind {
	case tyTimeseries:
		pts, err := decodeSeries(s)
		if err != nil {
			return NullDatum()
		}

		out := make([]TimeSeriesDataPoint, 0, len(pts))
		for _, p := range pts {
			out = append(out, TimeSeriesDataPoint{Time: formatTS(p.t), Value: renderJSON(p.v, *t.elem)})
		}

		return Datum{TimeSeriesValue: out}
	case tyArray:
		var arr []any
		if !decodeJSONText(strings.TrimPrefix(s, arrayMark), &arr) {
			return NullDatum()
		}

		return renderJSON(arr, t)
	default:
	}

	var obj any
	if !decodeJSONText(strings.TrimPrefix(s, arrayMark), &obj) {
		return NullDatum()
	}

	return renderJSON(obj, t)
}

func rowKeys(obj map[string]any, t tsType) []string {
	keys := make([]string, len(t.fields))

	var order []any

	if o, ok := obj[rowOrderKey].([]any); ok {
		order = o
	}

	for i, f := range t.fields {
		switch {
		case i < len(order):
			keys[i], _ = order[i].(string)
		case f.name != "":
			keys[i] = f.name
		default:
			keys[i] = "\x00" + strconv.Itoa(i)
		}
	}

	return keys
}

// renderJSON formats a decoded JSON value (array, row object or scalar) as a Datum of type t.
func renderJSON(v any, t tsType) Datum {
	v = decodeJSONScalar(v)
	if v == nil {
		return NullDatum()
	}

	switch x := v.(type) {
	case []any:
		if t.kind != tyArray {
			break
		}

		out := make([]Datum, len(x))
		for i, e := range x {
			out[i] = renderJSON(e, *t.elem)
		}

		return Datum{ArrayValue: out}
	case map[string]any:
		if t.kind != tyRow {
			break
		}

		data := make([]Datum, len(t.fields))
		for i, key := range rowKeys(x, t) {
			data[i] = renderJSON(x[key], t.fields[i].typ)
		}

		return Datum{RowValue: &Row{Data: data}}
	}

	return renderScalar(v, t.kind)
}
