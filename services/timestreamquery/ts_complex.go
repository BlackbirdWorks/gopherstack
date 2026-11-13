package timestreamquery

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const kindSeries, kindArray = "\x00timeseries", "\x00array"

func stringOf(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	default:
		return "", false
	}
}

// complexKind reports whether a column holds timeseries or array values (marked JSON text).
func complexKind(col []any) string {
	for _, v := range col {
		s, ok := stringOf(v)

		switch {
		case !ok:
			continue
		case strings.HasPrefix(s, seriesMark):
			return kindSeries
		case strings.HasPrefix(s, arrayMark) && strings.HasPrefix(strings.TrimPrefix(s, arrayMark), "["):
			return kindArray
		}
	}

	return ""
}

func jsonScalarType(v any) string {
	switch x := v.(type) {
	case int64:
		return ScalarTypeBigint
	case float64:
		return ScalarTypeDouble
	case bool:
		return ScalarTypeBoolean
	case jsonx.Number:
		if _, isInt := decodeJSONScalar(x).(int64); isInt {
			return ScalarTypeBigint
		}

		return ScalarTypeDouble
	case string:
		if reTSText.MatchString(x) {
			return ScalarTypeTimestamp
		}
	}

	return ScalarTypeVarchar
}

func decodeArray(s string) []any {
	dec := jsontext.NewDecoder(strings.NewReader(strings.TrimPrefix(s, arrayMark)))

	var out []any
	if json.UnmarshalDecode(dec, &out, jsonx.V1(), jsonx.UseNumber()) != nil {
		return nil
	}

	return out
}

func complexColumnInfo(name, kind string, col []any) ColumnInfo {
	elem := ScalarTypeDouble

	if kind == kindArray {
		elem = ScalarTypeVarchar
	}

search:
	for _, v := range col {
		s, ok := stringOf(v)
		if !ok {
			continue
		}

		if kind == kindSeries {
			if pts, err := decodeSeries(s); err == nil && len(pts) > 0 {
				elem = jsonScalarType(pts[0].v)

				break search
			}

			continue
		}

		if arr := decodeArray(s); len(arr) > 0 {
			elem = jsonScalarType(arr[0])

			break search
		}
	}

	inner := &ColumnInfo{Type: ColumnType{ScalarType: elem}}
	if kind == kindSeries {
		return ColumnInfo{Name: name, Type: ColumnType{TimeSeriesMeasureValueColumnInfo: inner}}
	}

	return ColumnInfo{Name: name, Type: ColumnType{ArrayColumnInfo: inner}}
}

func scalarJSONDatum(v any) Datum {
	switch x := v.(type) {
	case nil:
		return NullDatum()
	case bool:
		return ScalarDatum(strconv.FormatBool(x))
	case int64:
		return ScalarDatum(strconv.FormatInt(x, 10))
	case float64:
		return ScalarDatum(javaDouble(x))
	case jsonx.Number:
		return scalarJSONDatum(decodeJSONScalar(x))
	case string:
		return ScalarDatum(x)
	default:
		b, _ := json.Marshal(x, jsonx.V1())

		return ScalarDatum(string(b))
	}
}

func datumFor(v any, typ string) Datum {
	if v == nil {
		return NullDatum()
	}

	s, ok := stringOf(v)
	if !ok || (typ != kindSeries && typ != kindArray) {
		return datumOf(v, typ)
	}

	if typ == kindSeries {
		pts, err := decodeSeries(s)
		if err != nil {
			return NullDatum()
		}

		out := make([]TimeSeriesDataPoint, 0, len(pts))
		for _, p := range pts {
			out = append(out, TimeSeriesDataPoint{Time: formatTS(p.t), Value: scalarJSONDatum(p.v)})
		}

		return Datum{TimeSeriesValue: out}
	}

	arr := decodeArray(s)
	vals := make([]Datum, 0, len(arr))

	for _, e := range arr {
		vals = append(vals, scalarJSONDatum(e))
	}

	return Datum{ArrayValue: vals}
}
