package timestreamquery

import (
	"database/sql/driver"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxRepeatCount = 10000

const maxZipArrays = 5

type rawElems []string

func decodeRawArray(v driver.Value) (rawElems, bool, error) {
	if v == nil {
		return nil, true, nil
	}

	s, ok := textOf(v)
	if !ok || !strings.HasPrefix(s, arrayMark+"[") {
		return nil, false, errOperandType
	}

	var items []jsontext.Value

	if err := json.Unmarshal([]byte(strings.TrimPrefix(s, arrayMark)), &items, jsonx.V1()); err != nil {
		return nil, false, errOperandType
	}

	out := make(rawElems, len(items))
	for i, it := range items {
		out[i] = string(it)
	}

	return out, false, nil
}

func (r rawElems) text() string { return "[" + strings.Join(r, ",") + "]" }

func (r rawElems) encode() driver.Value { return arrayMark + r.text() }

func elemKey(raw string) string {
	raw = strings.TrimSpace(raw)

	switch {
	case strings.HasPrefix(raw, `"`):
		var s string
		if json.Unmarshal([]byte(raw), &s, jsonx.V1()) == nil {
			return "s" + s
		}
	case raw == kwNull, raw == kwTrue, raw == kwFalse:
		return raw
	default:
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			return "n" + strconv.FormatFloat(f, 'g', -1, bits64)
		}
	}

	return "j" + raw
}

func distinctRaw(in rawElems) rawElems {
	seen := make(map[string]bool, len(in))
	out := rawElems{}

	for _, e := range in {
		if k := elemKey(e); !seen[k] {
			seen[k] = true
			out = append(out, e)
		}
	}

	return out
}

func keySet(in rawElems) map[string]bool {
	set := make(map[string]bool, len(in))
	for _, e := range in {
		set[elemKey(e)] = true
	}

	return set
}

// setOp is a two-array set function: array_union, array_intersect or array_except.
func setOp(keep func(inY bool) bool, includeY bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		x, nullX, err := decodeRawArray(args[0])
		if err != nil {
			return nil, err
		}

		y, nullY, err := decodeRawArray(args[1])
		if err != nil {
			return nil, err
		}

		if nullX || nullY {
			return nil, nil
		}

		inY := keySet(y)

		var out rawElems

		for _, e := range x {
			if keep(inY[elemKey(e)]) {
				out = append(out, e)
			}
		}

		if includeY {
			out = append(out, y...)
		}

		return distinctRaw(out).encode(), nil
	}
}

func udfArraysOverlap(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	x, nullX, err := decodeRawArray(args[0])
	if err != nil {
		return nil, err
	}

	y, nullY, err := decodeRawArray(args[1])
	if err != nil {
		return nil, err
	}

	if nullX || nullY {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	inY := keySet(y)
	hasNull := inY[kwNull]

	for _, e := range x {
		k := elemKey(e)
		if k == kwNull {
			hasNull = true

			continue
		}

		if inY[k] {
			return int64(1), nil
		}
	}

	if hasNull {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	return int64(0), nil
}

func udfShuffle(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	x, isNull, err := decodeRawArray(args[0])
	if err != nil || isNull {
		return nil, err
	}

	rand.Shuffle(len(x), func(i, j int) { x[i], x[j] = x[j], x[i] }) //nolint:gosec // not security sensitive

	return x.encode(), nil
}

// jsonRaw renders an engine value as the JSON text of one array element.
func jsonRaw(v driver.Value) string {
	if v == nil {
		return kwNull
	}

	if s, ok := textOf(v); ok && strings.HasPrefix(s, arrayMark) {
		return strings.TrimPrefix(s, arrayMark)
	}

	if f, ok := v.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
		return quoteJSON(javaDouble(f))
	}

	return arrayElem(v)
}

func udfRepeat(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[1] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	n, ok := toInt64(args[1])
	if !ok {
		return nil, errOperandType
	}

	if n < 0 {
		return nil, evalError("count argument of repeat function must be greater than or equal to 0")
	}

	if n > maxRepeatCount {
		return nil, evalError("count argument of repeat function must be less than or equal to " +
			strconv.Itoa(maxRepeatCount))
	}

	elem := jsonRaw(args[0])
	out := make(rawElems, n)

	for i := range out {
		out[i] = elem
	}

	return out.encode(), nil
}

func udfZip(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if len(args) < argsTwo || len(args) > maxZipArrays {
		return nil, evalError("zip takes between 2 and 5 arrays")
	}

	arrays := make([]rawElems, len(args))
	width := 0

	for i, a := range args {
		arr, isNull, err := decodeRawArray(a)
		if err != nil {
			return nil, err
		}

		if isNull {
			return nil, nil //nolint:nilnil // SQL NULL
		}

		arrays[i] = arr
		width = max(width, len(arr))
	}

	rows := make(rawElems, width)

	for r := range rows {
		fields := make([]string, len(arrays))

		for c, arr := range arrays {
			val := kwNull
			if r < len(arr) {
				val = arr[r]
			}

			fields[c] = quoteJSON("\x00"+strconv.Itoa(c)) + ":" + val
		}

		rows[r] = "{" + strings.Join(fields, ",") + "}"
	}

	return rows.encode(), nil
}

func udfReverseArray(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	x, isNull, err := decodeRawArray(args[0])
	if err != nil || isNull {
		return nil, err
	}

	out := make(rawElems, len(x))
	for i, e := range x {
		out[len(x)-1-i] = e
	}

	return out.encode(), nil
}

func udfConcatArrays(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	var out rawElems

	for _, a := range args {
		arr, isNull, err := decodeRawArray(a)
		if err != nil {
			return nil, err
		}

		if isNull {
			return nil, nil //nolint:nilnil // SQL NULL
		}

		out = append(out, arr...)
	}

	if out == nil {
		out = rawElems{}
	}

	return out.encode(), nil
}
