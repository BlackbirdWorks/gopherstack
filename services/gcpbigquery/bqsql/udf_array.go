package bqsql

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"strings"
)

func decodeJSONArray(v driver.Value) ([]any, error) {
	dec := json.NewDecoder(strings.NewReader(toStr(v)))
	dec.UseNumber()

	var arr []any
	if err := dec.Decode(&arr); err != nil {
		return nil, errBQ("expected an ARRAY value")
	}

	return arr, nil
}

func encodeJSON(v any) string {
	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)

	return strings.TrimSpace(buf.String())
}

// jsonToSQL converts a decoded JSON element into the value SQLite would see from json_extract.
func jsonToSQL(v any) driver.Value {
	switch t := v.(type) {
	case nil:
		return nil
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return n
		}

		f, _ := t.Float64()

		return f
	case bool:
		return boolVal(t)
	case string:
		return t
	}

	return encodeJSON(v)
}

func addArrayUDFs(d map[string]udfDef) {
	d["array_concat"] = udfDef{n: -1, fn: func(a []driver.Value) (driver.Value, error) {
		out := []any{}

		for _, v := range a {
			if v == nil {
				return nil, nil //nolint:nilnil // NULL in, NULL out
			}

			arr, err := decodeJSONArray(v)
			if err != nil {
				return nil, err
			}

			out = append(out, arr...)
		}

		return encodeJSON(out), nil
	}}
	d["array_reverse"] = udfDef{n: 1, fn: func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil //nolint:nilnil // NULL in, NULL out
		}

		arr, err := decodeJSONArray(a[0])
		if err != nil {
			return nil, err
		}

		for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
			arr[i], arr[j] = arr[j], arr[i]
		}

		return encodeJSON(arr), nil
	}}
	d["array_first"] = udfDef{n: 1, fn: edgeElem(false)}
	d["array_last"] = udfDef{n: 1, fn: edgeElem(true)}
	d["array_to_string"] = udfDef{n: -1, fn: udfArrayToString}
	d["generate_array"] = udfDef{n: -1, fn: udfGenerateArray}
}

func edgeElem(last bool) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		arr, err := decodeJSONArray(a[0])
		if err != nil {
			return nil, err
		}

		if len(arr) == 0 {
			return nil, errBQ("Cannot get the first or last element of an empty array")
		}

		if last {
			return jsonToSQL(arr[len(arr)-1]), nil
		}

		return jsonToSQL(arr[0]), nil
	}
}

func udfArrayToString(a []driver.Value) (driver.Value, error) {
	if a[0] == nil || a[1] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	arr, err := decodeJSONArray(a[0])
	if err != nil {
		return nil, err
	}

	parts := make([]string, 0, len(arr))

	for _, e := range arr {
		if e == nil {
			if len(a) > 2 && a[2] != nil {
				parts = append(parts, toStr(a[2]))
			}

			continue
		}

		parts = append(parts, toStr(jsonToSQL(e)))
	}

	return strings.Join(parts, toStr(a[1])), nil
}

const maxGeneratedElems = 1_000_000

func udfGenerateArray(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	floats := false

	nums := make([]float64, 0, len(a))

	for _, v := range a {
		floats = floats || isFloatVal(v)

		f, err := toFloat(v)
		if err != nil {
			return nil, err
		}

		nums = append(nums, f)
	}

	start, end, step := nums[0], nums[1], 1.0
	if len(nums) > argc2 {
		step = nums[argc2]
	}

	if step == 0 {
		return nil, errBQ("Sequence step in the GENERATE_ARRAY function must not be 0")
	}

	out := []any{}

	for x := start; (step > 0 && x <= end) || (step < 0 && x >= end); x += step {
		if len(out) >= maxGeneratedElems {
			return nil, errBQ("generated array is too large")
		}

		if floats {
			out = append(out, x)
		} else {
			out = append(out, int64(x))
		}
	}

	return encodeJSON(out), nil
}
