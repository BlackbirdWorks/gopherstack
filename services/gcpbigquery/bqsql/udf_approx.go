package bqsql

import (
	"cmp"
	"database/sql/driver"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func addApproxUDFs(d map[string]udfDef) {
	d["array_limit"] = udfDef{n: argc2, fn: udfArrayLimit}
	d["quantiles"] = udfDef{n: argc2, fn: udfQuantiles}
	d["top_count"] = udfDef{n: argc2, fn: udfTopCount}
}

func countArg(v driver.Value, what string) (int, error) {
	n, err := toFloat(v)
	if err != nil || n < 0 || n != float64(int64(n)) {
		return 0, errBQ("%s must be a non-negative INT64", what)
	}

	return int(n), nil
}

func udfArrayLimit(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	n, err := countArg(a[1], "LIMIT")
	if err != nil {
		return nil, err
	}

	arr, err := decodeJSONArray(a[0])
	if err != nil {
		return nil, err
	}

	return encodeJSON(arr[:min(n, len(arr))]), nil
}

func jsonRank(v any) int {
	switch v.(type) {
	case nil:
		return 0
	case bool:
		return 1
	case jsonx.Number:
		return 2 //nolint:mnd // numbers sort after booleans
	case string:
		return 3 //nolint:mnd // strings sort after numbers
	}

	return 4 //nolint:mnd // composite values last
}

func compareJSON(a, b any) int {
	if ra, rb := jsonRank(a), jsonRank(b); ra != rb {
		return cmp.Compare(ra, rb)
	}

	switch x := a.(type) {
	case bool:
		y, _ := b.(bool)

		return cmp.Compare(boolInt(x), boolInt(y))
	case jsonx.Number:
		y, _ := b.(jsonx.Number)
		fa, _ := x.Float64()
		fb, _ := y.Float64()

		return cmp.Compare(fa, fb)
	case string:
		y, _ := b.(string)

		return cmp.Compare(x, y)
	}

	return cmp.Compare(encodeJSON(a), encodeJSON(b))
}

// udfQuantiles returns number+1 boundaries at offset floor(i*(N-1)/number), which matches the documented examples.
func udfQuantiles(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	n, err := countArg(a[1], "The number of quantiles")
	if err != nil || n == 0 {
		return nil, errBQ("The number of quantiles must be a positive INT64")
	}

	arr, err := decodeJSONArray(a[0])
	if err != nil {
		return nil, err
	}

	if !slices.ContainsFunc(arr, func(v any) bool { return v != nil }) {
		return nil, nil //nolint:nilnil // every value NULL (or none): NULL result
	}

	slices.SortStableFunc(arr, compareJSON)

	out := make([]any, n+1)
	for i := range out {
		out[i] = arr[i*(len(arr)-1)/n]
	}

	return encodeJSON(out), nil
}

// udfTopCount returns the most frequent values (NULL included) as {value,count}, ties broken by value.
func udfTopCount(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	n, err := countArg(a[1], "The number of values")
	if err != nil || n == 0 {
		return nil, errBQ("The number of values must be a positive INT64")
	}

	arr, err := decodeJSONArray(a[0])
	if err != nil {
		return nil, err
	}

	if len(arr) == 0 {
		return nil, nil //nolint:nilnil // no input rows
	}

	type tally struct {
		val   any
		count int64
	}

	seen := map[string]*tally{}

	var order []*tally

	for _, v := range arr {
		k := encodeJSON(v)
		if t, ok := seen[k]; ok {
			t.count++

			continue
		}

		t := &tally{val: v, count: 1}
		seen[k] = t
		order = append(order, t)
	}

	slices.SortStableFunc(order, func(x, y *tally) int {
		if c := cmp.Compare(y.count, x.count); c != 0 {
			return c
		}

		return compareJSON(x.val, y.val)
	})

	parts := make([]string, 0, min(n, len(order)))
	for _, t := range order[:min(n, len(order))] {
		parts = append(parts, `{"value":`+encodeJSON(t.val)+`,"count":`+strconv.FormatInt(t.count, decimalBase)+`}`)
	}

	return "[" + strings.Join(parts, ",") + "]", nil
}
