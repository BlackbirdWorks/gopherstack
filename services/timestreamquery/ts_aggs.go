package timestreamquery

import (
	"cmp"
	"database/sql/driver"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	sqlitedriver "modernc.org/sqlite"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxTopN = 10000

var errAggNoWindow = errors.New("not supported as a window function")

func orderedValue(v driver.Value) (float64, string, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), "", false
	case float64:
		return x, "", false
	case []byte:
		return 0, string(x), true
	case string:
		return 0, x, true
	}

	return 0, "", false
}

func compareValues(a, b driver.Value) int {
	fa, sa, textA := orderedValue(a)
	fb, sb, textB := orderedValue(b)

	if textA && textB {
		return strings.Compare(sa, sb)
	}

	return cmp.Compare(fa, fb)
}

func arrayElem(v driver.Value) string {
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		s := strconv.FormatFloat(x, 'f', -1, bits64)
		if !strings.ContainsAny(s, ".IN") {
			s += ".0"
		}

		return s
	case []byte:
		return quoteJSON(string(x))
	case string:
		return quoteJSON(x)
	}

	return kwNull
}

func quoteJSON(s string) string {
	b, _ := json.Marshal(s, jsonx.V1())

	return string(b)
}

func arrayText(vals []driver.Value) driver.Value {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = arrayElem(v)
	}

	return arrayMark + "[" + strings.Join(parts, ",") + "]"
}

// topNAgg is max(x, n) (largest first) or min(x, n) (smallest first): the n extreme non-null values as an array.
type topNAgg struct {
	vals    []driver.Value
	n       int64
	largest bool
}

func (a *topNAgg) Step(_ *fnCtx, args []driver.Value) error {
	n, ok := toInt64(args[1])
	if !ok {
		return evalError("n must be an integer")
	}

	if n <= 0 || n > maxTopN {
		return evalError(fmt.Sprintf("second argument must be positive and at most %d: %d", maxTopN, n))
	}

	a.n = n

	if args[0] != nil {
		a.vals = append(a.vals, args[0])
	}

	return nil
}

func (a *topNAgg) Final(_ *fnCtx) {}

func (a *topNAgg) WindowValue(*fnCtx) (driver.Value, error) {
	if len(a.vals) == 0 {
		return nil, nil //nolint:nilnil // aggregate over no values is SQL NULL
	}

	sorted := slices.Clone(a.vals)

	slices.SortStableFunc(sorted, func(x, y driver.Value) int {
		if a.largest {
			return compareValues(y, x)
		}

		return compareValues(x, y)
	})

	return arrayText(sorted[:min(int(a.n), len(sorted))]), nil
}

// percentilesAgg is approx_percentile(x, ARRAY[p, ...]): the value at each percentage, in the given order.
type percentilesAgg struct {
	ps   []float64
	vals []driver.Value
}

func (a *percentilesAgg) Step(_ *fnCtx, args []driver.Value) error {
	if a.ps == nil {
		ps, err := percentageList(args[1])
		if err != nil {
			return err
		}

		a.ps = ps
	}

	if args[0] != nil {
		a.vals = append(a.vals, args[0])
	}

	return nil
}

func (a *percentilesAgg) Final(_ *fnCtx) {}

func (a *percentilesAgg) WindowValue(*fnCtx) (driver.Value, error) {
	if len(a.vals) == 0 {
		return nil, nil //nolint:nilnil // aggregate over no values is SQL NULL
	}

	sorted := slices.Clone(a.vals)
	slices.SortStableFunc(sorted, compareValues)

	out := make([]driver.Value, len(a.ps))

	for i, p := range a.ps {
		idx := int(math.Ceil(p*float64(len(sorted)))) - 1
		out[i] = sorted[max(0, min(idx, len(sorted)-1))]
	}

	return arrayText(out), nil
}

func percentageList(v driver.Value) ([]float64, error) {
	s, ok := textOf(v)
	if !ok {
		return nil, evalError("percentages must be an array")
	}

	var ps []float64
	if err := json.Unmarshal([]byte(strings.TrimPrefix(s, arrayMark)), &ps, jsonx.V1()); err != nil {
		return nil, evalError("percentages must be an array of numbers")
	}

	for _, p := range ps {
		if math.IsNaN(p) || p < 0 || p > 1 {
			return nil, evalError(fmt.Sprintf("percentage must be between 0 and 1: %s", arrayElem(p)))
		}
	}

	if ps == nil {
		ps = []float64{}
	}

	return ps, nil
}

func (a *topNAgg) WindowInverse(*fnCtx, []driver.Value) error { return errAggNoWindow }

func (a *percentilesAgg) WindowInverse(*fnCtx, []driver.Value) error { return errAggNoWindow }

func registerAggFuncs() {
	for _, largest := range []bool{true, false} {
		name := "ts_min_n"
		if largest {
			name = "ts_max_n"
		}

		sqlitedriver.MustRegisterFunction(name, &sqlitedriver.FunctionImpl{
			NArgs: argsTwo, Deterministic: true,
			MakeAggregate: func(_ fnCtx) (sqlitedriver.AggregateFunction, error) {
				return &topNAgg{largest: largest}, nil
			},
		})
	}

	sqlitedriver.MustRegisterFunction("ts_approx_percentile_array", &sqlitedriver.FunctionImpl{
		NArgs: argsTwo, Deterministic: true,
		MakeAggregate: func(_ fnCtx) (sqlitedriver.AggregateFunction, error) { return &percentilesAgg{}, nil },
	})
}
