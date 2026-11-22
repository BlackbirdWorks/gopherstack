package timestreamquery

import (
	"database/sql/driver"
	"fmt"
	"math"
	"slices"
	"strconv"

	sqlitedriver "modernc.org/sqlite"
)

const (
	minApproxError = 0.0040625
	maxApproxError = 0.26
	minSkewRows    = 3
	skewTwo        = 2.0
	skewPower      = 1.5
)

type aggRow = []driver.Value

// bufferedAgg keeps every input row so the result can be recomputed after a window frame drops one.
type bufferedAgg struct {
	finish func(rows []aggRow) (driver.Value, error)
	check  func(args aggRow) error
	rows   []aggRow
}

func cloneArgs(args []driver.Value) aggRow {
	out := make(aggRow, len(args))

	for i, a := range args {
		if b, ok := a.([]byte); ok {
			a = slices.Clone(b)
		}

		out[i] = a
	}

	return out
}

func sameRow(a, b aggRow) bool {
	return slices.EqualFunc(a, b, func(x, y driver.Value) bool {
		bx, okX := x.([]byte)
		by, okY := y.([]byte)

		if okX || okY {
			return okX && okY && slices.Equal(bx, by)
		}

		return x == y
	})
}

func (a *bufferedAgg) Step(_ *fnCtx, args []driver.Value) error {
	if a.check != nil {
		if err := a.check(args); err != nil {
			return err
		}
	}

	a.rows = append(a.rows, cloneArgs(args))

	return nil
}

func (a *bufferedAgg) WindowInverse(_ *fnCtx, args []driver.Value) error {
	row := cloneArgs(args)

	if i := slices.IndexFunc(a.rows, func(r aggRow) bool { return sameRow(r, row) }); i >= 0 {
		a.rows = slices.Delete(a.rows, i, i+1)
	}

	return nil
}

func (a *bufferedAgg) WindowValue(*fnCtx) (driver.Value, error) { return a.finish(a.rows) }

func (*bufferedAgg) Final(*fnCtx) {}

func registerBuffered(name string, nargs int32, check func(aggRow) error, finish func([]aggRow) (driver.Value, error)) {
	sqlitedriver.MustRegisterFunction(name, &sqlitedriver.FunctionImpl{
		NArgs: nargs, Deterministic: true,
		MakeAggregate: func(_ fnCtx) (sqlitedriver.AggregateFunction, error) {
			return &bufferedAgg{finish: finish, check: check}, nil
		},
	})
}

func nonNull(rows []aggRow, cols ...int) []aggRow {
	return slices.DeleteFunc(slices.Clone(rows), func(r aggRow) bool {
		for _, c := range cols {
			if r[c] == nil {
				return true
			}
		}

		return false
	})
}

func floatColumn(rows []aggRow, col int) ([]float64, error) {
	out := make([]float64, len(rows))

	for i, r := range rows {
		f, ok := toFloat64(r[col])
		if !ok {
			return nil, errOperandType
		}

		out[i] = f
	}

	return out, nil
}

func bitwiseAgg(apply func(a, b int64) int64) func([]aggRow) (driver.Value, error) {
	return func(rows []aggRow) (driver.Value, error) {
		rows = nonNull(rows, 0)
		if len(rows) == 0 {
			return nil, nil
		}

		acc, ok := toInt64(rows[0][0])
		if !ok {
			return nil, errOperandType
		}

		for _, r := range rows[1:] {
			n, isInt := toInt64(r[0])
			if !isInt {
				return nil, errOperandType
			}

			acc = apply(acc, n)
		}

		return acc, nil
	}
}

// skewnessOf follows the streaming central-moment update used for the population skewness.
func skewnessOf(rows []aggRow) (driver.Value, error) {
	xs, err := floatColumn(nonNull(rows, 0), 0)
	if err != nil {
		return nil, err
	}

	if len(xs) < minSkewRows {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	var n, mean, m2, m3 float64

	for _, x := range xs {
		n++

		delta := x - mean
		deltaN := delta / n
		m3 += -3*deltaN*m2 + delta*deltaN*deltaN*(n-1)*(n-skewTwo)
		m2 += delta * deltaN * (n - 1)
		mean += deltaN
	}

	return math.Sqrt(n) * m3 / math.Pow(m2, skewPower), nil
}

// regression returns slope and intercept of y on x, or ok=false when x has no variance.
func regression(rows []aggRow) (float64, float64, bool, error) {
	rows = nonNull(rows, 0, 1)

	ys, err := floatColumn(rows, 0)
	if err != nil {
		return 0, 0, false, err
	}

	xs, err := floatColumn(rows, 1)
	if err != nil {
		return 0, 0, false, err
	}

	var n, meanX, meanY, c2, m2x float64

	for i := range xs {
		n++

		dx := xs[i] - meanX
		meanX += dx / n
		meanY += (ys[i] - meanY) / n
		c2 += dx * (ys[i] - meanY)
		m2x += dx * (xs[i] - meanX)
	}

	if n == 0 || m2x == 0 {
		return 0, 0, false, nil
	}

	slope := c2 / m2x

	return slope, meanY - slope*meanX, true, nil
}

func regressionAgg(intercept bool) func([]aggRow) (driver.Value, error) {
	return func(rows []aggRow) (driver.Value, error) {
		slope, icpt, ok, err := regression(rows)
		if err != nil || !ok {
			return nil, err
		}

		if intercept {
			return icpt, nil
		}

		return slope, nil
	}
}

func checkApproxError(args aggRow) error {
	if args[1] == nil {
		return nil
	}

	e, ok := toFloat64(args[1])
	if !ok {
		return errOperandType
	}

	if math.IsNaN(e) || e < minApproxError || e > maxApproxError {
		return evalError("Max standard error must be in [0.0040625, 0.26000]: " + javaDouble(e))
	}

	return nil
}

func approxDistinct(rows []aggRow) (driver.Value, error) {
	seen := map[string]bool{}

	for _, r := range nonNull(rows, 0) {
		seen[fmt.Sprintf("%T:%v", r[0], r[0])] = true
	}

	return int64(len(seen)), nil
}

func topByKey(largest bool) func([]aggRow) (driver.Value, error) {
	return func(rows []aggRow) (driver.Value, error) {
		rows = nonNull(rows, 1)
		if len(rows) == 0 {
			return nil, nil
		}

		n, _ := toInt64(rows[0][argsTwo])

		slices.SortStableFunc(rows, func(a, b aggRow) int {
			if largest {
				return compareValues(b[1], a[1])
			}

			return compareValues(a[1], b[1])
		})

		vals := make([]driver.Value, 0, min(int(n), len(rows)))
		for _, r := range rows[:min(int(n), len(rows))] {
			vals = append(vals, r[0])
		}

		return arrayText(vals), nil
	}
}

func checkTopN(name string) func(aggRow) error {
	return func(args aggRow) error {
		n, ok := toInt64(args[argsTwo])
		if !ok {
			return evalError("third argument of " + name + " must be an integer")
		}

		if n <= 0 {
			return evalError("third argument of " + name + " must be a positive integer")
		}

		if n > maxTopN {
			return evalError("third argument of " + name + " must be less than or equal to " +
				strconv.Itoa(maxTopN) + "; found " + strconv.FormatInt(n, 10))
		}

		return nil
	}
}

func checkWeight(args aggRow) error {
	if args[1] == nil {
		return nil
	}

	w, ok := toInt64(args[1])
	if !ok || w < 1 {
		return evalError("weight must be an integer of at least one")
	}

	return nil
}

func percentileTargets(args aggRow) ([]float64, bool, error) {
	if s, ok := textOf(args[argsTwo]); ok {
		ps, err := percentageList(s)

		return ps, true, err
	}

	p, ok := toFloat64(args[argsTwo])
	if !ok || math.IsNaN(p) || p < 0 || p > 1 {
		return nil, false, evalError("percentage must be between 0 and 1")
	}

	return []float64{p}, false, nil
}

func checkAccuracy(args aggRow) error {
	if len(args) <= argsThree || args[argsThree] == nil {
		return nil
	}

	acc, ok := toFloat64(args[argsThree])
	if !ok || acc <= 0 || acc >= 1 {
		return evalError("accuracy must be greater than 0 and less than 1")
	}

	return nil
}

func checkWeightedPercentile(args aggRow) error {
	if err := checkWeight(args); err != nil {
		return err
	}

	if err := checkAccuracy(args); err != nil {
		return err
	}

	if args[argsTwo] == nil {
		return nil
	}

	_, _, err := percentileTargets(args)

	return err
}

type weighted struct {
	val driver.Value
	w   int64
}

func weightedPercentile(rows []aggRow) (driver.Value, error) {
	rows = nonNull(rows, 0, 1)
	if len(rows) == 0 {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	ps, isArray, err := percentileTargets(rows[0])
	if err != nil {
		return nil, err
	}

	items := make([]weighted, len(rows))

	var total int64

	for i, r := range rows {
		w, _ := toInt64(r[1])
		items[i] = weighted{val: r[0], w: w}

		if total > math.MaxInt64-w {
			return nil, evalError("total weight overflow")
		}

		total += w
	}

	slices.SortStableFunc(items, func(a, b weighted) int { return compareValues(a.val, b.val) })

	out := make([]driver.Value, len(ps))
	for i, p := range ps {
		out[i] = valueAtRank(items, max(1, int64(math.Ceil(p*float64(total)))))
	}

	if isArray {
		return arrayText(out), nil
	}

	return out[0], nil
}

func valueAtRank(items []weighted, rank int64) driver.Value {
	var cum int64

	for _, it := range items {
		cum += it.w
		if cum >= rank {
			return it.val
		}
	}

	return items[len(items)-1].val
}

func registerSetAggs() {
	registerBuffered("bitwise_and_agg", 1, nil, bitwiseAgg(func(a, b int64) int64 { return a & b }))
	registerBuffered("bitwise_or_agg", 1, nil, bitwiseAgg(func(a, b int64) int64 { return a | b }))
	registerBuffered("skewness", 1, nil, skewnessOf)
	registerBuffered("regr_slope", argsTwo, nil, regressionAgg(false))
	registerBuffered("regr_intercept", argsTwo, nil, regressionAgg(true))
	registerBuffered("ts_approx_distinct", argsTwo, checkApproxError, approxDistinct)
	registerBuffered("ts_max_by_n", argsThree, checkTopN("max_by"), topByKey(true))
	registerBuffered("ts_min_by_n", argsThree, checkTopN("min_by"), topByKey(false))
	registerBuffered("ts_approx_percentile_w", argsThree, checkWeightedPercentile, weightedPercentile)
	registerBuffered("ts_approx_percentile_wa", argsFour, checkWeightedPercentile, weightedPercentile)
}
