package bqsql

import (
	"database/sql/driver"
	"math"

	sqlitedriver "modernc.org/sqlite"
)

var errNoWindow = errBQ("this aggregate cannot be used as a window function")

type moments struct {
	n    float64
	mean float64
	m2   float64
}

func (m *moments) add(x float64) {
	m.n++
	d := x - m.mean
	m.mean += d / m.n
	m.m2 += d * (x - m.mean)
}

type momentAgg struct {
	m      moments
	sample bool
	root   bool
}

func (a *momentAgg) Step(_ *sqlitedriver.FunctionContext, args []driver.Value) error {
	if args[0] == nil {
		return nil
	}

	x, err := toFloat(args[0])
	if err != nil {
		return err
	}

	a.m.add(x)

	return nil
}

func (a *momentAgg) result() driver.Value {
	denom := a.m.n
	if a.sample {
		denom--
	}

	if a.m.n == 0 || denom <= 0 {
		return nil
	}

	v := a.m.m2 / denom
	if a.root {
		return math.Sqrt(v)
	}

	return v
}

func (a *momentAgg) WindowInverse(*sqlitedriver.FunctionContext, []driver.Value) error {
	return errNoWindow
}

func (a *momentAgg) WindowValue(*sqlitedriver.FunctionContext) (driver.Value, error) {
	return a.result(), nil
}

func (a *momentAgg) Final(*sqlitedriver.FunctionContext) {}

func registerAggregates() {
	for name, spec := range map[string][2]bool{
		"bq_stddev_samp": {true, true}, "bq_stddev_pop": {false, true}, "bq_var_samp": {true, false},
		"bq_var_pop": {false, false},
	} {
		sample, root := spec[0], spec[1]

		sqlitedriver.MustRegisterFunction(name, &sqlitedriver.FunctionImpl{
			NArgs: 1,
			MakeAggregate: func(sqlitedriver.FunctionContext) (sqlitedriver.AggregateFunction, error) {
				return &momentAgg{sample: sample, root: root}, nil
			},
		})
	}
}
