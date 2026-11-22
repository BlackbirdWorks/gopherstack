package athena

import (
	"database/sql/driver"
	"math"
)

const (
	minSkewnessRows = 3
	minKurtosisRows = 4
	momentThree     = 3
	momentSix       = 6
	momentFour      = 4
	skewExponent    = 1.5
)

func bitwiseAggregate(name string, apply func(a, b int64) int64) *aggState {
	var (
		acc  int64
		seen bool
	)

	return &aggState{
		step: func(args []driver.Value) error {
			if args[0] == nil {
				return nil
			}

			n, ok := bigintArg(args[0])
			if !ok {
				return errUnexpected(name)
			}

			if seen {
				acc = apply(acc, n)
			} else {
				acc, seen = n, true
			}

			return nil
		},
		final: func() driver.Value {
			if !seen {
				return sqlNull()
			}

			return acc
		},
	}
}

// centralMoments keeps running central centralMoments (Pebay's one-pass update).
type centralMoments struct {
	n, mean, m2, m3, m4 float64
}

func (m *centralMoments) add(x float64) {
	prev := m.n
	m.n++

	delta := x - m.mean
	deltaN := delta / m.n
	deltaN2 := deltaN * deltaN
	term := delta * deltaN * prev

	m.mean += deltaN
	m.m4 += term*deltaN2*(m.n*m.n-momentThree*m.n+momentThree) + momentSix*deltaN2*m.m2 -
		momentFour*deltaN*m.m3
	m.m3 += term*deltaN*(m.n-argsTwo) - momentThree*deltaN*m.m2
	m.m2 += term
}

func (m *centralMoments) skewness() driver.Value {
	if m.n < minSkewnessRows {
		return sqlNull()
	}

	return m.n * math.Sqrt(m.n-1) / (m.n - argsTwo) * m.m3 / math.Pow(m.m2, skewExponent)
}

func (m *centralMoments) kurtosis() driver.Value {
	if m.n < minKurtosisRows {
		return sqlNull()
	}

	n := m.n
	denom := (n - argsTwo) * (n - argsThree)

	return (n-1)*n*(n+1)/denom*m.m4/(m.m2*m.m2) - momentThree*(n-1)*(n-1)/denom
}

func momentAggregate(name string, result func(*centralMoments) driver.Value) *aggState {
	var m centralMoments

	return &aggState{
		step: func(args []driver.Value) error {
			if args[0] == nil {
				return nil
			}

			x, ok := toFloatArg(args[0])
			if !ok {
				return errUnexpected(name)
			}

			m.add(x)

			return nil
		},
		final: func() driver.Value { return result(&m) },
	}
}

// regressionAggregate regresses y (first argument) on x (second) over the rows where both are non-NULL.
func regressionAggregate(name string, intercept bool) *aggState {
	var n, meanX, meanY, cxy, m2x float64

	return &aggState{
		step: func(args []driver.Value) error {
			if args[0] == nil || args[1] == nil {
				return nil
			}

			y, yok := toFloatArg(args[0])
			x, xok := toFloatArg(args[1])

			if !yok || !xok {
				return errUnexpected(name)
			}

			n++

			dx := x - meanX
			meanX += dx / n
			meanY += (y - meanY) / n
			cxy += dx * (y - meanY)
			m2x += dx * (x - meanX)

			return nil
		},
		final: func() driver.Value {
			if n == 0 || m2x == 0 {
				return sqlNull()
			}

			slope := cxy / m2x
			if intercept {
				return meanY - slope*meanX
			}

			return slope
		},
	}
}

func registerStatAggregates() {
	registerAgg("bitwise_and_agg", 1, func() *aggState {
		return bitwiseAggregate("bitwise_and_agg", func(a, b int64) int64 { return a & b })
	})
	registerAgg("bitwise_or_agg", 1, func() *aggState {
		return bitwiseAggregate("bitwise_or_agg", func(a, b int64) int64 { return a | b })
	})
	registerAgg("bitwise_xor_agg", 1, func() *aggState {
		return bitwiseAggregate("bitwise_xor_agg", func(a, b int64) int64 { return a ^ b })
	})
	registerAgg("skewness", 1, func() *aggState { return momentAggregate("skewness", (*centralMoments).skewness) })
	registerAgg("kurtosis", 1, func() *aggState { return momentAggregate("kurtosis", (*centralMoments).kurtosis) })
	registerAgg("regr_slope", argsTwo, func() *aggState { return regressionAggregate("regr_slope", false) })
	registerAgg("regr_intercept", argsTwo, func() *aggState { return regressionAggregate("regr_intercept", true) })
}
