package cosmosdb

import (
	"encoding/json"
	"math"
	"strconv"
)

const (
	aggCount = "COUNT"
	aggSum   = "SUM"
	aggAvg   = "AVG"
	aggMin   = "MIN"
	aggMax   = "MAX"
	// aggCountIf counts the rows for which its argument is true (the full-text search page uses it for facets).
	aggCountIf = "COUNTIF"
)

// aggAcc folds one aggregate (COUNT/SUM/AVG/MIN/MAX) over a group's values.
type aggAcc struct {
	best     any
	name     string
	sumFloat float64
	sumInt   int64
	count    int
	numCount int
	floatSum bool
	haveBest bool
}

func (a *aggAcc) add(v any) {
	if isUndef(v) {
		return
	}

	switch a.name {
	case aggCount:
		a.count++
	case aggCountIf:
		if b, ok := v.(bool); ok && b {
			a.count++
		}
	case aggSum, aggAvg:
		n, ok := toNumber(v)
		if !ok {
			return
		}

		a.addNumber(n)
	default:
		a.addExtreme(v)
	}
}

func (a *aggAcc) addNumber(n json.Number) {
	a.numCount++
	a.sumFloat += numFloat(n)

	if a.floatSum {
		return
	}

	i, err := n.Int64()
	if err != nil {
		a.floatSum = true

		return
	}

	s := a.sumInt + i
	if (s > a.sumInt) != (i > 0) {
		a.floatSum = true

		return
	}

	a.sumInt = s
}

func (a *aggAcc) addExtreme(v any) {
	if !a.haveBest {
		a.best, a.haveBest = v, true

		return
	}

	c := compareTotal(v, a.best)
	if (a.name == aggMin && c < 0) || (a.name == aggMax && c > 0) {
		a.best = v
	}
}

func (a *aggAcc) result() any {
	switch a.name {
	case aggCount, aggCountIf:
		return intValue(a.count)
	case aggSum:
		switch {
		case a.numCount == 0:
			return undef{}
		case a.floatSum:
			return floatResult(a.sumFloat)
		default:
			return json.Number(strconv.FormatInt(a.sumInt, 10))
		}
	case aggAvg:
		if a.numCount == 0 {
			return undef{}
		}

		return floatResult(a.sumFloat / float64(a.numCount))
	default:
		if !a.haveBest {
			return undef{}
		}

		return a.best
	}
}

func (ec *evalCtx) evalAggregate(n *exprCall) any {
	if !ec.grouped {
		return undef{}
	}

	inner := *ec
	inner.grouped, inner.group = false, nil

	acc := aggAcc{name: n.name}

	for _, tuple := range ec.group {
		acc.add(inner.eval(n.args[0], tuple))
	}

	if math.IsNaN(acc.sumFloat) {
		return undef{}
	}

	return acc.result()
}
