package budgets

import (
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	hoursPerDay     = 24
	monthsPerQuart  = 3
	defaultCurrency = "USD"
	amountPrecision = 1e4
)

func startOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// periodStart aligns t to the start of its budget period.
func periodStart(unit string, t time.Time) time.Time {
	t = t.UTC()

	switch unit {
	case timeUnitDaily:
		return startOfDay(t)
	case timeUnitQuarterly:
		m := time.Month((int(t.Month())-1)/monthsPerQuart*monthsPerQuart + 1)

		return time.Date(t.Year(), m, 1, 0, 0, 0, 0, time.UTC)
	case timeUnitAnnually:
		return time.Date(t.Year(), time.January, 1, 0, 0, 0, 0, time.UTC)
	default:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
}

// nextPeriod returns the start of the period after the one starting at s.
func nextPeriod(unit string, s time.Time) time.Time {
	switch unit {
	case timeUnitDaily:
		return s.AddDate(0, 0, 1)
	case timeUnitQuarterly:
		return s.AddDate(0, monthsPerQuart, 0)
	case timeUnitAnnually:
		return s.AddDate(1, 0, 0)
	default:
		return s.AddDate(0, 1, 0)
	}
}

func prevPeriod(unit string, s time.Time) time.Time {
	switch unit {
	case timeUnitDaily:
		return s.AddDate(0, 0, -1)
	case timeUnitQuarterly:
		return s.AddDate(0, -monthsPerQuart, 0)
	case timeUnitAnnually:
		return s.AddDate(-1, 0, 0)
	default:
		return s.AddDate(0, -1, 0)
	}
}

func defaultEnd() time.Time {
	return time.Date(defaultEndYear, defaultEndMonth, defaultEndDay, 0, 0, 0, 0, time.UTC)
}

// currentPeriod returns the budget period containing now; ok is false outside the budget's TimePeriod.
func currentPeriod(bg *Budget, now time.Time) (time.Time, time.Time, bool) {
	tp := bg.TimePeriod
	if tp == nil || tp.Start == nil || tp.End == nil {
		return time.Time{}, time.Time{}, false
	}

	if now.Before(tp.Start.Time) || !now.Before(tp.End.Time) {
		return time.Time{}, time.Time{}, false
	}

	if bg.TimeUnit == timeUnitCustom {
		return tp.Start.Time, tp.End.Time, true
	}

	start := periodStart(bg.TimeUnit, now)

	return start, nextPeriod(bg.TimeUnit, start), true
}

func metricFor(bg *Budget) string {
	if bg.BudgetType == budgetTypeUsage {
		return "UsageQuantity"
	}

	if len(bg.Metrics) > 0 {
		return bg.Metrics[0]
	}

	if ct := bg.CostTypes; ct != nil {
		switch {
		case ct.UseAmortized != nil && *ct.UseAmortized:
			return "AmortizedCost"
		case ct.UseBlended != nil && *ct.UseBlended:
			return "BlendedCost"
		}
	}

	return "UnblendedCost"
}

// filtersOf returns the CostFilters, or the Dimensions clauses of a FilterExpression flattened to the same keys.
func filtersOf(bg *Budget) map[string][]string {
	if len(bg.CostFilters) > 0 || bg.FilterExpression == nil {
		return bg.CostFilters
	}

	out := map[string][]string{}
	collectDimensions(bg.FilterExpression, out)

	return out
}

func collectDimensions(e *Expression, out map[string][]string) {
	if d := e.Dimensions; d != nil {
		if key := dimensionFilterKey(d.Key); key != "" {
			out[key] = append(out[key], d.Values...)
		}
	}

	for i := range e.And {
		collectDimensions(&e.And[i], out)
	}
}

func dimensionFilterKey(dim string) string {
	switch dim {
	case "SERVICE":
		return "Service"
	case "REGION":
		return "Region"
	case "USAGE_TYPE":
		return "UsageType"
	case "LINKED_ACCOUNT":
		return "LinkedAccount"
	default:
		return ""
	}
}

func formatAmount(v float64) string {
	r := math.Round(v*amountPrecision) / amountPrecision

	return strconv.FormatFloat(r, 'f', -1, 64)
}

func parseAmount(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)

	return v
}

func limitUnit(bg *Budget) string {
	if bg.BudgetLimit != nil && bg.BudgetLimit.Unit != "" {
		return bg.BudgetLimit.Unit
	}

	if bg.BudgetType == budgetTypeUsage {
		return "N/A"
	}

	return defaultCurrency
}

// periodSpendLocked sums spend over [start, end); ok is false without a source or cost data.
func (b *InMemoryBackend) periodSpendLocked(bg *Budget, start, end time.Time) (float64, bool) {
	if b.spend == nil || (bg.BudgetType != budgetTypeCost && bg.BudgetType != budgetTypeUsage) {
		return 0, false
	}

	return b.spend.Spend(start, end, filtersOf(bg), metricFor(bg))
}

// limitForPeriod returns the limit that applies to the period starting at start.
func limitForPeriod(bg *Budget, start time.Time) *Spend {
	if len(bg.PlannedBudgetLimits) == 0 || bg.TimePeriod == nil || bg.TimePeriod.Start == nil {
		return bg.BudgetLimit
	}

	idx := 0
	for p := periodStart(bg.TimeUnit, bg.TimePeriod.Start.Time); p.Before(start); p = nextPeriod(bg.TimeUnit, p) {
		idx++
	}

	best, bestKey := bg.BudgetLimit, -1

	for k, v := range bg.PlannedBudgetLimits {
		if n, err := strconv.Atoi(k); err == nil && n <= idx && n > bestKey {
			best, bestKey = &Spend{Amount: v.Amount, Unit: v.Unit}, n
		}
	}

	return best
}

// calcSpendLocked derives CalculatedSpend for the current period.
func (b *InMemoryBackend) calcSpendLocked(bg *Budget) *CalculatedSpend {
	if bg.BudgetType != budgetTypeCost && bg.BudgetType != budgetTypeUsage {
		return &CalculatedSpend{ActualSpend: Spend{Amount: "0", Unit: unitPercentage}}
	}

	unit := limitUnit(bg)
	out := &CalculatedSpend{ActualSpend: Spend{Amount: "0", Unit: unit}}

	now := b.now()

	start, end, ok := currentPeriod(bg, now)
	if !ok {
		return out
	}

	actual, ok := b.periodSpendLocked(bg, start, minTime(end, startOfDay(now).AddDate(0, 0, 1)))
	if !ok {
		return out
	}

	out.ActualSpend.Amount = formatAmount(actual)
	out.ForecastedSpend = &Spend{Amount: formatAmount(forecast(actual, start, end, now)), Unit: unit}

	return out
}

// forecast projects the period total linearly from spend so far.
func forecast(actual float64, start, end, now time.Time) float64 {
	elapsed := max(now.Sub(start), time.Hour*hoursPerDay)

	total := end.Sub(start)
	if elapsed >= total {
		return actual
	}

	return actual * float64(total) / float64(elapsed)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}

	return b
}
