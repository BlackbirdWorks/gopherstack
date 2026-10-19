package budgets

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	historyDaily     = 60
	historyMonthly   = 13
	historyQuarterly = 4
	historyAnnually  = 2
)

func historyDepth(unit string) int {
	switch unit {
	case timeUnitDaily:
		return historyDaily
	case timeUnitQuarterly:
		return historyQuarterly
	case timeUnitAnnually:
		return historyAnnually
	default:
		return historyMonthly
	}
}

// BudgetPerformance returns per-period budgeted and actual amounts, newest period last.
func (b *InMemoryBackend) BudgetPerformance(
	account, name string, tp *TimePeriod, token string, maxResults *int32,
) (*PerformanceHistory, string, error) {
	if err := validateAccount(account); err != nil {
		return nil, "", err
	}

	b.mu.RLock("BudgetPerformance")
	defer b.mu.RUnlock()

	rec, err := b.budgetLocked(account, name)
	if err != nil {
		return nil, "", err
	}

	bg := &rec.Budget
	now := b.now()

	var periods []BudgetedAndActualAmounts

	for _, start := range b.historyStartsLocked(bg, now, tp) {
		end := nextPeriod(bg.TimeUnit, start)
		if bg.TimeUnit == timeUnitCustom {
			end = bg.TimePeriod.End.Time
		}

		item := BudgetedAndActualAmounts{TimePeriod: &TimePeriod{Start: ts(start), End: ts(end)}}

		if l := limitForPeriod(bg, start); l != nil {
			item.BudgetedAmount = &Spend{Amount: l.Amount, Unit: l.Unit}
		}

		actual, _ := b.periodSpendLocked(bg, start, minTime(end, startOfDay(now).AddDate(0, 0, 1)))
		item.ActualAmount = &Spend{Amount: formatAmount(actual), Unit: limitUnit(bg)}
		periods = append(periods, item)
	}

	pg, err := paginate(periods, token, maxResults, maxPageSize)
	if err != nil {
		return nil, "", err
	}

	return performanceOf(bg, pg), pg.Next, nil
}

func performanceOf(bg *Budget, pg page.Page[BudgetedAndActualAmounts]) *PerformanceHistory {
	cp := deepCopy(bg)

	return &PerformanceHistory{
		BillingViewArn: cp.BillingViewArn, BudgetName: cp.BudgetName, BudgetType: cp.BudgetType,
		TimeUnit: cp.TimeUnit, CostFilters: cp.CostFilters, CostTypes: cp.CostTypes,
		FilterExpression: cp.FilterExpression, Metrics: cp.Metrics, BudgetedAndActualAmountsList: pg.Data,
	}
}

// historyStartsLocked lists period starts from the budget start (or the default depth) up to the current period.
func (b *InMemoryBackend) historyStartsLocked(bg *Budget, now time.Time, tp *TimePeriod) []time.Time {
	if bg.TimeUnit == timeUnitCustom {
		return []time.Time{bg.TimePeriod.Start.Time}
	}

	cur := periodStart(bg.TimeUnit, now)
	earliest := cur

	for i := 1; i < historyDepth(bg.TimeUnit); i++ {
		earliest = prevPeriod(bg.TimeUnit, earliest)
	}

	if s := periodStart(bg.TimeUnit, bg.TimePeriod.Start.Time); s.After(earliest) {
		earliest = s
	}

	var out []time.Time

	for s := earliest; !s.After(cur); s = nextPeriod(bg.TimeUnit, s) {
		if tp != nil && ((tp.Start != nil && nextPeriod(bg.TimeUnit, s).Before(tp.Start.Time)) ||
			(tp.End != nil && s.After(tp.End.Time))) {
			continue
		}

		out = append(out, s)
	}

	return out
}
