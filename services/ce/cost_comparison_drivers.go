package ce

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

const costDriverTypeUsageChange = "USAGE_CHANGE"

// costDriver is one usage-type level contribution to a group's cost delta.
type costDriver struct {
	Metrics map[string]comparisonMetricValue `json:"Metrics,omitempty"`
	Name    string                           `json:"Name"`
	Type    string                           `json:"Type"`
}

// costComparisonDriver mirrors types.CostComparisonDriver.
type costComparisonDriver struct {
	CostSelector *ceExpression                    `json:"CostSelector,omitempty"`
	Metrics      map[string]comparisonMetricValue `json:"Metrics,omitempty"`
	CostDrivers  []costDriver                     `json:"CostDrivers"`
}

type driverTotals struct {
	baseline   float64
	comparison float64
}

func (t driverTotals) diff() float64 { return t.comparison - t.baseline }

// costComparisonDrivers compares ledger spend across two periods and
// attributes each group's delta to the usage types that changed.
func (b *InMemoryBackend) costComparisonDrivers(
	base, cmp [2]string, metric, groupKey string, filter *ceExpression,
) []costComparisonDriver {
	b.mu.RLock("costComparisonDrivers")
	defer b.mu.RUnlock()

	groups := map[string]map[string]*driverTotals{}

	collect := func(period [2]string, isBaseline bool) {
		entries := filterEntriesByExpression(b.costLedgerInBucket(period[0], period[1]), filter)

		for _, e := range entries {
			group := ""
			if groupKey != "" {
				group = extractGroupKeys(e, []GroupBySpec{{Type: "DIMENSION", Key: groupKey}})[0]
			}

			if groups[group] == nil {
				groups[group] = map[string]*driverTotals{}
			}

			t := groups[group][e.UsageType]
			if t == nil {
				t = &driverTotals{}
				groups[group][e.UsageType] = t
			}

			if isBaseline {
				t.baseline += getMetricValue(e, metric)
			} else {
				t.comparison += getMetricValue(e, metric)
			}
		}
	}

	collect(base, true)
	collect(cmp, false)

	out := make([]costComparisonDriver, 0, len(groups))

	for _, group := range collections.SortedKeys(groups) {
		if d, ok := buildComparisonDriver(group, groupKey, metric, groups[group]); ok {
			out = append(out, d)
		}
	}

	return out
}

func buildComparisonDriver(
	group, groupKey, metric string, usage map[string]*driverTotals,
) (costComparisonDriver, bool) {
	var total driverTotals

	drivers := make([]costDriver, 0, len(usage))

	for _, ut := range collections.SortedKeys(usage) {
		t := usage[ut]
		total.baseline += t.baseline
		total.comparison += t.comparison

		if t.diff() == 0 {
			continue
		}

		drivers = append(drivers, costDriver{
			Name:    ut,
			Type:    costDriverTypeUsageChange,
			Metrics: comparisonMetricEntry(t.baseline, t.comparison, metric),
		})
	}

	if len(drivers) == 0 {
		return costComparisonDriver{}, false
	}

	sort.SliceStable(drivers, func(i, j int) bool {
		return absDiff(drivers[i].Metrics[metric]) > absDiff(drivers[j].Metrics[metric])
	})

	d := costComparisonDriver{
		Metrics:     comparisonMetricEntry(total.baseline, total.comparison, metric),
		CostDrivers: drivers,
	}

	if groupKey != "" {
		d.CostSelector = &ceExpression{Dimensions: &ceDimensionValues{Key: groupKey, Values: []string{group}}}
	}

	return d, true
}

func absDiff(m comparisonMetricValue) float64 {
	var v float64

	_, _ = fmt.Sscanf(m.Difference, "%f", &v)
	if v < 0 {
		return -v
	}

	return v
}
