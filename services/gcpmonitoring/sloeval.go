package gcpmonitoring

import (
	"math"
	"regexp"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	selectBurnRate = "select_slo_burn_rate"
	selectHealth   = "select_slo_health"
	daysPerWeek    = 7
	daysPerFortnit = 14
)

var sloSelectorRe = regexp.MustCompile(
	`^\s*(select_slo_burn_rate|select_slo_health)\(\s*"([^"]+)"\s*(?:,\s*"([^"]+)"\s*)?\)\s*$`)

// sloSelector is a select_slo_burn_rate or select_slo_health time series selector.
type sloSelector struct {
	kind     string
	slo      string
	lookback time.Duration
}

func parseSLOSelector(filter string) (sloSelector, bool, error) {
	m := sloSelectorRe.FindStringSubmatch(filter)
	if m == nil {
		return sloSelector{}, false, nil
	}

	sel := sloSelector{kind: m[1], slo: m[2]}

	if _, _, err := sloParts(sel.slo); err != nil {
		return sel, true, err
	}

	if m[3] != "" {
		d, err := time.ParseDuration(m[3])
		if err != nil || d <= 0 {
			return sel, true, errInvalid("The lookback period %q of %s is not a valid duration.", m[3], sel.kind)
		}

		sel.lookback = d
	}

	if sel.kind == selectBurnRate && sel.lookback == 0 {
		return sel, true, errInvalid("%s requires a lookback period.", selectBurnRate)
	}

	return sel, true, nil
}

// SLOStatus is the standing of an objective over its current compliance period.
type SLOStatus struct {
	PeriodStart    time.Time
	PeriodEnd      time.Time
	Good           float64
	Total          float64
	Compliance     float64
	BudgetFraction float64
	BurnRate       float64
}

// SLOStatus evaluates the objective against the stored series at the backend clock's time.
func (b *InMemoryBackend) SLOStatus(name string) (SLOStatus, error) {
	if _, _, err := sloParts(name); err != nil {
		return SLOStatus{}, err
	}

	b.mu.RLock("SLOStatus")
	defer b.mu.RUnlock()

	slo, ok := b.slos[name]
	if !ok {
		return SLOStatus{}, errNotFound("Service level objective %s does not exist.", name)
	}

	now := b.now()
	st := SLOStatus{PeriodStart: sloPeriodStart(slo, now), PeriodEnd: now}
	project, _, _ := splitName(sloServiceOf(name), collServices)

	good, total, ok := b.sloCountsLocked(project, slo, window{start: st.PeriodStart, end: now})
	if !ok || total == 0 {
		st.BudgetFraction, st.Compliance = 1, 1

		return st, nil
	}

	st.Good, st.Total = good, total
	st.Compliance = good / total
	st.BurnRate = burnRate(good, total, slo.GetGoal())
	st.BudgetFraction = 1 - st.BurnRate

	return st, nil
}

func sloServiceOf(name string) string {
	parent, _, _ := sloParts(name)

	return parent
}

func burnRate(good, total, goal float64) float64 {
	return (total - good) / total / (1 - goal)
}

func sloPeriodStart(slo *monitoringpb.ServiceLevelObjective, now time.Time) time.Time {
	if r, ok := slo.GetPeriod().(*monitoringpb.ServiceLevelObjective_RollingPeriod); ok {
		return now.Add(-r.RollingPeriod.AsDuration())
	}

	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	switch slo.GetCalendarPeriod() {
	case calendarperiod.CalendarPeriod_WEEK:
		return midnight.AddDate(0, 0, -((int(now.Weekday()) + daysPerWeek - 1) % daysPerWeek))
	case calendarperiod.CalendarPeriod_FORTNIGHT:
		return midnight.AddDate(0, 0, -(now.YearDay()-1)%daysPerFortnit)
	case calendarperiod.CalendarPeriod_MONTH:
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	case calendarperiod.CalendarPeriod_CALENDAR_PERIOD_UNSPECIFIED, calendarperiod.CalendarPeriod_DAY,
		calendarperiod.CalendarPeriod_QUARTER, calendarperiod.CalendarPeriod_HALF, calendarperiod.CalendarPeriod_YEAR:
	}

	return midnight
}

// sloCountsLocked returns the good and total units (requests or windows) of the objective's SLI in the window.
func (b *InMemoryBackend) sloCountsLocked(
	project string, slo *monitoringpb.ServiceLevelObjective, w window,
) (float64, float64, bool) {
	svc := b.services[sloServiceOf(slo.GetName())]

	switch t := slo.GetServiceLevelIndicator().GetType().(type) {
	case *monitoringpb.ServiceLevelIndicator_BasicSli:
		return b.basicCountsLocked(project, svc, t.BasicSli, w)
	case *monitoringpb.ServiceLevelIndicator_RequestBased:
		return b.requestCountsLocked(project, t.RequestBased, w)
	case *monitoringpb.ServiceLevelIndicator_WindowsBased:
		return b.windowCountsLocked(project, svc, t.WindowsBased, w)
	}

	return 0, 0, false
}

func (b *InMemoryBackend) sumLocked(project, filterSrc string, w window) (float64, bool) {
	if filterSrc == "" || !w.end.After(w.start) {
		return 0, false
	}

	a := &monitoringpb.Aggregation{
		PerSeriesAligner: alignDelta, AlignmentPeriod: durationpb.New(w.end.Sub(w.start)),
	}

	q, ok := conditionQuery(project, filterSrc, []*monitoringpb.Aggregation{a}, w)
	if !ok {
		return 0, false
	}

	series, err := b.selectLocked(q)
	if err != nil {
		return 0, false
	}

	var total float64

	for _, s := range series {
		for _, p := range s.pts {
			total += number(p.GetValue())
		}
	}

	return total, true
}

func (b *InMemoryBackend) requestCountsLocked(
	project string, r *monitoringpb.RequestBasedSli, w window,
) (float64, float64, bool) {
	switch m := r.GetMethod().(type) {
	case *monitoringpb.RequestBasedSli_GoodTotalRatio:
		good, hasGood := b.sumLocked(project, m.GoodTotalRatio.GetGoodServiceFilter(), w)
		bad, hasBad := b.sumLocked(project, m.GoodTotalRatio.GetBadServiceFilter(), w)
		total, hasTotal := b.sumLocked(project, m.GoodTotalRatio.GetTotalServiceFilter(), w)

		switch {
		case hasGood && hasTotal:
			return good, total, true
		case hasGood && hasBad:
			return good, good + bad, true
		case hasBad && hasTotal:
			return total - bad, total, true
		}
	case *monitoringpb.RequestBasedSli_DistributionCut:
		return b.cutCountsLocked(project, m.DistributionCut, w)
	}

	return 0, 0, false
}

func (b *InMemoryBackend) cutCountsLocked(
	project string, c *monitoringpb.DistributionCut, w window,
) (float64, float64, bool) {
	if !w.end.After(w.start) {
		return 0, 0, false
	}

	a := &monitoringpb.Aggregation{
		PerSeriesAligner: alignDelta, AlignmentPeriod: durationpb.New(w.end.Sub(w.start)),
	}

	q, ok := conditionQuery(project, c.GetDistributionFilter(), []*monitoringpb.Aggregation{a}, w)
	if !ok {
		return 0, 0, false
	}

	series, err := b.selectLocked(q)
	if err != nil {
		return 0, 0, false
	}

	var good, total float64

	for _, s := range series {
		for _, p := range s.pts {
			d := p.GetValue().GetDistributionValue()
			total += float64(d.GetCount())
			good += countInRange(d, c.GetRange().GetMin(), c.GetRange().GetMax())
		}
	}

	return good, total, true
}

// countInRange is the number of samples in [lo, hi], assuming samples spread evenly inside each finite bucket.
// The underflow and overflow buckets count wholly in or out by their single finite edge.
func countInRange(d *distribution.Distribution, lo, hi float64) float64 {
	if bucketCount(d) != len(d.GetBucketCounts()) {
		return 0
	}

	last := len(d.GetBucketCounts()) - 1

	var n float64

	for i, c := range d.GetBucketCounts() {
		if c == 0 {
			continue
		}

		bl, bu := bucketBounds(d, i)

		if i == 0 || i == last {
			if bl >= lo && bl <= hi {
				n += float64(c)
			}

			continue
		}

		n += float64(c) * overlap(bl, bu, lo, hi)
	}

	return n
}

func overlap(bl, bu, lo, hi float64) float64 {
	l, u := math.Max(bl, lo), math.Min(bu, hi)
	if u < l || bu == bl {
		return 0
	}

	return (u - l) / (bu - bl)
}

func (b *InMemoryBackend) windowCountsLocked(
	project string, svc *monitoringpb.Service, wb *monitoringpb.WindowsBasedSli, w window,
) (float64, float64, bool) {
	period := wb.GetWindowPeriod().AsDuration()
	if period <= 0 {
		return 0, 0, false
	}

	var good, total float64

	for start := w.start; start.Add(period).Compare(w.end) <= 0; start = start.Add(period) {
		g, ok := b.windowGoodLocked(project, svc, wb, window{start: start, end: start.Add(period)})
		if !ok {
			continue
		}

		total++

		if g {
			good++
		}
	}

	return good, total, total > 0
}

func (b *InMemoryBackend) windowGoodLocked(
	project string, svc *monitoringpb.Service, wb *monitoringpb.WindowsBasedSli, w window,
) (bool, bool) {
	switch c := wb.GetWindowCriterion().(type) {
	case *monitoringpb.WindowsBasedSli_GoodBadMetricFilter:
		return b.anyTrueLocked(project, c.GoodBadMetricFilter, w)
	case *monitoringpb.WindowsBasedSli_GoodTotalRatioThreshold:
		perf := c.GoodTotalRatioThreshold

		var good, total float64

		var ok bool

		switch t := perf.GetType().(type) {
		case *monitoringpb.WindowsBasedSli_PerformanceThreshold_Performance:
			good, total, ok = b.requestCountsLocked(project, t.Performance, w)
		case *monitoringpb.WindowsBasedSli_PerformanceThreshold_BasicSliPerformance:
			good, total, ok = b.basicCountsLocked(project, svc, t.BasicSliPerformance, w)
		}

		if !ok || total == 0 {
			return false, false
		}

		return good/total >= perf.GetThreshold(), true
	case *monitoringpb.WindowsBasedSli_MetricMeanInRange:
		return b.metricInRangeLocked(project, c.MetricMeanInRange, w, true)
	case *monitoringpb.WindowsBasedSli_MetricSumInRange:
		return b.metricInRangeLocked(project, c.MetricSumInRange, w, false)
	}

	return false, false
}

func (b *InMemoryBackend) rawSeriesLocked(project, filterSrc string, w window) []*aSeries {
	q, ok := conditionQuery(project, filterSrc, nil, w)
	if !ok {
		return nil
	}

	series, err := b.selectLocked(q)
	if err != nil {
		return nil
	}

	return series
}

func (b *InMemoryBackend) anyTrueLocked(project, filterSrc string, w window) (bool, bool) {
	series := b.rawSeriesLocked(project, filterSrc, w)
	seen := false

	for _, s := range series {
		for _, p := range s.pts {
			seen = true

			if p.GetValue().GetBoolValue() {
				return true, true
			}
		}
	}

	return false, seen
}

func (b *InMemoryBackend) metricInRangeLocked(
	project string, r *monitoringpb.WindowsBasedSli_MetricRange, w window, mean bool,
) (bool, bool) {
	var total float64

	n := 0

	for _, s := range b.rawSeriesLocked(project, r.GetTimeSeries(), w) {
		for _, p := range s.pts {
			total += number(p.GetValue())
			n++
		}
	}

	if n == 0 {
		return false, false
	}

	if mean {
		total /= float64(n)
	}

	return total >= r.GetRange().GetMin() && total <= r.GetRange().GetMax(), true
}

func (b *InMemoryBackend) basicCountsLocked(
	project string, svc *monitoringpb.Service, basic *monitoringpb.BasicSli, w window,
) (float64, float64, bool) {
	r, ok := expandBasicSli(svc, basic)
	if !ok {
		return 0, 0, false
	}

	return b.requestCountsLocked(project, r, w)
}

// sloSelectorSeriesLocked evaluates a selector at now and the preceding steps, one point per period.
func (b *InMemoryBackend) sloSelectorSeriesLocked(
	sel sloSelector, period time.Duration, steps int, now time.Time,
) []*aSeries {
	slo, ok := b.slos[sel.slo]
	if !ok {
		return nil
	}

	project, _, _ := splitName(sloServiceOf(sel.slo), collServices)
	s := &aSeries{
		metric: &metricpb.Metric{Type: sel.kind},
		resource: &monitoredres.MonitoredResource{
			Type:   "service_level_objective",
			Labels: map[string]string{"slo": sel.slo},
		},
		kind: kindGauge, vtype: vtDouble,
	}

	for k := steps - 1; k >= 0; k-- {
		at := now.Add(-time.Duration(k) * period)
		span := period

		if sel.kind == selectBurnRate {
			span = sel.lookback
		}

		good, total, has := b.sloCountsLocked(project, slo, window{start: at.Add(-span), end: at})
		if !has || total == 0 {
			continue
		}

		v := good / total
		if sel.kind == selectBurnRate {
			v = burnRate(good, total, slo.GetGoal())
		}

		s.pts = append(s.pts, pointAt(at, at, doubleValue(v)))
	}

	if len(s.pts) == 0 {
		return nil
	}

	return []*aSeries{s}
}
