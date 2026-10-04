package cloudwatch

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	defaultInsightContributors = 10
	maxInsightContributors     = 100
	orderByMaximum             = "Maximum"

	statUniqueContributors  = "UniqueContributors"
	statMaxContributorValue = "MaxContributorValue"
	statSampleCount         = "SampleCount"
	statMinimum             = "Minimum"

	aggregationStatSum   = "SUM"
	aggregationStatCount = "COUNT"
)

// LogEventSource supplies CloudWatch Logs events to Contributor Insights rules.
type LogEventSource interface {
	InsightEvents(region string, logGroupPatterns []string, start, end time.Time) []InsightLogEvent
}

// InsightLogEvent is one log event as seen by a Contributor Insights rule.
type InsightLogEvent struct {
	Timestamp time.Time
	Message   string
}

// InsightRuleReportRequest is the validated input of GetInsightRuleReport.
type InsightRuleReportRequest struct {
	StartTime       time.Time
	EndTime         time.Time
	RuleName        string
	OrderBy         string
	Metrics         []string
	Period          int
	MaxContributors int
}

// InsightRuleContributorDatapoint is one contributor value in one period.
type InsightRuleContributorDatapoint struct {
	Timestamp        time.Time
	ApproximateValue float64
}

// InsightRuleContributor is one unique key combination and its aggregate value.
type InsightRuleContributor struct {
	Keys                      []string
	Datapoints                []InsightRuleContributorDatapoint
	ApproximateAggregateValue float64
}

// InsightRuleMetricDatapoint carries only the statistics the request asked for.
type InsightRuleMetricDatapoint struct {
	Timestamp           time.Time
	UniqueContributors  *float64
	MaxContributorValue *float64
	SampleCount         *float64
	Sum                 *float64
	Minimum             *float64
	Maximum             *float64
	Average             *float64
}

// InsightRuleReport is the GetInsightRuleReport result.
type InsightRuleReport struct {
	AggregationStatistic   string
	KeyLabels              []string
	Contributors           []InsightRuleContributor
	MetricDatapoints       []InsightRuleMetricDatapoint
	AggregateValue         float64
	ApproximateUniqueCount int64
}

func validInsightMetrics() []string {
	return []string{
		statUniqueContributors, statMaxContributorValue, statSampleCount, aggregateSum, statMinimum,
		orderByMaximum, widgetDefaultStat,
	}
}

// Validate applies the documented GetInsightRuleReport parameter constraints.
func (r *InsightRuleReportRequest) Validate() error {
	if r.RuleName == "" || r.StartTime.IsZero() || r.EndTime.IsZero() || r.Period == 0 {
		return fmt.Errorf("%w: RuleName, StartTime, EndTime and Period are required", ErrMissingParameter)
	}

	switch {
	case r.Period < 1:
		return fmt.Errorf("%w: Period must be at least 1", ErrValidation)
	case !r.StartTime.Before(r.EndTime):
		return fmt.Errorf("%w: StartTime must be before EndTime", ErrValidation)
	case r.MaxContributors < 0 || r.MaxContributors > maxInsightContributors:
		return fmt.Errorf("%w: MaxContributorCount must be between 1 and %d", ErrValidation, maxInsightContributors)
	case r.OrderBy != "" && r.OrderBy != aggregateSum && r.OrderBy != orderByMaximum:
		return fmt.Errorf("%w: OrderBy must be Sum or Maximum", ErrValidation)
	}

	for _, m := range r.Metrics {
		if !slices.Contains(validInsightMetrics(), m) {
			return fmt.Errorf("%w: unknown Metrics value %q", ErrValidation, m)
		}
	}

	return nil
}

// SetLogEventSource wires the CloudWatch Logs events Contributor Insights rules evaluate.
func (b *InMemoryBackend) SetLogEventSource(src LogEventSource) {
	b.mu.Lock("SetLogEventSource")
	defer b.mu.Unlock()
	b.logEvents = src
}

// SetLogEventSource wires src into the home backend and every regional sibling.
func (h *Handler) SetLogEventSource(src LogEventSource) {
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		bk.SetLogEventSource(src)
	}

	for _, p := range h.peers.All() {
		if bk, ok := p.Backend.(*InMemoryBackend); ok {
			bk.SetLogEventSource(src)
		}
	}
}

// GetInsightRuleReport evaluates the rule over the region's CloudWatch Logs events.
func (b *InMemoryBackend) GetInsightRuleReport(req InsightRuleReportRequest) (*InsightRuleReport, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	b.mu.RLock("GetInsightRuleReport")
	rule, ok := b.insightRules.Get(req.RuleName)
	if !ok {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: %s", ErrInsightRuleNotFound, req.RuleName)
	}

	cp := *rule
	src, region := b.logEvents, b.region
	b.mu.RUnlock()

	spec := evaluableSpec(&cp)
	if spec == nil {
		return &InsightRuleReport{AggregationStatistic: aggregationStatCount, KeyLabels: []string{}}, nil
	}

	report := &InsightRuleReport{AggregationStatistic: aggregationStatCount, KeyLabels: spec.keyLabels()}
	if spec.aggregatesSum() {
		report.AggregationStatistic = aggregationStatSum
	}

	if cp.State == insightRuleStateDisabled || src == nil {
		return report, nil
	}

	events := src.InsightEvents(region, spec.LogGroupNames, req.StartTime, req.EndTime)
	agg := aggregateInsightEvents(spec, events, req)
	agg.fill(report, req)

	return report, nil
}

// evaluableSpec parses a custom rule, or returns nil for managed or unusable rules.
func evaluableSpec(rule *InsightRule) *insightRuleSpec {
	if rule.ManagedRule {
		return nil
	}

	spec, err := parseInsightRuleSpec(rule.Definition)
	if err != nil || len(spec.Contribution.Keys) == 0 {
		return nil
	}

	return spec
}

type contributorAgg struct {
	points map[int64]float64
	keys   []string
	total  float64
}

type bucketAgg struct {
	contrib map[string]float64
	samples float64
	sum     float64
	minV    float64
	maxV    float64
}

type insightAggregation struct {
	contribs map[string]*contributorAgg
	buckets  map[int64]*bucketAgg
}

func aggregateInsightEvents(
	spec *insightRuleSpec,
	events []InsightLogEvent,
	req InsightRuleReportRequest,
) insightAggregation {
	agg := insightAggregation{contribs: map[string]*contributorAgg{}, buckets: map[int64]*bucketAgg{}}
	period := time.Duration(req.Period) * time.Second

	for _, ev := range events {
		keys, val, ok := spec.observe(ev.Message)
		if !ok {
			continue
		}

		id := strings.Join(keys, "\x00")
		bucket := int64(ev.Timestamp.Sub(req.StartTime) / period)

		c := agg.contribs[id]
		if c == nil {
			c = &contributorAgg{keys: keys, points: map[int64]float64{}}
			agg.contribs[id] = c
		}

		c.total += val
		c.points[bucket] += val

		bk := agg.buckets[bucket]
		if bk == nil {
			bk = &bucketAgg{contrib: map[string]float64{}, minV: val, maxV: val}
			agg.buckets[bucket] = bk
		}

		bk.contrib[id] += val
		bk.samples++
		bk.sum += val
		bk.minV = min(bk.minV, val)
		bk.maxV = max(bk.maxV, val)
	}

	return agg
}

func (a insightAggregation) fill(report *InsightRuleReport, req InsightRuleReportRequest) {
	period := time.Duration(req.Period) * time.Second
	all := make([]InsightRuleContributor, 0, len(a.contribs))

	for _, c := range a.contribs {
		report.AggregateValue += c.total
		all = append(all, c.toContributor(req.StartTime, period))
	}

	report.ApproximateUniqueCount = int64(len(all))
	report.Contributors = rankContributors(all, req.OrderBy, req.MaxContributors)

	idxs := make([]int64, 0, len(a.buckets))
	for i := range a.buckets {
		idxs = append(idxs, i)
	}

	slices.Sort(idxs)

	for _, i := range idxs {
		report.MetricDatapoints = append(
			report.MetricDatapoints, a.buckets[i].datapoint(req.StartTime.Add(time.Duration(i)*period), req.Metrics),
		)
	}
}

func (c *contributorAgg) toContributor(start time.Time, period time.Duration) InsightRuleContributor {
	idxs := make([]int64, 0, len(c.points))
	for i := range c.points {
		idxs = append(idxs, i)
	}

	slices.Sort(idxs)

	out := InsightRuleContributor{Keys: c.keys, ApproximateAggregateValue: c.total}
	for _, i := range idxs {
		out.Datapoints = append(out.Datapoints, InsightRuleContributorDatapoint{
			Timestamp: start.Add(time.Duration(i) * period), ApproximateValue: c.points[i],
		})
	}

	return out
}

func rankContributors(all []InsightRuleContributor, orderBy string, limit int) []InsightRuleContributor {
	score := func(c InsightRuleContributor) float64 {
		if orderBy != orderByMaximum {
			return c.ApproximateAggregateValue
		}

		best := 0.0
		for _, d := range c.Datapoints {
			best = max(best, d.ApproximateValue)
		}

		return best
	}

	slices.SortFunc(all, func(x, y InsightRuleContributor) int {
		if d := cmp.Compare(score(y), score(x)); d != 0 {
			return d
		}

		return slices.Compare(x.Keys, y.Keys)
	})

	if limit == 0 {
		limit = defaultInsightContributors
	}

	if len(all) > limit {
		all = all[:limit]
	}

	return all
}

func (b *bucketAgg) datapoint(ts time.Time, metrics []string) InsightRuleMetricDatapoint {
	dp := InsightRuleMetricDatapoint{Timestamp: ts}

	for _, m := range metrics {
		switch m {
		case statUniqueContributors:
			dp.UniqueContributors = new(float64(len(b.contrib)))
		case statMaxContributorValue:
			top := 0.0
			for _, v := range b.contrib {
				top = max(top, v)
			}

			dp.MaxContributorValue = new(top)
		case statSampleCount:
			dp.SampleCount = new(b.samples)
		case aggregateSum:
			dp.Sum = new(b.sum)
		case statMinimum:
			dp.Minimum = new(b.minV)
		case orderByMaximum:
			dp.Maximum = new(b.maxV)
		case widgetDefaultStat:
			dp.Average = new(b.sum / b.samples)
		}
	}

	return dp
}
