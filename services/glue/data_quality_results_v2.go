package glue

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// DQMetricValues mirrors types.DataQualityMetricValues.
type DQMetricValues struct {
	ActualValue   *float64 `json:"ActualValue,omitempty"`
	ExpectedValue *float64 `json:"ExpectedValue,omitempty"`
	LowerLimit    *float64 `json:"LowerLimit,omitempty"`
	UpperLimit    *float64 `json:"UpperLimit,omitempty"`
}

// DQMetricBasedObservation mirrors types.MetricBasedObservation.
type DQMetricBasedObservation struct {
	MetricValues *DQMetricValues `json:"MetricValues,omitempty"`
	MetricName   string          `json:"MetricName,omitempty"`
	StatisticID  string          `json:"StatisticId,omitempty"`
	NewRules     []string        `json:"NewRules,omitempty"`
}

// DQObservation mirrors types.DataQualityObservation.
type DQObservation struct {
	MetricBasedObservation *DQMetricBasedObservation `json:"MetricBasedObservation,omitempty"`
	Description            string                    `json:"Description,omitempty"`
}

// DQDistributionData mirrors types.DistributionData.
type DQDistributionData struct {
	DataType string   `json:"DataType,omitempty"`
	BinEdges []string `json:"BinEdges,omitempty"`
	Count    []int32  `json:"Count,omitempty"`
}

// DQAnalyzerResult mirrors types.DataQualityAnalyzerResult.
type DQAnalyzerResult struct {
	EvaluatedDistributions map[string]DQDistributionData `json:"EvaluatedDistributions,omitempty"`
	EvaluatedMetrics       map[string]float64            `json:"EvaluatedMetrics,omitempty"`
	Name                   string                        `json:"Name,omitempty"`
	Description            string                        `json:"Description,omitempty"`
	EvaluationMessage      string                        `json:"EvaluationMessage,omitempty"`
}

// DQRuleResult mirrors types.DataQualityRuleResult.
type DQRuleResult struct {
	EvaluatedMetrics  map[string]float64 `json:"EvaluatedMetrics,omitempty"`
	Labels            map[string]string  `json:"Labels,omitempty"`
	RuleMetrics       map[string]float64 `json:"RuleMetrics,omitempty"`
	Name              string             `json:"Name,omitempty"`
	Description       string             `json:"Description,omitempty"`
	EvaluatedRule     string             `json:"EvaluatedRule,omitempty"`
	EvaluationMessage string             `json:"EvaluationMessage,omitempty"`
	Result            string             `json:"Result,omitempty"`
}

// DQAggregatedMetrics mirrors types.DataQualityAggregatedMetrics.
type DQAggregatedMetrics struct {
	TotalRowsFailed     *float64 `json:"TotalRowsFailed,omitempty"`
	TotalRowsPassed     *float64 `json:"TotalRowsPassed,omitempty"`
	TotalRowsProcessed  *float64 `json:"TotalRowsProcessed,omitempty"`
	TotalRulesFailed    *float64 `json:"TotalRulesFailed,omitempty"`
	TotalRulesPassed    *float64 `json:"TotalRulesPassed,omitempty"`
	TotalRulesProcessed *float64 `json:"TotalRulesProcessed,omitempty"`
}

// DataQualityResultV2 is a result published through PublishDataQualityResultV2.
// Field names double as the persisted JSON and the DataQualityResultV2Item
// wire shape; ClientToken is stored for idempotent replay and stripped from
// responses.
type DataQualityResultV2 struct {
	ExecutionStartedOn   *awstime.EpochTime   `json:"ExecutionStartedOn,omitempty"`
	ExecutionCompletedOn *awstime.EpochTime   `json:"ExecutionCompletedOn,omitempty"`
	ResultPublishedOn    *awstime.EpochTime   `json:"ResultPublishedOn,omitempty"`
	AggregatedMetrics    *DQAggregatedMetrics `json:"AggregatedMetrics,omitempty"`
	Dimensions           map[string]string    `json:"Dimensions,omitempty"`
	Descriptors          map[string]string    `json:"Descriptors,omitempty"`
	Tags                 map[string]string    `json:"Tags,omitempty"`
	ResultID             string               `json:"ResultId"`
	EvaluationContext    string               `json:"EvaluationContext,omitempty"`
	RulesetName          string               `json:"RulesetName,omitempty"`
	ClientToken          string               `json:"ClientToken,omitempty"`
	RuleResults          []DQRuleResult       `json:"RuleResults,omitempty"`
	AnalyzerResults      []DQAnalyzerResult   `json:"AnalyzerResults,omitempty"`
	Observations         []DQObservation      `json:"Observations,omitempty"`
	Score                float64              `json:"Score"`
}

func dataQualityResultV2KeyFn(v *DataQualityResultV2) string { return v.ResultID }

// DataQualityResultV2Filter mirrors DataQualityResultsV2FilterCriteria.
type DataQualityResultV2Filter struct {
	PublishedAfter    *time.Time
	PublishedBefore   *time.Time
	Dimensions        map[string]string
	EvaluationContext string
	// HasDataSource is set when the caller filtered on DataSource; published
	// results never carry one, so such a filter matches nothing.
	HasDataSource bool
}

func (f *DataQualityResultV2Filter) matches(r *DataQualityResultV2) bool {
	if f == nil {
		return true
	}

	if f.HasDataSource {
		return false
	}

	if f.EvaluationContext != "" && r.EvaluationContext != f.EvaluationContext {
		return false
	}

	for k, v := range f.Dimensions {
		if got, ok := r.Dimensions[k]; !ok || got != v {
			return false
		}
	}

	if f.PublishedAfter == nil && f.PublishedBefore == nil {
		return true
	}

	if r.ResultPublishedOn == nil {
		return false
	}

	at := r.ResultPublishedOn.Time

	if f.PublishedAfter != nil && !at.After(*f.PublishedAfter) {
		return false
	}

	return f.PublishedBefore == nil || at.Before(*f.PublishedBefore)
}

func cloneDQResultV2(r *DataQualityResultV2) *DataQualityResultV2 {
	cp := *r
	cp.Dimensions = maps.Clone(r.Dimensions)
	cp.Descriptors = maps.Clone(r.Descriptors)
	cp.Tags = maps.Clone(r.Tags)
	cp.RuleResults = slices.Clone(r.RuleResults)
	cp.AnalyzerResults = slices.Clone(r.AnalyzerResults)
	cp.Observations = slices.Clone(r.Observations)

	return &cp
}

// PublishDataQualityResultV2 stores in (ResultId is assigned here) and mirrors
// it into the V1 result table so GetDataQualityResult and friends see it. A
// repeated ClientToken returns the originally published ResultId.
func (b *InMemoryBackend) PublishDataQualityResultV2(in DataQualityResultV2) (string, error) {
	for _, rr := range in.RuleResults {
		if rr.Result != "" && !slices.Contains([]string{"PASS", "FAIL", "ERROR"}, rr.Result) {
			return "", fmt.Errorf("%w: invalid rule result status %q", ErrValidation, rr.Result)
		}
	}

	b.mu.Lock("PublishDataQualityResultV2")
	defer b.mu.Unlock()

	if in.ClientToken != "" {
		for _, existing := range b.dataQualityResultV2.All() {
			if existing.ClientToken == in.ClientToken {
				return existing.ResultID, nil
			}
		}
	}

	rec := cloneDQResultV2(&in)
	rec.ResultID = awsid.New("dqresult-")
	published := awstime.NewEpochTime(time.Now().UTC())
	rec.ResultPublishedOn = &published

	b.dataQualityResultV2.Put(rec)
	b.dataQualityResult.Put(&DataQualityResult{ResultID: rec.ResultID, Score: rec.Score})

	return rec.ResultID, nil
}

// dqResultV2Locked resolves id from the V2 table, falling back to a V1-only
// result (seeded without the V2 detail). Caller holds b.mu.
func (b *InMemoryBackend) dqResultV2Locked(id string) (*DataQualityResultV2, bool) {
	if r, ok := b.dataQualityResultV2.Get(id); ok {
		return cloneDQResultV2(r), true
	}

	if v1, ok := b.dataQualityResult.Get(id); ok {
		return &DataQualityResultV2{ResultID: v1.ResultID, Score: v1.Score}, true
	}

	return nil, false
}

// BatchGetDataQualityResultV2 returns the found results and the IDs not found.
func (b *InMemoryBackend) BatchGetDataQualityResultV2(ids []string) ([]*DataQualityResultV2, []string) {
	b.mu.RLock("BatchGetDataQualityResultV2")
	defer b.mu.RUnlock()

	found := make([]*DataQualityResultV2, 0, len(ids))
	notFound := make([]string, 0)

	for _, id := range ids {
		if r, ok := b.dqResultV2Locked(id); ok {
			found = append(found, r)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// ListDataQualityResultsV2 returns every result matching filter, ordered by
// publish time then ResultId. V1-only results are included.
func (b *InMemoryBackend) ListDataQualityResultsV2(filter *DataQualityResultV2Filter) []*DataQualityResultV2 {
	b.mu.RLock("ListDataQualityResultsV2")
	defer b.mu.RUnlock()

	out := make([]*DataQualityResultV2, 0, b.dataQualityResultV2.Len()+b.dataQualityResult.Len())

	for _, r := range b.dataQualityResultV2.All() {
		if filter.matches(r) {
			out = append(out, cloneDQResultV2(r))
		}
	}

	for _, v1 := range b.dataQualityResult.All() {
		if _, isV2 := b.dataQualityResultV2.Get(v1.ResultID); isV2 {
			continue
		}

		r := &DataQualityResultV2{ResultID: v1.ResultID, Score: v1.Score}
		if filter.matches(r) {
			out = append(out, r)
		}
	}

	slices.SortFunc(out, func(a, c *DataQualityResultV2) int {
		var ta, tc time.Time
		if a.ResultPublishedOn != nil {
			ta = a.ResultPublishedOn.Time
		}

		if c.ResultPublishedOn != nil {
			tc = c.ResultPublishedOn.Time
		}

		if d := ta.Compare(tc); d != 0 {
			return d
		}

		return strings.Compare(a.ResultID, c.ResultID)
	})

	return out
}
