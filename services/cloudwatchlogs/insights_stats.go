package cloudwatchlogs

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// percentBase is the divisor used to convert a percentile rank to a fraction.
const percentBase = 100.0

// medianPercentile is the percentile used by the median aggregation.
const medianPercentile = 50.0

// aggTerm is one aggregation column in a stats command.
type aggTerm struct {
	factory func() aggregator
	alias   string
}

// aggregator accumulates values for a single group and produces a result.
type aggregator interface {
	add(rec *insightsRecord)
	result() cwValue
}

// parseStatsCommand parses "stats agg [as alias], ... [by field, ...]".
func parseStatsCommand(q *insightsQuery, rest string) error {
	aggPart, byPart := splitStatsBy(rest)

	terms, err := parseAggTerms(aggPart)
	if err != nil {
		return err
	}

	if len(terms) == 0 {
		return fmt.Errorf("%w: stats requires an aggregation", ErrParseExpr)
	}

	groupFields := parseGroupBy(byPart)

	outputs := make([]string, 0, len(groupFields)+len(terms))
	outputs = append(outputs, groupFields...)
	for _, t := range terms {
		outputs = append(outputs, t.alias)
	}

	q.hasStats = true
	q.stages = append(q.stages, &statsStage{terms: terms, groupFields: groupFields})
	q.outputFields = outputs

	return nil
}

// splitStatsBy splits the stats body into its aggregation and group-by segments.
func splitStatsBy(rest string) (string, string) {
	lower := strings.ToLower(rest)
	idx := indexTopLevelWord(lower, "by")
	if idx < 0 {
		return strings.TrimSpace(rest), ""
	}

	return strings.TrimSpace(rest[:idx]), strings.TrimSpace(rest[idx+len(" by "):])
}

// parseGroupBy parses the comma-separated group-by field list.
func parseGroupBy(byPart string) []string {
	if byPart == "" {
		return nil
	}

	var fields []string
	for _, f := range splitTopLevelCommas(byPart) {
		if f != "" {
			fields = append(fields, f)
		}
	}

	return fields
}

// parseAggTerms parses the comma-separated aggregation terms.
func parseAggTerms(aggPart string) ([]aggTerm, error) {
	var terms []aggTerm

	for _, term := range splitTopLevelCommas(aggPart) {
		if term == "" {
			continue
		}

		exprText, alias := splitAlias(term)
		if alias == "" {
			alias = strings.TrimSpace(exprText)
		}

		factory, err := buildAggregator(exprText)
		if err != nil {
			return nil, err
		}

		terms = append(terms, aggTerm{factory: factory, alias: alias})
	}

	return terms, nil
}

// aggFuncArgs holds a parsed aggregation function call.
type aggFuncArgs struct {
	name string
	args []string
}

// parseAggCall splits "name(a, b)" into its name and argument list.
func parseAggCall(exprText string) (aggFuncArgs, error) {
	open := strings.IndexByte(exprText, '(')
	if open < 0 || !strings.HasSuffix(strings.TrimSpace(exprText), ")") {
		return aggFuncArgs{}, fmt.Errorf("%w: invalid aggregation %q", ErrParseExpr, exprText)
	}

	name := strings.ToLower(strings.TrimSpace(exprText[:open]))
	inner := strings.TrimSpace(exprText[open+1 : strings.LastIndexByte(exprText, ')')])

	var args []string
	for _, a := range splitTopLevelCommas(inner) {
		if a != "" {
			args = append(args, a)
		}
	}

	return aggFuncArgs{name: name, args: args}, nil
}

// buildAggregator maps an aggregation expression to an aggregator factory.
func buildAggregator(exprText string) (func() aggregator, error) {
	call, err := parseAggCall(exprText)
	if err != nil {
		return nil, err
	}

	if call.name == "count" {
		return func() aggregator { return &countAgg{} }, nil
	}

	return buildValueAggregator(call)
}

// buildValueAggregator builds aggregators that consume a field/expression arg.
func buildValueAggregator(call aggFuncArgs) (func() aggregator, error) {
	if len(call.args) == 0 {
		return nil, fmt.Errorf("%w: %s requires an argument", ErrParseExpr, call.name)
	}

	node, err := parseExpression(call.args[0])
	if err != nil {
		return nil, err
	}

	if factory := simpleValueAgg(call.name, node); factory != nil {
		return factory, nil
	}

	return specialValueAgg(call, node)
}

// simpleValueAgg returns a factory for the single-argument numeric aggregations.
func simpleValueAgg(name string, node exprNode) func() aggregator {
	switch name {
	case "sum":
		return func() aggregator { return &sumAgg{node: node} }
	case "avg", "average":
		return func() aggregator { return &avgAgg{node: node} }
	case "min":
		return func() aggregator { return &minMaxAgg{node: node, wantMax: false} }
	case "max":
		return func() aggregator { return &minMaxAgg{node: node, wantMax: true} }
	case "count_distinct", "countdistinct":
		return func() aggregator { return newDistinctAgg(node) }
	case "stddev":
		return func() aggregator { return &stddevAgg{node: node} }
	case "median":
		return func() aggregator { return newPctAgg(node, medianPercentile) }
	default:
		return nil
	}
}

// specialValueAgg builds the earliest/latest/pct aggregations.
func specialValueAgg(call aggFuncArgs, node exprNode) (func() aggregator, error) {
	switch call.name {
	case "earliest":
		return func() aggregator { return &edgeAgg{node: node, wantLatest: false} }, nil
	case "latest":
		return func() aggregator { return &edgeAgg{node: node, wantLatest: true} }, nil
	case "pct", "percentile":
		p := medianPercentile
		if len(call.args) >= pctArgCount {
			if parsed, err := parsePercentileArg(call.args[1]); err == nil {
				p = parsed
			}
		}

		return func() aggregator { return newPctAgg(node, p) }, nil
	default:
		return nil, fmt.Errorf("%w: unknown aggregation %q", ErrParseExpr, call.name)
	}
}

// pctArgCount is the number of arguments a percentile call carries.
const pctArgCount = 2

// parsePercentileArg parses the numeric percentile argument (e.g. 95 or p95).
func parsePercentileArg(arg string) (float64, error) {
	arg = strings.TrimSpace(arg)
	arg = strings.TrimPrefix(strings.ToLower(arg), "p")

	node, err := parseExpression(arg)
	if err != nil {
		return 0, err
	}

	return node.eval(newInsightsRecord(&OutputLogEvent{})).num, nil
}

// statsStage groups records and computes aggregations, replacing the record set
// with one synthetic record per group.
type statsStage struct {
	terms       []aggTerm
	groupFields []string
}

func (s *statsStage) apply(recs []*insightsRecord) []*insightsRecord {
	groups := make(map[string][]aggregator)
	groupKeys := make(map[string][]string)
	var order []string

	for _, rec := range recs {
		key, vals := s.groupKey(rec)

		aggs, ok := groups[key]
		if !ok {
			aggs = s.newAggregators()
			groups[key] = aggs
			groupKeys[key] = vals
			order = append(order, key)
		}

		for _, a := range aggs {
			a.add(rec)
		}
	}

	return s.buildRows(order, groups, groupKeys)
}

// groupKey computes the grouping key and the raw group-field values for a record.
func (s *statsStage) groupKey(rec *insightsRecord) (string, []string) {
	if len(s.groupFields) == 0 {
		return "", nil
	}

	vals := make([]string, len(s.groupFields))
	for i, f := range s.groupFields {
		vals[i] = rec.resolveString(f)
	}

	return strings.Join(vals, "\x00"), vals
}

func (s *statsStage) newAggregators() []aggregator {
	aggs := make([]aggregator, len(s.terms))
	for i, t := range s.terms {
		aggs[i] = t.factory()
	}

	return aggs
}

// buildRows converts accumulated groups into output records.
func (s *statsStage) buildRows(
	order []string, groups map[string][]aggregator, groupKeys map[string][]string,
) []*insightsRecord {
	out := make([]*insightsRecord, 0, len(order))

	for _, key := range order {
		rec := newInsightsRecord(&OutputLogEvent{})
		for i, f := range s.groupFields {
			rec.setField(f, stringValue(groupKeys[key][i]))
		}

		aggs := groups[key]
		for i, t := range s.terms {
			rec.setField(t.alias, aggs[i].result())
		}

		out = append(out, rec)
	}

	return out
}

// countAgg counts records.
type countAgg struct {
	n int64
}

func (a *countAgg) add(_ *insightsRecord) { a.n++ }
func (a *countAgg) result() cwValue       { return numFromInt(a.n) }

// distinctAgg counts distinct string values.
type distinctAgg struct {
	node exprNode
	seen map[string]struct{}
}

func newDistinctAgg(node exprNode) *distinctAgg {
	return &distinctAgg{node: node, seen: make(map[string]struct{})}
}

func (a *distinctAgg) add(rec *insightsRecord) {
	a.seen[a.node.eval(rec).String()] = struct{}{}
}

func (a *distinctAgg) result() cwValue { return numFromInt(int64(len(a.seen))) }

// sumAgg sums numeric values.
type sumAgg struct {
	node exprNode
	sum  float64
}

func (a *sumAgg) add(rec *insightsRecord) { a.sum += a.node.eval(rec).num }
func (a *sumAgg) result() cwValue         { return numFromFloat(a.sum) }

// avgAgg averages numeric values.
type avgAgg struct {
	node exprNode
	sum  float64
	n    int64
}

func (a *avgAgg) add(rec *insightsRecord) {
	a.sum += a.node.eval(rec).num
	a.n++
}

func (a *avgAgg) result() cwValue {
	if a.n == 0 {
		return numFromFloat(0)
	}

	return numFromFloat(a.sum / float64(a.n))
}

// minMaxAgg tracks the minimum or maximum value.
type minMaxAgg struct {
	node    exprNode
	best    cwValue
	wantMax bool
	set     bool
}

func (a *minMaxAgg) add(rec *insightsRecord) {
	v := a.node.eval(rec)
	if !a.set {
		a.best = v
		a.set = true

		return
	}

	cmp := compareValues(v, a.best)
	if (a.wantMax && cmp > 0) || (!a.wantMax && cmp < 0) {
		a.best = v
	}
}

func (a *minMaxAgg) result() cwValue {
	if !a.set {
		return numFromFloat(0)
	}

	return a.best
}

// edgeAgg tracks the value of a field for the earliest/latest @timestamp.
type edgeAgg struct {
	node       exprNode
	val        cwValue
	edgeTS     int64
	wantLatest bool
	set        bool
}

func (a *edgeAgg) add(rec *insightsRecord) {
	ts := rec.ev.Timestamp
	if !a.set || (a.wantLatest && ts > a.edgeTS) || (!a.wantLatest && ts < a.edgeTS) {
		a.edgeTS = ts
		a.val = a.node.eval(rec)
		a.set = true
	}
}

func (a *edgeAgg) result() cwValue {
	if !a.set {
		return literalString("")
	}

	return a.val
}

// pctAgg computes a percentile over numeric values.
type pctAgg struct {
	node exprNode
	vals []float64
	pct  float64
}

func newPctAgg(node exprNode, pct float64) *pctAgg {
	return &pctAgg{node: node, vals: nil, pct: pct}
}

func (a *pctAgg) add(rec *insightsRecord) {
	a.vals = append(a.vals, a.node.eval(rec).num)
}

func (a *pctAgg) result() cwValue {
	if len(a.vals) == 0 {
		return numFromFloat(0)
	}

	sort.Float64s(a.vals)

	rank := a.pct / percentBase * float64(len(a.vals)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return numFromFloat(a.vals[lo])
	}

	frac := rank - float64(lo)

	return numFromFloat(a.vals[lo] + frac*(a.vals[hi]-a.vals[lo]))
}

// stddevAgg computes the population standard deviation.
type stddevAgg struct {
	node exprNode
	vals []float64
}

func (a *stddevAgg) add(rec *insightsRecord) {
	a.vals = append(a.vals, a.node.eval(rec).num)
}

func (a *stddevAgg) result() cwValue {
	if len(a.vals) == 0 {
		return numFromFloat(0)
	}

	var sum float64
	for _, v := range a.vals {
		sum += v
	}

	mean := sum / float64(len(a.vals))

	var variance float64
	for _, v := range a.vals {
		variance += (v - mean) * (v - mean)
	}

	return numFromFloat(math.Sqrt(variance / float64(len(a.vals))))
}
