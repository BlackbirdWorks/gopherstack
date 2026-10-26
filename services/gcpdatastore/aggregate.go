package gcpdatastore

import (
	"strconv"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxAggregations = 5

const (
	aggCount = "count"
	aggSum   = "sum"
	aggAvg   = "avg"
)

type aggSpec struct {
	alias string
	kind  string
	prop  string
	upTo  int64
	hasUp bool
}

func aggSpecs(aggs []*datastorepb.AggregationQuery_Aggregation) ([]aggSpec, error) {
	if len(aggs) == 0 || len(aggs) > maxAggregations {
		return nil, errInvalid("An aggregation query needs between 1 and %d aggregations.", maxAggregations)
	}

	seen := map[string]struct{}{}
	unnamed := 0
	out := make([]aggSpec, 0, len(aggs))

	for _, a := range aggs {
		var s aggSpec

		switch op := a.GetOperator().(type) {
		case *datastorepb.AggregationQuery_Aggregation_Count_:
			s.kind = aggCount

			if op.Count.GetUpTo() != nil {
				if op.Count.GetUpTo().GetValue() < 0 {
					return nil, errInvalid("COUNT_UP_TO must be non-negative.")
				}

				s.upTo, s.hasUp = op.Count.GetUpTo().GetValue(), true
			}
		case *datastorepb.AggregationQuery_Aggregation_Sum_:
			s.kind, s.prop = aggSum, op.Sum.GetProperty().GetName()
		case *datastorepb.AggregationQuery_Aggregation_Avg_:
			s.kind, s.prop = aggAvg, op.Avg.GetProperty().GetName()
		default:
			return nil, errInvalid("An aggregation requires an operator.")
		}

		if s.kind != aggCount && s.prop == "" {
			return nil, errInvalid("The %s aggregation requires a property.", s.kind)
		}

		s.alias = a.GetAlias()
		if s.alias == "" {
			unnamed++
			s.alias = "property_" + strconv.Itoa(unnamed)
		}

		if _, dup := seen[s.alias]; dup {
			return nil, errInvalid("Aggregation alias %q is used more than once.", s.alias)
		}

		seen[s.alias] = struct{}{}

		out = append(out, s)
	}

	return out, nil
}

func (s aggSpec) compute(rows []*row) *datastorepb.Value {
	if s.kind == aggCount {
		n := int64(len(rows))
		if s.hasUp {
			n = min(n, s.upTo)
		}

		return intValue(n)
	}

	var (
		isum    int64
		fsum    float64
		count   int64
		float   bool
		overflo bool
	)

	for _, r := range rows {
		v, ok := numericProp(r.ent, s.prop)
		if !ok {
			continue
		}

		count++

		if d, isD := v.GetValueType().(*datastorepb.Value_DoubleValue); isD {
			float = true
			fsum += d.DoubleValue

			continue
		}

		n := v.GetIntegerValue()
		sum, over := addChecked(isum, n)
		overflo = overflo || over
		isum = sum
		fsum += float64(n)
	}

	if s.kind == aggAvg {
		if count == 0 {
			return nullValue()
		}

		return doubleValue(fsum / float64(count))
	}

	if float || overflo {
		return doubleValue(fsum)
	}

	return intValue(isum)
}

func numericProp(e *datastorepb.Entity, path string) (*datastorepb.Value, bool) {
	if v, ok := e.GetProperties()[path]; ok {
		return v, isNumber(v)
	}

	vals := propValues(e, path)
	if len(vals) == 1 && isNumber(vals[0]) {
		return vals[0], true
	}

	return nil, false
}

// RunAggregationQuery computes count, sum and avg over a query.
func (b *InMemoryBackend) RunAggregationQuery(
	in *datastorepb.RunAggregationQueryRequest,
) (*datastorepb.RunAggregationQueryResponse, error) {
	req, err := b.resolve(in.GetProjectId(), in.GetDatabaseId())
	if err != nil {
		return nil, err
	}

	if in.GetExplainOptions() != nil {
		return nil, errUnimplemented("Query explain is not supported.")
	}

	part, err := queryPartition(req, in.GetPartitionId())
	if err != nil {
		return nil, err
	}

	aq, err := resolveAggregation(in)
	if err != nil {
		return nil, err
	}

	specs, err := aggSpecs(aq.GetAggregations())
	if err != nil {
		return nil, err
	}

	nested := aq.GetNestedQuery()
	if nested == nil {
		return nil, errInvalid("An aggregation query requires a nested query.")
	}

	plan, err := buildPlan(part, nested)
	if err != nil {
		return nil, err
	}

	plan.proj, plan.keysOnly, plan.unbounded = nil, false, true

	resp := &datastorepb.RunAggregationQueryResponse{Query: aq}

	err = b.read(in.GetReadOptions(), req, func(rv *readView) error {
		res, qerr := b.execQuery(plan, rv)
		if qerr != nil {
			return qerr
		}

		props := make(map[string]*datastorepb.Value, len(specs))
		for _, s := range specs {
			props[s.alias] = s.compute(res.rows)
		}

		resp.Batch = &datastorepb.AggregationResultBatch{
			AggregationResults: []*datastorepb.AggregationResult{{AggregateProperties: props}},
			MoreResults:        datastorepb.QueryResultBatch_NO_MORE_RESULTS,
			ReadTime:           timestamppb.New(rv.at),
		}
		resp.Transaction = rv.newTxnID

		return nil
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func resolveAggregation(in *datastorepb.RunAggregationQueryRequest) (*datastorepb.AggregationQuery, error) {
	switch qt := in.GetQueryType().(type) {
	case *datastorepb.RunAggregationQueryRequest_AggregationQuery:
		return qt.AggregationQuery, nil
	case *datastorepb.RunAggregationQueryRequest_GqlQuery:
		g, err := parseGQL(qt.GqlQuery)
		if err != nil {
			return nil, err
		}

		if g.agg == nil {
			return nil, errInvalid("The GQL query is not an aggregation query.")
		}

		return g.agg, nil
	default:
		return nil, errInvalid("An aggregation_query or gql_query is required.")
	}
}
