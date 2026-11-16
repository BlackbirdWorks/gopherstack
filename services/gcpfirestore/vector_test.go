package gcpfirestore_test

import (
	"math"
	"testing"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func seedVectors(t *testing.T, e *env) {
	t.Helper()

	e.seed(t, "vec", map[string]map[string]any{
		"a":     {"emb": firestore.Vector32{1, 0}, "tag": "x"},
		"b":     {"emb": firestore.Vector32{0, 1}, "tag": "x"},
		"c":     {"emb": firestore.Vector32{1, 1}, "tag": "y"},
		"far":   {"emb": firestore.Vector32{10, 10}, "tag": "x"},
		"other": {"emb": firestore.Vector32{1, 2, 3}, "tag": "x"},
		"none":  {"tag": "x"},
	})
}

func TestFindNearest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opts    *firestore.FindNearestOptions
		name    string
		query   []float32
		want    []string
		measure firestore.DistanceMeasure
		limit   int
	}{
		{
			name:    "euclidean",
			measure: firestore.DistanceMeasureEuclidean,
			query:   []float32{1, 0.1},
			limit:   2,
			want:    []string{"a", "c"},
		},
		{
			name:    "cosine",
			measure: firestore.DistanceMeasureCosine,
			query:   []float32{1, 0},
			limit:   3,
			want:    []string{"a", "c", "far"},
		},
		{
			name:    "dot product larger is closer",
			measure: firestore.DistanceMeasureDotProduct,
			query:   []float32{1, 1},
			limit:   2,
			want:    []string{"far", "c"},
		},
		{name: "threshold", measure: firestore.DistanceMeasureEuclidean, query: []float32{1, 0}, limit: 10,
			opts: &firestore.FindNearestOptions{DistanceThreshold: new(1.0)}, want: []string{"a", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			seedVectors(t, e)

			q := e.client.Collection("vec").FindNearest("emb", tt.query, tt.limit, tt.measure, tt.opts)
			assert.Equal(
				t,
				tt.want,
				ids(t, q.Documents(t.Context())),
				"documents with another dimension or no vector are skipped",
			)
		})
	}
}

func TestFindNearestResultFieldAndFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	seedVectors(t, e)

	q := e.client.Collection("vec").Where("tag", "==", "x").
		FindNearest("emb", []float32{1, 0}, 2, firestore.DistanceMeasureEuclidean,
			&firestore.FindNearestOptions{DistanceResultField: "dist"})

	snaps, err := q.Documents(t.Context()).GetAll()
	require.NoError(t, err)
	require.Len(t, snaps, 2)
	assert.Equal(t, "a", snaps[0].Ref.ID)
	assert.InDelta(t, 0.0, snaps[0].Data()["dist"], 1e-9)
	assert.InDelta(t, 1.4142135, snaps[1].Data()["dist"], 1e-6)
	assert.Equal(t, "b", snaps[1].Ref.ID)
}

func TestFindNearestValidation(t *testing.T) {
	t.Parallel()

	vec := func(vals ...float64) *firestorepb.Value {
		arr := make([]*firestorepb.Value, len(vals))
		for i, v := range vals {
			arr[i] = &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: v}}
		}

		return &firestorepb.Value{
			ValueType: &firestorepb.Value_MapValue{
				MapValue: &firestorepb.MapValue{Fields: map[string]*firestorepb.Value{
					"__type__": {ValueType: &firestorepb.Value_StringValue{StringValue: "__vector__"}},
					"value": {
						ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: arr}},
					},
				}},
			},
		}
	}

	good := func() *firestorepb.StructuredQuery_FindNearest {
		return &firestorepb.StructuredQuery_FindNearest{
			VectorField: &firestorepb.StructuredQuery_FieldReference{FieldPath: "emb"}, QueryVector: vec(1, 0),
			DistanceMeasure: firestorepb.StructuredQuery_FindNearest_EUCLIDEAN, Limit: wrapInt(2),
		}
	}

	tests := []struct {
		mutate func(*firestorepb.StructuredQuery_FindNearest)
		name   string
	}{
		{func(f *firestorepb.StructuredQuery_FindNearest) { f.Limit = wrapInt(0) }, "zero limit"},
		{func(f *firestorepb.StructuredQuery_FindNearest) { f.Limit = wrapInt(1001) }, "limit too large"},
		{func(f *firestorepb.StructuredQuery_FindNearest) { f.QueryVector = vec() }, "empty vector"},
		{func(f *firestorepb.StructuredQuery_FindNearest) {
			f.DistanceMeasure = firestorepb.StructuredQuery_FindNearest_DISTANCE_MEASURE_UNSPECIFIED
		}, "no measure"},
		{func(f *firestorepb.StructuredQuery_FindNearest) { f.VectorField = nil }, "no field"},
		{func(f *firestorepb.StructuredQuery_FindNearest) { f.QueryVector = vec(1, math.NaN()) }, "nan component"},
		{func(f *firestorepb.StructuredQuery_FindNearest) {
			f.QueryVector = vec(math.Inf(1), 0)
		}, "infinite component"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			fn := good()
			tt.mutate(fn)

			stream, err := e.raw.RunQuery(t.Context(), &firestorepb.RunQueryRequest{
				Parent: docRoot,
				QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: &firestorepb.StructuredQuery{
					From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "vec"}}, FindNearest: fn,
				}},
			})
			require.NoError(t, err)

			_, err = stream.Recv()
			assert.Equal(t, codes.InvalidArgument, code(err))
		})
	}
}

func TestExplainQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		analyze     bool
		wantResults int
	}{
		{"plan only", false, 0},
		{"analyze", true, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.seed(t, "ex", map[string]map[string]any{"a": {"n": 1}, "b": {"n": 2}, "c": {"n": 3}})

			stream, err := e.raw.RunQuery(t.Context(), &firestorepb.RunQueryRequest{
				Parent:         docRoot,
				ExplainOptions: &firestorepb.ExplainOptions{Analyze: tt.analyze},
				QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: &firestorepb.StructuredQuery{
					From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "ex"}},
					OrderBy: []*firestorepb.StructuredQuery_Order{
						{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "n"}},
					},
					Limit: wrapInt(2),
				}},
			})
			require.NoError(t, err)

			var (
				docs    int
				metrics *firestorepb.ExplainMetrics
			)

			for {
				resp, rerr := stream.Recv()
				if rerr != nil {
					break
				}

				if resp.GetDocument() != nil {
					docs++
				}

				if resp.GetExplainMetrics() != nil {
					metrics = resp.GetExplainMetrics()
				}
			}

			assert.Equal(t, tt.wantResults, docs)
			require.NotNil(t, metrics)
			require.Len(t, metrics.GetPlanSummary().GetIndexesUsed(), 1)

			idx := metrics.GetPlanSummary().GetIndexesUsed()[0].AsMap()
			assert.Equal(t, "Collection", idx["query_scope"])
			assert.Equal(t, "(n ASC, __name__ ASC)", idx["properties"])

			if !tt.analyze {
				assert.Nil(t, metrics.GetExecutionStats())

				return
			}

			assert.EqualValues(t, 2, metrics.GetExecutionStats().GetResultsReturned())
			assert.NotNil(t, metrics.GetExecutionStats().GetDebugStats())
		})
	}
}

func TestExplainAggregation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seed(t, "ag", map[string]map[string]any{"a": {"n": 1}, "b": {"n": 2}})

	agg := &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "ag"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{{
			Alias: "n", Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
				Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
			},
		}},
	}

	for _, analyze := range []bool{false, true} {
		stream, err := e.raw.RunAggregationQuery(t.Context(), &firestorepb.RunAggregationQueryRequest{
			Parent:         docRoot,
			ExplainOptions: &firestorepb.ExplainOptions{Analyze: analyze},
			QueryType: &firestorepb.RunAggregationQueryRequest_StructuredAggregationQuery{
				StructuredAggregationQuery: agg,
			},
		})
		require.NoError(t, err)

		resp, err := stream.Recv()
		require.NoError(t, err)
		require.NotNil(t, resp.GetExplainMetrics())
		assert.Equal(t, analyze, resp.GetResult() != nil, "plan-only runs return no result")
		assert.Equal(t, analyze, resp.GetExplainMetrics().GetExecutionStats() != nil)
	}
}

func wrapInt(n int32) *wrapperspb.Int32Value { return wrapperspb.Int32(n) }
