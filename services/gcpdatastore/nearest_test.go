package gcpdatastore_test

import (
	"math"
	"testing"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func vecValue(vals ...float64) *datastorepb.Value {
	elems := make([]*datastorepb.Value, len(vals))
	for i, f := range vals {
		elems[i] = &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: f}}
	}

	return &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{
		Properties: map[string]*datastorepb.Value{
			"__type__": {ValueType: &datastorepb.Value_StringValue{StringValue: "__vector__"}},
			"value":    {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: elems}}},
		},
	}}}
}

func (e *env) putVectors(t *testing.T, vecs map[string]*datastorepb.Value) {
	t.Helper()

	muts := make([]*datastorepb.Mutation, 0, len(vecs))

	for name, v := range vecs {
		muts = append(muts, &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: &datastorepb.Entity{
			Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{
				Kind: "Doc", IdType: &datastorepb.Key_PathElement_Name{Name: name},
			}}},
			Properties: map[string]*datastorepb.Value{"emb": v},
		}}})
	}

	_, err := e.raw.Commit(t.Context(), &datastorepb.CommitRequest{
		ProjectId: project, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: muts,
	})
	require.NoError(t, err)
}

func TestFindNearest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn      *datastorepb.FindNearest
		name    string
		distKey string
		want    []string
		dists   []float64
		code    codes.Code
	}{
		{
			name: "euclidean",
			fn: &datastorepb.FindNearest{
				VectorProperty: &datastorepb.PropertyReference{Name: "emb"}, QueryVector: vecValue(0, 0),
				DistanceMeasure: datastorepb.FindNearest_EUCLIDEAN, Limit: wrapperspb.Int32(2),
				DistanceResultProperty: "dist",
			},
			want: []string{"near", "mid"}, distKey: "dist", dists: []float64{1, 5},
		},
		{
			name: "dot product orders descending",
			fn: &datastorepb.FindNearest{
				VectorProperty: &datastorepb.PropertyReference{Name: "emb"}, QueryVector: vecValue(1, 0),
				DistanceMeasure: datastorepb.FindNearest_DOT_PRODUCT, Limit: wrapperspb.Int32(3),
			},
			want: []string{"far", "mid", "near"},
		},
		{
			name: "threshold",
			fn: &datastorepb.FindNearest{
				VectorProperty: &datastorepb.PropertyReference{Name: "emb"}, QueryVector: vecValue(0, 0),
				DistanceMeasure: datastorepb.FindNearest_EUCLIDEAN, Limit: wrapperspb.Int32(10),
				DistanceThreshold: wrapperspb.Double(5),
			},
			want: []string{"near", "mid"},
		},
		{
			name: "dimension mismatch is skipped",
			fn: &datastorepb.FindNearest{
				VectorProperty: &datastorepb.PropertyReference{Name: "emb"}, QueryVector: vecValue(0, 0, 0),
				DistanceMeasure: datastorepb.FindNearest_EUCLIDEAN, Limit: wrapperspb.Int32(10),
			},
		},
		{
			name: "limit above 100",
			fn: &datastorepb.FindNearest{
				VectorProperty: &datastorepb.PropertyReference{Name: "emb"}, QueryVector: vecValue(0, 0),
				DistanceMeasure: datastorepb.FindNearest_EUCLIDEAN, Limit: wrapperspb.Int32(101),
			},
			code: codes.InvalidArgument,
		},
		{
			name: "missing measure",
			fn: &datastorepb.FindNearest{
				VectorProperty: &datastorepb.PropertyReference{Name: "emb"}, QueryVector: vecValue(0, 0),
				Limit: wrapperspb.Int32(1),
			},
			code: codes.InvalidArgument,
		},
		{
			name: "query vector not a vector",
			fn: &datastorepb.FindNearest{
				VectorProperty:  &datastorepb.PropertyReference{Name: "emb"},
				QueryVector:     &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 1}},
				DistanceMeasure: datastorepb.FindNearest_EUCLIDEAN, Limit: wrapperspb.Int32(1),
			},
			code: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.putVectors(t, map[string]*datastorepb.Value{
				"near": vecValue(1, 0), "mid": vecValue(3, 4), "far": vecValue(6, 8),
				"plain": {ValueType: &datastorepb.Value_StringValue{StringValue: "x"}},
			})

			resp, err := e.raw.RunQuery(t.Context(), &datastorepb.RunQueryRequest{
				ProjectId: project,
				QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
					Kind: []*datastorepb.KindExpression{{Name: "Doc"}}, FindNearest: tt.fn,
				}},
			})
			if tt.code != codes.OK {
				require.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)

			var got []string

			for i, r := range resp.GetBatch().GetEntityResults() {
				got = append(got, r.GetEntity().GetKey().GetPath()[0].GetName())

				if tt.distKey != "" {
					assert.InDelta(t, tt.dists[i], r.GetEntity().GetProperties()[tt.distKey].GetDoubleValue(), 1e-9)
				}
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func (e *env) nearest(t *testing.T, q *datastorepb.Query) ([]string, error) {
	t.Helper()

	resp, err := e.raw.RunQuery(t.Context(), &datastorepb.RunQueryRequest{
		ProjectId: project, QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
	})
	if err != nil {
		return nil, err
	}

	var got []string

	for _, r := range resp.GetBatch().GetEntityResults() {
		got = append(got, r.GetEntity().GetKey().GetPath()[0].GetName())
	}

	return got, nil
}

func nearestQuery(fn *datastorepb.FindNearest) *datastorepb.Query {
	fn.VectorProperty = &datastorepb.PropertyReference{Name: "emb"}

	return &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Doc"}}, FindNearest: fn}
}

func TestFindNearestMeasuresAndThresholds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   *datastorepb.FindNearest
		name string
		want []string
	}{
		{
			name: "cosine orders by angle",
			fn: &datastorepb.FindNearest{
				QueryVector: vecValue(
					1,
					0,
				), DistanceMeasure: datastorepb.FindNearest_COSINE, Limit: wrapperspb.Int32(10),
			},
			want: []string{"east", "northeast", "north", "west"},
		},
		{
			name: "cosine threshold keeps distance at or below",
			fn: &datastorepb.FindNearest{
				QueryVector: vecValue(
					1,
					0,
				), DistanceMeasure: datastorepb.FindNearest_COSINE, Limit: wrapperspb.Int32(10),
				DistanceThreshold: wrapperspb.Double(1),
			},
			want: []string{"east", "northeast", "north"},
		},
		{
			name: "dot product threshold keeps distance at or above",
			fn: &datastorepb.FindNearest{
				QueryVector: vecValue(
					1,
					0,
				), DistanceMeasure: datastorepb.FindNearest_DOT_PRODUCT, Limit: wrapperspb.Int32(10),
				DistanceThreshold: wrapperspb.Double(0),
			},
			want: []string{"east", "northeast", "north"},
		},
		{
			name: "equal distances order by key",
			fn: &datastorepb.FindNearest{
				QueryVector: vecValue(
					0,
					1,
				), DistanceMeasure: datastorepb.FindNearest_EUCLIDEAN, Limit: wrapperspb.Int32(2),
			},
			want: []string{"north", "northeast"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.putVectors(t, map[string]*datastorepb.Value{
				"west": vecValue(-1, 0), "north": vecValue(0, 1), "northeast": vecValue(1, 0.5),
				"east": vecValue(2, 0),
			})

			got, err := e.nearest(t, nearestQuery(tt.fn))
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFindNearestRejectsNonFiniteAndNeverReturnsNaN(t *testing.T) {
	t.Parallel()

	nan, inf := math.NaN(), math.Inf(1)

	tests := []struct {
		query   *datastorepb.Value
		name    string
		measure datastorepb.FindNearest_DistanceMeasure
		code    codes.Code
	}{
		{
			name:    "nan query",
			query:   vecValue(nan, 1),
			measure: datastorepb.FindNearest_EUCLIDEAN,
			code:    codes.InvalidArgument,
		},
		{
			name:    "inf query",
			query:   vecValue(inf, 1),
			measure: datastorepb.FindNearest_DOT_PRODUCT,
			code:    codes.InvalidArgument,
		},
		{name: "zero query cosine", query: vecValue(0, 0), measure: datastorepb.FindNearest_COSINE},
		{name: "overflow euclidean", query: vecValue(1e300, 1e300), measure: datastorepb.FindNearest_EUCLIDEAN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.putVectors(t, map[string]*datastorepb.Value{
				"ok": vecValue(1, 1), "nan": vecValue(nan, 1), "huge": vecValue(-1e300, -1e300),
			})

			q := nearestQuery(&datastorepb.FindNearest{
				QueryVector: tt.query, DistanceMeasure: tt.measure, Limit: wrapperspb.Int32(10),
				DistanceResultProperty: "dist",
			})

			resp, err := e.raw.RunQuery(t.Context(), &datastorepb.RunQueryRequest{
				ProjectId: project, QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
			})
			if tt.code != codes.OK {
				require.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)

			for _, r := range resp.GetBatch().GetEntityResults() {
				d := r.GetEntity().GetProperties()["dist"].GetDoubleValue()
				assert.False(t, math.IsNaN(d) || math.IsInf(d, 0), r.GetEntity().GetKey().GetPath()[0].GetName())
			}
		})
	}
}

func TestFindNearestCombinations(t *testing.T) {
	t.Parallel()

	fn := func() *datastorepb.FindNearest {
		return &datastorepb.FindNearest{
			QueryVector: vecValue(0, 0), DistanceMeasure: datastorepb.FindNearest_EUCLIDEAN, Limit: wrapperspb.Int32(5),
		}
	}

	tests := []struct {
		mod  func(q *datastorepb.Query)
		name string
		want []string
		code codes.Code
	}{
		{
			name: "projection is accepted",
			mod: func(q *datastorepb.Query) {
				q.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
			},
			want: []string{"a", "b"},
		},
		{
			name: "distinct on is accepted",
			mod: func(q *datastorepb.Query) {
				q.DistinctOn = []*datastorepb.PropertyReference{{Name: "__key__"}}
			},
			want: []string{"a", "b"},
		},
		{
			name: "start cursor is rejected",
			mod:  func(q *datastorepb.Query) { q.StartCursor = []byte("x") },
			code: codes.InvalidArgument,
		},
		{
			name: "query limit applies after search",
			mod:  func(q *datastorepb.Query) { q.Limit = wrapperspb.Int32(1) },
			want: []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.putVectors(t, map[string]*datastorepb.Value{"a": vecValue(1, 0), "b": vecValue(2, 0)})

			q := nearestQuery(fn())
			tt.mod(q)

			got, err := e.nearest(t, q)
			if tt.code != codes.OK {
				require.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestVectorValueRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	want := vecValue(0.5, -1, 3)
	e.putVectors(t, map[string]*datastorepb.Value{"v": want})

	resp, err := e.raw.Lookup(t.Context(), &datastorepb.LookupRequest{
		ProjectId: project,
		Keys: []*datastorepb.Key{{Path: []*datastorepb.Key_PathElement{{
			Kind: "Doc", IdType: &datastorepb.Key_PathElement_Name{Name: "v"},
		}}}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetFound(), 1)
	assert.True(t, proto.Equal(want, resp.GetFound()[0].GetEntity().GetProperties()["emb"]))
}
