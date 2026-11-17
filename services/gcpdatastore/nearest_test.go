package gcpdatastore_test

import (
	"testing"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
