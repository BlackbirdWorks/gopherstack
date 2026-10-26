package gcpdatastore_test

import (
	"testing"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func param(v *datastorepb.Value) *datastorepb.GqlQueryParameter {
	return &datastorepb.GqlQueryParameter{ParameterType: &datastorepb.GqlQueryParameter_Value{Value: v}}
}

func (e *env) gql(t *testing.T, g *datastorepb.GqlQuery) ([]string, error) {
	t.Helper()

	resp, err := e.raw.RunQuery(t.Context(), &datastorepb.RunQueryRequest{
		ProjectId: project, QueryType: &datastorepb.RunQueryRequest_GqlQuery{GqlQuery: g},
	})
	if err != nil {
		return nil, err
	}

	var out []string
	for _, r := range resp.GetBatch().GetEntityResults() {
		out = append(out, r.GetEntity().GetKey().GetPath()[len(r.GetEntity().GetKey().GetPath())-1].GetName())
	}

	return out, nil
}

func TestGQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		gql  *datastorepb.GqlQuery
		name string
		want []string
	}{
		{&datastorepb.GqlQuery{QueryString: "SELECT * FROM Task"}, "select all", []string{"a", "b", "c", "d", "e"}},
		{
			&datastorepb.GqlQuery{
				QueryString: "SELECT * FROM Task WHERE Done = @1 AND Priority > @2",
				PositionalBindings: []*datastorepb.GqlQueryParameter{
					param(&datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: true}}),
					param(intVal(1)),
				},
			},
			"positional bindings", []string{"c", "e"},
		},
		{
			&datastorepb.GqlQuery{
				QueryString:   "SELECT * FROM Task WHERE Priority >= @lo ORDER BY Priority DESC LIMIT @n",
				NamedBindings: map[string]*datastorepb.GqlQueryParameter{"lo": param(intVal(2)), "n": param(intVal(2))},
			},
			"named bindings order limit", []string{"e", "d"},
		},
		{
			&datastorepb.GqlQuery{
				QueryString:   "SELECT * FROM Task WHERE Priority IN (1, 3) OR Tags = 'z'",
				AllowLiterals: true,
			},
			"literals in and or",
			[]string{"a", "c"},
		},
		{
			&datastorepb.GqlQuery{
				QueryString:   "SELECT * FROM Task WHERE Priority NOT IN ARRAY(1,2,3) ORDER BY Priority",
				AllowLiterals: true,
			},
			"not in",
			[]string{"d", "e"},
		},
		{
			&datastorepb.GqlQuery{
				QueryString:   "SELECT * FROM Task WHERE Tags CONTAINS 'x' ORDER BY Priority LIMIT 1, 5",
				AllowLiterals: true,
			},
			"contains and limit offset",
			[]string{"e"},
		},
		{
			&datastorepb.GqlQuery{
				QueryString:   "SELECT * FROM Task WHERE __key__ = KEY(Task, 'c')",
				AllowLiterals: true,
			},
			"key literal",
			[]string{"c"},
		},
		{
			&datastorepb.GqlQuery{QueryString: "SELECT * FROM Task WHERE Priority > 3 OFFSET 1", AllowLiterals: true},
			"offset clause", []string{"e"},
		},
		{
			&datastorepb.GqlQuery{
				QueryString:   "SELECT __key__ FROM Task WHERE Done = true LIMIT 2",
				AllowLiterals: true,
			},
			"key only",
			[]string{"a", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newLooseEnv(t)

			got, err := e.gql(t, tt.gql)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGQLErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		gql  *datastorepb.GqlQuery
		name string
	}{
		{&datastorepb.GqlQuery{QueryString: "SELECT * FROM Task WHERE Done = true"}, "literals not allowed"},
		{&datastorepb.GqlQuery{QueryString: "SELEKT * FROM Task"}, "bad syntax"},
		{&datastorepb.GqlQuery{QueryString: "SELECT * FROM Task WHERE a = @missing"}, "unbound name"},
		{&datastorepb.GqlQuery{QueryString: "SELECT * FROM Task WHERE a = @1"}, "unbound position"},
		{&datastorepb.GqlQuery{QueryString: "SELECT COUNT(*) FROM Task"}, "aggregation via runquery"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newLooseEnv(t)

			_, err := e.gql(t, tt.gql)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestAggregations(t *testing.T) {
	t.Parallel()

	t.Run("client count sum avg", func(t *testing.T) {
		t.Parallel()

		e := newLooseEnv(t)
		aq := datastore.NewQuery("Task").
			NewAggregationQuery().
			WithCount("n").
			WithSum("Priority", "total").
			WithAvg("Score", "mean")

		res, err := e.client.RunAggregationQuery(t.Context(), aq)
		require.NoError(t, err)

		assert.EqualValues(t, 5, res["n"].(*datastorepb.Value).GetIntegerValue())
		assert.EqualValues(t, 15, res["total"].(*datastorepb.Value).GetIntegerValue())
		assert.InDelta(t, 3.5, res["mean"].(*datastorepb.Value).GetDoubleValue(), 1e-9)
	})

	t.Run("count with filter and limit", func(t *testing.T) {
		t.Parallel()

		e := newLooseEnv(t)

		n, err := e.client.Count(t.Context(), datastore.NewQuery("Task").FilterField("Done", "=", true))
		require.NoError(t, err)
		assert.Equal(t, 3, n)

		res, err := e.client.RunAggregationQuery(
			t.Context(),
			datastore.NewQuery("Task").Limit(2).NewAggregationQuery().WithCount("c"),
		)
		require.NoError(t, err)
		assert.EqualValues(t, 2, res["c"].(*datastorepb.Value).GetIntegerValue())
	})

	t.Run("gql aggregations and default aliases", func(t *testing.T) {
		t.Parallel()

		e := newLooseEnv(t)

		resp, err := e.raw.RunAggregationQuery(t.Context(), &datastorepb.RunAggregationQueryRequest{
			ProjectId: project,
			QueryType: &datastorepb.RunAggregationQueryRequest_GqlQuery{GqlQuery: &datastorepb.GqlQuery{
				QueryString: "AGGREGATE COUNT_UP_TO(3) AS capped, COUNT(*), SUM(Score) " +
					"OVER (SELECT * FROM Task WHERE Done = true)",
				AllowLiterals: true,
			}},
		})
		require.NoError(t, err)

		props := resp.GetBatch().GetAggregationResults()[0].GetAggregateProperties()
		assert.EqualValues(t, 3, props["capped"].GetIntegerValue())
		assert.EqualValues(t, 3, props["property_1"].GetIntegerValue())
		assert.InDelta(t, 10.5, props["property_2"].GetDoubleValue(), 1e-9)

		short, err := e.raw.RunAggregationQuery(t.Context(), &datastorepb.RunAggregationQueryRequest{
			ProjectId: project,
			QueryType: &datastorepb.RunAggregationQueryRequest_GqlQuery{GqlQuery: &datastorepb.GqlQuery{
				QueryString: "SELECT COUNT(*) AS n FROM Task", AllowLiterals: true,
			}},
		})
		require.NoError(t, err)
		assert.EqualValues(
			t,
			5,
			short.GetBatch().GetAggregationResults()[0].GetAggregateProperties()["n"].GetIntegerValue(),
		)
	})

	t.Run("sum of nothing and avg of nothing", func(t *testing.T) {
		t.Parallel()

		e := newLooseEnv(t)
		res, err := e.client.RunAggregationQuery(
			t.Context(),
			datastore.NewQuery("Task").
				FilterField("Priority", ">", 100).
				NewAggregationQuery().
				WithSum("Priority", "s").
				WithAvg("Priority", "a"),
		)
		require.NoError(t, err)
		assert.EqualValues(t, 0, res["s"].(*datastorepb.Value).GetIntegerValue())
		_, isNull := res["a"].(*datastorepb.Value).GetValueType().(*datastorepb.Value_NullValue)
		assert.True(t, isNull)
	})

	t.Run("duplicate alias rejected", func(t *testing.T) {
		t.Parallel()

		e := newLooseEnv(t)
		_, err := e.client.RunAggregationQuery(
			t.Context(),
			datastore.NewQuery("Task").NewAggregationQuery().WithCount("x").WithCount("x"),
		)
		assert.Error(t, err)
	})
}

func TestQueryValidation(t *testing.T) {
	t.Parallel()

	prop := func(n string, op datastorepb.PropertyFilter_Operator, v *datastorepb.Value) *datastorepb.Filter {
		return &datastorepb.Filter{
			FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{
				Property: &datastorepb.PropertyReference{Name: n}, Op: op, Value: v,
			}},
		}
	}

	tests := []struct {
		query *datastorepb.Query
		name  string
	}{
		{&datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "A"}, {Name: "B"}}}, "two kinds"},
		{&datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "A"}}, Offset: -1}, "negative offset"},
		{&datastorepb.Query{
			Kind: []*datastorepb.KindExpression{
				{Name: "A"},
			},
			Filter: prop("x", datastorepb.PropertyFilter_IN, intVal(1)),
		}, "in needs array"},
		{&datastorepb.Query{
			Kind:   []*datastorepb.KindExpression{{Name: "A"}},
			Filter: prop("x", datastorepb.PropertyFilter_GREATER_THAN, intVal(1)),
			Order:  []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "y"}}},
		}, "inequality must sort first"},
		{&datastorepb.Query{
			Kind:   []*datastorepb.KindExpression{{Name: "A"}},
			Filter: prop("x", datastorepb.PropertyFilter_HAS_ANCESTOR, intVal(1)),
		}, "ancestor needs key"},
		{
			&datastorepb.Query{Filter: prop("x", datastorepb.PropertyFilter_EQUAL, intVal(1))},
			"kindless property filter",
		},
		{&datastorepb.Query{
			Kind: []*datastorepb.KindExpression{
				{Name: "A"},
			},
			Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{
				CompositeFilter: &datastorepb.CompositeFilter{
					Op: datastorepb.CompositeFilter_AND,
					Filters: []*datastorepb.Filter{
						prop(
							"x",
							datastorepb.PropertyFilter_NOT_EQUAL,
							intVal(1),
						),
						prop("y", datastorepb.PropertyFilter_NOT_EQUAL, intVal(2)),
					},
				},
			}},
		}, "two not equal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newLooseEnv(t)
			_, err := e.raw.RunQuery(t.Context(), &datastorepb.RunQueryRequest{
				ProjectId: project, QueryType: &datastorepb.RunQueryRequest_Query{Query: tt.query},
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}
