package gcpdatastore_test

import (
	"testing"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExplainOptions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seed(t, "Job", seedTasks())

	query := &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Job"}},
		Order: []*datastorepb.PropertyOrder{{
			Property:  &datastorepb.PropertyReference{Name: "Priority"},
			Direction: datastorepb.PropertyOrder_DESCENDING,
		}},
	}

	tests := []struct {
		name    string
		analyze bool
		results int
	}{
		{name: "plan only"},
		{name: "analyze", analyze: true, results: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.raw.RunQuery(t.Context(), &datastorepb.RunQueryRequest{
				ProjectId:      project,
				QueryType:      &datastorepb.RunQueryRequest_Query{Query: query},
				ExplainOptions: &datastorepb.ExplainOptions{Analyze: tt.analyze},
			})
			require.NoError(t, err)

			idx := resp.GetExplainMetrics().GetPlanSummary().GetIndexesUsed()
			require.Len(t, idx, 1)
			assert.Equal(t, "(Priority DESC, __name__ ASC)", idx[0].GetFields()["properties"].GetStringValue())
			assert.Len(t, resp.GetBatch().GetEntityResults(), tt.results)

			stats := resp.GetExplainMetrics().GetExecutionStats()
			if !tt.analyze {
				assert.Nil(t, stats)

				return
			}

			assert.Equal(t, int64(5), stats.GetResultsReturned())
			assert.Equal(t, int64(5), stats.GetReadOperations())
		})
	}

	t.Run("aggregation", func(t *testing.T) {
		t.Parallel()

		resp, err := e.raw.RunAggregationQuery(t.Context(), &datastorepb.RunAggregationQueryRequest{
			ProjectId: project,
			QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
				AggregationQuery: &datastorepb.AggregationQuery{
					QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: query},
					Aggregations: []*datastorepb.AggregationQuery_Aggregation{
						{
							Alias: "n",
							Operator: &datastorepb.AggregationQuery_Aggregation_Count_{
								Count: &datastorepb.AggregationQuery_Aggregation_Count{},
							},
						},
					},
				},
			},
			ExplainOptions: &datastorepb.ExplainOptions{Analyze: true},
		})
		require.NoError(t, err)
		assert.NotNil(t, resp.GetExplainMetrics().GetPlanSummary())
		assert.Equal(t, int64(1), resp.GetExplainMetrics().GetExecutionStats().GetResultsReturned())
	})
}
