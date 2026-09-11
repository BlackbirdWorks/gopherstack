package rds

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMatchesAllDBClusterBacktrackFilters exercises
// applyDBClusterBacktrackFilters' narrowing logic directly against literal
// DBClusterBacktrack values, since DescribeDBClusterBacktracks (db_clusters.go)
// has no store that ever persists a real backtrack for an end-to-end test
// through the real client (see TestDescribeDBClusterBacktracks_UnknownFilterErrors
// in describe_filters_batch2_test.go for the part of this contract that is
// observable that way).
func TestMatchesAllDBClusterBacktrackFilters(t *testing.T) {
	t.Parallel()

	backtracks := []DBClusterBacktrack{
		{DBClusterIdentifier: "flt-clu", BacktrackIdentifier: "bt-a", Status: "completed"},
		{DBClusterIdentifier: "flt-clu", BacktrackIdentifier: "bt-b", Status: "applying"},
	}

	tests := []struct {
		name    string
		query   string
		wantIDs []string
	}{
		{
			name:    "db-cluster-backtrack-id narrows to matching backtrack",
			query:   "Filters.Filter.1.Name=db-cluster-backtrack-id&Filters.Filter.1.Values.Value.1=bt-a",
			wantIDs: []string{"bt-a"},
		},
		{
			name:    "db-cluster-backtrack-status narrows to matching backtrack",
			query:   "Filters.Filter.1.Name=db-cluster-backtrack-status&Filters.Filter.1.Values.Value.1=applying",
			wantIDs: []string{"bt-b"},
		},
		{
			name:    "no filters returns everything",
			query:   "",
			wantIDs: []string{"bt-a", "bt-b"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			vals, err := url.ParseQuery(tc.query)
			require.NoError(t, err)

			got, err := applyDBClusterBacktrackFilters(vals, backtracks)
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(got))
			for _, bt := range got {
				gotIDs = append(gotIDs, bt.BacktrackIdentifier)
			}
			assert.ElementsMatch(t, tc.wantIDs, gotIDs)
		})
	}

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		vals, err := url.ParseQuery("Filters.Filter.1.Name=bogus&Filters.Filter.1.Values.Value.1=x")
		require.NoError(t, err)

		_, err = applyDBClusterBacktrackFilters(vals, backtracks)
		require.ErrorIs(t, err, ErrInvalidParameter)
	})
}

// TestApplyDBParameterFilters_EngineDefaults exercises applyDBParameterFilters'
// narrowing logic directly against literal DBParameter values in the shape
// DescribeEngineDefaultParameters would carry, since this backend never
// generates default-parameter data for any family (parameter_groups.go's own
// comment on DescribeEngineDefaultParameters) for an end-to-end test through
// the real client (see TestDescribeEngineDefaultParameters_UnknownFilterErrors
// in describe_filters_batch2_test.go for the part of this contract that is
// observable that way). applyDBParameterFilters itself is already covered
// end-to-end by TestDescribeDBParameters_Filters and
// TestDescribeDBClusterParameters_Filters (describe_filters_batch1_test.go);
// this pins the same contract for this op's own data shape.
func TestApplyDBParameterFilters_EngineDefaults(t *testing.T) {
	t.Parallel()

	params := []DBParameter{
		{ParameterName: "max_connections", ParameterValue: "100"},
		{ParameterName: "work_mem", ParameterValue: "4096"},
	}

	vals, err := url.ParseQuery(
		"Filters.Filter.1.Name=parameter-name&Filters.Filter.1.Values.Value.1=work_mem",
	)
	require.NoError(t, err)

	got, err := applyDBParameterFilters(vals, params)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "work_mem", got[0].ParameterName)

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		badVals, parseErr := url.ParseQuery(
			"Filters.Filter.1.Name=bogus&Filters.Filter.1.Values.Value.1=x",
		)
		require.NoError(t, parseErr)

		_, applyErr := applyDBParameterFilters(badVals, params)
		require.ErrorIs(t, applyErr, ErrInvalidParameter)
	})
}
