package rds

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
