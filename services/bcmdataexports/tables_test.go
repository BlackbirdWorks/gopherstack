package bcmdataexports_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bcmsdk "github.com/aws/aws-sdk-go-v2/service/bcmdataexports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListTables(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	out, err := f.client.ListTables(t.Context(), &bcmsdk.ListTablesInput{})
	require.NoError(t, err)

	names := make([]string, 0, len(out.Tables))
	for _, tb := range out.Tables {
		names = append(names, aws.ToString(tb.TableName))
	}

	assert.ElementsMatch(t, []string{
		"COST_AND_USAGE_REPORT", "FOCUS_1_0_AWS", "CARBON_EMISSIONS", "COST_OPTIMIZATION_RECOMMENDATIONS",
	}, names)

	p, err := f.client.ListTables(t.Context(), &bcmsdk.ListTablesInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	assert.Len(t, p.Tables, 1)
	assert.NotNil(t, p.NextToken)
}

func TestGetTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		props    map[string]string
		name     string
		table    string
		code     string
		wantCol  string
		absent   string
		wantProp string
	}{
		{
			nil,
			"cur_defaults",
			"COST_AND_USAGE_REPORT",
			"",
			"line_item_usage_account_id",
			"line_item_resource_id",
			"HOURLY",
		},
		{
			map[string]string{"INCLUDE_RESOURCES": "TRUE", "TIME_GRANULARITY": "DAILY"},
			"cur_resources",
			"COST_AND_USAGE_REPORT",
			"",
			"line_item_resource_id",
			"",
			"DAILY",
		},
		{nil, "focus", "FOCUS_1_0_AWS", "", "BilledCost", "", "HOURLY"},
		{nil, "carbon", "CARBON_EMISSIONS", "", "total_mbm_emissions_value", "", ""},
		{nil, "recs", "COST_OPTIMIZATION_RECOMMENDATIONS", "", "recommendation_id", "", ""},
		{nil, "unknown_table", "NOPE", "ResourceNotFoundException", "", "", ""},
		{map[string]string{"BOGUS": "x"}, "bad_prop", "FOCUS_1_0_AWS", "ValidationException", "", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			out, err := f.client.GetTable(t.Context(), &bcmsdk.GetTableInput{
				TableName: aws.String(tc.table), TableProperties: tc.props,
			})
			require.Equal(t, tc.code, apiCode(err))

			if tc.code != "" {
				return
			}

			cols := map[string]bool{}
			for _, c := range out.Schema {
				cols[aws.ToString(c.Name)] = true
			}

			assert.True(t, cols[tc.wantCol])

			if tc.absent != "" {
				assert.False(t, cols[tc.absent])
			}

			if tc.wantProp != "" {
				assert.Equal(t, tc.wantProp, out.TableProperties["TIME_GRANULARITY"])
			}
		})
	}
}
