package timestreamquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryIntervalColumnType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"direct", "SELECT parse_duration('42.8ms') AS d, 1 AS n", []string{"INTERVAL_DAY_TO_SECOND", "BIGINT"}},
		{
			"through an alias", "SELECT d FROM (SELECT parse_duration('1d') AS d) AS t",
			[]string{"INTERVAL_DAY_TO_SECOND"},
		},
		{"not an interval", "SELECT length(parse_duration('1d')) AS n", []string{"BIGINT"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp := parseResponse(t, doRequest(t, newTestHandler(), "Query", map[string]any{"QueryString": tc.query}))
			cols, _ := resp["ColumnInfo"].([]any)
			require.Len(t, cols, len(tc.want))

			for i, want := range tc.want {
				typ := cols[i].(map[string]any)["Type"].(map[string]any)
				assert.Equal(t, want, typ["ScalarType"])
			}
		})
	}
}
