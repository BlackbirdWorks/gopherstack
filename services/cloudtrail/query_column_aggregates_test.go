package cloudtrail_test

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

func TestQueryExecution_ColumnAggregates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCells  map[string]string
		name       string
		stmt       string
		wantStatus string
	}{
		{
			name: "sum_avg_min_max",
			stmt: "SELECT SUM(additionalEventData.n) AS s, AVG(additionalEventData.n) AS a, " +
				"MIN(additionalEventData.n) AS lo, MAX(additionalEventData.n) AS hi FROM eds-1",
			wantStatus: "FINISHED",
			wantCells:  map[string]string{"s": "12", "a": "4", "lo": "2", "hi": "6"},
		},
		{
			name:       "unaliased_positional_name",
			stmt:       "SELECT MAX(additionalEventData.n) FROM eds-1",
			wantStatus: "FINISHED",
			wantCells:  map[string]string{"_col0": "6"},
		},
		{
			name:       "having_filters_groups",
			stmt:       "SELECT eventName, COUNT(*) AS c FROM eds-1 GROUP BY eventName HAVING COUNT(*) >= 2",
			wantStatus: "FINISHED",
			wantCells:  map[string]string{"eventName": "PutObject", "c": "3"},
		},
		{
			name:       "having_alias_excludes_all",
			stmt:       "SELECT eventName, COUNT(*) AS c FROM eds-1 GROUP BY eventName HAVING c > 5",
			wantStatus: "FINISHED",
		},
		{
			name:       "having_unknown_alias_fails",
			stmt:       "SELECT eventName, COUNT(*) AS c FROM eds-1 GROUP BY eventName HAVING zz > 5",
			wantStatus: "FAILED",
		},
		{
			name:       "sum_over_text_fails",
			stmt:       "SELECT SUM(eventName) FROM eds-1",
			wantStatus: "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
			for _, n := range []string{"2", "4", "6"} {
				b.RecordEvent(cloudtrail.Event{
					EventName:       "PutObject",
					EventSource:     "s3.amazonaws.com",
					CloudTrailEvent: `{"additionalEventData":{"n":` + n + `}}`,
				})
			}

			q, err := b.StartQuery(tt.stmt, "eds-1", "", "", "")
			require.NoError(t, err)

			got, err := b.GetQueryResults(q.QueryID)
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, got.QueryStatus, got.ErrorMessage)

			if tt.wantStatus == "FAILED" {
				assert.NotEmpty(t, got.ErrorMessage)

				return
			}

			if tt.wantCells == nil {
				assert.Empty(t, got.QueryResultRows)

				return
			}

			require.Len(t, got.QueryResultRows, 1)

			cells := map[string]string{}
			for _, c := range got.QueryResultRows[0] {
				maps.Copy(cells, c)
			}

			assert.Equal(t, tt.wantCells, cells)
		})
	}
}
