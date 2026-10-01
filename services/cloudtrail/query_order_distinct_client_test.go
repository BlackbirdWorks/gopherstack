package cloudtrail_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

func orderedColumn(rows [][]map[string]string, col string) []string {
	var out []string

	for _, row := range rows {
		for _, cell := range row {
			if v, ok := cell[col]; ok {
				out = append(out, v)
			}
		}
	}

	return out
}

func TestQueryGrammar_OrderByAndDistinct(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	seedQueryGrammarEvents(backend)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))
	edsARN := newQueryGrammarEDS(t, client)

	tests := []struct {
		name       string
		sql        string
		col        string
		wantStatus string
		want       []string
	}{
		{
			name: "asc_default", sql: "SELECT eventName FROM %s ORDER BY eventName", col: "eventName",
			wantStatus: "FINISHED",
			want:       []string{"CreateBucket", "DeleteBucket", "RunInstances", "TerminateInstances"},
		},
		{
			name: "desc_with_limit", sql: "SELECT eventName FROM %s ORDER BY eventName DESC LIMIT 2", col: "eventName",
			wantStatus: "FINISHED",
			want:       []string{"TerminateInstances", "RunInstances"},
		},
		{
			name:       "multi_key",
			sql:        "SELECT eventName FROM %s ORDER BY username ASC, eventName DESC",
			col:        "eventName",
			wantStatus: "FINISHED",
			want:       []string{"RunInstances", "CreateBucket", "DeleteBucket", "TerminateInstances"},
		},
		{
			name:       "non_projected_column",
			sql:        "SELECT eventName FROM %s ORDER BY username DESC, eventName",
			col:        "eventName",
			wantStatus: "FINISHED",
			want:       []string{"TerminateInstances", "DeleteBucket", "CreateBucket", "RunInstances"},
		},
		{
			name:       "count_alias_desc",
			sql:        "SELECT username, COUNT(*) AS n FROM %s GROUP BY username ORDER BY n DESC, username",
			col:        "username",
			wantStatus: "FINISHED",
			want:       []string{"alice", "bob", "carol"},
		},
		{
			name: "distinct_ordered", sql: "SELECT DISTINCT eventSource FROM %s ORDER BY eventSource DESC",
			col: "eventSource", wantStatus: "FINISHED",
			want: []string{"s3.amazonaws.com", "ec2.amazonaws.com"},
		},
		{
			name: "distinct_with_count_fails", sql: "SELECT DISTINCT COUNT(*) FROM %s",
			col: "_col0", wantStatus: "FAILED",
		},
		{
			name:       "agg_order_by_unknown_fails",
			sql:        "SELECT username, COUNT(*) AS n FROM %s GROUP BY username ORDER BY eventName",
			col:        "username",
			wantStatus: "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := runLakeQuery(t, client, fmt.Sprintf(tt.sql, edsARN))
			require.Equal(t, tt.wantStatus, string(res.QueryStatus))
			assert.Equal(t, tt.want, orderedColumn(res.QueryResultRows, tt.col))
		})
	}
}
