package cloudtrail_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

func TestQueryGrammar_JoinsSetOpsAndSubqueries(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	seedQueryGrammarEvents(backend)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))
	eds := newQueryGrammarEDS(t, client)

	s3 := "SELECT eventName FROM " + eds + " WHERE eventSource = 's3.amazonaws.com'"
	create := "SELECT eventName FROM " + eds + " WHERE eventName = 'CreateBucket'"
	alice := "SELECT eventName FROM " + eds + " WHERE username = 'alice'"

	tests := []struct {
		name string
		stmt string
		col  string
		want []string
	}{
		{
			name: "inner_join_on_event_id",
			stmt: "SELECT a.eventName, b.username FROM " + eds + " AS a JOIN " + eds +
				" AS b ON a.eventId = b.eventId WHERE a.eventSource = 's3.amazonaws.com'",
			col:  "eventName",
			want: []string{"CreateBucket", "DeleteBucket"},
		},
		{
			name: "inner_join_fans_out_on_shared_user",
			stmt: "SELECT b.eventName AS other FROM " + eds + " a INNER JOIN " + eds +
				" b ON a.username = b.username WHERE a.eventName = 'CreateBucket'",
			col:  "other",
			want: []string{"CreateBucket", "RunInstances"},
		},
		{
			name: "left_join_keeps_unmatched_left",
			stmt: "SELECT a.eventName FROM " + eds + " a LEFT OUTER JOIN (SELECT eventId FROM " + eds +
				" WHERE eventName = 'CreateBucket') b ON a.eventId = b.eventId",
			col:  "eventName",
			want: []string{"CreateBucket", "DeleteBucket", "RunInstances", "TerminateInstances"},
		},
		{
			name: "inner_join_drops_unmatched_left",
			stmt: "SELECT a.eventName FROM " + eds + " a JOIN (SELECT eventId FROM " + eds +
				" WHERE eventName = 'CreateBucket') b ON a.eventId = b.eventId",
			col:  "eventName",
			want: []string{"CreateBucket"},
		},
		{
			name: "right_join_keeps_unmatched_right",
			stmt: "SELECT b.eventName FROM (SELECT eventId FROM " + eds + " WHERE eventName = 'CreateBucket') a RIGHT JOIN " +
				eds + " b ON a.eventId = b.eventId",
			col:  "eventName",
			want: []string{"CreateBucket", "DeleteBucket", "RunInstances", "TerminateInstances"},
		},
		{
			name: "union_distinct",
			stmt: s3 + " UNION " + create,
			col:  "eventName",
			want: []string{"CreateBucket", "DeleteBucket"},
		},
		{
			name: "union_all",
			stmt: s3 + " UNION ALL " + create,
			col:  "eventName",
			want: []string{"CreateBucket", "CreateBucket", "DeleteBucket"},
		},
		{name: "intersect", stmt: s3 + " INTERSECT " + alice, col: "eventName", want: []string{"CreateBucket"}},
		{name: "except", stmt: s3 + " EXCEPT " + create, col: "eventName", want: []string{"DeleteBucket"}},
		{
			name: "intersect_binds_tighter_than_union",
			stmt: create + " UNION " + s3 + " INTERSECT " + alice,
			col:  "eventName",
			want: []string{"CreateBucket"},
		},
		{
			name: "compound_order_by_limit",
			stmt: s3 + " UNION " + create + " ORDER BY eventName DESC LIMIT 1",
			col:  "eventName",
			want: []string{"DeleteBucket"},
		},
		{
			name: "in_subquery",
			stmt: "SELECT eventName FROM " + eds + " WHERE username IN (SELECT username FROM " + eds +
				" WHERE eventName = 'DeleteBucket')",
			col:  "eventName",
			want: []string{"DeleteBucket"},
		},
		{
			name: "not_in_subquery",
			stmt: "SELECT eventName FROM " + eds + " WHERE eventName NOT IN (SELECT eventName FROM " + eds +
				" WHERE eventSource = 's3.amazonaws.com')",
			col:  "eventName",
			want: []string{"RunInstances", "TerminateInstances"},
		},
		{
			name: "scalar_subquery",
			stmt: "SELECT eventName FROM " + eds + " WHERE eventId = (SELECT eventId FROM " + eds +
				" WHERE eventName = 'RunInstances')",
			col:  "eventName",
			want: []string{"RunInstances"},
		},
		{
			name: "exists_subquery",
			stmt: "SELECT eventName FROM " + eds + " WHERE username = 'bob' AND EXISTS (SELECT eventName FROM " + eds +
				" WHERE eventName = 'RunInstances')",
			col:  "eventName",
			want: []string{"DeleteBucket"},
		},
		{
			name: "derived_table_count",
			stmt: "SELECT COUNT(*) FROM (SELECT eventName FROM " + eds + " WHERE eventSource = 'ec2.amazonaws.com') t",
			col:  "_col0",
			want: []string{"2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := runLakeQuery(t, client, tt.stmt)
			require.Equal(t, "FINISHED", string(res.QueryStatus), "%v", res.ErrorMessage)
			assert.Equal(t, tt.want, rowColumnValues(res.QueryResultRows, tt.col))
		})
	}
}
