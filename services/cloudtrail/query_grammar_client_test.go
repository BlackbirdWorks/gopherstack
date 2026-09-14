package cloudtrail_test

import (
	"maps"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// seedQueryGrammarEvents records four distinct management events (the same
// RecordManagementEvent path pkgs/service.wrapCloudTrailCapture uses for
// real cross-service activity -- see TestGetQueryResults_RealRowsAndPagination
// for the established seeding pattern) for the SQL-grammar tests below to
// query against: two S3 events (one per user) and two EC2 events (one per
// user), so WHERE/LIKE/GROUP BY have real column diversity to filter on.
func seedQueryGrammarEvents(b *cloudtrail.InMemoryBackend) {
	b.RecordManagementEvent(service.CloudTrailEventInput{
		EventName: "CreateBucket", EventSource: "s3.amazonaws.com", Username: "alice",
	})
	b.RecordManagementEvent(service.CloudTrailEventInput{
		EventName: "DeleteBucket", EventSource: "s3.amazonaws.com", Username: "bob",
	})
	b.RecordManagementEvent(service.CloudTrailEventInput{
		EventName: "RunInstances", EventSource: "ec2.amazonaws.com", Username: "alice",
	})
	b.RecordManagementEvent(service.CloudTrailEventInput{
		EventName: "TerminateInstances", EventSource: "ec2.amazonaws.com", Username: "carol",
	})
}

func newQueryGrammarEDS(t *testing.T, client *cloudtrailsdk.Client) string {
	t.Helper()

	out, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
		Name: aws.String("grammar-eds"),
	})
	require.NoError(t, err)

	return aws.ToString(out.EventDataStoreArn)
}

// runLakeQuery starts and reads back a query through the real typed client,
// failing the test if either call errors (GetQueryResults surfaces query
// failures via QueryStatus/ErrorMessage in its response, not a client
// error, so a non-nil err here means something else broke).
func runLakeQuery(t *testing.T, client *cloudtrailsdk.Client, stmt string) *cloudtrailsdk.GetQueryResultsOutput {
	t.Helper()

	start, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{QueryStatement: aws.String(stmt)})
	require.NoError(t, err)

	res, err := client.GetQueryResults(t.Context(), &cloudtrailsdk.GetQueryResultsInput{QueryId: start.QueryId})
	require.NoError(t, err)

	return res
}

// rowColumnValues extracts every value of a single column name across a
// GetQueryResultsOutput's QueryResultRows (a []map[string]string per row).
func rowColumnValues(rows [][]map[string]string, col string) []string {
	var out []string

	for _, row := range rows {
		for _, cell := range row {
			if v, ok := cell[col]; ok {
				out = append(out, v)
			}
		}
	}

	sort.Strings(out)

	return out
}

// TestQueryGrammar_WhereClause drives WHERE-clause OR/LIKE/NOT/IN/
// parenthesized-precedence support through the real aws-sdk-go-v2 cloudtrail
// client. Every case here fails against the pre-fix parser: OR/LIKE/NOT/IN
// were not in the supported grammar at all (any WHERE clause using them was
// outside query_exec.go's old regex-based subset, which only understood
// ANDed equality/inequality -- see PARITY.md's now-fixed gap), so each
// query below used to reach FINISHED with zero rows regardless of which
// events matched.
func TestQueryGrammar_WhereClause(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	seedQueryGrammarEvents(backend)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))
	edsARN := newQueryGrammarEDS(t, client)

	tests := []struct {
		name    string
		where   string
		wantEvt []string
	}{
		{
			name:    "or_narrows_to_matching_branches",
			where:   "eventName = 'CreateBucket' OR eventName = 'RunInstances'",
			wantEvt: []string{"CreateBucket", "RunInstances"},
		},
		{
			name:    "like_percent_wildcard",
			where:   "eventName LIKE '%Bucket'",
			wantEvt: []string{"CreateBucket", "DeleteBucket"},
		},
		{
			name:    "like_underscore_wildcard",
			where:   "eventName LIKE 'Run_nstances'",
			wantEvt: []string{"RunInstances"},
		},
		{
			name:    "like_is_case_sensitive",
			where:   "eventName LIKE '%bucket'",
			wantEvt: nil,
		},
		{
			name:    "not_prefix",
			where:   "NOT eventSource = 's3.amazonaws.com'",
			wantEvt: []string{"RunInstances", "TerminateInstances"},
		},
		{
			name:    "in_membership",
			where:   "eventName IN ('CreateBucket', 'RunInstances')",
			wantEvt: []string{"CreateBucket", "RunInstances"},
		},
		{
			name:    "not_in_membership",
			where:   "eventName NOT IN ('CreateBucket', 'RunInstances')",
			wantEvt: []string{"DeleteBucket", "TerminateInstances"},
		},
		{
			name: "parens_override_default_and_or_precedence",
			where: "eventSource = 's3.amazonaws.com' AND " +
				"(username = 'alice' OR eventName = 'TerminateInstances')",
			wantEvt: []string{"CreateBucket"},
		},
		{
			name: "without_parens_and_binds_tighter_than_or",
			where: "eventSource = 's3.amazonaws.com' AND username = 'alice' " +
				"OR eventName = 'TerminateInstances'",
			wantEvt: []string{"CreateBucket", "TerminateInstances"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := runLakeQuery(t, client, "SELECT eventName FROM "+edsARN+" WHERE "+tt.where)
			assert.Equal(t, "FINISHED", string(res.QueryStatus))
			assert.Equal(t, tt.wantEvt, rowColumnValues(res.QueryResultRows, "eventName"))
		})
	}
}

// TestQueryGrammar_CountAggregate verifies COUNT(*) with and without GROUP
// BY, through the real client. Both fail against the pre-fix parser: no
// aggregate support existed at all, so a query using COUNT reached FINISHED
// with zero rows regardless of how many events matched.
func TestQueryGrammar_CountAggregate(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	seedQueryGrammarEvents(backend)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))
	edsARN := newQueryGrammarEDS(t, client)

	t.Run("count_star_no_group_by", func(t *testing.T) {
		t.Parallel()

		res := runLakeQuery(t, client, "SELECT COUNT(*) FROM "+edsARN)
		require.Equal(t, "FINISHED", string(res.QueryStatus))
		require.Len(t, res.QueryResultRows, 1)
		assert.Equal(t, "4", res.QueryResultRows[0][0]["_col0"], "unaliased COUNT(*) is named _col0, Trino-positional")
	})

	t.Run("count_star_group_by", func(t *testing.T) {
		t.Parallel()

		res := runLakeQuery(t, client, "SELECT eventSource, COUNT(*) FROM "+edsARN+" GROUP BY eventSource")
		require.Equal(t, "FINISHED", string(res.QueryStatus))
		require.Len(t, res.QueryResultRows, 2)

		counts := map[string]string{}

		for _, row := range res.QueryResultRows {
			flat := map[string]string{}
			for _, cell := range row {
				maps.Copy(flat, cell)
			}

			counts[flat["eventSource"]] = flat["_col1"]
		}

		assert.Equal(t, map[string]string{"s3.amazonaws.com": "2", "ec2.amazonaws.com": "2"}, counts)
	})
}

// TestQueryGrammar_JoinReachesFailed verifies a JOIN query (a real
// CloudTrail Lake feature this emulator does not implement -- see
// PARITY.md) reaches QueryStatus FAILED with a populated ErrorMessage via
// DescribeQuery, never a silent FINISHED with zero rows. This fails against
// the pre-fix code, which always returned FINISHED regardless of grammar
// support.
func TestQueryGrammar_JoinReachesFailed(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	seedQueryGrammarEvents(backend)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))
	edsARN := newQueryGrammarEDS(t, client)

	stmt := "SELECT edsA.eventName FROM " + edsARN + " AS edsA LEFT JOIN " + edsARN +
		" AS edsB ON edsA.eventId = edsB.eventId"

	start, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{QueryStatement: aws.String(stmt)})
	require.NoError(t, err, "StartQuery must accept syntactically valid Lake SQL synchronously")

	desc, err := client.DescribeQuery(t.Context(), &cloudtrailsdk.DescribeQueryInput{QueryId: start.QueryId})
	require.NoError(t, err)
	assert.Equal(t, "FAILED", string(desc.QueryStatus))
	assert.NotEmpty(t, aws.ToString(desc.ErrorMessage))
}

// TestListQueries_ThroughClient_UnknownStoreErrors verifies ListQueries
// rejects an EventDataStore that doesn't exist with
// EventDataStoreNotFoundException, through the real typed client. Fails
// against the pre-fix handler, which left the EventDataStore filter
// permissive (an unknown/empty value matched nothing but never errored).
func TestListQueries_ThroughClient_UnknownStoreErrors(t *testing.T) {
	t.Parallel()

	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

	_, err := client.ListQueries(t.Context(), &cloudtrailsdk.ListQueriesInput{
		EventDataStore: aws.String("eds-does-not-exist"),
	})
	require.Error(t, err)

	var target *cttypes.EventDataStoreNotFoundException
	require.ErrorAs(t, err, &target)
}
