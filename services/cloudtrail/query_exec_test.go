package cloudtrail_test

import (
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestQueryExecution_SelectStarNoFilter verifies GetQueryResults executes a
// plain "SELECT * FROM <eds>" against recorded events and returns real rows,
// not the always-empty QueryResultRows this backend previously returned
// unconditionally.
func TestQueryExecution_SelectStarNoFilter(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	b.RecordEvent(cloudtrail.Event{
		EventName:   "CreateBucket",
		EventSource: "s3.amazonaws.com",
		EventTime:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	b.RecordEvent(cloudtrail.Event{
		EventName:   "RunInstances",
		EventSource: "ec2.amazonaws.com",
		EventTime:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	})

	q, err := b.StartQuery("SELECT * FROM eds-000001", "eds-000001", "", "")
	require.NoError(t, err)
	require.Equal(t, "QUEUED", q.QueryStatus, "StartQuery must leave the query cancellable, not pre-executed")

	got, err := b.GetQueryResults(q.QueryID)
	require.NoError(t, err)
	assert.Equal(t, "FINISHED", got.QueryStatus, "first read must materialize (execute) the query")
	assert.Len(t, got.QueryResultRows, 2)
	assert.EqualValues(t, 2, got.EventsScanned)
	assert.EqualValues(t, 2, got.EventsMatched)
}

// TestQueryExecution_SelectColumnsWithWhereAndLimit verifies column
// projection, a WHERE equality filter, and LIMIT all take effect.
func TestQueryExecution_SelectColumnsWithWhereAndLimit(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	b.RecordEvent(cloudtrail.Event{EventName: "CreateBucket", EventSource: "s3.amazonaws.com"})
	b.RecordEvent(cloudtrail.Event{EventName: "DeleteBucket", EventSource: "s3.amazonaws.com"})
	b.RecordEvent(cloudtrail.Event{EventName: "RunInstances", EventSource: "ec2.amazonaws.com"})

	q, err := b.StartQuery(
		"SELECT eventName, eventSource FROM eds-000001 WHERE eventSource = 's3.amazonaws.com' LIMIT 1",
		"eds-000001", "", "",
	)
	require.NoError(t, err)

	got, err := b.GetQueryResults(q.QueryID)
	require.NoError(t, err)
	require.Len(t, got.QueryResultRows, 1, "LIMIT 1 must cap the returned rows")
	assert.EqualValues(
		t,
		2,
		got.EventsMatched,
		"EventsMatched counts all WHERE matches, not just the LIMIT-truncated page",
	)

	row := got.QueryResultRows[0]
	require.Len(t, row, 2, "SELECT eventName, eventSource projects exactly two columns")

	cols := map[string]string{}
	for _, colMap := range row {
		maps.Copy(cols, colMap)
	}
	assert.Equal(t, "s3.amazonaws.com", cols["eventSource"])
	assert.Contains(t, []string{"CreateBucket", "DeleteBucket"}, cols["eventName"])
}

// TestQueryExecution_UnsupportedGrammarReachesFailed verifies a
// syntactically valid but unsupported (outside the emulator's parsed
// subset -- a JOIN, here) Lake SQL statement reaches QueryStatus FAILED with
// a populated ErrorMessage, not a silent empty FINISHED. StartQuery itself
// must not reject it synchronously (mirrors AWS's async execution model --
// see materializeQueryLocked): only the first read discovers the failure.
func TestQueryExecution_UnsupportedGrammarReachesFailed(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	b.RecordEvent(cloudtrail.Event{EventName: "CreateBucket", EventSource: "s3.amazonaws.com"})

	q, err := b.StartQuery(
		"SELECT edsA.eventName FROM eds-000001 AS edsA LEFT JOIN eds-000002 AS edsB ON edsA.eventId = edsB.eventId",
		"eds-000001", "", "",
	)
	require.NoError(t, err, "StartQuery must accept syntactically valid CloudTrail Lake SQL synchronously")
	require.Equal(t, "QUEUED", q.QueryStatus)

	got, err := b.GetQueryResults(q.QueryID)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", got.QueryStatus)
	assert.Empty(t, got.QueryResultRows)
	assert.NotEmpty(t, got.ErrorMessage, "a FAILED query must explain why, not just fail silently")

	desc, err := b.DescribeQuery(q.QueryID)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", desc.QueryStatus)
	assert.Equal(t, got.ErrorMessage, desc.ErrorMessage)
}

// TestQueryExecution_CancelBeforeReadStaysQueued verifies a query cancelled
// before its first GetQueryResults/DescribeQuery call never executes (stays
// un-materialized), and a cancelled query cannot be re-cancelled.
func TestQueryExecution_CancelBeforeReadStaysQueued(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)

	q, err := b.StartQuery("SELECT * FROM eds-000001", "eds-000001", "", "")
	require.NoError(t, err)

	cancelled, err := b.CancelQuery(q.QueryID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", cancelled.QueryStatus)

	_, err = b.CancelQuery(q.QueryID)
	require.ErrorIs(t, err, cloudtrail.ErrQueryInactive)

	// DescribeQuery on an already-terminal (CANCELLED) query must not
	// clobber its status back to FINISHED.
	desc, err := b.DescribeQuery(q.QueryID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", desc.QueryStatus)
}

// TestQueryExecution_QueryIDNotFound verifies the not-found error code is
// QueryIdNotFoundException (not the previous, incorrect
// InactiveQueryException -- that code is reserved for cancelling an
// already-terminal query).
func TestQueryExecution_QueryIDNotFound(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.DescribeQuery("query-missing")
	require.ErrorIs(t, err, cloudtrail.ErrQueryIDNotFound)

	_, err = b.GetQueryResults("query-missing")
	require.ErrorIs(t, err, cloudtrail.ErrQueryIDNotFound)

	_, err = b.CancelQuery("query-missing")
	require.ErrorIs(t, err, cloudtrail.ErrQueryIDNotFound)
}
