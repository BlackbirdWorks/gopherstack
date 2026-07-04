package cloudwatchlogs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestQuery_StreamWindowNarrowing verifies that a query with a time window only
// scans streams whose event-time range overlaps the window; a stream entirely
// outside the window contributes neither results nor scanned records.
func TestQuery_StreamWindowNarrowing(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, err := b.CreateLogGroup(context.Background(), "/grp", "", "")
	require.NoError(t, err)

	_, err = b.CreateLogStream(context.Background(), "/grp", "old")
	require.NoError(t, err)
	_, err = b.CreateLogStream(context.Background(), "/grp", "new")
	require.NoError(t, err)

	_, err = b.PutLogEvents(context.Background(), "/grp", "old", "", []cloudwatchlogs.InputLogEvent{
		{Message: "old-1", Timestamp: 1000},
		{Message: "old-2", Timestamp: 2000},
	})
	require.NoError(t, err)
	_, err = b.PutLogEvents(context.Background(), "/grp", "new", "", []cloudwatchlogs.InputLogEvent{
		{Message: "new-1", Timestamp: 9000},
		{Message: "new-2", Timestamp: 9500},
	})
	require.NoError(t, err)

	tests := []struct {
		name        string
		start       int64
		end         int64
		wantResults int
		wantScanned float64
	}{
		{name: "only_new_window", start: 8000, end: 10000, wantResults: 2, wantScanned: 2},
		{name: "only_old_window", start: 500, end: 3000, wantResults: 2, wantScanned: 2},
		{name: "full_window", start: 0, end: 0, wantResults: 4, wantScanned: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			qid := "q-" + tt.name
			_, startErr := b.StartQuery(
				context.Background(), qid, "fields @message", []string{"/grp"}, tt.start, tt.end,
			)
			require.NoError(t, startErr)

			results, stats, _, getErr := b.GetQueryResults(qid)
			require.NoError(t, getErr)
			assert.Len(t, results, tt.wantResults)
			// The out-of-window stream is skipped entirely, so only the overlapping
			// stream's records are scanned.
			assert.InDelta(t, tt.wantScanned, stats.RecordsScanned, 0.1)
		})
	}
}
