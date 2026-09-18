package cloudwatchlogs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// filterLogEventsMinStartFromHeadFalseMs mirrors the unexported constant in
// log_events.go (Jan 1, 2024 00:00:00 UTC in epoch ms) so this test can pick
// startTime values on either side of the real StartFromHead=false cutoff.
const filterLogEventsMinStartFromHeadFalseMs = 1704067200000

// TestFilterLogEvents_StartFromHead covers gopherstack-xhu2t: FilterLogEvents
// previously dropped startFromHead entirely (always sorted oldest-first,
// api_op_FilterLogEvents.go's documented default), so a real client asking
// for newest-first ordering got the opposite of what it requested.
func TestFilterLogEvents_StartFromHead(t *testing.T) {
	t.Parallel()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())
	client, _ := newCapturingWireTestCloudWatchLogsClient(t, h)
	ctx := t.Context()

	const logGroup = "/filter/startfromhead"
	const logStream = "stream-1"
	const total = 5

	seedPtrLeakEvents(t, client, logGroup, logStream, total)

	recentStart := aws.Int64(filterLogEventsMinStartFromHeadFalseMs)

	t.Run("default is oldest first", func(t *testing.T) {
		t.Parallel()

		out, err := client.FilterLogEvents(ctx, &cwlsdk.FilterLogEventsInput{
			LogGroupName: aws.String(logGroup),
		})
		require.NoError(t, err)
		require.Len(t, out.Events, total)

		assert.True(t, sortedAscendingByTimestamp(out.Events),
			"default (unset startFromHead) must return oldest events first")
	})

	t.Run("explicit true is oldest first", func(t *testing.T) {
		t.Parallel()

		out, err := client.FilterLogEvents(ctx, &cwlsdk.FilterLogEventsInput{
			LogGroupName:  aws.String(logGroup),
			StartFromHead: aws.Bool(true),
		})
		require.NoError(t, err)
		require.Len(t, out.Events, total)

		assert.True(t, sortedAscendingByTimestamp(out.Events),
			"startFromHead=true must return oldest events first")
	})

	t.Run("explicit false is newest first", func(t *testing.T) {
		t.Parallel()

		out, err := client.FilterLogEvents(ctx, &cwlsdk.FilterLogEventsInput{
			LogGroupName:  aws.String(logGroup),
			StartFromHead: aws.Bool(false),
			StartTime:     recentStart,
		})
		require.NoError(t, err)
		require.Len(t, out.Events, total)

		assert.True(t, sortedDescendingByTimestamp(out.Events),
			"startFromHead=false must return newest events first")
	})

	t.Run("false without a recent starttime is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := client.FilterLogEvents(ctx, &cwlsdk.FilterLogEventsInput{
			LogGroupName:  aws.String(logGroup),
			StartFromHead: aws.Bool(false),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InvalidParameterException")
	})

	t.Run("pagination continues in the direction the token was issued for", func(t *testing.T) {
		t.Parallel()

		first, err := client.FilterLogEvents(ctx, &cwlsdk.FilterLogEventsInput{
			LogGroupName:  aws.String(logGroup),
			StartFromHead: aws.Bool(false),
			StartTime:     recentStart,
			Limit:         aws.Int32(2),
		})
		require.NoError(t, err)
		require.Len(t, first.Events, 2)
		require.NotNil(t, first.NextToken)

		second, err := client.FilterLogEvents(ctx, &cwlsdk.FilterLogEventsInput{
			LogGroupName: aws.String(logGroup),
			NextToken:    first.NextToken,
		})
		require.NoError(t, err)
		require.NotEmpty(t, second.Events)

		assert.Less(t, aws.ToInt64(second.Events[0].Timestamp), aws.ToInt64(first.Events[1].Timestamp),
			"a nextToken from a descending page must keep paging newest-to-oldest without startFromHead resent")
	})
}

func sortedAscendingByTimestamp(events []cwltypes.FilteredLogEvent) bool {
	for i := 1; i < len(events); i++ {
		if aws.ToInt64(events[i].Timestamp) < aws.ToInt64(events[i-1].Timestamp) {
			return false
		}
	}

	return true
}

func sortedDescendingByTimestamp(events []cwltypes.FilteredLogEvent) bool {
	for i := 1; i < len(events); i++ {
		if aws.ToInt64(events[i].Timestamp) > aws.ToInt64(events[i-1].Timestamp) {
			return false
		}
	}

	return true
}
