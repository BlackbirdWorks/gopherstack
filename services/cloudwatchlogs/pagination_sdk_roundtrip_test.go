package cloudwatchlogs_test

import (
	"context"
	"encoding/base64"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestGetLogEvents_SDKRoundTrip_StaleNextTokenDoesNotPanic drives
// GetLogEvents through the real aws-sdk-go-v2 cloudwatchlogs client with a
// nextToken naming an offset past the current event count -- the scenario
// this pass found panicking in InMemoryBackend.GetLogEvents
// (services/cloudwatchlogs/log_events.go): a stale token (e.g. one minted
// before the retention janitor swept older events, or a corrupted/replayed
// token) sliced filtered[startIdx:end] without clamping startIdx first,
// producing "slice bounds out of range" whenever startIdx exceeded the
// current event count. Ties the unit-level reproduction in
// log_events_test.go's TestCloudWatchLogsBackend_GetLogEvents_StaleTokenPastEnd
// to observable behaviour through the typed SDK client and its own
// deserializer.
func TestGetLogEvents_SDKRoundTrip_StaleNextTokenDoesNotPanic(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	h := cloudwatchlogs.NewHandler(backend)
	client := newTestCloudWatchLogsClient(t, h)

	_, err := backend.CreateLogGroup(context.Background(), "grp", "", "")
	require.NoError(t, err)
	_, err = backend.CreateLogStream(context.Background(), "grp", "stream")
	require.NoError(t, err)
	_, err = backend.PutLogEvents(context.Background(), "grp", "stream", "", []cloudwatchlogs.InputLogEvent{
		{Message: "a", Timestamp: 1},
		{Message: "b", Timestamp: 2},
	})
	require.NoError(t, err)

	staleToken := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(1000)))

	require.NotPanics(t, func() {
		out, getErr := client.GetLogEvents(t.Context(), &cwlsdk.GetLogEventsInput{
			LogGroupName:  aws.String("grp"),
			LogStreamName: aws.String("stream"),
			NextToken:     aws.String(staleToken),
		})
		require.NoError(t, getErr)
		assert.Empty(t, out.Events)
	})
}
