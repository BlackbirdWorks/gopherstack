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

// TestCreateLookupTable_FromQueryID drives CreateLookupTable with QueryId
// instead of TableBody through the real aws-sdk-go-v2 client.
// CreateLookupTableInput's own doc comment (cloudwatchlogs@v1.81.1
// api_op_CreateLookupTable.go:55) says "You must specify either tableBody or
// queryId, but not both" -- gopherstack's createLookupTableInput previously
// had no QueryId field at all, so a real caller using the query-results path
// (as opposed to raw CSV) always fell through to "tableBody is required",
// even though AWS's own client-side validator (validateOpCreateLookupTableInput)
// does not require either field, so the request reaches the wire unmodified.
func TestCreateLookupTable_FromQueryID(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
		LogGroupName: aws.String("query-source-group"),
	})
	require.NoError(t, err)

	_, err = client.CreateLogStream(t.Context(), &cwlsdk.CreateLogStreamInput{
		LogGroupName:  aws.String("query-source-group"),
		LogStreamName: aws.String("stream1"),
	})
	require.NoError(t, err)

	_, err = client.PutLogEvents(t.Context(), &cwlsdk.PutLogEventsInput{
		LogGroupName:  aws.String("query-source-group"),
		LogStreamName: aws.String("stream1"),
		LogEvents: []cwltypes.InputLogEvent{
			{Message: aws.String("alpha"), Timestamp: aws.Int64(1000)},
			{Message: aws.String("bravo"), Timestamp: aws.Int64(2000)},
		},
	})
	require.NoError(t, err)

	sq, err := client.StartQuery(t.Context(), &cwlsdk.StartQueryInput{
		LogGroupName: aws.String("query-source-group"),
		QueryString:  aws.String("fields @message"),
		StartTime:    aws.Int64(0),
		EndTime:      aws.Int64(10000),
	})
	require.NoError(t, err)

	out, err := client.CreateLookupTable(t.Context(), &cwlsdk.CreateLookupTableInput{
		LookupTableName: aws.String("from_query"),
		QueryId:         sq.QueryId,
	})
	require.NoError(t, err)
	require.NotNil(t, out.LookupTableArn)

	got, err := client.GetLookupTable(t.Context(), &cwlsdk.GetLookupTableInput{
		LookupTableArn: out.LookupTableArn,
	})
	require.NoError(t, err)
	require.NotNil(t, got.TableBody)
	assert.Contains(t, *got.TableBody, "@message")
	assert.Contains(t, *got.TableBody, "alpha")
	assert.Contains(t, *got.TableBody, "bravo")
}

// TestCreateLookupTable_TableBodyAndQueryIDMutualExclusion asserts the
// server-side "either tableBody or queryId, but not both / at least one"
// rule -- not enforced by validateOpCreateLookupTableInput, so both branches
// reach the wire via the real client unmodified.
func TestCreateLookupTable_TableBodyAndQueryIDMutualExclusion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tableBody *string
		queryID   *string
		name      string
	}{
		{name: "both set", tableBody: aws.String("a,b\n1,2\n"), queryID: aws.String("some-query-id")},
		{name: "neither set"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudwatchlogs.NewInMemoryBackend()
			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

			_, err := client.CreateLookupTable(t.Context(), &cwlsdk.CreateLookupTableInput{
				LookupTableName: aws.String("bad_table"),
				TableBody:       tt.tableBody,
				QueryId:         tt.queryID,
			})
			require.Error(t, err)

			var ipe *cwltypes.InvalidParameterException
			require.ErrorAs(t, err, &ipe, "expected InvalidParameterException, got %v", err)
		})
	}
}
