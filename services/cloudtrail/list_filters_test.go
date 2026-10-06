package cloudtrail_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestListOps_RejectMalformedNextToken covers InvalidNextTokenException (cloudtrail@v1.58.4).
func TestListOps_RejectMalformedNextToken(t *testing.T) {
	t.Parallel()

	tok := aws.String("not-a-token")

	tests := []struct {
		call func(ctx context.Context, c *cloudtrailsdk.Client, queryID, eds *string) error
		name string
	}{
		{name: "channels", call: func(ctx context.Context, c *cloudtrailsdk.Client, _, _ *string) error {
			_, err := c.ListChannels(ctx, &cloudtrailsdk.ListChannelsInput{NextToken: tok})

			return err
		}},
		{name: "event_data_stores", call: func(ctx context.Context, c *cloudtrailsdk.Client, _, _ *string) error {
			_, err := c.ListEventDataStores(ctx, &cloudtrailsdk.ListEventDataStoresInput{NextToken: tok})

			return err
		}},
		{name: "imports", call: func(ctx context.Context, c *cloudtrailsdk.Client, _, _ *string) error {
			_, err := c.ListImports(ctx, &cloudtrailsdk.ListImportsInput{NextToken: tok})

			return err
		}},
		{name: "import_failures", call: func(ctx context.Context, c *cloudtrailsdk.Client, _, _ *string) error {
			_, err := c.ListImportFailures(ctx, &cloudtrailsdk.ListImportFailuresInput{
				ImportId: aws.String("imp"), NextToken: tok,
			})

			return err
		}},
		{name: "queries", call: func(ctx context.Context, c *cloudtrailsdk.Client, _, eds *string) error {
			_, err := c.ListQueries(ctx, &cloudtrailsdk.ListQueriesInput{EventDataStore: eds, NextToken: tok})

			return err
		}},
		{name: "query_results", call: func(ctx context.Context, c *cloudtrailsdk.Client, q, _ *string) error {
			_, err := c.GetQueryResults(ctx, &cloudtrailsdk.GetQueryResultsInput{QueryId: q, NextToken: tok})

			return err
		}},
		{name: "lookup_events", call: func(ctx context.Context, c *cloudtrailsdk.Client, _, _ *string) error {
			_, err := c.LookupEvents(ctx, &cloudtrailsdk.LookupEventsInput{NextToken: tok})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudTrailClient(t, newCTHandler())
			eds := newQueryGrammarEDS(t, client)
			start, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{
				QueryStatement: aws.String("SELECT eventName FROM " + eds),
			})
			require.NoError(t, err)

			err = tt.call(t.Context(), client, start.QueryId, aws.String(eds))

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidNextTokenException", apiErr.ErrorCode())
		})
	}
}

// TestListQueries_TimeRange covers StartTime/EndTime (api_op_ListQueries.go).
func TestListQueries_TimeRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		start time.Duration
		end   time.Duration
		want  int
	}{
		{name: "open", want: 1},
		{name: "start_in_future", start: time.Hour, want: 0},
		{name: "end_in_past", end: -time.Hour, want: 0},
		{name: "window_around_now", start: -time.Hour, end: time.Hour, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudTrailClient(t, newCTHandler())
			eds := newQueryGrammarEDS(t, client)
			_, err := client.StartQuery(t.Context(), &cloudtrailsdk.StartQueryInput{
				QueryStatement: aws.String("SELECT eventName FROM " + eds),
			})
			require.NoError(t, err)

			now := time.Now()
			in := &cloudtrailsdk.ListQueriesInput{EventDataStore: aws.String(eds)}

			if tt.start != 0 {
				in.StartTime = aws.Time(now.Add(tt.start))
			}

			if tt.end != 0 {
				in.EndTime = aws.Time(now.Add(tt.end))
			}

			out, err := client.ListQueries(t.Context(), in)
			require.NoError(t, err)
			assert.Len(t, out.Queries, tt.want)
		})
	}
}

// TestLookupEvents_FractionalTimes sends sub-second StartTime/EndTime as the SDK does (serializers.go Double).
func TestLookupEvents_FractionalTimes(t *testing.T) {
	t.Parallel()

	client := newTestCloudTrailClient(t, newCTHandler())
	now := time.Now()

	_, err := client.LookupEvents(t.Context(), &cloudtrailsdk.LookupEventsInput{
		StartTime: aws.Time(now.Add(-time.Hour).Truncate(time.Second).Add(250 * time.Millisecond)),
		EndTime:   aws.Time(now.Add(time.Hour).Truncate(time.Second).Add(500 * time.Millisecond)),
	})
	require.NoError(t, err)
}

func newCTHandler() *cloudtrail.Handler {
	return cloudtrail.NewHandler(cloudtrail.NewInMemoryBackend("000000000000", ctTagsRTRegion))
}
