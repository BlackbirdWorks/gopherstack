package cloudwatchlogs_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestScheduledQuery_IdentifierRealClient covers gopherstack-wksweep-cwl-1:
// DeleteScheduledQuery, UpdateScheduledQuery, GetScheduledQuery, and
// GetScheduledQueryHistory all take an Identifier member on the real SDK
// (cloudwatchlogs@v1.81.1 api_op_{Delete,Update,Get,GetHistory}ScheduledQuery.go),
// not ScheduledQueryArn. A real client only ever sends "identifier" on the
// wire; before the fix, gopherstack read "scheduledQueryArn" instead, so
// these four ops could never resolve the query a real client asked for.
func TestScheduledQuery_IdentifierRealClient(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateScheduledQuery(ctx, &cwlsdk.CreateScheduledQueryInput{
		Name:               aws.String("sq-identifier-rt"),
		QueryString:        aws.String("fields @message | limit 100"),
		QueryLanguage:      types.QueryLanguageCwli,
		ScheduleExpression: aws.String("cron(0 * * * ? *)"),
		ExecutionRoleArn:   aws.String("arn:aws:iam::123456789012:role/r"),
	})
	require.NoError(t, err)
	arn := aws.ToString(created.ScheduledQueryArn)
	require.NotEmpty(t, arn)

	get, err := client.GetScheduledQuery(ctx, &cwlsdk.GetScheduledQueryInput{
		Identifier: aws.String(arn),
	})
	require.NoError(t, err, "GetScheduledQuery must resolve the query by the real Identifier member")
	assert.Equal(t, arn, aws.ToString(get.ScheduledQueryArn))
	assert.Equal(t, "sq-identifier-rt", aws.ToString(get.Name))

	_, err = client.UpdateScheduledQuery(ctx, &cwlsdk.UpdateScheduledQueryInput{
		Identifier:         aws.String(arn),
		ExecutionRoleArn:   aws.String("arn:aws:iam::123456789012:role/r"),
		QueryLanguage:      types.QueryLanguageCwli,
		QueryString:        aws.String("fields @message | limit 100"),
		ScheduleExpression: aws.String("cron(0 * * * ? *)"),
		State:              types.ScheduledQueryStateDisabled,
	})
	require.NoError(t, err, "UpdateScheduledQuery must resolve the query by the real Identifier member")

	updated, err := client.GetScheduledQuery(ctx, &cwlsdk.GetScheduledQueryInput{Identifier: aws.String(arn)})
	require.NoError(t, err)
	assert.Equal(t, types.ScheduledQueryStateDisabled, updated.State)

	_, err = client.GetScheduledQueryHistory(ctx, &cwlsdk.GetScheduledQueryHistoryInput{
		Identifier: aws.String(arn),
		StartTime:  aws.Int64(0),
		EndTime:    aws.Int64(9999999999),
	})
	require.NoError(t, err, "GetScheduledQueryHistory must resolve the query by the real Identifier member")

	_, err = client.DeleteScheduledQuery(ctx, &cwlsdk.DeleteScheduledQueryInput{Identifier: aws.String(arn)})
	require.NoError(t, err, "DeleteScheduledQuery must resolve the query by the real Identifier member")

	_, err = client.GetScheduledQuery(ctx, &cwlsdk.GetScheduledQueryInput{Identifier: aws.String(arn)})
	require.Error(t, err, "scheduled query must actually be gone after DeleteScheduledQuery")
}

// TestListLogAnomalyDetectors_FilterLogGroupArnRealClient covers
// gopherstack-wksweep-cwl-2: ListLogAnomalyDetectorsInput's real filter
// member is the singular FilterLogGroupArn *string (cloudwatchlogs@v1.81.1
// api_op_ListLogAnomalyDetectors.go), not a list. Before the fix, gopherstack
// read a nonexistent "filterLogGroupArnList" field, so a real client's
// filter was always silently dropped and every detector was returned
// regardless of the requested log group.
func TestListLogAnomalyDetectors_FilterLogGroupArnRealClient(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	lg1, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/flt/one")})
	require.NoError(t, err)
	_ = lg1

	lg2, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/flt/two")})
	require.NoError(t, err)
	_ = lg2

	desc1, err := client.DescribeLogGroups(ctx, &cwlsdk.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String("/flt/one"),
	})
	require.NoError(t, err)
	require.Len(t, desc1.LogGroups, 1)
	arn1 := aws.ToString(desc1.LogGroups[0].Arn)

	desc2, err := client.DescribeLogGroups(ctx, &cwlsdk.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String("/flt/two"),
	})
	require.NoError(t, err)
	require.Len(t, desc2.LogGroups, 1)
	arn2 := aws.ToString(desc2.LogGroups[0].Arn)

	_, err = client.CreateLogAnomalyDetector(ctx, &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{arn1},
		DetectorName:    aws.String("det-one"),
	})
	require.NoError(t, err)

	_, err = client.CreateLogAnomalyDetector(ctx, &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{arn2},
		DetectorName:    aws.String("det-two"),
	})
	require.NoError(t, err)

	all, err := client.ListLogAnomalyDetectors(ctx, &cwlsdk.ListLogAnomalyDetectorsInput{})
	require.NoError(t, err)
	require.Len(t, all.AnomalyDetectors, 2, "sanity: both detectors exist before filtering")

	filtered, err := client.ListLogAnomalyDetectors(ctx, &cwlsdk.ListLogAnomalyDetectorsInput{
		FilterLogGroupArn: aws.String(arn1),
	})
	require.NoError(t, err)
	require.Len(t, filtered.AnomalyDetectors, 1,
		"FilterLogGroupArn must actually filter; pre-fix it was silently ignored and returned both")
	assert.Equal(t, "det-one", aws.ToString(filtered.AnomalyDetectors[0].DetectorName))
}

// TestDescribeResourcePolicies_FullPagination creates more account-scoped
// resource policies than one page holds and drives the real SDK client
// through the full pagination loop, asserting the union is exactly the
// created set with no duplicates and nothing missing.
// DescribeResourcePoliciesInput's Limit/NextToken (api_op_
// DescribeResourcePolicies.go:29-42) were previously decoded nowhere:
// gopherstack always returned every policy in one call, ignoring both.
func TestDescribeResourcePolicies_FullPagination(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	const total = 9

	want := make(map[string]bool, total)

	for i := range total {
		name := fmt.Sprintf("policy-%02d", i)
		_, err := client.PutResourcePolicy(ctx, &cwlsdk.PutResourcePolicyInput{
			PolicyName:     aws.String(name),
			PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.NoError(t, err)
		want[name] = true
	}

	got := make(map[string]bool, total)

	var nextToken *string
	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination loop did not terminate")

		out, err := client.DescribeResourcePolicies(ctx, &cwlsdk.DescribeResourcePoliciesInput{
			Limit:     aws.Int32(4),
			NextToken: nextToken,
		})
		require.NoError(t, err)
		require.LessOrEqualf(t, len(out.ResourcePolicies), 4,
			"Limit must actually truncate the page; pre-fix it was silently ignored")

		for _, p := range out.ResourcePolicies {
			name := aws.ToString(p.PolicyName)
			require.Falsef(t, got[name], "policy %q returned twice across pages", name)
			got[name] = true
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, want, got)
}

// TestGetQueryResults_FullPagination inserts more matching log events than
// one page holds and drives the real SDK client through the full
// GetQueryResults pagination loop, asserting the union is exactly the
// expected set with no duplicates and nothing missing.
// GetQueryResultsInput.MaxItems/NextToken (api_op_GetQueryResults.go:56-66,
// "up to 10,000 log event results ... paginating with the nextToken") were
// previously decoded nowhere: gopherstack always returned every result row
// in one call.
func TestGetQueryResults_FullPagination(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	const logGroup = "/query-pagination"
	const logStream = "stream-1"

	_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String(logGroup)})
	require.NoError(t, err)
	_, err = client.CreateLogStream(ctx, &cwlsdk.CreateLogStreamInput{
		LogGroupName: aws.String(logGroup), LogStreamName: aws.String(logStream),
	})
	require.NoError(t, err)

	const total = 25

	want := make(map[string]bool, total)
	events := make([]types.InputLogEvent, 0, total)
	now := time.Now()

	for i := range total {
		msg := fmt.Sprintf("event-%02d", i)
		want[msg] = true
		events = append(events, types.InputLogEvent{
			Message:   aws.String(msg),
			Timestamp: aws.Int64(now.Add(time.Duration(i) * time.Millisecond).UnixMilli()),
		})
	}

	_, err = client.PutLogEvents(ctx, &cwlsdk.PutLogEventsInput{
		LogGroupName:  aws.String(logGroup),
		LogStreamName: aws.String(logStream),
		LogEvents:     events,
	})
	require.NoError(t, err)

	// StartTime/EndTime of 0 mean "unbounded" on this backend
	// (streamOutsideWindow/scanStreamEvents, queries.go:158-172); real,
	// non-zero bounds are left alone here since StartQueryInput documents
	// them in epoch seconds while PutLogEvents' Timestamp is epoch
	// milliseconds and gopherstack's StartQuery handler forwards the wire
	// value unconverted -- a pre-existing, unrelated bug outside this
	// pass's pagination scope.
	started, err := client.StartQuery(ctx, &cwlsdk.StartQueryInput{
		LogGroupName: aws.String(logGroup),
		QueryString:  aws.String("fields @message | limit 10000"),
		StartTime:    aws.Int64(0),
		EndTime:      aws.Int64(0),
	})
	require.NoError(t, err)

	got := make(map[string]bool, total)

	var nextToken *string
	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination loop did not terminate")

		out, pageErr := client.GetQueryResults(ctx, &cwlsdk.GetQueryResultsInput{
			QueryId:   started.QueryId,
			MaxItems:  aws.Int32(10),
			NextToken: nextToken,
		})
		require.NoError(t, pageErr)
		require.LessOrEqualf(t, len(out.Results), 10,
			"MaxItems must actually truncate the page; pre-fix it was silently ignored")

		for _, row := range out.Results {
			for _, f := range row {
				if aws.ToString(f.Field) != "@message" {
					continue
				}
				msg := aws.ToString(f.Value)
				require.Falsef(t, got[msg], "result %q returned twice across pages", msg)
				got[msg] = true
			}
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, want, got)
}

// TestListLogGroupsForQuery_FullPagination starts a query against more log
// groups than one page holds and drives the real SDK client through the
// full pagination loop, asserting the union is exactly the expected set
// with no duplicates and nothing missing.
// ListLogGroupsForQueryInput.MaxResults/NextToken were previously decoded
// nowhere: gopherstack always returned every log group name in one call.
func TestListLogGroupsForQuery_FullPagination(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	const total = 9

	want := make(map[string]bool, total)
	names := make([]string, 0, total)

	for i := range total {
		name := fmt.Sprintf("/query-groups/%02d", i)
		_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String(name)})
		require.NoError(t, err)
		names = append(names, name)
		want[name] = true
	}

	now := time.Now()

	started, err := client.StartQuery(ctx, &cwlsdk.StartQueryInput{
		LogGroupNames: names,
		QueryString:   aws.String("fields @message | limit 100"),
		StartTime:     aws.Int64(now.Add(-1 * time.Hour).Unix()),
		EndTime:       aws.Int64(now.Add(1 * time.Hour).Unix()),
	})
	require.NoError(t, err)

	got := make(map[string]bool, total)

	var nextToken *string
	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination loop did not terminate")

		out, pageErr := client.ListLogGroupsForQuery(ctx, &cwlsdk.ListLogGroupsForQueryInput{
			QueryId:    started.QueryId,
			MaxResults: aws.Int32(4),
			NextToken:  nextToken,
		})
		require.NoError(t, pageErr)
		require.LessOrEqualf(t, len(out.LogGroupIdentifiers), 4,
			"MaxResults must actually truncate the page; pre-fix it was silently ignored")

		for _, name := range out.LogGroupIdentifiers {
			require.Falsef(t, got[name], "log group %q returned twice across pages", name)
			got[name] = true
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, want, got)
}

// TestDescribeDeliveryDestinations_FullPagination proves gopherstack-wksweep-cwl-2:
// DescribeDeliveryDestinationsInput.Limit/NextToken (api_op_DescribeDeliveryDestinations.go)
// were previously decoded nowhere -- the handler discarded its whole request
// body (`_ []byte`) and the backend method took no paging arguments at all,
// so every call always returned the complete unpaginated list.
func TestDescribeDeliveryDestinations_FullPagination(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	const total = 9

	want := make(map[string]bool, total)

	for i := range total {
		name := fmt.Sprintf("dest-%02d", i)
		_, err := client.PutDeliveryDestination(ctx, &cwlsdk.PutDeliveryDestinationInput{
			Name: aws.String(name),
			DeliveryDestinationConfiguration: &types.DeliveryDestinationConfiguration{
				DestinationResourceArn: aws.String(fmt.Sprintf("arn:aws:s3:::bucket-%02d", i)),
			},
			DeliveryDestinationType: types.DeliveryDestinationTypeS3,
		})
		require.NoError(t, err)
		want[name] = true
	}

	got := make(map[string]bool, total)

	var nextToken *string
	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination loop did not terminate")

		out, err := client.DescribeDeliveryDestinations(ctx, &cwlsdk.DescribeDeliveryDestinationsInput{
			Limit:     aws.Int32(4),
			NextToken: nextToken,
		})
		require.NoError(t, err)
		require.LessOrEqualf(t, len(out.DeliveryDestinations), 4,
			"Limit must actually truncate the page; pre-fix it was silently ignored")

		for _, d := range out.DeliveryDestinations {
			name := aws.ToString(d.Name)
			require.Falsef(t, got[name], "delivery destination %q returned twice across pages", name)
			got[name] = true
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, want, got)
}

// TestDescribeDeliverySources_FullPagination is the same proof as
// TestDescribeDeliveryDestinations_FullPagination for
// DescribeDeliverySourcesInput.Limit/NextToken (api_op_DescribeDeliverySources.go).
func TestDescribeDeliverySources_FullPagination(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	const total = 9

	want := make(map[string]bool, total)

	for i := range total {
		name := fmt.Sprintf("src-%02d", i)
		_, err := client.PutDeliverySource(ctx, &cwlsdk.PutDeliverySourceInput{
			Name:        aws.String(name),
			ResourceArn: aws.String(fmt.Sprintf("arn:aws:lambda:us-east-1:123456789012:function:fn-%02d", i)),
			LogType:     aws.String("APPLICATION_LOGS"),
		})
		require.NoError(t, err)
		want[name] = true
	}

	got := make(map[string]bool, total)

	var nextToken *string
	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination loop did not terminate")

		out, err := client.DescribeDeliverySources(ctx, &cwlsdk.DescribeDeliverySourcesInput{
			Limit:     aws.Int32(4),
			NextToken: nextToken,
		})
		require.NoError(t, err)
		require.LessOrEqualf(t, len(out.DeliverySources), 4,
			"Limit must actually truncate the page; pre-fix it was silently ignored")

		for _, s := range out.DeliverySources {
			name := aws.ToString(s.Name)
			require.Falsef(t, got[name], "delivery source %q returned twice across pages", name)
			got[name] = true
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, want, got)
}

// TestDescribeIndexPolicies_FiltersByLogGroupIdentifiers proves
// gopherstack-wksweep-cwl-3: DescribeIndexPoliciesInput.LogGroupIdentifiers
// is a required member (api_op_DescribeIndexPolicies.go) that scopes which
// log groups' index policies come back. gopherstack previously discarded
// its whole request body (`_ []byte`) and always returned every stored
// index policy regardless of which log groups the caller asked about -- an
// unfiltered full list rather than the requested subset, and a required
// field silently accepted as absent.
func TestDescribeIndexPolicies_FiltersByLogGroupIdentifiers(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	_, err := client.PutIndexPolicy(ctx, &cwlsdk.PutIndexPolicyInput{
		LogGroupIdentifier: aws.String("/aws/lambda/wanted"),
		PolicyDocument:     aws.String(`{"Fields":["@message"]}`),
	})
	require.NoError(t, err)

	_, err = client.PutIndexPolicy(ctx, &cwlsdk.PutIndexPolicyInput{
		LogGroupIdentifier: aws.String("/aws/lambda/other"),
		PolicyDocument:     aws.String(`{"Fields":["@message"]}`),
	})
	require.NoError(t, err)

	out, err := client.DescribeIndexPolicies(ctx, &cwlsdk.DescribeIndexPoliciesInput{
		LogGroupIdentifiers: []string{"/aws/lambda/wanted"},
	})
	require.NoError(t, err)
	require.Len(t, out.IndexPolicies, 1,
		"must return only the requested log group's index policy, not every stored one")
	assert.Equal(t, "/aws/lambda/wanted", aws.ToString(out.IndexPolicies[0].LogGroupIdentifier))

	_, err = client.DescribeIndexPolicies(ctx, &cwlsdk.DescribeIndexPoliciesInput{})
	require.Error(t, err, "LogGroupIdentifiers is required and must not be silently accepted as absent")
}
