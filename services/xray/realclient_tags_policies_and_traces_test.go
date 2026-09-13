package xray_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	xraytypes "github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/xray"
)

// newXRayClientWithHandler stands up the real aws-sdk-go-v2 X-Ray client against
// an httptest server for an already-constructed Handler -- unlike
// newTestXRayClient (wire_field_fixes_test.go), which builds its own
// backend internally, this variant lets a test seed backend state (e.g.
// AddInsightInternal) before the server starts.
func newXRayClientWithHandler(t *testing.T, h *xray.Handler) *xraysdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(config.DefaultRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return xraysdk.NewFromConfig(cfg, func(o *xraysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestRealClient_TagsPoliciesAndTraces drives xray's remaining typed-
// client-blind ops through the real aws-sdk-go-v2 client (gopherstack-n3zi).
func TestRealClient_TagsPoliciesAndTraces(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "tag resource real wire shape",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestXRayClient(t)
				ctx := t.Context()

				grpOut, err := client.CreateGroup(
					ctx,
					&xraysdk.CreateGroupInput{GroupName: aws.String("s27-tag-group")},
				)
				require.NoError(t, err)
				arn := grpOut.Group.GroupARN

				// TagResourceInput.Tags is a JSON array of {Key,Value} objects
				// (types.Tag), not a map -- this is the real wire shape a real
				// client sends.
				_, err = client.TagResource(ctx, &xraysdk.TagResourceInput{
					ResourceARN: arn,
					Tags:        []xraytypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				listOut, err := client.ListTagsForResource(ctx, &xraysdk.ListTagsForResourceInput{ResourceARN: arn})
				require.NoError(t, err)
				require.Len(t, listOut.Tags, 1)
				assert.Equal(t, "env", aws.ToString(listOut.Tags[0].Key))
				assert.Equal(t, "prod", aws.ToString(listOut.Tags[0].Value))

				_, err = client.UntagResource(ctx, &xraysdk.UntagResourceInput{
					ResourceARN: arn, TagKeys: []string{"env"},
				})
				require.NoError(t, err)

				listOut2, err := client.ListTagsForResource(ctx, &xraysdk.ListTagsForResourceInput{ResourceARN: arn})
				require.NoError(t, err)
				assert.Empty(t, listOut2.Tags)
			},
		},
		{
			name: "resource policies",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestXRayClient(t)
				ctx := t.Context()

				_, err := client.PutResourcePolicy(ctx, &xraysdk.PutResourcePolicyInput{
					PolicyName: aws.String("s27-policy"), PolicyDocument: aws.String(`{"Version":"2012-10-17"}`),
				})
				require.NoError(t, err)

				listOut, err := client.ListResourcePolicies(ctx, &xraysdk.ListResourcePoliciesInput{})
				require.NoError(t, err)
				require.Len(t, listOut.ResourcePolicies, 1)
				assert.Equal(t, "s27-policy", aws.ToString(listOut.ResourcePolicies[0].PolicyName))

				_, err = client.DeleteResourcePolicy(ctx, &xraysdk.DeleteResourcePolicyInput{
					PolicyName: aws.String("s27-policy"),
				})
				require.NoError(t, err)

				listOut2, err := client.ListResourcePolicies(ctx, &xraysdk.ListResourcePoliciesInput{})
				require.NoError(t, err)
				assert.Empty(t, listOut2.ResourcePolicies)
			},
		},
		{
			name: "indexing rules",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestXRayClient(t)
				ctx := t.Context()

				getOut, err := client.GetIndexingRules(ctx, &xraysdk.GetIndexingRulesInput{})
				require.NoError(t, err)
				require.Len(t, getOut.IndexingRules, 1)
				assert.Equal(t, "Default", aws.ToString(getOut.IndexingRules[0].Name))

				updOut, err := client.UpdateIndexingRule(ctx, &xraysdk.UpdateIndexingRuleInput{
					Name: aws.String("Default"),
					Rule: &xraytypes.IndexingRuleValueUpdateMemberProbabilistic{
						Value: xraytypes.ProbabilisticRuleValueUpdate{DesiredSamplingPercentage: aws.Float64(42.0)},
					},
				})
				require.NoError(t, err)
				require.NotNil(t, updOut.IndexingRule)

				prob, ok := updOut.IndexingRule.Rule.(*xraytypes.IndexingRuleValueMemberProbabilistic)
				require.True(t, ok)
				assert.InDelta(t, 42.0, aws.ToFloat64(prob.Value.DesiredSamplingPercentage), 0.001)
			},
		},
		{
			name: "insight family",
			run: func(t *testing.T) {
				t.Helper()

				backend := xray.NewInMemoryBackend("123456789012", config.DefaultRegion)
				backend.AddInsightInternal(xray.Insight{
					InsightID: "s27-insight",
					GroupName: "s27-group",
					State:     "ACTIVE",
					Summary:   "elevated fault rate",
					StartTime: time.Now().Add(-time.Hour),
				})
				backend.AddInsightEventInternal(xray.InsightEvent{
					InsightID: "s27-insight",
					Summary:   "fault spike",
					EventTime: time.Now().Add(-30 * time.Minute),
				})

				client := newXRayClientWithHandler(t, xray.NewHandler(backend))
				ctx := t.Context()

				getOut, err := client.GetInsight(ctx, &xraysdk.GetInsightInput{InsightId: aws.String("s27-insight")})
				require.NoError(t, err)
				require.NotNil(t, getOut.Insight)
				assert.Equal(t, "elevated fault rate", aws.ToString(getOut.Insight.Summary))

				eventsOut, err := client.GetInsightEvents(ctx, &xraysdk.GetInsightEventsInput{
					InsightId: aws.String("s27-insight"),
				})
				require.NoError(t, err)
				require.Len(t, eventsOut.InsightEvents, 1)
				assert.Equal(t, "fault spike", aws.ToString(eventsOut.InsightEvents[0].Summary))

				graphOut, err := client.GetInsightImpactGraph(ctx, &xraysdk.GetInsightImpactGraphInput{
					InsightId: aws.String("s27-insight"),
					StartTime: aws.Time(time.Now().Add(-time.Hour)),
					EndTime:   aws.Time(time.Now()),
				})
				require.NoError(t, err)
				assert.Equal(t, "s27-insight", aws.ToString(graphOut.InsightId))
			},
		},
		{
			name: "trace retrieval and telemetry",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestXRayClient(t)
				ctx := t.Context()

				startOut, err := client.StartTraceRetrieval(ctx, &xraysdk.StartTraceRetrievalInput{
					TraceIds:  []string{"1-5f5a1234-000000000000000000000000"},
					StartTime: aws.Time(time.Now().Add(-time.Hour)),
					EndTime:   aws.Time(time.Now()),
				})
				require.NoError(t, err)
				token := startOut.RetrievalToken
				require.NotEmpty(t, aws.ToString(token))

				graphOut, err := client.GetRetrievedTracesGraph(ctx, &xraysdk.GetRetrievedTracesGraphInput{
					RetrievalToken: token,
				})
				require.NoError(t, err)
				assert.NotEmpty(t, graphOut.RetrievalStatus)

				_, err = client.CancelTraceRetrieval(ctx, &xraysdk.CancelTraceRetrievalInput{RetrievalToken: token})
				require.NoError(t, err)

				_, err = client.GetRetrievedTracesGraph(
					ctx,
					&xraysdk.GetRetrievedTracesGraphInput{RetrievalToken: token},
				)
				require.Error(t, err, "a cancelled retrieval token must no longer resolve")

				_, err = client.PutTelemetryRecords(ctx, &xraysdk.PutTelemetryRecordsInput{
					TelemetryRecords: []xraytypes.TelemetryRecord{{
						Timestamp:             aws.Time(time.Now()),
						SegmentsReceivedCount: aws.Int32(10),
						SegmentsSentCount:     aws.Int32(8),
					}},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "sampling statistics and targets",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestXRayClient(t)
				ctx := t.Context()

				_, err := client.CreateSamplingRule(ctx, &xraysdk.CreateSamplingRuleInput{
					SamplingRule: &xraytypes.SamplingRule{
						RuleName:      aws.String("s27-rule"),
						ResourceARN:   aws.String("*"),
						ServiceName:   aws.String("*"),
						ServiceType:   aws.String("*"),
						Host:          aws.String("*"),
						HTTPMethod:    aws.String("*"),
						URLPath:       aws.String("*"),
						FixedRate:     0.05,
						Priority:      aws.Int32(100),
						ReservoirSize: 1,
						Version:       aws.Int32(1),
					},
				})
				require.NoError(t, err)

				targetsOut, err := client.GetSamplingTargets(ctx, &xraysdk.GetSamplingTargetsInput{
					SamplingStatisticsDocuments: []xraytypes.SamplingStatisticsDocument{{
						RuleName:     aws.String("s27-rule"),
						ClientID:     aws.String("s27-client"),
						Timestamp:    aws.Time(time.Now()),
						RequestCount: 100,
						SampledCount: 5,
					}},
				})
				require.NoError(t, err)
				require.Len(t, targetsOut.SamplingTargetDocuments, 1)
				assert.Equal(t, "s27-rule", aws.ToString(targetsOut.SamplingTargetDocuments[0].RuleName))
				assert.Empty(t, targetsOut.UnprocessedStatistics)

				summariesOut, err := client.GetSamplingStatisticSummaries(
					ctx,
					&xraysdk.GetSamplingStatisticSummariesInput{},
				)
				require.NoError(t, err)
				require.Len(t, summariesOut.SamplingStatisticSummaries, 1)
				assert.Equal(t, "s27-rule", aws.ToString(summariesOut.SamplingStatisticSummaries[0].RuleName))

				updOut, err := client.UpdateSamplingRule(ctx, &xraysdk.UpdateSamplingRuleInput{
					SamplingRuleUpdate: &xraytypes.SamplingRuleUpdate{
						RuleName:  aws.String("s27-rule"),
						FixedRate: aws.Float64(0.5),
						Priority:  aws.Int32(50),
					},
				})
				require.NoError(t, err)
				require.NotNil(t, updOut.SamplingRuleRecord)
				require.NotNil(t, updOut.SamplingRuleRecord.SamplingRule)
				assert.InDelta(t, 0.5, updOut.SamplingRuleRecord.SamplingRule.FixedRate, 0.001)
				assert.Equal(t, int32(50), aws.ToInt32(updOut.SamplingRuleRecord.SamplingRule.Priority))
			},
		},
		{
			name: "trace segment destination",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestXRayClient(t)
				ctx := t.Context()

				getOut, err := client.GetTraceSegmentDestination(ctx, &xraysdk.GetTraceSegmentDestinationInput{})
				require.NoError(t, err)
				assert.Equal(t, xraytypes.TraceSegmentDestinationXRay, getOut.Destination)

				updOut, err := client.UpdateTraceSegmentDestination(ctx, &xraysdk.UpdateTraceSegmentDestinationInput{
					Destination: xraytypes.TraceSegmentDestinationCloudWatchLogs,
				})
				require.NoError(t, err)
				assert.Equal(t, xraytypes.TraceSegmentDestinationCloudWatchLogs, updOut.Destination)
			},
		},
		{
			name: "batch get traces and trace graph",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestXRayClient(t)
				ctx := t.Context()

				putOut, err := client.PutTraceSegments(ctx, &xraysdk.PutTraceSegmentsInput{
					TraceSegmentDocuments: []string{
						`{"id":"6226467e3f845502","trace_id":"1-581cf771-a006649127e371903a2de979",` +
							`"name":"s27-service","start_time":1.478293361271E9,"end_time":1.478293361449E9}`,
					},
				})
				require.NoError(t, err)
				assert.Empty(t, putOut.UnprocessedTraceSegments)

				batchOut, err := client.BatchGetTraces(ctx, &xraysdk.BatchGetTracesInput{
					TraceIds: []string{"1-581cf771-a006649127e371903a2de979", "1-581cf771-000000000000000000000001"},
				})
				require.NoError(t, err)
				require.Len(t, batchOut.Traces, 1)
				assert.Equal(t, "1-581cf771-a006649127e371903a2de979", aws.ToString(batchOut.Traces[0].Id))
				require.Len(t, batchOut.UnprocessedTraceIds, 1)

				graphOut, err := client.GetTraceGraph(ctx, &xraysdk.GetTraceGraphInput{
					TraceIds: []string{"1-581cf771-a006649127e371903a2de979"},
				})
				require.NoError(t, err)
				require.Len(t, graphOut.Services, 1)
				assert.Equal(t, "s27-service", aws.ToString(graphOut.Services[0].Name))
			},
		},
		{
			name: "group update",
			run: func(t *testing.T) {
				t.Helper()

				client := newTestXRayClient(t)
				ctx := t.Context()

				grpOut, err := client.CreateGroup(ctx, &xraysdk.CreateGroupInput{
					GroupName:        aws.String("s27-update-group"),
					FilterExpression: aws.String("service(\"a\")"),
				})
				require.NoError(t, err)

				updOut, err := client.UpdateGroup(ctx, &xraysdk.UpdateGroupInput{
					GroupName:        grpOut.Group.GroupName,
					FilterExpression: aws.String("service(\"b\")"),
				})
				require.NoError(t, err)
				require.NotNil(t, updOut.Group)
				assert.Equal(t, "service(\"b\")", aws.ToString(updOut.Group.FilterExpression))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
