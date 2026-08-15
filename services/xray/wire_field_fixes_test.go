package xray_test

import (
	"fmt"
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

// newTestXRayClient stands up the real aws-sdk-go-v2 X-Ray client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production -- the router-inclusive
// path needed to catch REST-path bugs, not just handler-level ones.
func newTestXRayClient(t *testing.T) *xraysdk.Client {
	t.Helper()

	h := xray.NewHandler(xray.NewInMemoryBackend("123456789012", config.DefaultRegion))

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

// TestGetTraceSummaries_Annotations_RealClient covers a wrapper-key/nested-shape
// bug (gopherstack-6flj): GetTraceSummariesOutput.TraceSummaries[].Annotations was
// emitted as a flat map[string]<scalar>, but the real shape (types.TraceSummary,
// confirmed against xray@v1.39.4 deserializers.go's
// awsRestjson1_deserializeDocumentAnnotations) is
// map[string][]ValueWithServiceIds{AnnotationValue,ServiceIds} -- a JSON ARRAY of
// tagged-union objects per key, not a bare value. A real client's deserializer
// calls value.([]interface{}) on each map value and hard-errors
// ("unexpected JSON type") on anything else, so every real GetTraceSummaries call
// against a trace with at least one annotation previously failed outright, not
// just silently emptied.
//
// This also proves the multi-service merge: two segments in the same trace that
// report the SAME annotation value collapse into one occurrence listing both
// reporting services, matching the real API's per-value ServiceIds semantics.
func TestGetTraceSummaries_Annotations_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestXRayClient(t)
	ctx := t.Context()

	const traceID = "1-5f84c7a1-caea19a8b0d5e5b6b1af1a2e"

	root := fmt.Sprintf(
		`{"trace_id":%q,"id":"root","name":"svc-a","start_time":1700000000,`+
			`"end_time":1700000001,"annotations":{"env":"prod","tier":2,"active":true}}`,
		traceID,
	)
	child := fmt.Sprintf(
		`{"trace_id":%q,"id":"child","parent_id":"root","name":"svc-b","start_time":1700000000.5,`+
			`"end_time":1700000001,"annotations":{"env":"prod","env2":"staging"}}`,
		traceID,
	)

	_, err := client.PutTraceSegments(ctx, &xraysdk.PutTraceSegmentsInput{
		TraceSegmentDocuments: []string{root, child},
	})
	require.NoError(t, err)

	out, err := client.GetTraceSummaries(ctx, &xraysdk.GetTraceSummariesInput{
		StartTime: aws.Time(time.Unix(1699999999, 0)),
		EndTime:   aws.Time(time.Unix(1700000100, 0)),
	})
	require.NoError(t, err, "a real client must be able to decode Annotations without a deserialization error")
	require.Len(t, out.TraceSummaries, 1)

	ann := out.TraceSummaries[0].Annotations

	// "env" was reported with the SAME value ("prod") by both segments: one
	// occurrence, two reporting services.
	require.Contains(t, ann, "env")
	require.Len(t, ann["env"], 1, "same value from two services must merge into one occurrence")

	envOcc := ann["env"][0]
	envStr, ok := envOcc.AnnotationValue.(*xraytypes.AnnotationValueMemberStringValue)
	require.True(t, ok, "env AnnotationValue must be the StringValue union member")
	assert.Equal(t, "prod", envStr.Value)

	gotServices := make([]string, 0, len(envOcc.ServiceIds))
	for _, s := range envOcc.ServiceIds {
		gotServices = append(gotServices, aws.ToString(s.Name))
	}

	assert.ElementsMatch(t, []string{"svc-a", "svc-b"}, gotServices)

	// "tier" is a number-typed annotation, reported only by svc-a.
	require.Contains(t, ann, "tier")
	require.Len(t, ann["tier"], 1)

	tierNum, ok := ann["tier"][0].AnnotationValue.(*xraytypes.AnnotationValueMemberNumberValue)
	require.True(t, ok, "tier AnnotationValue must be the NumberValue union member")
	assert.InEpsilon(t, 2.0, tierNum.Value, 0.0001)

	// "active" is a boolean-typed annotation, reported only by svc-a.
	require.Contains(t, ann, "active")
	require.Len(t, ann["active"], 1)

	activeBool, ok := ann["active"][0].AnnotationValue.(*xraytypes.AnnotationValueMemberBooleanValue)
	require.True(t, ok, "active AnnotationValue must be the BooleanValue union member")
	assert.True(t, activeBool.Value)

	// "env2" was only reported by svc-b.
	require.Contains(t, ann, "env2")
	require.Len(t, ann["env2"], 1)
	assert.Equal(t, []string{"svc-b"}, []string{aws.ToString(ann["env2"][0].ServiceIds[0].Name)})
}

// TestGetInsightSummaries_GroupAndTimeFiltering covers a discarded-filter bug
// (gopherstack-6flj): GetInsightSummariesInput's GroupARN/GroupName and
// StartTime/EndTime (all required or one-of-required per the real
// api_op_GetInsightSummaries.go doc comments) were parsed by the handler but
// never passed to the backend at all -- every group and every time window
// returned the exact same unfiltered set of insights. A real client scoping
// its query to one group, or to a time window that excludes an insight's
// active period, silently got back insights it never asked for.
func TestGetInsightSummaries_GroupAndTimeFiltering(t *testing.T) {
	t.Parallel()

	client := newTestXRayClient(t)
	ctx := t.Context()

	// Trigger real fault-rate insight detection: this backend's detector
	// (detectInsights) unconditionally labels every insight GroupName="default"
	// (see GetInsightSummaries' own doc comment on the structural limitation
	// this leaves -- disclosed in PARITY.md).
	const traceID = "1-insight-trace-0000000000001"
	const svcName = "flaky-service"

	segs := make([]string, 0, 20)

	for range 10 {
		segs = append(segs, fmt.Sprintf(
			`{"trace_id":%q,"id":%q,"name":%q,"start_time":%d,"end_time":%d,"fault":true}`,
			traceID, fmt.Sprintf("seg-fault-%d", len(segs)), svcName, time.Now().Unix(), time.Now().Unix()+1,
		))
	}

	for range 10 {
		segs = append(segs, fmt.Sprintf(
			`{"trace_id":%q,"id":%q,"name":%q,"start_time":%d,"end_time":%d,"fault":false}`,
			traceID, fmt.Sprintf("seg-ok-%d", len(segs)), svcName, time.Now().Unix(), time.Now().Unix()+1,
		))
	}

	_, err := client.PutTraceSegments(ctx, &xraysdk.PutTraceSegmentsInput{TraceSegmentDocuments: segs})
	require.NoError(t, err)

	now := time.Now()

	// Missing both GroupARN and GroupName: real GetInsightSummaries declares
	// InvalidRequestException as its only non-throttle error.
	_, err = client.GetInsightSummaries(ctx, &xraysdk.GetInsightSummariesInput{
		StartTime: aws.Time(now.Add(-time.Hour)),
		EndTime:   aws.Time(now.Add(time.Hour)),
	})
	require.Error(t, err, "GroupName/GroupARN must be required, matching the real API")

	// Matching group, window bracketing now: the detected insight comes back.
	out, err := client.GetInsightSummaries(ctx, &xraysdk.GetInsightSummariesInput{
		GroupName: aws.String("default"),
		StartTime: aws.Time(now.Add(-time.Hour)),
		EndTime:   aws.Time(now.Add(time.Hour)),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.InsightSummaries, "the detected insight belongs to group \"default\"")

	// Different group, same window: must NOT see the same insight -- this is
	// the fix. Pre-fix, every group returned the same set.
	out, err = client.GetInsightSummaries(ctx, &xraysdk.GetInsightSummariesInput{
		GroupName: aws.String("some-other-group"),
		StartTime: aws.Time(now.Add(-time.Hour)),
		EndTime:   aws.Time(now.Add(time.Hour)),
	})
	require.NoError(t, err)
	assert.Empty(t, out.InsightSummaries, "group scoping must exclude insights from a different group")

	// Matching group, window entirely in the past: must NOT see the insight --
	// this is the fix. Pre-fix, StartTime/EndTime were parsed and discarded.
	out, err = client.GetInsightSummaries(ctx, &xraysdk.GetInsightSummariesInput{
		GroupName: aws.String("default"),
		StartTime: aws.Time(now.Add(-48 * time.Hour)),
		EndTime:   aws.Time(now.Add(-24 * time.Hour)),
	})
	require.NoError(t, err)
	assert.Empty(t, out.InsightSummaries, "a non-overlapping time window must exclude the insight")
}
