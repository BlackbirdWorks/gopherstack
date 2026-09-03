package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplicationDateRangeKpi_RequiredTimeRange covers gopherstack-6flj:
// ApplicationDateRangeKpiResponse/CampaignDateRangeKpiResponse/
// JourneyDateRangeKpiResponse all mark StartTime/EndTime "This member is
// required." (pinpoint@v1.42.4 types/types.go) even though the request's
// start-time/end-time query params are optional. A prior version never
// emitted either field on any of the three ops -- a real client's typed
// *time.Time fields stayed nil regardless of what was requested.
func TestApplicationDateRangeKpi_RequiredTimeRange(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("kpi-time-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)

	out, err := client.GetApplicationDateRangeKpi(t.Context(), &pinpointsdk.GetApplicationDateRangeKpiInput{
		ApplicationId: aws.String(appID),
		KpiName:       aws.String("successful-endpoint-deliveries"),
		StartTime:     aws.Time(start),
		EndTime:       aws.Time(end),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ApplicationDateRangeKpiResponse.StartTime)
	require.NotNil(t, out.ApplicationDateRangeKpiResponse.EndTime)
	assert.True(t, start.Equal(*out.ApplicationDateRangeKpiResponse.StartTime))
	assert.True(t, end.Equal(*out.ApplicationDateRangeKpiResponse.EndTime))

	// Omitting the query params entirely (journey variant, a distinct real
	// type from the application variant above) must still populate both
	// required fields via the default range, never leave them nil.
	journeyOut, err := client.CreateJourney(t.Context(), &pinpointsdk.CreateJourneyInput{
		ApplicationId:       aws.String(appID),
		WriteJourneyRequest: &types.WriteJourneyRequest{Name: aws.String("kpi-time-journey")},
	})
	require.NoError(t, err)

	jkOut, err := client.GetJourneyDateRangeKpi(t.Context(), &pinpointsdk.GetJourneyDateRangeKpiInput{
		ApplicationId: aws.String(appID),
		JourneyId:     journeyOut.JourneyResponse.Id,
		KpiName:       aws.String("x"),
	})
	require.NoError(t, err)
	require.NotNil(t, jkOut.JourneyDateRangeKpiResponse.StartTime)
	require.NotNil(t, jkOut.JourneyDateRangeKpiResponse.EndTime)
}

// TestJourneyExecutionMetrics_LastEvaluatedTime covers gopherstack-6flj:
// JourneyExecutionMetricsResponse/JourneyExecutionActivityMetricsResponse/
// JourneyRunExecutionMetricsResponse/JourneyRunExecutionActivityMetricsResponse
// all mark LastEvaluatedTime "This member is required."
// (pinpoint@v1.42.4 types/types.go); a prior version never emitted it on any
// of the four ops.
func TestJourneyExecutionMetrics_LastEvaluatedTime(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("journey-metrics-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	journeyOut, err := client.CreateJourney(t.Context(), &pinpointsdk.CreateJourneyInput{
		ApplicationId:       aws.String(appID),
		WriteJourneyRequest: &types.WriteJourneyRequest{Name: aws.String("metrics-journey")},
	})
	require.NoError(t, err)
	journeyID := journeyOut.JourneyResponse.Id

	metricsOut, err := client.GetJourneyExecutionMetrics(t.Context(), &pinpointsdk.GetJourneyExecutionMetricsInput{
		ApplicationId: aws.String(appID),
		JourneyId:     journeyID,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(metricsOut.JourneyExecutionMetricsResponse.LastEvaluatedTime))

	activityMetricsOut, err := client.GetJourneyExecutionActivityMetrics(
		t.Context(), &pinpointsdk.GetJourneyExecutionActivityMetricsInput{
			ApplicationId:     aws.String(appID),
			JourneyId:         journeyID,
			JourneyActivityId: aws.String("act-1"),
		})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(activityMetricsOut.JourneyExecutionActivityMetricsResponse.LastEvaluatedTime))
}

// TestJourneyRuns_CreationAndUpdateTime covers gopherstack-6flj:
// JourneyRunResponse marks CreationTime/LastUpdateTime "This member is
// required." (pinpoint@v1.42.4 types/types.go); a prior version never
// emitted either, so a real client's GetJourneyRuns items had both fields
// nil despite Status/RunId being present.
func TestJourneyRuns_CreationAndUpdateTime(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("journey-runs-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	journeyOut, err := client.CreateJourney(t.Context(), &pinpointsdk.CreateJourneyInput{
		ApplicationId:       aws.String(appID),
		WriteJourneyRequest: &types.WriteJourneyRequest{Name: aws.String("runs-journey")},
	})
	require.NoError(t, err)
	journeyID := journeyOut.JourneyResponse.Id

	_, err = client.UpdateJourneyState(t.Context(), &pinpointsdk.UpdateJourneyStateInput{
		ApplicationId:       aws.String(appID),
		JourneyId:           journeyID,
		JourneyStateRequest: &types.JourneyStateRequest{State: types.StateActive},
	})
	require.NoError(t, err)

	runsOut, err := client.GetJourneyRuns(t.Context(), &pinpointsdk.GetJourneyRunsInput{
		ApplicationId: aws.String(appID),
		JourneyId:     journeyID,
	})
	require.NoError(t, err)
	require.Len(t, runsOut.JourneyRunsResponse.Item, 1)
	run := runsOut.JourneyRunsResponse.Item[0]
	assert.NotEmpty(t, aws.ToString(run.CreationTime))
	assert.NotEmpty(t, aws.ToString(run.LastUpdateTime))
}

// TestApplicationSettings_JourneyLimits covers gopherstack-6flj:
// ApplicationSettingsResource.JourneyLimits is a real member
// (pinpoint@v1.42.4 types/types.go) that a prior version never emitted at
// all, even though CampaignHook/Limits/QuietTime -- the type's other
// document-shaped members -- were already round-tripped correctly.
func TestApplicationSettings_JourneyLimits(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("journey-limits-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	updateOut, err := client.UpdateApplicationSettings(t.Context(), &pinpointsdk.UpdateApplicationSettingsInput{
		ApplicationId: aws.String(appID),
		WriteApplicationSettingsRequest: &types.WriteApplicationSettingsRequest{
			JourneyLimits: &types.ApplicationSettingsJourneyLimits{
				DailyCap: aws.Int32(42),
				TotalCap: aws.Int32(100),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.ApplicationSettingsResource.JourneyLimits)
	assert.Equal(t, int32(42), aws.ToInt32(updateOut.ApplicationSettingsResource.JourneyLimits.DailyCap))
	assert.Equal(t, int32(100), aws.ToInt32(updateOut.ApplicationSettingsResource.JourneyLimits.TotalCap))

	getOut, err := client.GetApplicationSettings(t.Context(), &pinpointsdk.GetApplicationSettingsInput{
		ApplicationId: aws.String(appID),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.ApplicationSettingsResource.JourneyLimits)
	assert.Equal(t, int32(42), aws.ToInt32(getOut.ApplicationSettingsResource.JourneyLimits.DailyCap))
	assert.Equal(t, int32(100), aws.ToInt32(getOut.ApplicationSettingsResource.JourneyLimits.TotalCap))
}

// TestCreateSegment_RawImportDefinitionFieldIgnored covers
// gopherstack-wksweep-pp-1: the real WriteSegmentRequest (pinpoint@v1.42.4
// types/types.go:7240, used by both CreateSegment and UpdateSegment) has no
// ImportDefinition member -- it's only ever derived from CreateImportJob
// (see TestSegment_ImportType in segments_test.go for that real path). A
// typed client can't even construct a WriteSegmentRequest with the field, so
// this is the raw-body fail-before/pass-after proof: before the fix,
// gopherstack's createSegmentRequest read an "ImportDefinition" key no real
// client can send. Sending it directly must have no effect.
func TestCreateSegment_RawImportDefinitionFieldIgnored(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "wire-fix-import-def-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments", map[string]any{
		"Name": "wire-fix-import-def-seg",
		"ImportDefinition": map[string]any{
			"S3Url":   "s3://bucket/should-not-apply.csv",
			"RoleArn": "arn:aws:iam::123456789012:role/R",
			"Format":  "CSV",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "DIMENSIONAL", out["SegmentType"],
		"CreateSegment must not accept ImportDefinition; the real WriteSegmentRequest has no such member")
	assert.Nil(t, out["ImportDefinition"])
}

// TestCreateJourney_RawTagsFieldIgnored covers gopherstack-wksweep-pp-2: the
// real WriteJourneyRequest and JourneyResponse (pinpoint@v1.42.4
// types/types.go:7118, 4227) have no Tags member at all -- journeys are
// taggable only through the generic TagResource/ListTagsForResource ARN-based
// API, not via CreateJourney. A typed client can't construct a
// WriteJourneyRequest with Tags, so this is the raw-body fail-before/
// pass-after proof: before the fix, gopherstack's createJourneyRequest read
// a "tags" key no real client can send, and echoed it back in
// journeyResponse too. Sending it directly must have no effect on either
// side, and the real TagResource path must still work.
func TestCreateJourney_RawTagsFieldIgnored(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "wire-fix-journey-tags-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys", map[string]any{
		"Name": "wire-fix-journey-tags",
		"tags": map[string]string{"env": "shouldNotApply"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Nil(t, out["tags"],
		"CreateJourney must not accept or echo tags; the real WriteJourneyRequest/JourneyResponse have no such member")

	journeyARN := out["Arn"].(string)

	client := newTestPinpointClient(t, h)
	_, err := client.TagResource(t.Context(), &pinpointsdk.TagResourceInput{
		ResourceArn: aws.String(journeyARN),
		TagsModel:   &types.TagsModel{Tags: map[string]string{"env": "prod"}},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(t.Context(), &pinpointsdk.ListTagsForResourceInput{
		ResourceArn: aws.String(journeyARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", tagsOut.TagsModel.Tags["env"])
}
