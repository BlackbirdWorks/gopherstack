package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCoverage_CampaignCRUD covers GetCampaign, GetCampaigns, UpdateCampaign,
// DeleteCampaign, GetCampaignActivities, GetCampaignVersions, GetCampaignVersion,
// GetCampaignDateRangeKpi.
func TestCoverage_CampaignCRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "campaign-crud-app")

	// Create campaign.
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{"Name": "test-campaign", "SegmentId": "seg-1"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	campaignID, _ := createResp["Id"].(string)
	require.NotEmpty(t, campaignID)

	// GetCampaign.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaigns.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateCampaign.
	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/campaigns/"+campaignID,
		map[string]any{"Name": "updated-campaign"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaignActivities.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID+"/activities", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaignVersions.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaignVersion.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID+"/versions/1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaignDateRangeKpi.
	rec = doPinpointRequest(
		t,
		h,
		http.MethodGet,
		"/v1/apps/"+appID+"/campaigns/"+campaignID+"/kpis/daterange/test-kpi",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteCampaign.
	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/campaigns/"+campaignID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetCampaign not found after delete.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/campaigns/"+campaignID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCoverage_SegmentCRUD covers GetSegment, GetSegments, UpdateSegment, DeleteSegment,
// GetSegmentVersion, GetSegmentVersions, GetSegmentExportJobs, GetSegmentImportJobs.
func TestCoverage_SegmentCRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "segment-crud-app")

	// Create segment.
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "test-segment"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	segmentID, _ := createResp["Id"].(string)
	require.NotEmpty(t, segmentID)

	// GetSegment.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegments.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateSegment.
	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/segments/"+segmentID,
		map[string]any{"Name": "updated-segment"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegmentVersion.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID+"/versions/1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegmentVersions.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID+"/versions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegmentExportJobs.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID+"/jobs/export", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSegmentImportJobs.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segmentID+"/jobs/import", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteSegment.
	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/segments/"+segmentID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCoverage_JourneyCRUD covers GetJourney, ListJourneys, UpdateJourney,
// UpdateJourneyState, DeleteJourney, GetJourneyDateRangeKpi,
// GetJourneyExecutionMetrics, GetJourneyExecutionActivityMetrics,
// GetJourneyRuns, GetJourneyRunExecutionMetrics, GetJourneyRunExecutionActivityMetrics.
func TestCoverage_JourneyCRUD(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-crud-app")

	// Create journey.
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{"Name": "test-journey"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	journeyID, _ := createResp["Id"].(string)
	require.NotEmpty(t, journeyID)

	// GetJourney.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+journeyID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListJourneys.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateJourney.
	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/journeys/"+journeyID,
		map[string]any{"Name": "updated-journey"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateJourneyState.
	rec = doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/journeys/"+journeyID+"/state",
		map[string]any{"State": "ACTIVE"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetJourneyDateRangeKpi.
	rec = doPinpointRequest(
		t,
		h,
		http.MethodGet,
		"/v1/apps/"+appID+"/journeys/"+journeyID+"/kpis/daterange/test-kpi",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetJourneyExecutionMetrics.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+journeyID+"/execution-metrics", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetJourneyExecutionActivityMetrics.
	rec = doPinpointRequest(
		t,
		h,
		http.MethodGet,
		"/v1/apps/"+appID+"/journeys/"+journeyID+"/activities/act-1/execution-metrics",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetJourneyRuns.
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/journeys/"+journeyID+"/runs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetJourneyRunExecutionMetrics.
	rec = doPinpointRequest(
		t,
		h,
		http.MethodGet,
		"/v1/apps/"+appID+"/journeys/"+journeyID+"/runs/run-1/execution-metrics",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetJourneyRunExecutionActivityMetrics.
	rec = doPinpointRequest(
		t,
		h,
		http.MethodGet,
		"/v1/apps/"+appID+"/journeys/"+journeyID+"/runs/run-1/activities/act-1/execution-metrics",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteJourney.
	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/journeys/"+journeyID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCoverage_ChannelOperations covers GetChannel, GetChannels, UpdateChannel, DeleteChannel.
func TestCoverage_ChannelOperations(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "channel-ops-app")

	// GetChannels.
	rec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateChannel (email).
	rec = doPinpointRequest(
		t,
		h,
		http.MethodPut,
		"/v1/apps/"+appID+"/channels/email",
		map[string]any{
			"Enabled":     true,
			"FromAddress": "test@example.com",
			"Identity":    "arn:aws:ses:us-east-1:123:identity/test@example.com",
		},
	)
	assert.True(t, rec.Code == http.StatusOK || rec.Code == http.StatusCreated, "expected 200 or 201, got %d", rec.Code)

	// GetChannel (email).
	rec = doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/email", nil)
	assert.True(
		t,
		rec.Code == http.StatusOK || rec.Code == http.StatusNotFound,
		"expected 200 or 404, got %d",
		rec.Code,
	)

	// DeleteChannel (email).
	rec = doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID+"/channels/email", nil)
	assert.True(
		t,
		rec.Code == http.StatusOK || rec.Code == http.StatusNotFound,
		"expected 200 or 404, got %d",
		rec.Code,
	)
}
