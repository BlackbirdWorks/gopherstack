package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ──────────────────────────────────────────────────
// Finding 1: GetApplicationSettings LastModifiedDate not stored
//
// Before fix: handler called nowRFC3339() on every GET, so LastModifiedDate
// changed with each request. After fix: LastModifiedDate is set when settings
// are updated and returned consistently on subsequent GETs.
// ──────────────────────────────────────────────────

func TestBatch2_AppSettings_LastModifiedDate_ConsistentAfterUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "cloudwatch_enabled",
			body: map[string]any{"CloudWatchMetricsEnabled": true},
		},
		{
			name: "quiet_time_set",
			body: map[string]any{
				"QuietTime": map[string]any{"Start": "22:00", "End": "06:00"},
			},
		},
		{
			name: "limits_set",
			body: map[string]any{
				"Limits": map[string]any{"Daily": 500},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "settings-lmd-app")

			putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings", tc.body)
			require.Equal(t, http.StatusOK, putRec.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
			putDate, _ := putResp["LastModifiedDate"].(string)
			require.NotEmpty(t, putDate, "PUT must return non-empty LastModifiedDate")

			getRec1 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec1.Code)
			var getResp1 map[string]any
			require.NoError(t, json.Unmarshal(getRec1.Body.Bytes(), &getResp1))

			getRec2 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
			require.Equal(t, http.StatusOK, getRec2.Code)
			var getResp2 map[string]any
			require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getResp2))

			date1, _ := getResp1["LastModifiedDate"].(string)
			date2, _ := getResp2["LastModifiedDate"].(string)

			assert.Equal(t, putDate, date1, "GET must return same LastModifiedDate as PUT")
			assert.Equal(t, date1, date2, "consecutive GETs must return identical LastModifiedDate")
		})
	}
}

func TestBatch2_AppSettings_LastModifiedDate_UpdatesOnSecondPut(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "settings-lmd-update-app")

	put1 := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{"CloudWatchMetricsEnabled": true})
	require.Equal(t, http.StatusOK, put1.Code)
	var r1 map[string]any
	require.NoError(t, json.Unmarshal(put1.Body.Bytes(), &r1))
	date1, _ := r1["LastModifiedDate"].(string)
	require.NotEmpty(t, date1)

	// Second PUT with different settings.
	put2 := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/settings",
		map[string]any{"EventTaggingEnabled": true})
	require.Equal(t, http.StatusOK, put2.Code)
	var r2 map[string]any
	require.NoError(t, json.Unmarshal(put2.Body.Bytes(), &r2))
	date2, _ := r2["LastModifiedDate"].(string)
	require.NotEmpty(t, date2)

	// GET must reflect the second PUT's date, not the first.
	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/settings", nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	getDate, _ := getResp["LastModifiedDate"].(string)

	assert.Equal(t, date2, getDate, "GET must return the most recent PUT's LastModifiedDate")
}

// ──────────────────────────────────────────────────
// Finding 2: UpdateEndpoint returned full endpoint instead of MessageBody
//
// Before fix: handler returned toEndpointResponse(e) (HTTP 202 + endpoint JSON).
// AWS returns HTTP 202 + {"Message":"Accepted"} (MessageBody).
// After fix: handler returns messageBodyResponse{Message: "Accepted"}.
// ──────────────────────────────────────────────────

func TestBatch2_UpdateEndpoint_ReturnsMessageBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "email_endpoint",
			body: map[string]any{"ChannelType": "EMAIL", "Address": "user@example.com"},
		},
		{
			name: "sms_endpoint",
			body: map[string]any{"ChannelType": "SMS", "Address": "+15555550100"},
		},
		{
			name: "endpoint_with_user",
			body: map[string]any{
				"ChannelType": "EMAIL",
				"Address":     "b@example.com",
				"User":        map[string]any{"UserId": "u-1"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "ep-msgbody-app")

			rec := doPinpointRequest(t, h, http.MethodPut,
				"/v1/apps/"+appID+"/endpoints/ep-1", tc.body)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			msg, _ := resp["Message"].(string)
			assert.Equal(t, "Accepted", msg,
				"UpdateEndpoint must return {Message:Accepted} not the endpoint body")

			// Must NOT contain endpoint-specific fields.
			assert.NotContains(t, resp, "ApplicationId",
				"UpdateEndpoint response must not contain ApplicationId (endpoint field)")
			assert.NotContains(t, resp, "ChannelType",
				"UpdateEndpoint response must not contain ChannelType (endpoint field)")
		})
	}
}

// ──────────────────────────────────────────────────
// Finding 3: GetSegmentImportJobs ignored segmentID
//
// Before fix: backend used _ for segmentID and returned ALL import jobs for
// the app. AWS returns only the import job(s) that created the specified segment.
// After fix: backend stores the segment ID on the import job and filters by it.
// ──────────────────────────────────────────────────

func TestBatch2_GetSegmentImportJobs_FiltersBySegment(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-import-filter-app")

	// Create import job — backend creates a new IMPORT segment.
	importRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123456789012:role/ImportRole",
			"S3Url":       "s3://bucket/data.csv",
			"Format":      "CSV",
			"SegmentName": "my-import-segment",
		})
	require.Equal(t, http.StatusCreated, importRec.Code)

	// Find the segment that the import job created.
	segsRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	require.Equal(t, http.StatusOK, segsRec.Code)
	var segsResp map[string]any
	require.NoError(t, json.Unmarshal(segsRec.Body.Bytes(), &segsResp))

	items, _ := segsResp["Item"].([]any)
	require.Len(t, items, 1, "import job must materialise exactly one segment")
	importSegmentID := items[0].(map[string]any)["Id"].(string)

	// GetSegmentImportJobs for the import segment → must return 1 job.
	jobsRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments/"+importSegmentID+"/jobs/import", nil)
	require.Equal(t, http.StatusOK, jobsRec.Code)

	var jobsResp map[string]any
	require.NoError(t, json.Unmarshal(jobsRec.Body.Bytes(), &jobsResp))

	jobs, _ := jobsResp["Item"].([]any)
	assert.Len(t, jobs, 1, "GetSegmentImportJobs must return the job that created the segment")
}

func TestBatch2_GetSegmentImportJobs_OtherSegmentReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-import-empty-app")

	// Create import job (creates its own IMPORT segment).
	doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/r",
			"S3Url":   "s3://b/f.csv",
			"Format":  "CSV",
		})

	// Create a second, unrelated DIMENSIONAL segment.
	segRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "unrelated-segment"})
	require.Equal(t, http.StatusCreated, segRec.Code)
	var segResp map[string]any
	require.NoError(t, json.Unmarshal(segRec.Body.Bytes(), &segResp))
	unrelatedSegID := segResp["Id"].(string)

	// GetSegmentImportJobs for the unrelated segment → must return empty list.
	jobsRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments/"+unrelatedSegID+"/jobs/import", nil)
	require.Equal(t, http.StatusOK, jobsRec.Code)

	var jobsResp map[string]any
	require.NoError(t, json.Unmarshal(jobsRec.Body.Bytes(), &jobsResp))

	jobs, _ := jobsResp["Item"].([]any)
	assert.Empty(t, jobs, "GetSegmentImportJobs must return empty for segments not created by an import job")
}

func TestBatch2_GetSegmentImportJobs_MultipleJobs_IsolatedBySegment(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-import-multi-app")

	// Create two import jobs, each creating its own segment.
	doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123:role/r",
			"S3Url":       "s3://b/a.csv",
			"Format":      "CSV",
			"SegmentName": "seg-a",
		})
	doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123:role/r",
			"S3Url":       "s3://b/b.csv",
			"Format":      "CSV",
			"SegmentName": "seg-b",
		})

	segsRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	require.Equal(t, http.StatusOK, segsRec.Code)
	var segsResp map[string]any
	require.NoError(t, json.Unmarshal(segsRec.Body.Bytes(), &segsResp))
	items, _ := segsResp["Item"].([]any)
	require.Len(t, items, 2)

	// Each segment's import job endpoint must return exactly 1 job (not 2).
	for _, item := range items {
		seg := item.(map[string]any)
		segID := seg["Id"].(string)

		jobsRec := doPinpointRequest(t, h, http.MethodGet,
			"/v1/apps/"+appID+"/segments/"+segID+"/jobs/import", nil)
		require.Equal(t, http.StatusOK, jobsRec.Code)

		var jobsResp map[string]any
		require.NoError(t, json.Unmarshal(jobsRec.Body.Bytes(), &jobsResp))
		jobs, _ := jobsResp["Item"].([]any)
		assert.Len(t, jobs, 1,
			"each segment's GetSegmentImportJobs must return only its own job, segment %s", segID)
	}
}
