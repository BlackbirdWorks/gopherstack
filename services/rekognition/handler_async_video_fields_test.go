package rekognition_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// JobTag and Video are real GetXxxOutput members (e.g.
// api_op_GetLabelDetection.go's JobId/JobTag/Video fields) that every
// Start* handler previously parsed and then discarded. Table covers every
// async video job family sharing the getJobBase helper.
// ---------------------------------------------------------------------------

func TestAsyncVideoJob_JobTagAndVideoRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startBody   map[string]any
		startAction string
		getAction   string
		resultKey   string
		name        string
	}{
		{
			name:        "label detection",
			startAction: "StartLabelDetection",
			getAction:   "GetLabelDetection",
			resultKey:   "Labels",
		},
		{
			name:        "content moderation",
			startAction: "StartContentModeration",
			getAction:   "GetContentModeration",
			resultKey:   "ModerationLabels",
		},
		{
			name:        "celebrity recognition",
			startAction: "StartCelebrityRecognition",
			getAction:   "GetCelebrityRecognition",
			resultKey:   "Celebrities",
		},
		{
			name:        "face detection",
			startAction: "StartFaceDetection",
			getAction:   "GetFaceDetection",
			resultKey:   "Faces",
		},
		{
			name:        "text detection",
			startAction: "StartTextDetection",
			getAction:   "GetTextDetection",
			resultKey:   "TextDetections",
		},
		{
			name:        "person tracking",
			startAction: "StartPersonTracking",
			getAction:   "GetPersonTracking",
			resultKey:   "Persons",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, tc.startAction, map[string]any{
				"Video": map[string]any{
					"S3Object": map[string]any{"Bucket": "my-video-bucket", "Name": "clip.mp4", "Version": "v3"},
				},
				"JobTag": "my-job-tag",
			})
			require.Equal(t, http.StatusOK, rec.Code, tc.startAction)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			jobID, ok := startResp["JobId"].(string)
			require.True(t, ok)

			rec = doRequest(t, h, tc.getAction, map[string]any{"JobId": jobID})
			require.Equal(t, http.StatusOK, rec.Code, tc.getAction)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, "my-job-tag", getResp["JobTag"])

			video, _ := getResp["Video"].(map[string]any)
			require.NotNil(t, video)
			s3Object, _ := video["S3Object"].(map[string]any)
			require.NotNil(t, s3Object)
			assert.Equal(t, "my-video-bucket", s3Object["Bucket"])
			assert.Equal(t, "clip.mp4", s3Object["Name"])
			assert.Equal(t, "v3", s3Object["Version"])
		})
	}
}

func TestAsyncVideoJob_NoVideoOmitsVideoField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartLabelDetection", map[string]any{"Video": map[string]any{}})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID, ok := startResp["JobId"].(string)
	require.True(t, ok)

	rec = doRequest(t, h, "GetLabelDetection", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.NotContains(t, getResp, "Video")
	assert.NotContains(t, getResp, "JobTag")
}

// ---------------------------------------------------------------------------
// GetSegmentDetection.SelectedSegmentTypes echoes the SegmentTypes requested
// via StartSegmentDetection (ModelVersion intentionally omitted -- no
// legitimate source for that AWS-internal string, see handler_media_analysis.go).
// ---------------------------------------------------------------------------

func TestGetSegmentDetection_SelectedSegmentTypesEchoesRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartSegmentDetection", map[string]any{
		"Video":        map[string]any{},
		"SegmentTypes": []string{"SHOT", "TECHNICAL_CUE"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID, ok := startResp["JobId"].(string)
	require.True(t, ok)

	rec = doRequest(t, h, "GetSegmentDetection", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	selected, ok := getResp["SelectedSegmentTypes"].([]any)
	require.True(t, ok)
	require.Len(t, selected, 2)

	assert.Equal(t, "SHOT", selected[0].(map[string]any)["Type"])
	assert.Equal(t, "TECHNICAL_CUE", selected[1].(map[string]any)["Type"])
	assert.NotContains(t, selected[0].(map[string]any), "ModelVersion")
}

// ---------------------------------------------------------------------------
// GetRequestMetadata (GetLabelDetection/GetContentModeration) echoes the
// current call's SortBy/AggregateBy, applying the documented default when
// omitted.
// ---------------------------------------------------------------------------

func TestGetLabelDetection_GetRequestMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reqExtra        map[string]any
		wantSortBy      string
		wantAggregateBy string
		name            string
	}{
		{
			name:            "defaults when omitted",
			reqExtra:        map[string]any{},
			wantSortBy:      "TIMESTAMP",
			wantAggregateBy: "",
		},
		{
			name:            "echoes explicit values",
			reqExtra:        map[string]any{"SortBy": "NAME", "AggregateBy": "SEGMENTS"},
			wantSortBy:      "NAME",
			wantAggregateBy: "SEGMENTS",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "StartLabelDetection", map[string]any{"Video": map[string]any{}})
			require.Equal(t, http.StatusOK, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			jobID, ok := startResp["JobId"].(string)
			require.True(t, ok)

			getBody := map[string]any{"JobId": jobID}
			maps.Copy(getBody, tc.reqExtra)

			rec = doRequest(t, h, "GetLabelDetection", getBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			metadata, _ := getResp["GetRequestMetadata"].(map[string]any)
			require.NotNil(t, metadata)
			assert.Equal(t, tc.wantSortBy, metadata["SortBy"])

			if tc.wantAggregateBy == "" {
				assert.NotContains(t, metadata, "AggregateBy")
			} else {
				assert.Equal(t, tc.wantAggregateBy, metadata["AggregateBy"])
			}
		})
	}
}

func TestGetContentModeration_GetRequestMetadataDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartContentModeration", map[string]any{"Video": map[string]any{}})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID, ok := startResp["JobId"].(string)
	require.True(t, ok)

	rec = doRequest(t, h, "GetContentModeration", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	metadata, _ := getResp["GetRequestMetadata"].(map[string]any)
	require.NotNil(t, metadata)
	assert.Equal(t, "TIMESTAMP", metadata["SortBy"])
	assert.Equal(t, "TIMESTAMPS", metadata["AggregateBy"])
}
