package iotanalytics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_TagResource_Returns204 verifies TagResource returns HTTP 204 (not 200).
func TestParity_TagResource_Returns204(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name:       "valid_tag_returns_204",
			tags:       []map[string]string{{"key": "env", "value": "prod"}},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "invalid_key_returns_400",
			tags:       []map[string]string{{"key": "aws:reserved", "value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{"channelName": "tag204ch"})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn, _ := resp["channelArn"].(string)
			require.NotEmpty(t, arn)

			tagRec := doRequest(t, h, http.MethodPost, "/tags?resourceArn="+arn, map[string]any{"tags": tt.tags})
			assert.Equal(t, tt.wantStatus, tagRec.Code)
		})
	}
}

// TestParity_CreateChannel_RetentionPeriodInResponse verifies CreateChannel returns retentionPeriod.
func TestParity_CreateChannel_RetentionPeriodInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantRetention bool
	}{
		{
			name: "with_retention_period",
			body: map[string]any{
				"channelName":     "retch1",
				"retentionPeriod": map[string]any{"numberOfDays": 30},
			},
			wantRetention: true,
		},
		{
			name:          "without_retention_period",
			body:          map[string]any{"channelName": "retch2"},
			wantRetention: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/channels", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, hasRP := resp["retentionPeriod"]
			assert.Equal(t, tt.wantRetention, hasRP)
		})
	}
}

// TestParity_CreateDatastore_RetentionPeriodInResponse verifies CreateDatastore returns retentionPeriod.
func TestParity_CreateDatastore_RetentionPeriodInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantRetention bool
	}{
		{
			name: "with_retention_period",
			body: map[string]any{
				"datastoreName":   "retds1",
				"retentionPeriod": map[string]any{"numberOfDays": 7},
			},
			wantRetention: true,
		},
		{
			name:          "without_retention_period",
			body:          map[string]any{"datastoreName": "retds2"},
			wantRetention: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/datastores", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, hasRP := resp["retentionPeriod"]
			assert.Equal(t, tt.wantRetention, hasRP)
		})
	}
}

// TestParity_DescribeChannel_IncludeStatistics verifies DescribeChannel returns statistics when requested.
func TestParity_DescribeChannel_IncludeStatistics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		includeStats   string
		wantStatistics bool
	}{
		{
			name:           "include_statistics_true",
			includeStats:   "true",
			wantStatistics: true,
		},
		{
			name:           "include_statistics_false",
			includeStats:   "false",
			wantStatistics: false,
		},
		{
			name:           "include_statistics_absent",
			includeStats:   "",
			wantStatistics: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/channels", map[string]string{"channelName": "statsch"})

			path := "/channels/statsch"
			if tt.includeStats != "" {
				path += "?includeStatistics=" + tt.includeStats
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			ch, _ := resp["channel"].(map[string]any)
			_, hasStats := ch["statistics"]
			assert.Equal(t, tt.wantStatistics, hasStats)
		})
	}
}

// TestParity_DescribeDatastore_IncludeStatistics verifies DescribeDatastore returns statistics when requested.
func TestParity_DescribeDatastore_IncludeStatistics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		includeStats   string
		wantStatistics bool
	}{
		{
			name:           "include_statistics_true",
			includeStats:   "true",
			wantStatistics: true,
		},
		{
			name:           "no_statistics",
			includeStats:   "",
			wantStatistics: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/datastores", map[string]string{"datastoreName": "statsds"})

			path := "/datastores/statsds"
			if tt.includeStats != "" {
				path += "?includeStatistics=" + tt.includeStats
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			ds, _ := resp["datastore"].(map[string]any)
			_, hasStats := ds["statistics"]
			assert.Equal(t, tt.wantStatistics, hasStats)
		})
	}
}

// TestParity_ListChannels_ChannelStorage verifies channelStorage appears in ListChannels summaries.
func TestParity_ListChannels_ChannelStorage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"channelName": "storech",
		"channelStorage": map[string]any{
			"serviceManagedS3": map[string]any{},
		},
	}
	doRequest(t, h, http.MethodPost, "/channels", body)

	rec := doRequest(t, h, http.MethodGet, "/channels", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, _ := resp["channelSummaries"].([]any)
	require.Len(t, summaries, 1)
	summary, _ := summaries[0].(map[string]any)
	_, hasStorage := summary["channelStorage"]
	assert.True(t, hasStorage, "channelStorage must appear in ListChannels summary")
}

// TestParity_ListDatastores_DatastoreStorage verifies datastoreStorage appears in ListDatastores summaries.
func TestParity_ListDatastores_DatastoreStorage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"datastoreName": "storeds",
		"datastoreStorage": map[string]any{
			"serviceManagedS3": map[string]any{},
		},
	}
	doRequest(t, h, http.MethodPost, "/datastores", body)

	rec := doRequest(t, h, http.MethodGet, "/datastores", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, _ := resp["datastoreSummaries"].([]any)
	require.Len(t, summaries, 1)
	summary, _ := summaries[0].(map[string]any)
	_, hasStorage := summary["datastoreStorage"]
	assert.True(t, hasStorage, "datastoreStorage must appear in ListDatastores summary")
}

// TestParity_GetDatasetContent_VersionAndEntries verifies GetDatasetContent returns versionId and entries.
func TestParity_GetDatasetContent_VersionAndEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "contds"})
	doRequest(t, h, http.MethodPost, "/datasets/contds/content", nil)

	rec := doRequest(t, h, http.MethodGet, "/datasets/contds/content", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["versionId"], "GetDatasetContent must return versionId")
	_, hasEntries := resp["entries"]
	assert.True(t, hasEntries, "GetDatasetContent must return entries array")
}

// TestParity_DeleteDatasetContent_RequiresVersionId verifies DeleteDatasetContent requires versionId.
func TestParity_DeleteDatasetContent_RequiresVersionId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "no_version_id_returns_400",
			path:       "/datasets/delcontds/content",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "with_version_id_returns_204",
			path:       "", // set dynamically
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "delcontds"})
			createRec := doRequest(t, h, http.MethodPost, "/datasets/delcontds/content", nil)
			require.Equal(t, http.StatusOK, createRec.Code)

			deletePath := tt.path
			if deletePath == "" {
				var cr map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
				vid, _ := cr["versionId"].(string)
				require.NotEmpty(t, vid)
				deletePath = "/datasets/delcontds/content?versionId=" + vid
			}

			rec := doRequest(t, h, http.MethodDelete, deletePath, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_ListDatasetContents_Pagination verifies ListDatasetContents returns a nextToken when more results exist.
func TestParity_ListDatasetContents_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "pageds"})

	for range 3 {
		doRequest(t, h, http.MethodPost, "/datasets/pageds/content", nil)
	}

	rec := doRequest(t, h, http.MethodGet, "/datasets/pageds/contents?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["nextToken"], "nextToken must be set when results are truncated")
	summaries, _ := resp["datasetContentSummaries"].([]any)
	assert.Len(t, summaries, 2)
}

// TestParity_PipelineReprocessingSummary_StartTime verifies StartTime appears in reprocessing summaries.
func TestParity_PipelineReprocessingSummary_StartTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/pipelines", map[string]string{"pipelineName": "rp_start_pl"})

	body := map[string]any{
		"startTime": 1000.0,
		"endTime":   2000.0,
	}
	startRec := doRequest(t, h, http.MethodPost, "/pipelines/rp_start_pl/reprocessing", body)
	require.Equal(t, http.StatusCreated, startRec.Code)

	descRec := doRequest(t, h, http.MethodGet, "/pipelines/rp_start_pl", nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	pl, _ := resp["pipeline"].(map[string]any)
	summaries, _ := pl["reprocessingSummaries"].([]any)
	require.Len(t, summaries, 1)
	summary, _ := summaries[0].(map[string]any)
	assert.NotZero(t, summary["startTime"], "startTime must appear in reprocessing summary")
}

// TestParity_RunPipelineActivity_PayloadLimit verifies RunPipelineActivity rejects more than 10 payloads.
func TestParity_RunPipelineActivity_PayloadLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		count      int
		wantStatus int
	}{
		{
			name:       "10_payloads_ok",
			count:      10,
			wantStatus: http.StatusOK,
		},
		{
			name:       "11_payloads_rejected",
			count:      11,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "1_payload_ok",
			count:      1,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			payloads := make([][]byte, tt.count)

			for i := range payloads {
				payloads[i] = []byte(`{"x":1}`)
			}

			body := map[string]any{
				"pipelineActivity": map[string]any{},
				"payloads":         payloads,
			}
			rec := doRequest(t, h, http.MethodPost, "/pipelineactivities/run", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_BatchPutMessage_NoChannelNameInErrorEntry verifies BatchPutMessage errors omit channelName.
func TestParity_BatchPutMessage_NoChannelNameInErrorEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// intentionally NOT creating the channel → all messages get ResourceNotFoundException
	body := map[string]any{
		"channelName": "ghost_channel",
		"messages": []map[string]any{
			{"messageId": "m1", "payload": []byte(`{"k":"v"}`)},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/messages/batch", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	entries, _ := resp["batchPutMessageErrorEntries"].([]any)
	require.Len(t, entries, 1)
	entry, _ := entries[0].(map[string]any)
	_, hasChannelName := entry["channelName"]
	assert.False(t, hasChannelName, "error entry must not include channelName")
	assert.Equal(t, "m1", entry["messageId"])
}
