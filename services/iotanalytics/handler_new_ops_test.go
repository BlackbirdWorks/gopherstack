package iotanalytics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// TestHandler_Datastores covers datastore CRUD operations.
func TestHandler_Datastores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		datastoreName string
		op            string
		wantStatus    int
	}{
		{
			name:          "create_success",
			datastoreName: "my_ds",
			op:            "create",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "list",
			datastoreName: "list_ds",
			op:            "list",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "describe_success",
			datastoreName: "desc_ds",
			op:            "describe",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "update_success",
			datastoreName: "update_ds",
			op:            "update",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "delete_success",
			datastoreName: "delete_ds",
			op:            "delete",
			wantStatus:    http.StatusNoContent,
		},
		{
			name:          "describe_not_found",
			datastoreName: "no_such_ds",
			op:            "describe_only",
			wantStatus:    http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.op {
			case "create":
				rec := doRequest(
					t,
					h,
					http.MethodPost,
					"/datastores",
					map[string]string{"datastoreName": tt.datastoreName},
				)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "list":
				rec := doRequest(
					t,
					h,
					http.MethodPost,
					"/datastores",
					map[string]string{"datastoreName": tt.datastoreName},
				)
				require.Equal(t, http.StatusOK, rec.Code)
				listRec := doRequest(t, h, http.MethodGet, "/datastores", nil)
				assert.Equal(t, tt.wantStatus, listRec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				summaries, ok := resp["datastoreSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, 1)

			case "describe":
				doRequest(t, h, http.MethodPost, "/datastores", map[string]string{"datastoreName": tt.datastoreName})
				rec := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "update":
				doRequest(t, h, http.MethodPost, "/datastores", map[string]string{"datastoreName": tt.datastoreName})
				rec := doRequest(t, h, http.MethodPut, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "delete":
				doRequest(t, h, http.MethodPost, "/datastores", map[string]string{"datastoreName": tt.datastoreName})
				rec := doRequest(t, h, http.MethodDelete, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "describe_only":
				rec := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_Datasets covers dataset CRUD operations.
func TestHandler_Datasets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		op          string
		wantStatus  int
	}{
		{
			name:        "list",
			datasetName: "list_dataset",
			op:          "list",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "describe_success",
			datasetName: "desc_dataset",
			op:          "describe",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "update_success",
			datasetName: "update_dataset",
			op:          "update",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "delete_success",
			datasetName: "delete_dataset",
			op:          "delete",
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "describe_not_found",
			datasetName: "no_such_dataset",
			op:          "describe_only",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.op {
			case "list":
				doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				listRec := doRequest(t, h, http.MethodGet, "/datasets", nil)
				assert.Equal(t, tt.wantStatus, listRec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				summaries, ok := resp["datasetSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, 1)

			case "describe":
				doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				rec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "update":
				doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				rec := doRequest(t, h, http.MethodPut, "/datasets/"+tt.datasetName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "delete":
				doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				rec := doRequest(t, h, http.MethodDelete, "/datasets/"+tt.datasetName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "describe_only":
				rec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_Pipelines covers pipeline CRUD operations.
func TestHandler_Pipelines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
		op           string
		wantStatus   int
	}{
		{
			name:         "list",
			pipelineName: "list_pipeline",
			op:           "list",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "update_success",
			pipelineName: "update_pipeline",
			op:           "update",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "delete_success",
			pipelineName: "delete_pipeline",
			op:           "delete",
			wantStatus:   http.StatusNoContent,
		},
		{
			name:         "update_not_found",
			pipelineName: "no_such_pipeline",
			op:           "update_only",
			wantStatus:   http.StatusNotFound,
		},
		{
			name:         "delete_not_found",
			pipelineName: "no_such_pipeline",
			op:           "delete_only",
			wantStatus:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.op {
			case "list":
				doRequest(t, h, http.MethodPost, "/pipelines", map[string]string{"pipelineName": tt.pipelineName})
				listRec := doRequest(t, h, http.MethodGet, "/pipelines", nil)
				assert.Equal(t, tt.wantStatus, listRec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				summaries, ok := resp["pipelineSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, 1)

			case "update":
				doRequest(t, h, http.MethodPost, "/pipelines", map[string]string{"pipelineName": tt.pipelineName})
				rec := doRequest(t, h, http.MethodPut, "/pipelines/"+tt.pipelineName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "delete":
				doRequest(t, h, http.MethodPost, "/pipelines", map[string]string{"pipelineName": tt.pipelineName})
				rec := doRequest(t, h, http.MethodDelete, "/pipelines/"+tt.pipelineName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "update_only":
				rec := doRequest(t, h, http.MethodPut, "/pipelines/"+tt.pipelineName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "delete_only":
				rec := doRequest(t, h, http.MethodDelete, "/pipelines/"+tt.pipelineName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_Channels_UpdateDelete covers channel update/delete operations.
func TestHandler_Channels_UpdateDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		op          string
		wantStatus  int
	}{
		{
			name:        "update_success",
			channelName: "update_channel",
			op:          "update",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "delete_success",
			channelName: "delete_channel",
			op:          "delete",
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "delete_not_found",
			channelName: "no_such_channel",
			op:          "delete_only",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.op {
			case "update":
				doRequest(t, h, http.MethodPost, "/channels", map[string]string{"channelName": tt.channelName})
				rec := doRequest(t, h, http.MethodPut, "/channels/"+tt.channelName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "delete":
				doRequest(t, h, http.MethodPost, "/channels", map[string]string{"channelName": tt.channelName})
				rec := doRequest(t, h, http.MethodDelete, "/channels/"+tt.channelName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "delete_only":
				rec := doRequest(t, h, http.MethodDelete, "/channels/"+tt.channelName, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_TagOperations covers ListTagsForResource/TagResource/UntagResource.
func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "list_tags_success",
			op:         "list",
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag_resource",
			op:         "tag",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "untag_resource",
			op:         "untag",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "list_tags_missing_arn",
			op:         "list_missing_arn",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "untag_missing_tagkeys",
			op:         "untag_missing_keys",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a channel so we have a valid ARN.
			createRec := doRequest(
				t,
				h,
				http.MethodPost,
				"/channels",
				map[string]string{"channelName": "tag_test_channel"},
			)
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			arn, _ := createResp["channelArn"].(string)
			require.NotEmpty(t, arn)

			switch tt.op {
			case "list":
				rec := doRequest(t, h, http.MethodGet, "/tags?resourceArn="+arn, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "tag":
				rec := doRequest(t, h, http.MethodPost, "/tags?resourceArn="+arn, map[string]any{
					"tags": []map[string]string{{"key": "env", "value": "test"}},
				})
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "untag":
				rec := doRequest(t, h, http.MethodDelete, "/tags?resourceArn="+arn+"&tagKeys=env", nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "list_missing_arn":
				rec := doRequest(t, h, http.MethodGet, "/tags", nil)
				assert.Equal(t, tt.wantStatus, rec.Code)

			case "untag_missing_keys":
				rec := doRequest(t, h, http.MethodDelete, "/tags?resourceArn="+arn, nil)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_DatasetContent_GetVersionSpecific tests retrieving a specific version.
func TestHandler_DatasetContent_GetVersionSpecific(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	datasetName := "version_test_dataset"

	// Create dataset.
	rec := doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": datasetName})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create content.
	createRec := doRequest(t, h, http.MethodPost, "/datasets/"+datasetName+"/content", nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	versionID, ok := createResp["versionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, versionID)

	// Get by specific version ID.
	getRec := doRequest(t, h, http.MethodGet, "/datasets/"+datasetName+"/content?versionId="+versionID, nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Get non-existent version ID.
	getRec = doRequest(t, h, http.MethodGet, "/datasets/"+datasetName+"/content?versionId=nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)

	// Delete specific version.
	deleteRec := doRequest(t, h, http.MethodDelete, "/datasets/"+datasetName+"/content?versionId="+versionID, nil)
	assert.Equal(t, http.StatusNoContent, deleteRec.Code)

	// Get non-existent after delete.
	getRec = doRequest(t, h, http.MethodGet, "/datasets/"+datasetName+"/content", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

// TestHandler_SampleChannelData_WithMessages tests sampling a channel that has messages.
func TestHandler_SampleChannelData_WithMessages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelName := "sample_data_channel"

	// Create channel.
	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{"channelName": channelName})
	require.Equal(t, http.StatusOK, rec.Code)

	// Ingest messages via BatchPutMessage.
	batchRec := doRequest(t, h, http.MethodPost, "/messages/batch", map[string]any{
		"channelName": channelName,
		"messages": []map[string]any{
			{"messageId": "m1", "payload": []byte(`{"val":1}`)},
			{"messageId": "m2", "payload": []byte(`{"val":2}`)},
		},
	})
	require.Equal(t, http.StatusOK, batchRec.Code)

	// Sample the channel.
	sampleRec := doRequest(t, h, http.MethodGet, "/channels/"+channelName+"/sample", nil)
	require.Equal(t, http.StatusOK, sampleRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sampleRec.Body.Bytes(), &resp))
	payloads, ok := resp["payloads"].([]any)
	require.True(t, ok)
	assert.Len(t, payloads, 2)
}

// TestHandler_CancelPipelineReprocessing_NotFound tests cancel with invalid reprocessing ID.
func TestHandler_CancelPipelineReprocessing_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	pipelineName := "cancel_test_pipeline"

	// Create pipeline.
	rec := doRequest(t, h, http.MethodPost, "/pipelines", map[string]string{"pipelineName": pipelineName})
	require.Equal(t, http.StatusOK, rec.Code)

	// Cancel with non-existent reprocessing ID.
	cancelRec := doRequest(t, h, http.MethodDelete, "/pipelines/"+pipelineName+"/reprocessing/nonexistent-id", nil)
	assert.Equal(t, http.StatusNotFound, cancelRec.Code)
}

// TestHandler_GetSupportedOperations verifies all new ops are listed.
func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := iotanalytics.NewHandler(iotanalytics.NewInMemoryBackend())
	ops := h.GetSupportedOperations()

	expectedNewOps := []string{
		"BatchPutMessage",
		"CancelPipelineReprocessing",
		"CreateDatasetContent",
		"DeleteDatasetContent",
		"DescribeLoggingOptions",
		"GetDatasetContent",
		"ListDatasetContents",
		"PutLoggingOptions",
		"RunPipelineActivity",
		"SampleChannelData",
		"StartPipelineReprocessing",
	}

	for _, op := range expectedNewOps {
		assert.Contains(t, ops, op, "expected operation %s in GetSupportedOperations", op)
	}
}
