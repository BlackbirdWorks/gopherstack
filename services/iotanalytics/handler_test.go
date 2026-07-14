package iotanalytics_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// newTestHandler creates an in-memory backend + handler for HTTP tests.
func newTestHandler(t *testing.T) *iotanalytics.Handler {
	t.Helper()

	return iotanalytics.NewHandler(iotanalytics.NewInMemoryBackend())
}

// doRequest performs an HTTP request against the handler.
func doRequest(t *testing.T, h *iotanalytics.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateAndDescribeChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		channel    string
		wantStatus int
	}{
		{
			name:       "success",
			channel:    "test_channel",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_name",
			channel:    "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{
				"channelName": tt.channel,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, http.MethodGet, "/channels/"+tt.channel, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

func TestHandler_ListChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		seed    []string
		wantLen int
	}{
		{
			name:    "empty",
			wantLen: 0,
		},
		{
			name:    "with_channels",
			seed:    []string{"ch1", "ch2"},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.seed {
				rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{
					"channelName": name,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/channels", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			err := json.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)

			summaries, ok := resp["channelSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantLen)
		})
	}
}

func TestHandler_DeleteChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		seed        bool
		wantStatus  int
	}{
		{
			name:        "success",
			channelName: "to_delete",
			seed:        true,
			wantStatus:  http.StatusNoContent,
		},
		{
			name:        "not_found",
			channelName: "nonexistent",
			seed:        false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seed {
				rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{
					"channelName": tt.channelName,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodDelete, "/channels/"+tt.channelName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateAndDescribeDatastore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		datastoreName string
		wantStatus    int
	}{
		{
			name:          "success",
			datastoreName: "test_datastore",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "empty_name",
			datastoreName: "",
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/datastores", map[string]string{
				"datastoreName": tt.datastoreName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

func TestHandler_CreateAndDescribeDataset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		wantStatus  int
	}{
		{
			name:        "success",
			datasetName: "test_dataset",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "empty_name",
			datasetName: "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/datasets", map[string]string{
				"datasetName": tt.datasetName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateAndDescribePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
		wantStatus   int
	}{
		{
			name:         "success",
			pipelineName: "test_pipeline",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "empty_name",
			pipelineName: "",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/pipelines", map[string]string{
				"pipelineName": tt.pipelineName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				rec2 := doRequest(t, h, http.MethodGet, "/pipelines/"+tt.pipelineName, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := iotanalytics.NewHandler(iotanalytics.NewInMemoryBackend())
	matcher := h.RouteMatcher()

	tests := []struct {
		name    string
		path    string
		service string
		want    bool
	}{
		{
			name:    "channels",
			path:    "/channels",
			service: "iotanalytics",
			want:    true,
		},
		{
			name:    "channels_name",
			path:    "/channels/my-channel",
			service: "iotanalytics",
			want:    true,
		},
		{
			name: "channels_without_iotanalytics_service",
			path: "/channels",
			want: false,
		},
		{
			name: "datastores",
			path: "/datastores",
			want: true,
		},
		{
			name: "datasets",
			path: "/datasets",
			want: true,
		},
		{
			name: "pipelines",
			path: "/pipelines",
			want: true,
		},
		{
			name:    "tags_with_iotanalytics",
			path:    "/tags",
			service: "iotanalytics",
			want:    true,
		},
		{
			name:    "tags_without_service",
			path:    "/tags",
			service: "",
			want:    false,
		},
		{
			name:    "logging_with_iotanalytics",
			path:    "/logging",
			service: "iotanalytics",
			want:    true,
		},
		{
			name:    "logging_without_service",
			path:    "/logging",
			service: "",
			want:    false,
		},
		{
			name:    "messages_with_iotanalytics",
			path:    "/messages/batch",
			service: "iotanalytics",
			want:    true,
		},
		{
			name:    "pipelineactivities_with_iotanalytics",
			path:    "/pipelineactivities/run",
			service: "iotanalytics",
			want:    true,
		},
		{
			name: "other_path",
			path: "/vaults",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			if tt.service != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/"+tt.service+"/aws4_request",
				)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_BatchPutMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		messages    any
		name        string
		channelSeed string
		wantStatus  int
		wantErrors  int
	}{
		{
			name:        "success_existing_channel",
			channelSeed: "my_channel",
			messages: map[string]any{
				"channelName": "my_channel",
				"messages": []map[string]any{
					{"messageId": "msg1", "payload": []byte("hello")},
				},
			},
			wantStatus: http.StatusOK,
			wantErrors: 0,
		},
		{
			name:        "unknown_channel_returns_error_entry",
			channelSeed: "",
			messages: map[string]any{
				"channelName": "no-such-channel",
				"messages": []map[string]any{
					{"messageId": "msg1", "payload": []byte("hello")},
				},
			},
			wantStatus: http.StatusOK,
			wantErrors: 1,
		},
		{
			name:       "invalid_body",
			messages:   "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.channelSeed != "" {
				rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{"channelName": tt.channelSeed})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, "/messages/batch", tt.messages)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				entries, _ := resp["batchPutMessageErrorEntries"].([]any)
				assert.Len(t, entries, tt.wantErrors)
			}
		})
	}
}

func TestHandler_SampleChannelData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		seed        bool
		wantStatus  int
	}{
		{
			name:        "empty_channel",
			channelName: "empty_channel",
			seed:        true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "not_found",
			channelName: "no-such-channel",
			seed:        false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seed {
				rec := doRequest(t, h, http.MethodPost, "/channels", map[string]string{"channelName": tt.channelName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/channels/"+tt.channelName+"/sample", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_StartAndCancelPipelineReprocessing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
		seed         bool
		wantStart    int
		wantCancel   int
	}{
		{
			name:         "success",
			pipelineName: "my_pipeline",
			seed:         true,
			wantStart:    http.StatusCreated,
			wantCancel:   http.StatusNoContent,
		},
		{
			name:         "pipeline_not_found",
			pipelineName: "nonexistent",
			seed:         false,
			wantStart:    http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seed {
				rec := doRequest(
					t,
					h,
					http.MethodPost,
					"/pipelines",
					map[string]string{"pipelineName": tt.pipelineName},
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			startRec := doRequest(t, h, http.MethodPost, "/pipelines/"+tt.pipelineName+"/reprocessing", nil)
			assert.Equal(t, tt.wantStart, startRec.Code)

			if tt.wantStart == http.StatusCreated {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &resp))
				reprocessingID, ok := resp["reprocessingId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, reprocessingID)

				cancelRec := doRequest(
					t,
					h,
					http.MethodDelete,
					"/pipelines/"+tt.pipelineName+"/reprocessing/"+reprocessingID,
					nil,
				)
				assert.Equal(t, tt.wantCancel, cancelRec.Code)
			}
		})
	}
}

func TestHandler_DatasetContentLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		seed        bool
		wantCreate  int
		wantGet     int
		wantList    int
		wantDelete  int
	}{
		{
			name:        "full_lifecycle",
			datasetName: "my_dataset",
			seed:        true,
			wantCreate:  http.StatusOK,
			wantGet:     http.StatusOK,
			wantList:    http.StatusOK,
			wantDelete:  http.StatusNoContent,
		},
		{
			name:        "dataset_not_found",
			datasetName: "nonexistent",
			seed:        false,
			wantCreate:  http.StatusNotFound,
			wantGet:     http.StatusNotFound,
			wantList:    http.StatusNotFound,
			wantDelete:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.seed {
				rec := doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": tt.datasetName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			createRec := doRequest(t, h, http.MethodPost, "/datasets/"+tt.datasetName+"/content", nil)
			assert.Equal(t, tt.wantCreate, createRec.Code)

			getRec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName+"/content", nil)
			assert.Equal(t, tt.wantGet, getRec.Code)

			listRec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName+"/contents", nil)
			assert.Equal(t, tt.wantList, listRec.Code)

			var versionID string

			if tt.wantList == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
				summaries, ok := resp["datasetContentSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, 1)

				if len(summaries) > 0 {
					if entry, ok2 := summaries[0].(map[string]any); ok2 {
						versionID, _ = entry["version"].(string)
					}
				}
			}

			deletePath := "/datasets/" + tt.datasetName + "/content"
			if versionID != "" {
				deletePath += "?versionId=" + versionID
			}

			deleteRec := doRequest(t, h, http.MethodDelete, deletePath, nil)
			assert.Equal(t, tt.wantDelete, deleteRec.Code)
		})
	}
}

func TestHandler_LoggingOptions(t *testing.T) {
	t.Parallel()

	type testCase struct {
		loggingOpts  any
		name         string
		wantDescribe int
		wantPut      int
		putFirst     bool
	}

	tests := []testCase{
		{
			name:         "describe_not_set",
			putFirst:     false,
			wantDescribe: http.StatusNotFound,
		},
		{
			name:     "put_and_describe",
			putFirst: true,
			loggingOpts: map[string]any{
				"loggingOptions": map[string]any{
					"roleArn": "arn:aws:iam::000000000000:role/test-role",
					"level":   "ERROR",
					"enabled": true,
				},
			},
			wantPut:      http.StatusNoContent,
			wantDescribe: http.StatusOK,
		},
		{
			name:        "put_invalid_body",
			putFirst:    true,
			loggingOpts: "not-json",
			wantPut:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.putFirst && tt.loggingOpts != nil {
				putRec := doRequest(t, h, http.MethodPut, "/logging", tt.loggingOpts)
				assert.Equal(t, tt.wantPut, putRec.Code)
			}

			if tt.wantDescribe != 0 {
				descRec := doRequest(t, h, http.MethodGet, "/logging", nil)
				assert.Equal(t, tt.wantDescribe, descRec.Code)

				if tt.wantDescribe == http.StatusOK {
					var resp map[string]any
					require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
					opts, ok := resp["loggingOptions"].(map[string]any)
					require.True(t, ok)
					assert.Equal(t, "ERROR", opts["level"])
					assert.Equal(t, true, opts["enabled"])
				}
			}
		})
	}
}

func TestHandler_RunPipelineActivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantLen    int
	}{
		{
			name: "with_payloads",
			body: map[string]any{
				"pipelineActivity": map[string]any{
					"channel": map[string]any{"name": "ch", "channelName": "test"},
				},
				"payloads": [][]byte{[]byte(`{"val":1}`), []byte(`{"val":2}`)},
			},
			wantStatus: http.StatusOK,
			wantLen:    2,
		},
		{
			name:       "empty_payloads",
			body:       map[string]any{"pipelineActivity": map[string]any{}, "payloads": [][]byte{}},
			wantStatus: http.StatusOK,
			wantLen:    0,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/pipelineactivities/run", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				payloads, _ := resp["payloads"].([]any)
				assert.Len(t, payloads, tt.wantLen)
			}
		})
	}
}

// TestHandler_CreateResource_InvalidTagsRejected verifies that Create* operations validate
// tags up front, the same way TagResource does, instead of persisting a resource with tags
// that would be rejected had they been attached via a separate TagResource call.
func TestHandler_CreateResource_InvalidTagsRejected(t *testing.T) {
	t.Parallel()

	invalidTags := []map[string]string{{"key": "aws:reserved", "value": "v"}}
	validTags := []map[string]string{{"key": "env", "value": "prod"}}

	tests := []struct {
		body func(tags []map[string]string) map[string]any
		name string
		path string
	}{
		{
			name: "channel",
			path: "/channels",
			body: func(tags []map[string]string) map[string]any {
				return map[string]any{"channelName": "invtag_ch", "tags": tags}
			},
		},
		{
			name: "datastore",
			path: "/datastores",
			body: func(tags []map[string]string) map[string]any {
				return map[string]any{"datastoreName": "invtag_ds", "tags": tags}
			},
		},
		{
			name: "dataset",
			path: "/datasets",
			body: func(tags []map[string]string) map[string]any {
				return map[string]any{"datasetName": "invtag_dset", "tags": tags}
			},
		},
		{
			name: "pipeline",
			path: "/pipelines",
			body: func(tags []map[string]string) map[string]any {
				return map[string]any{"pipelineName": "invtag_pl", "tags": tags}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body(invalidTags))
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidRequestException", errResp["__type"])

			okRec := doRequest(t, h, http.MethodPost, tt.path, tt.body(validTags))
			assert.Equal(t, http.StatusOK, okRec.Code)
		})
	}
}

// TestHandler_CreateChannel_TooManyTagsRejected verifies the 50-tag-per-resource limit is
// enforced at creation time, not only when tags are added later via TagResource.
func TestHandler_CreateChannel_TooManyTagsRejected(t *testing.T) {
	t.Parallel()

	tags := make([]map[string]string, 51)
	for i := range tags {
		tags[i] = map[string]string{"key": "k" + strconv.Itoa(i), "value": "v"}
	}

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{
		"channelName": "toomanytags_ch",
		"tags":        tags,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_GetDatasetContent_MagicVersionStrings verifies GetDatasetContent honors the
// AWS-documented "$LATEST" and "$LATEST_SUCCEEDED" versionId sentinels (uppercase, as sent
// verbatim by the SDK), not just an omitted versionId.
func TestHandler_GetDatasetContent_MagicVersionStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		versionID string
	}{
		{name: "omitted", versionID: ""},
		{name: "latest", versionID: "$LATEST"},
		{name: "latest_succeeded", versionID: "$LATEST_SUCCEEDED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "magicver_ds"})
			createRec := doRequest(t, h, http.MethodPost, "/datasets/magicver_ds/content", nil)
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			wantVersion, _ := createResp["versionId"].(string)
			require.NotEmpty(t, wantVersion)

			path := "/datasets/magicver_ds/content"
			if tt.versionID != "" {
				path += "?versionId=" + tt.versionID
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, wantVersion, resp["versionId"])
		})
	}
}

// TestHandler_DeleteDatasetContent_OmittedVersionDeletesOnlyLatest verifies that omitting
// versionId (equivalent to the AWS default "$LATEST_SUCCEEDED") removes exactly one content
// version -- the most recently created -- not every version for the dataset.
func TestHandler_DeleteDatasetContent_OmittedVersionDeletesOnlyLatest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "delonlylatest_ds"})

	var firstVersion string

	for i := range 3 {
		rec := doRequest(t, h, http.MethodPost, "/datasets/delonlylatest_ds/content", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		if i == 0 {
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			firstVersion, _ = resp["versionId"].(string)
			require.NotEmpty(t, firstVersion)
		}
	}

	deleteRec := doRequest(t, h, http.MethodDelete, "/datasets/delonlylatest_ds/content", nil)
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/datasets/delonlylatest_ds/contents", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries, _ := listResp["datasetContentSummaries"].([]any)
	require.Len(t, summaries, 2, "omitted versionId must delete exactly one version")

	var remainingHasFirst bool

	for _, s := range summaries {
		summary, _ := s.(map[string]any)
		if summary["version"] == firstVersion {
			remainingHasFirst = true
		}
	}

	assert.True(t, remainingHasFirst, "the two oldest versions must remain; only the newest is deleted")
}

// TestHandler_ListDatasetContents_PaginationAcrossPages verifies that paging through
// ListDatasetContents with a small maxResults returns every content version exactly once,
// with no duplicates or gaps, across repeated backend calls driven by successive nextTokens.
func TestHandler_ListDatasetContents_PaginationAcrossPages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/datasets", map[string]string{"datasetName": "pageall_ds"})

	wantVersions := make(map[string]bool)

	for range 5 {
		rec := doRequest(t, h, http.MethodPost, "/datasets/pageall_ds/content", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		vid, _ := resp["versionId"].(string)
		require.NotEmpty(t, vid)
		wantVersions[vid] = true
	}

	seen := make(map[string]bool)
	path := "/datasets/pageall_ds/contents?maxResults=2"

	for path != "" {
		rec := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		summaries, _ := resp["datasetContentSummaries"].([]any)
		for _, s := range summaries {
			summary, _ := s.(map[string]any)
			version, _ := summary["version"].(string)
			require.False(t, seen[version], "version %q must not be returned twice across pages", version)
			seen[version] = true
		}

		nextToken, _ := resp["nextToken"].(string)
		if nextToken == "" {
			break
		}

		path = "/datasets/pageall_ds/contents?maxResults=2&nextToken=" + nextToken
	}

	assert.Len(t, seen, len(wantVersions), "every created version must be returned exactly once across pages")

	for vid := range wantVersions {
		assert.True(t, seen[vid], "version %q must appear in some page", vid)
	}
}
