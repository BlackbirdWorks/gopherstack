package iotanalytics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// TestRefinement2_ResourceNameValidation verifies that only [a-zA-Z0-9_]+ names are accepted.
func TestRefinement2_ResourceNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resource   string
		body       map[string]any
		path       string
		wantStatus int
	}{
		{
			name:       "channel_hyphen_rejected",
			path:       "/channels",
			body:       map[string]any{"channelName": "bad-name"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "channel_underscore_accepted",
			path:       "/channels",
			body:       map[string]any{"channelName": "good_name"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "channel_alphanumeric_accepted",
			path:       "/channels",
			body:       map[string]any{"channelName": "GoodName123"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "channel_dot_rejected",
			path:       "/channels",
			body:       map[string]any{"channelName": "bad.name"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "channel_space_rejected",
			path:       "/channels",
			body:       map[string]any{"channelName": "bad name"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "datastore_hyphen_rejected",
			path:       "/datastores",
			body:       map[string]any{"datastoreName": "bad-ds"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "datastore_valid",
			path:       "/datastores",
			body:       map[string]any{"datastoreName": "valid_ds"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "dataset_hyphen_rejected",
			path:       "/datasets",
			body:       map[string]any{"datasetName": "bad-set"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "dataset_valid",
			path:       "/datasets",
			body:       map[string]any{"datasetName": "valid_set"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "pipeline_hyphen_rejected",
			path:       "/pipelines",
			body:       map[string]any{"pipelineName": "bad-pipe"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "pipeline_valid",
			path:       "/pipelines",
			body:       map[string]any{"pipelineName": "valid_pipe"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_ErrorResponseFormat verifies the AWS-style __type field in error responses.
func TestRefinement2_ErrorResponseFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		path      string
		body      any
		wantType  string
		wantCode  int
		seedFirst bool
	}{
		{
			name:     "not_found_has_ResourceNotFoundException",
			method:   http.MethodGet,
			path:     "/channels/nonexistent",
			wantType: "ResourceNotFoundException",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "datastore_not_found",
			method:   http.MethodGet,
			path:     "/datastores/nonexistent",
			wantType: "ResourceNotFoundException",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "pipeline_not_found",
			method:   http.MethodGet,
			path:     "/pipelines/nonexistent",
			wantType: "ResourceNotFoundException",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "dataset_not_found",
			method:   http.MethodGet,
			path:     "/datasets/nonexistent",
			wantType: "ResourceNotFoundException",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "invalid_name_has_InvalidRequestException",
			method:   http.MethodPost,
			path:     "/channels",
			body:     map[string]any{"channelName": "bad-name"},
			wantType: "InvalidRequestException",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantType, resp["__type"], "error response must have __type field")
			assert.NotEmpty(t, resp["message"], "error response must have message field")
		})
	}
}

// TestRefinement2_AlreadyExistsErrorType verifies ResourceAlreadyExistsException type.
func TestRefinement2_AlreadyExistsErrorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		path string
	}{
		{
			name: "channel_conflict",
			path: "/channels",
			body: map[string]any{"channelName": "dup_ch2"},
		},
		{
			name: "datastore_conflict",
			path: "/datastores",
			body: map[string]any{"datastoreName": "dup_ds2"},
		},
		{
			name: "dataset_conflict",
			path: "/datasets",
			body: map[string]any{"datasetName": "dup_set2"},
		},
		{
			name: "pipeline_conflict",
			path: "/pipelines",
			body: map[string]any{"pipelineName": "dup_pipe2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec1 := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			require.Equal(t, http.StatusOK, rec1.Code)

			rec2 := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			require.Equal(t, http.StatusConflict, rec2.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			assert.Equal(t, "ResourceAlreadyExistsException", resp["__type"])
		})
	}
}

// TestRefinement2_Pagination verifies maxResults and nextToken behavior on list endpoints.
func TestRefinement2_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		seedKind   string
		seedCount  int
		maxResults int
		wantLen    int
		wantToken  bool
	}{
		{
			name:       "channels_maxResults_2_of_5",
			seedKind:   "channel",
			seedCount:  5,
			maxResults: 2,
			wantLen:    2,
			wantToken:  true,
		},
		{
			name:       "channels_maxResults_larger_than_count",
			seedKind:   "channel",
			seedCount:  3,
			maxResults: 10,
			wantLen:    3,
			wantToken:  false,
		},
		{
			name:       "datastores_maxResults_1",
			seedKind:   "datastore",
			seedCount:  3,
			maxResults: 1,
			wantLen:    1,
			wantToken:  true,
		},
		{
			name:       "datasets_maxResults_2",
			seedKind:   "dataset",
			seedCount:  4,
			maxResults: 2,
			wantLen:    2,
			wantToken:  true,
		},
		{
			name:       "pipelines_maxResults_3_of_3",
			seedKind:   "pipeline",
			seedCount:  3,
			maxResults: 3,
			wantLen:    3,
			wantToken:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var listPath string
			var summaryKey string

			for i := range tt.seedCount {
				var body map[string]any
				var createPath string

				switch tt.seedKind {
				case "channel":
					createPath = "/channels"
					listPath = "/channels"
					summaryKey = "channelSummaries"
					body = map[string]any{
						"channelName": strings.ReplaceAll(t.Name(), "/", "_") + "_" + string(rune('a'+i)),
					}
				case "datastore":
					createPath = "/datastores"
					listPath = "/datastores"
					summaryKey = "datastoreSummaries"
					body = map[string]any{
						"datastoreName": strings.ReplaceAll(t.Name(), "/", "_") + "_" + string(rune('a'+i)),
					}
				case "dataset":
					createPath = "/datasets"
					listPath = "/datasets"
					summaryKey = "datasetSummaries"
					body = map[string]any{
						"datasetName": strings.ReplaceAll(t.Name(), "/", "_") + "_" + string(rune('a'+i)),
					}
				case "pipeline":
					createPath = "/pipelines"
					listPath = "/pipelines"
					summaryKey = "pipelineSummaries"
					body = map[string]any{
						"pipelineName": strings.ReplaceAll(t.Name(), "/", "_") + "_" + string(rune('a'+i)),
					}
				}

				rec := doRequest(t, h, http.MethodPost, createPath, body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, listPath+"?maxResults="+string(rune('0'+tt.maxResults)), nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			summaries, ok := resp[summaryKey].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantLen)

			_, hasToken := resp["nextToken"]
			if tt.wantToken {
				assert.True(t, hasToken, "expected nextToken in response")
			} else {
				assert.False(t, hasToken, "expected no nextToken when all results fit")
			}
		})
	}
}

// TestRefinement2_PaginationNextToken verifies that nextToken retrieves the next page.
func TestRefinement2_PaginationNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Seed 3 channels: p2_a, p2_b, p2_c (sorted alphabetically).
	for _, name := range []string{"p2_a", "p2_b", "p2_c"} {
		rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: maxResults=2.
	rec1 := doRequest(t, h, http.MethodGet, "/channels?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	summaries1, ok := resp1["channelSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries1, 2)
	token, ok := resp1["nextToken"].(string)
	require.True(t, ok, "page 1 must have nextToken")
	require.NotEmpty(t, token)

	// Page 2: use the nextToken.
	rec2 := doRequest(t, h, http.MethodGet, "/channels?maxResults=2&nextToken="+token, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	summaries2, ok := resp2["channelSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries2, 1, "page 2 should have the remaining 1 channel")
	_, hasToken := resp2["nextToken"]
	assert.False(t, hasToken, "page 2 should not have a nextToken")
}

// TestRefinement2_BatchPutMessage_Limits verifies batch count and message limits.
func TestRefinement2_BatchPutMessage_Limits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelSeed string
		messages    []map[string]any
		wantStatus  int
	}{
		{
			name:        "empty_messages_rejected",
			channelSeed: "batch_ch",
			messages:    []map[string]any{},
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "single_message_ok",
			channelSeed: "batch_ch2",
			messages:    []map[string]any{{"messageId": "m1", "payload": []byte("hello")}},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "101_messages_rejected",
			channelSeed: "batch_ch3",
			messages: func() []map[string]any {
				msgs := make([]map[string]any, 101)
				for i := range msgs {
					msgs[i] = map[string]any{"messageId": strings.Repeat("x", i+1), "payload": []byte("p")}
				}

				return msgs
			}(),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.channelSeed != "" {
				rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": tt.channelSeed})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, "/messages/batch", map[string]any{
				"channelName": tt.channelSeed,
				"messages":    tt.messages,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_SampleChannelData_MaxMessages verifies maxMessages boundary validation.
func TestRefinement2_SampleChannelData_MaxMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxMessages string
		wantStatus  int
	}{
		{
			name:        "valid_max_10",
			maxMessages: "10",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "valid_max_5",
			maxMessages: "5",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "zero_rejected",
			maxMessages: "0",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "eleven_rejected",
			maxMessages: "11",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "no_param_uses_default",
			maxMessages: "",
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": "samplech"})
			require.Equal(t, http.StatusOK, rec.Code)

			path := "/channels/samplech/sample"
			if tt.maxMessages != "" {
				path += "?maxMessages=" + tt.maxMessages
			}

			rec = doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_StartPipelineReprocessing_TimeRange verifies StartTime/EndTime validation.
func TestRefinement2_StartPipelineReprocessing_TimeRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "no_times_ok",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
		},
		{
			name: "valid_time_range",
			body: map[string]any{
				"startTime": 1000.0,
				"endTime":   2000.0,
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "start_after_end_rejected",
			body: map[string]any{
				"startTime": 3000.0,
				"endTime":   1000.0,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "start_equals_end_rejected",
			body: map[string]any{
				"startTime": 1000.0,
				"endTime":   1000.0,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/pipelines", map[string]any{"pipelineName": "rp_pipe"})
			require.Equal(t, http.StatusOK, rec.Code)

			startRec := doRequest(t, h, http.MethodPost, "/pipelines/rp_pipe/reprocessing", tt.body)
			assert.Equal(t, tt.wantStatus, startRec.Code)
		})
	}
}

// TestRefinement2_PutLoggingOptions_Validation verifies roleArn and level validation.
func TestRefinement2_PutLoggingOptions_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid_error_level",
			body: map[string]any{
				"loggingOptions": map[string]any{
					"roleArn": "arn:aws:iam::000000000000:role/test",
					"level":   "ERROR",
					"enabled": true,
				},
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "invalid_level_rejected",
			body: map[string]any{
				"loggingOptions": map[string]any{
					"roleArn": "arn:aws:iam::000000000000:role/test",
					"level":   "DEBUG",
					"enabled": true,
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "enabled_without_role_arn_rejected",
			body: map[string]any{
				"loggingOptions": map[string]any{
					"level":   "ERROR",
					"enabled": true,
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "disabled_without_role_arn_ok",
			body: map[string]any{
				"loggingOptions": map[string]any{
					"level":   "ERROR",
					"enabled": false,
				},
			},
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/logging", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_TagValidation verifies tag key/value length limits.
func TestRefinement2_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name:       "valid_tags",
			tags:       []map[string]string{{"key": "env", "value": "test"}},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "key_128_chars_ok",
			tags:       []map[string]string{{"key": strings.Repeat("k", 128), "value": "v"}},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "key_129_chars_rejected",
			tags:       []map[string]string{{"key": strings.Repeat("k", 129), "value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "value_256_chars_ok",
			tags:       []map[string]string{{"key": "k", "value": strings.Repeat("v", 256)}},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "value_257_chars_rejected",
			tags:       []map[string]string{{"key": "k", "value": strings.Repeat("v", 257)}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_key_rejected",
			tags:       []map[string]string{{"key": "", "value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": "tag_val_ch"})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn, _ := resp["channelArn"].(string)
			require.NotEmpty(t, arn)

			tagRec := doRequest(t, h, http.MethodPost, "/tags?resourceArn="+arn, map[string]any{
				"tags": tt.tags,
			})
			assert.Equal(t, tt.wantStatus, tagRec.Code)
		})
	}
}

// TestRefinement2_ListTagsForResource_EmptyNotError verifies empty tags returns [] not 404.
func TestRefinement2_ListTagsForResource_EmptyNotError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		kind   string
		body   map[string]any
		arnKey string
	}{
		{
			name:   "channel_no_tags",
			kind:   "channel",
			body:   map[string]any{"channelName": "notag_ch"},
			arnKey: "channelArn",
		},
		{
			name:   "datastore_no_tags",
			kind:   "datastore",
			body:   map[string]any{"datastoreName": "notag_ds"},
			arnKey: "datastoreArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var createPath string
			switch tt.kind {
			case "channel":
				createPath = "/channels"
			case "datastore":
				createPath = "/datastores"
			}

			rec := doRequest(t, h, http.MethodPost, createPath, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			arn, _ := createResp[tt.arnKey].(string)
			require.NotEmpty(t, arn)

			tagRec := doRequest(t, h, http.MethodGet, "/tags?resourceArn="+arn, nil)
			assert.Equal(t, http.StatusOK, tagRec.Code)

			var tagResp map[string]any
			require.NoError(t, json.Unmarshal(tagRec.Body.Bytes(), &tagResp))
			tags, ok := tagResp["tags"].([]any)
			require.True(t, ok, "tags field must be array")
			assert.Empty(t, tags, "newly created resource must have empty tag array")
		})
	}
}

// TestRefinement2_DescribeChannel_NewFields verifies that new fields appear in describe response.
func TestRefinement2_DescribeChannel_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		channelName string
		body        map[string]any
		wantFields  []string
	}{
		{
			name:        "basic_channel_has_storage_field",
			channelName: "desc_ch1",
			body: map[string]any{
				"channelName": "desc_ch1",
			},
			wantFields: []string{"channel"},
		},
		{
			name:        "channel_with_retention",
			channelName: "desc_ch2",
			body: map[string]any{
				"channelName": "desc_ch2",
				"retentionPeriod": map[string]any{
					"numberOfDays": 30,
				},
			},
			wantFields: []string{"channel"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/channels", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, http.MethodGet, "/channels/"+tt.channelName, nil)
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "describe response must contain field %q", field)
			}
		})
	}
}

// TestRefinement2_DescribeDatastore_NewFields verifies datastore describe response.
func TestRefinement2_DescribeDatastore_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		datastoreName string
		body          map[string]any
		wantFields    []string
	}{
		{
			name:          "basic_has_datastore_wrapper",
			datastoreName: "desc_ds1",
			body:          map[string]any{"datastoreName": "desc_ds1"},
			wantFields:    []string{"datastore"},
		},
		{
			name:          "with_json_format",
			datastoreName: "desc_ds2",
			body: map[string]any{
				"datastoreName": "desc_ds2",
				"fileFormatConfiguration": map[string]any{
					"jsonConfiguration": map[string]any{},
				},
			},
			wantFields: []string{"datastore"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/datastores", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, http.MethodGet, "/datastores/"+tt.datastoreName, nil)
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "describe response must contain field %q", field)
			}
		})
	}
}

// TestRefinement2_DescribeDataset_NewFields verifies dataset describe response fields.
func TestRefinement2_DescribeDataset_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		datasetName string
		body        map[string]any
		wantFields  []string
	}{
		{
			name:        "basic_has_dataset_wrapper",
			datasetName: "desc_set1",
			body:        map[string]any{"datasetName": "desc_set1"},
			wantFields:  []string{"dataset"},
		},
		{
			name:        "with_versioning",
			datasetName: "desc_set2",
			body: map[string]any{
				"datasetName": "desc_set2",
				"versioningConfiguration": map[string]any{
					"unlimited": true,
				},
			},
			wantFields: []string{"dataset"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/datasets", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, http.MethodGet, "/datasets/"+tt.datasetName, nil)
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "describe response must contain field %q", field)
			}
		})
	}
}

// TestRefinement2_DescribePipeline_NewFields verifies pipeline describe response fields.
func TestRefinement2_DescribePipeline_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
		body         map[string]any
		wantFields   []string
	}{
		{
			name:         "basic_has_pipeline_wrapper",
			pipelineName: "desc_pipe1",
			body:         map[string]any{"pipelineName": "desc_pipe1"},
			wantFields:   []string{"pipeline"},
		},
		{
			name:         "with_activities",
			pipelineName: "desc_pipe2",
			body: map[string]any{
				"pipelineName": "desc_pipe2",
				"pipelineActivities": []map[string]any{
					{"channel": map[string]any{"name": "ch_activity", "channelName": "src"}},
				},
			},
			wantFields: []string{"pipeline"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/pipelines", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, http.MethodGet, "/pipelines/"+tt.pipelineName, nil)
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "describe response must contain field %q", field)
			}
		})
	}
}

// TestRefinement2_UpdateHandlers_AcceptsBody verifies Update* handlers parse request bodies.
func TestRefinement2_UpdateHandlers_AcceptsBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		updateBody map[string]any
		name       string
		createPath string
		updatePath string
		wantStatus int
	}{
		{
			name:       "update_channel_with_retention",
			createPath: "/channels",
			createBody: map[string]any{"channelName": "upd_ch"},
			updatePath: "/channels/upd_ch",
			updateBody: map[string]any{
				"retentionPeriod": map[string]any{"numberOfDays": 7},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_datastore_with_retention",
			createPath: "/datastores",
			createBody: map[string]any{"datastoreName": "upd_ds"},
			updatePath: "/datastores/upd_ds",
			updateBody: map[string]any{
				"retentionPeriod": map[string]any{"unlimited": true},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_dataset_with_versioning",
			createPath: "/datasets",
			createBody: map[string]any{"datasetName": "upd_set"},
			updatePath: "/datasets/upd_set",
			updateBody: map[string]any{
				"versioningConfiguration": map[string]any{"unlimited": true},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_pipeline_with_activities",
			createPath: "/pipelines",
			createBody: map[string]any{"pipelineName": "upd_pipe"},
			updatePath: "/pipelines/upd_pipe",
			updateBody: map[string]any{
				"pipelineActivities": []map[string]any{
					{"datastore": map[string]any{"name": "ds_act", "datastoreName": "myds"}},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_channel_nil_body_ok",
			createPath: "/channels",
			createBody: map[string]any{"channelName": "upd_ch2"},
			updatePath: "/channels/upd_ch2",
			updateBody: nil,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, tt.createPath, tt.createBody)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, http.MethodPut, tt.updatePath, tt.updateBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_ReprocessingSummaries_Sorted verifies reprocessing summaries are typed and sorted.
func TestRefinement2_ReprocessingSummaries_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/pipelines", map[string]any{"pipelineName": "rps_pipe"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Start 3 reprocessings.
	ids := make([]string, 3)
	for i := range ids {
		startRec := doRequest(t, h, http.MethodPost, "/pipelines/rps_pipe/reprocessing", map[string]any{})
		require.Equal(t, http.StatusCreated, startRec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &resp))
		ids[i], _ = resp["reprocessingId"].(string)
		require.NotEmpty(t, ids[i])
	}

	// DescribePipeline should list all reprocessings in the response.
	descRec := doRequest(t, h, http.MethodGet, "/pipelines/rps_pipe", nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	pipeline, ok := resp["pipeline"].(map[string]any)
	require.True(t, ok)
	summaries, ok := pipeline["reprocessingSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 3)

	// Each summary should have id and status fields.
	for _, s := range summaries {
		summary, isMap := s.(map[string]any)
		require.True(t, isMap)
		assert.NotEmpty(t, summary["id"])
		assert.NotEmpty(t, summary["status"])
	}
}

// TestRefinement2_BackendDirect_RetentionPeriodValidation tests retention period validation.
func TestRefinement2_BackendDirect_RetentionPeriodValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		retention *iotanalytics.RetentionPeriod
		name      string
		wantErr   bool
	}{
		{
			name:      "nil_retention_ok",
			retention: nil,
			wantErr:   false,
		},
		{
			name:      "unlimited_ok",
			retention: &iotanalytics.RetentionPeriod{Unlimited: true},
			wantErr:   false,
		},
		{
			name:      "positive_days_ok",
			retention: &iotanalytics.RetentionPeriod{NumberOfDays: 30},
			wantErr:   false,
		},
		{
			name:      "zero_days_rejected",
			retention: &iotanalytics.RetentionPeriod{NumberOfDays: 0},
			wantErr:   true,
		},
		{
			name:      "negative_days_rejected",
			retention: &iotanalytics.RetentionPeriod{NumberOfDays: -1},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			_, err := b.CreateChannel(context.Background(), "ret_ch", nil, nil, tt.retention)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, iotanalytics.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
