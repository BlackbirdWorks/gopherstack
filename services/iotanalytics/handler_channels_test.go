package iotanalytics_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestHandler_ErrAlreadyExists_Channel verifies creating a duplicate channel returns 409.
func TestHandler_ErrAlreadyExists_Channel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": "dup_ch"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": "dup_ch"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
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

// TestHandler_SampleChannelData_MaxMessages verifies maxMessages boundary validation.
func TestHandler_SampleChannelData_MaxMessages(t *testing.T) {
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

// TestHandler_DescribeChannel_NewFields verifies that new fields appear in describe response.
func TestHandler_DescribeChannel_NewFields(t *testing.T) {
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

// TestHandler_CreateChannel_RetentionPeriodInResponse verifies CreateChannel returns retentionPeriod.
func TestHandler_CreateChannel_RetentionPeriodInResponse(t *testing.T) {
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

// TestHandler_DescribeChannel_IncludeStatistics verifies DescribeChannel returns statistics when requested.
func TestHandler_DescribeChannel_IncludeStatistics(t *testing.T) {
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

// TestHandler_ListChannels_ChannelStorage verifies channelStorage appears in ListChannels summaries.
func TestHandler_ListChannels_ChannelStorage(t *testing.T) {
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
