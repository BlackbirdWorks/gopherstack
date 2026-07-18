package iotanalytics_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestHandler_BatchPutMessage_Limits verifies batch count and message limits.
func TestHandler_BatchPutMessage_Limits(t *testing.T) {
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

// TestHandler_BatchPutMessage_NoChannelNameInErrorEntry verifies BatchPutMessage errors omit channelName.
func TestHandler_BatchPutMessage_NoChannelNameInErrorEntry(t *testing.T) {
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
