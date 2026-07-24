package iotdataplane_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetRetainedMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iotdataplane.InMemoryBackend)
		name        string
		method      string
		topic       string
		wantPayload string
		wantCode    int
	}{
		{
			name: "get_existing_retained_message",
			setup: func(b *iotdataplane.InMemoryBackend) {
				require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("42"), 0, nil))
			},
			method:      http.MethodGet,
			topic:       "sensor/temp",
			wantCode:    http.StatusOK,
			wantPayload: "sensor/temp",
		},
		{
			name:     "get_nonexistent_topic_returns_not_found",
			setup:    nil,
			method:   http.MethodGet,
			topic:    "sensor/humidity",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "missing_topic_returns_bad_request",
			setup:    nil,
			method:   http.MethodGet,
			topic:    "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "wrong_method_returns_method_not_allowed",
			setup:    nil,
			method:   http.MethodPost,
			topic:    "sensor/temp",
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			h := iotdataplane.NewHandler(b)

			path := "/retainedMessage/" + tt.topic
			rec := doRequest(t, h, tt.method, path, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantPayload != "" {
				assert.Contains(t, rec.Body.String(), tt.wantPayload)
			}
		})
	}
}
func TestHandler_ListRetainedMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*iotdataplane.InMemoryBackend)
		name       string
		method     string
		wantTopics []string
		wantCode   int
	}{
		{
			name: "list_with_multiple_retained_messages",
			setup: func(b *iotdataplane.InMemoryBackend) {
				require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("42"), 0, nil))
				require.NoError(t, b.StoreRetainedMessage("sensor/humidity", []byte("70"), 1, nil))
			},
			method:     http.MethodGet,
			wantCode:   http.StatusOK,
			wantTopics: []string{"sensor/humidity", "sensor/temp"},
		},
		{
			name:     "list_empty_returns_empty_array",
			setup:    nil,
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name:     "wrong_method_returns_method_not_allowed",
			setup:    nil,
			method:   http.MethodPost,
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			h := iotdataplane.NewHandler(b)

			rec := doRequest(t, h, tt.method, "/retainedMessage", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, topic := range tt.wantTopics {
				assert.Contains(t, rec.Body.String(), topic)
			}
		})
	}
}
func TestBackend_RetainedMessageLifecycle(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Store two retained messages.
	require.NoError(t, b.StoreRetainedMessage("a/b", []byte("hello"), 1, nil))
	require.NoError(t, b.StoreRetainedMessage("c/d", []byte("world"), 0, nil))

	// GetRetainedMessage returns exact data.
	msg, err := b.GetRetainedMessage("a/b")
	require.NoError(t, err)
	assert.Equal(t, "a/b", msg.Topic)
	assert.Equal(t, []byte("hello"), msg.Payload)
	assert.Equal(t, int32(1), msg.Qos)
	assert.NotZero(t, msg.LastModifiedTime)

	// ListRetainedMessages returns sorted summaries.
	msgs, err := b.ListRetainedMessages()
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, "a/b", msgs[0].Topic)
	assert.Equal(t, "c/d", msgs[1].Topic)

	// Storing empty payload removes the retained message.
	require.NoError(t, b.StoreRetainedMessage("a/b", []byte{}, 0, nil))
	_, err = b.GetRetainedMessage("a/b")
	require.Error(t, err)
}
func Test_MaxRetainedMessages_LRUEviction(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	// Fill to max (1000).
	for i := range 1000 {
		topic := fmt.Sprintf("t/%d", i)
		require.NoError(t, b.StoreRetainedMessage(topic, []byte("x"), 0, nil))
	}

	assert.Equal(t, 1000, iotdataplane.RetainedMessageCount(b))

	// Adding a new topic at cap must succeed via LRU eviction (not fail).
	require.NoError(t, b.StoreRetainedMessage("overflow/topic", []byte("y"), 0, nil))

	// Count stays at cap — one entry was evicted to make room.
	assert.Equal(t, 1000, iotdataplane.RetainedMessageCount(b))

	// The new entry must be stored.
	msg, err := b.GetRetainedMessage("overflow/topic")
	require.NoError(t, err)
	assert.Equal(t, []byte("y"), msg.Payload)
}
func Test_MaxRetainedMessages_UpdateExistingNotCapped(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("existing/topic", []byte("v1"), 0, nil))

	// Updating an existing topic should never fail even at the cap.
	for range 1010 {
		require.NoError(t, b.StoreRetainedMessage("existing/topic", []byte("v2"), 0, nil))
	}
}
func Test_ListRetainedMessages_Pagination(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for _, topic := range []string{"a/1", "b/2", "c/3", "d/4", "e/5"} {
		require.NoError(t, b.StoreRetainedMessage(topic, []byte("x"), 0, nil))
	}

	h := iotdataplane.NewHandler(b)

	// First page: maxResults=2.
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage?maxResults=2", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	topics1 := page1["retainedTopics"].([]any)
	assert.Len(t, topics1, 2)

	nextToken, hasNext := page1["nextToken"].(string)
	assert.True(t, hasNext, "should have nextToken when more pages exist")

	// Second page.
	rec = doRequest(t, h, http.MethodGet, "/retainedMessage?maxResults=2&nextToken="+nextToken, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	topics2 := page2["retainedTopics"].([]any)
	assert.Len(t, topics2, 2)
}
func Test_ListRetainedMessages_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	topics, ok := resp["retainedTopics"]
	require.True(t, ok)
	// Must be an array (not null).
	_, isSlice := topics.([]any)
	assert.True(t, isSlice, "retainedTopics should be a JSON array, not null")
}
func Test_StoreRetainedMessage_EmptyPayloadRemoves(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("42"), 0, nil))
	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b))

	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte{}, 0, nil))
	assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))
}
func Test_DeepCopy_RetainedMessage(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/data", []byte("original"), 0, nil))

	msg, err := b.GetRetainedMessage("sensor/data")
	require.NoError(t, err)

	// Mutate the returned payload – the stored copy must not change.
	msg.Payload[0] = 'X'

	msg2, err := b.GetRetainedMessage("sensor/data")
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), msg2.Payload)
}
func Test_DeepCopy_ListRetainedMessages(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("t/1", []byte("hello"), 0, nil))

	msgs, err := b.ListRetainedMessages()
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	// Mutate the slice element – the stored copy must be unchanged.
	msgs[0].Payload[0] = 'Z'

	msg, err := b.GetRetainedMessage("t/1")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), msg.Payload)
}
func Test_ListRetainedMessages_PageSizeParam(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for i := range 30 {
		require.NoError(t, b.StoreRetainedMessage(fmt.Sprintf("t/%02d", i), []byte("x"), 0, nil))
	}

	h := iotdataplane.NewHandler(b)

	tests := []struct {
		name          string
		query         string
		wantPageCount int
	}{
		{name: "pageSize_param", query: "?pageSize=5", wantPageCount: 5},
		{name: "maxResults_alias", query: "?maxResults=7", wantPageCount: 7},
		{name: "pageSize_wins_over_maxResults", query: "?pageSize=3&maxResults=10", wantPageCount: 3},
		{name: "default_is_25", query: "", wantPageCount: 25},
		{name: "cap_at_100", query: "?pageSize=200", wantPageCount: 30}, // only 30 messages
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet, "/retainedMessage"+tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			topics := resp["retainedTopics"].([]any)
			assert.Len(t, topics, tt.wantPageCount, "query: %s", tt.query)
		})
	}
}
func Test_ListRetainedMessages_PaginationOffByOneFixed(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	for _, topic := range []string{"a/1", "b/2", "c/3", "d/4", "e/5"} {
		require.NoError(t, b.StoreRetainedMessage(topic, []byte("x"), 0, nil))
	}

	h := iotdataplane.NewHandler(b)

	// Page 1: pageSize=2 → a/1, b/2; nextToken = c/3 (first item of next page).
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage?pageSize=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	topics1 := page1["retainedTopics"].([]any)
	require.Len(t, topics1, 2)
	assert.Equal(t, "a/1", topics1[0].(map[string]any)["topic"])
	assert.Equal(t, "b/2", topics1[1].(map[string]any)["topic"])

	nextToken, hasNext := page1["nextToken"].(string)
	require.True(t, hasNext, "page 1 must have nextToken")
	// nextToken is the first item of the next page (AWS convention).
	assert.Equal(t, "c/3", nextToken)

	// Page 2: cursor=c/3 → page starts at c/3 (nextToken IS the first item of the page).
	rec = doRequest(t, h, http.MethodGet, "/retainedMessage?pageSize=2&nextToken="+nextToken, nil)
	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	topics2 := page2["retainedTopics"].([]any)
	require.Len(t, topics2, 2)
	assert.Equal(t, "c/3", topics2[0].(map[string]any)["topic"], "page 2 starts at cursor token")
	assert.Equal(t, "d/4", topics2[1].(map[string]any)["topic"])
}
func Test_ListRetainedMessages_SummaryIncludesQos(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("25"), 1, nil))

	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/retainedMessage", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	topics := resp["retainedTopics"].([]any)
	require.Len(t, topics, 1)

	summary := topics[0].(map[string]any)
	// RetainedMessageSummary DOES include qos on the real wire (confirmed
	// against awsRestjson1_deserializeDocumentRetainedMessageSummary in
	// aws-sdk-go-v2/service/iotdataplane/deserializers.go) -- an earlier
	// gopherstack revision incorrectly omitted it.
	require.Contains(t, summary, "qos")
	assert.InDelta(t, float64(1), summary["qos"], 0)
	assert.Contains(t, summary, "topic")
	assert.Contains(t, summary, "payloadSize")
	assert.Contains(t, summary, "lastModifiedTime")
}
func Test_GetRetainedMessage_StillHasQos(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("25"), 1, nil))

	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/retainedMessage/sensor/temp", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "qos", "GetRetainedMessage full response must include qos")
}

// Test_GetRetainedMessage_UserPropertiesField verifies the userProperties
// field is present in the GetRetainedMessage response shape (present as a
// base64 string when set, JSON null when absent) -- matches
// GetRetainedMessageOutput.UserProperties in the real SDK.
func Test_GetRetainedMessage_UserPropertiesField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		userProperties []byte
		wantNull       bool
	}{
		{name: "present", userProperties: []byte(`[{"k":"v"}]`)},
		{name: "absent", userProperties: nil, wantNull: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("25"), 1, tt.userProperties))

			h := iotdataplane.NewHandler(b)
			rec := doRequest(t, h, http.MethodGet, "/retainedMessage/sensor/temp", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Contains(t, resp, "userProperties")

			if tt.wantNull {
				assert.Nil(t, resp["userProperties"])
			} else {
				decoded, err := base64.StdEncoding.DecodeString(resp["userProperties"].(string))
				require.NoError(t, err)
				assert.Equal(t, tt.userProperties, decoded)
			}
		})
	}
}
func Test_RetainedMessages_LRUEviction(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range 1000 {
		require.NoError(t, b.StoreRetainedMessage(fmt.Sprintf("t/%04d", i), []byte("x"), 0, nil))
	}

	require.Equal(t, 1000, iotdataplane.RetainedMessageCount(b))

	// Adding a new topic must succeed via LRU eviction.
	require.NoError(t, b.StoreRetainedMessage("new/topic", []byte("new"), 0, nil))

	// Count stays at cap.
	assert.Equal(t, 1000, iotdataplane.RetainedMessageCount(b))

	msg, err := b.GetRetainedMessage("new/topic")
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), msg.Payload)
}
func Test_RetainedMessages_LRUEviction_CountNeverExceedsCap(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range 1000 {
		require.NoError(t, b.StoreRetainedMessage(fmt.Sprintf("t/%04d", i), []byte("v"), 0, nil))
	}

	for i := range 10 {
		require.NoError(t, b.StoreRetainedMessage(fmt.Sprintf("new/%d", i), []byte("y"), 0, nil))
		assert.LessOrEqual(t, iotdataplane.RetainedMessageCount(b), 1000)
	}
}
func Test_RetainedMessage_GetNonexistent_404Shape(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage/no/such/topic", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["error"])
}
func Test_RetainedMessage_Lifecycle(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	require.NoError(t, b.StoreRetainedMessage("home/temp", []byte("72"), 0, nil))
	require.NoError(t, b.StoreRetainedMessage("home/humidity", []byte("45"), 1, nil))
	require.NoError(t, b.StoreRetainedMessage("home/co2", []byte("400"), 0, nil))

	assert.Equal(t, 3, iotdataplane.RetainedMessageCount(b))

	// Get one.
	msg, err := b.GetRetainedMessage("home/temp")
	require.NoError(t, err)
	assert.Equal(t, "home/temp", msg.Topic)
	assert.Equal(t, []byte("72"), msg.Payload)

	// Empty payload removes.
	require.NoError(t, b.StoreRetainedMessage("home/temp", []byte{}, 0, nil))
	assert.Equal(t, 2, iotdataplane.RetainedMessageCount(b))

	_, err = b.GetRetainedMessage("home/temp")
	require.ErrorIs(t, err, iotdataplane.ErrRetainedMessageNotFound)

	// List is sorted by topic.
	msgs, err := b.ListRetainedMessages()
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, "home/co2", msgs[0].Topic)
	assert.Equal(t, "home/humidity", msgs[1].Topic)
}
func Test_RetainedMessage_LastModifiedTime_IsMillis(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b.StoreRetainedMessage("sensor/data", []byte("val"), 0, nil))

	msg, err := b.GetRetainedMessage("sensor/data")
	require.NoError(t, err)

	// lastModifiedTime is epoch milliseconds: must be > 1e12 (year 2001+).
	assert.Greater(t, msg.LastModifiedTime, int64(1e12), "lastModifiedTime must be epoch milliseconds")
}
func Test_ListRetainedMessages_MaxResultsAlias(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	for i := range 10 {
		topic := fmt.Sprintf("sensor/%02d/data", i)
		require.NoError(t, b.StoreRetainedMessage(topic, []byte("v"), 0, nil))
	}

	h := iotdataplane.NewHandler(b)
	rec := doRequest(t, h, http.MethodGet, "/retainedMessage?maxResults=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	topics := resp["retainedTopics"].([]any)
	assert.Len(t, topics, 3)
	_, hasNext := resp["nextToken"]
	assert.True(t, hasNext, "nextToken must be present when more pages exist")
}

// Note: coverage for the RetainedMessageSummary field set (including qos)
// lives in Test_ListRetainedMessages_SummaryIncludesQos above.
