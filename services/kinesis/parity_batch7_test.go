package kinesis_test

// parity_batch7_test.go — §3 Single-service B (#22 Kinesis SubscribeToShard streaming)

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// ---------------------------------------------------------------------------
// #22 — Kinesis SubscribeToShard: long-lived streaming closes after idle polls
// ---------------------------------------------------------------------------

// subscribeAndCollect performs a single SubscribeToShard HTTP call and
// returns the raw response body bytes. It does NOT stream — the handler
// self-terminates after subscribeToShardMaxIdlePolls empty intervals.
func subscribeAndCollect(t *testing.T, h *kinesis.Handler, consumerARN, shardID string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body, err := json.Marshal(map[string]any{
		"ConsumerARN": consumerARN,
		"ShardId":     shardID,
		"StartingPosition": map[string]any{
			"Type": "TRIM_HORIZON",
		},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.SubscribeToShard")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestSubscribeToShard_StreamClosesAfterIdle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	streamName := "sub-idle-stream"

	streamARN := createStreamAndGetARN(t, h, streamName)

	shardID := getFirstShardID(t, h, streamName)

	consumerARN := registerConsumerAndGetARN(t, h, streamARN, "idle-consumer")

	// Subscribe with no records — stream should close after idle polls.
	rec := subscribeAndCollect(t, h, consumerARN, shardID)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
	// At minimum an initial-response event should be present.
	assert.NotEmpty(t, rec.Body.Bytes(), "expected at least initial-response event")
}

func TestSubscribeToShard_DeliversRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	streamName := "sub-records-stream"

	streamARN := createStreamAndGetARN(t, h, streamName)
	shardID := getFirstShardID(t, h, streamName)

	tests := []struct {
		label string
		data  []byte
	}{
		{"record_a", []byte("payload-A")},
		{"record_b", []byte("payload-B")},
	}

	for _, tt := range tests {
		rec := doRequest(t, h, "PutRecord", map[string]any{
			"StreamName":   streamName,
			"PartitionKey": tt.label,
			"Data":         tt.data,
		})
		require.Equal(t, http.StatusOK, rec.Code, "PutRecord %s", tt.label)
	}

	consumerARN := registerConsumerAndGetARN(t, h, streamARN, "records-consumer")

	rec := subscribeAndCollect(t, h, consumerARN, shardID)

	assert.Equal(t, http.StatusOK, rec.Code)
	// Body should contain the eventstream-encoded records.
	// Data is base64 inside JSON, but partition keys appear as plain text.
	body := rec.Body.String()
	assert.NotEmpty(t, body)
	assert.Contains(t, body, "record_a", "partition key record_a should appear in eventstream")
	assert.Contains(t, body, "record_b", "partition key record_b should appear in eventstream")
}

func TestSubscribeToShard_MultipleSubscriptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	streamName := "sub-multi-stream"

	streamARN := createStreamAndGetARN(t, h, streamName)
	shardID := getFirstShardID(t, h, streamName)

	doRequest(t, h, "PutRecord", map[string]any{
		"StreamName":   streamName,
		"PartitionKey": "pk",
		"Data":         []byte("the-record"),
	})

	for i := range 3 {
		consumerARN := registerConsumerAndGetARN(t, h, streamARN, "multi-consumer-"+string(rune('a'+i)))
		rec := subscribeAndCollect(t, h, consumerARN, shardID)
		assert.Equal(t, http.StatusOK, rec.Code, "subscription %d", i)
		// "the-record" data is base64 in the eventstream; "pk" partition key is plaintext.
		assert.Contains(t, rec.Body.String(), "\"pk\"", "subscription %d should deliver the record", i)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func getFirstShardID(t *testing.T, h *kinesis.Handler, streamName string) string {
	t.Helper()

	rec := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.StreamDescription.Shards)

	return resp.StreamDescription.Shards[0].ShardID
}

func registerConsumerAndGetARN(t *testing.T, h *kinesis.Handler, streamARN, consumerName string) string {
	t.Helper()

	rec := doRequest(t, h, "RegisterStreamConsumer", map[string]any{
		"StreamARN":    streamARN,
		"ConsumerName": consumerName,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Consumer struct {
			ConsumerARN string `json:"ConsumerARN"`
		} `json:"Consumer"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp.Consumer.ConsumerARN
}
