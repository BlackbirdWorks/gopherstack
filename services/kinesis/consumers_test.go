package kinesis_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestRegisterStreamConsumer_InvalidName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		consumerName string
	}{
		{"empty", ""},
		{"too_long", strings.Repeat("c", 129)},
		{"invalid_chars", "consumer name with spaces"},
		{"slash", "consumer/name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateStream", map[string]any{"StreamName": "consumer-name-stream", "ShardCount": 1})

			b := h.Backend.(*kinesis.InMemoryBackend)
			desc, err := b.DescribeStream(
				context.Background(),
				&kinesis.DescribeStreamInput{StreamName: "consumer-name-stream"},
			)
			require.NoError(t, err)

			rec := doRequest(t, h, "RegisterStreamConsumer", map[string]any{
				"StreamARN":    desc.StreamARN,
				"ConsumerName": tt.consumerName,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code, "consumer name %q should be rejected", tt.consumerName)

			var resp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "InvalidArgumentException", resp.Type)
		})
	}
}

func TestRegisterStreamConsumer_ValidNames(t *testing.T) {
	t.Parallel()

	tests := []string{
		"consumer1",
		"my-consumer",
		"my.consumer",
		"my_consumer",
		strings.Repeat("c", 128),
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateStream", map[string]any{"StreamName": "valid-consumer-stream", "ShardCount": 1})

			b := h.Backend.(*kinesis.InMemoryBackend)
			desc, err := b.DescribeStream(
				context.Background(),
				&kinesis.DescribeStreamInput{StreamName: "valid-consumer-stream"},
			)
			require.NoError(t, err)

			rec := doRequest(t, h, "RegisterStreamConsumer", map[string]any{
				"StreamARN":    desc.StreamARN,
				"ConsumerName": name,
			})
			assert.Equal(t, http.StatusOK, rec.Code, "consumer name %q should be accepted", name)
		})
	}
}

// TestRegisterStreamConsumer_LimitExceeded verifies that AWS's 20
// registered-consumers-per-stream cap is enforced. Previously
// RegisterStreamConsumer had no upper bound at all.
func TestRegisterStreamConsumer_LimitExceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		name          string
		preRegistered int
	}{
		{name: "under_limit_succeeds", preRegistered: 19, wantErr: nil},
		{name: "at_limit_rejected", preRegistered: 20, wantErr: kinesis.ErrLimitExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: "consumer-limit-stream",
				ShardCount: 1,
			}))

			desc, err := b.DescribeStream(
				context.Background(),
				&kinesis.DescribeStreamInput{StreamName: "consumer-limit-stream"},
			)
			require.NoError(t, err)

			for i := range tt.preRegistered {
				_, regErr := b.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
					StreamARN:    desc.StreamARN,
					ConsumerName: "consumer-" + strconv.Itoa(i),
				})
				require.NoError(t, regErr)
			}

			_, err = b.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
				StreamARN:    desc.StreamARN,
				ConsumerName: "one-more-consumer",
			})
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestListStreamConsumers_MaxResultsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "consumer-page-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "consumer-page-stream"},
	)
	require.NoError(t, err)

	// Register 5 consumers.
	for i := range 5 {
		rec := doRequest(t, h, "RegisterStreamConsumer", map[string]any{
			"StreamARN":    desc.StreamARN,
			"ConsumerName": strings.Repeat("c", 1) + string(rune('a'+i)),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: request 2.
	rec := doRequest(t, h, "ListStreamConsumers", map[string]any{
		"StreamARN":  desc.StreamARN,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken"`
		Consumers []struct {
			ConsumerName string `json:"ConsumerName"`
		} `json:"Consumers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	assert.Len(t, page1.Consumers, 2)
	assert.NotEmpty(t, page1.NextToken)

	// Page 2: use NextToken.
	rec = doRequest(t, h, "ListStreamConsumers", map[string]any{
		"StreamARN":  desc.StreamARN,
		"MaxResults": 2,
		"NextToken":  page1.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 struct {
		NextToken string `json:"NextToken"`
		Consumers []struct {
			ConsumerName string `json:"ConsumerName"`
		} `json:"Consumers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	assert.Len(t, page2.Consumers, 2)
	assert.NotEmpty(t, page2.NextToken)

	// Page 3: last page.
	rec = doRequest(t, h, "ListStreamConsumers", map[string]any{
		"StreamARN":  desc.StreamARN,
		"MaxResults": 2,
		"NextToken":  page2.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page3 struct {
		NextToken string `json:"NextToken"`
		Consumers []struct {
			ConsumerName string `json:"ConsumerName"`
		} `json:"Consumers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page3))
	assert.Len(t, page3.Consumers, 1)
	assert.Empty(t, page3.NextToken)

	// All 5 consumers found across 3 pages with no duplicates.
	all := make(map[string]bool)
	for _, c := range page1.Consumers {
		all[c.ConsumerName] = true
	}
	for _, c := range page2.Consumers {
		all[c.ConsumerName] = true
	}
	for _, c := range page3.Consumers {
		all[c.ConsumerName] = true
	}
	assert.Len(t, all, 5)
}

func TestListStreamConsumers_NoMaxResults_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "consumer-all-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "consumer-all-stream"})
	require.NoError(t, err)

	for i := range 3 {
		doRequest(t, h, "RegisterStreamConsumer", map[string]any{
			"StreamARN":    desc.StreamARN,
			"ConsumerName": strings.Repeat("d", 1) + string(rune('a'+i)),
		})
	}

	rec := doRequest(t, h, "ListStreamConsumers", map[string]any{"StreamARN": desc.StreamARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken string `json:"NextToken"`
		Consumers []struct {
			ConsumerName string `json:"ConsumerName"`
		} `json:"Consumers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Consumers, 3)
	assert.Empty(t, resp.NextToken)
}

func TestConsumerRegistrationAndList(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "consumer-lifecycle2"}),
	)

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/consumer-lifecycle2"

	// Register two consumers.
	regOut1, err := bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "app-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "app-1", regOut1.Consumer.ConsumerName)
	assert.NotEmpty(t, regOut1.Consumer.ConsumerARN)

	_, err = bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "app-2",
	})
	require.NoError(t, err)

	// Duplicate registration should fail.
	_, err = bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "app-1",
	})
	require.Error(t, err)

	// ListStreamConsumers returns both.
	listOut, err := bk.ListStreamConsumers(
		context.Background(),
		&kinesis.ListStreamConsumersInput{StreamARN: streamARN},
	)
	require.NoError(t, err)
	assert.Len(t, listOut.Consumers, 2)

	// DescribeStreamConsumer by ARN.
	descOut, err := bk.DescribeStreamConsumer(context.Background(), &kinesis.DescribeStreamConsumerInput{
		ConsumerARN: regOut1.Consumer.ConsumerARN,
	})
	require.NoError(t, err)
	assert.Equal(t, "app-1", descOut.ConsumerDescription.ConsumerName)

	// Deregister.
	require.NoError(t, bk.DeregisterStreamConsumer(context.Background(), &kinesis.DeregisterStreamConsumerInput{
		ConsumerARN: regOut1.Consumer.ConsumerARN,
	}))

	listOut, err = bk.ListStreamConsumers(context.Background(), &kinesis.ListStreamConsumersInput{StreamARN: streamARN})
	require.NoError(t, err)
	assert.Len(t, listOut.Consumers, 1)
	assert.Equal(t, "app-2", listOut.Consumers[0].ConsumerName)
}

func TestSubscribeToShard_ReturnsRecords(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(
			context.Background(),
			&kinesis.CreateStreamInput{StreamName: "subscribe-stream", ShardCount: 1},
		),
	)

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/subscribe-stream"

	regOut, err := bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "reader",
	})
	require.NoError(t, err)

	// Put some records.
	_, err = bk.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "subscribe-stream",
		PartitionKey: "pk1",
		Data:         []byte("hello"),
	})
	require.NoError(t, err)

	shardOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "subscribe-stream"})
	require.NoError(t, err)
	require.Len(t, shardOut.Shards, 1)
	shardID := shardOut.Shards[0].ShardID

	subOut, err := bk.SubscribeToShard(context.Background(), &kinesis.SubscribeToShardInput{
		ConsumerARN: regOut.Consumer.ConsumerARN,
		ShardID:     shardID,
		StartingPosition: kinesis.StartingPosition{
			Type: "TRIM_HORIZON",
		},
	})
	require.NoError(t, err)
	assert.Len(t, subOut.Event.Records, 1)
	assert.Equal(t, []byte("hello"), subOut.Event.Records[0].Data)
}

func TestDeregisterStreamConsumer_ByIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input         func(consumerARN string) *kinesis.DeregisterStreamConsumerInput
		name          string
		wantRemaining int
	}{
		{
			name: "by_consumer_arn",
			input: func(consumerARN string) *kinesis.DeregisterStreamConsumerInput {
				return &kinesis.DeregisterStreamConsumerInput{ConsumerARN: consumerARN}
			},
			wantRemaining: 0,
		},
		{
			name: "by_stream_arn_and_name",
			input: func(_ string) *kinesis.DeregisterStreamConsumerInput {
				return &kinesis.DeregisterStreamConsumerInput{
					StreamARN:    "arn:aws:kinesis:us-east-1:123456789012:stream/consumer-stream",
					ConsumerName: "consumer-a",
				}
			},
			wantRemaining: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := kinesis.NewInMemoryBackend()
			require.NoError(
				t,
				bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "consumer-stream"}),
			)

			registered, err := bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
				StreamARN:    "arn:aws:kinesis:us-east-1:123456789012:stream/consumer-stream",
				ConsumerName: "consumer-a",
			})
			require.NoError(t, err)

			err = bk.DeregisterStreamConsumer(context.Background(), tt.input(registered.Consumer.ConsumerARN))
			require.NoError(t, err)

			listOut, err := bk.ListStreamConsumers(context.Background(), &kinesis.ListStreamConsumersInput{
				StreamARN: "arn:aws:kinesis:us-east-1:123456789012:stream/consumer-stream",
			})
			require.NoError(t, err)
			assert.Len(t, listOut.Consumers, tt.wantRemaining)
		})
	}
}

func TestConsumer_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "consumer-test", 1)

	desc, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "consumer-test"})
	require.NoError(t, err)

	streamARN := desc.StreamARN

	// Step 1: register.
	regOut, err := b.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "my-consumer",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-consumer", regOut.Consumer.ConsumerName)
	assert.Equal(t, "ACTIVE", regOut.Consumer.ConsumerStatus)
	assert.NotEmpty(t, regOut.Consumer.ConsumerARN)

	// Step 2: describe by name.
	descOut, err := b.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "my-consumer",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-consumer", descOut.ConsumerDescription.ConsumerName)

	// Step 3: list.
	listOut, err := b.ListStreamConsumers(ctx, &kinesis.ListStreamConsumersInput{StreamARN: streamARN})
	require.NoError(t, err)
	require.Len(t, listOut.Consumers, 1)
	assert.Equal(t, "my-consumer", listOut.Consumers[0].ConsumerName)

	// Step 4: subscribe delivers records.
	_, err = b.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   "consumer-test",
		PartitionKey: "pk",
		Data:         []byte("fan-out"),
	})
	require.NoError(t, err)

	consumerARN := descOut.ConsumerDescription.ConsumerARN

	subOut, err := b.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
		ConsumerARN: consumerARN,
		ShardID:     "shardId-000000000000",
		StartingPosition: kinesis.StartingPosition{
			Type: "TRIM_HORIZON",
		},
	})
	require.NoError(t, err)
	assert.Len(t, subOut.Event.Records, 1)
	assert.Equal(t, []byte("fan-out"), subOut.Event.Records[0].Data)

	// Step 5: deregister.
	err = b.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "my-consumer",
	})
	require.NoError(t, err)

	listOut2, err := b.ListStreamConsumers(ctx, &kinesis.ListStreamConsumersInput{StreamARN: streamARN})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Consumers)

	// Step 6: duplicate registration rejected.
	_, err = b.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "dup-consumer",
	})
	require.NoError(t, err)

	_, err = b.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "dup-consumer",
	})
	require.Error(t, err, "duplicate consumer registration must be rejected")
}

// createStreamAndGetARN is a helper that creates a stream with one shard and returns its ARN.
func createStreamAndGetARN(t *testing.T, h *kinesis.Handler, streamName string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": streamName,
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			StreamARN string `json:"StreamARN"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.NotEmpty(t, descResp.StreamDescription.StreamARN)

	return descResp.StreamDescription.StreamARN
}

func TestConsumerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		consumerName   string
		expectedStatus string
	}{
		{
			name:           "single_consumer",
			consumerName:   "my-consumer",
			expectedStatus: "ACTIVE",
		},
		{
			name:           "consumer_with_dashes",
			consumerName:   "consumer-with-dashes",
			expectedStatus: "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			streamARN := createStreamAndGetARN(t, h, "consumer-stream-"+tt.name)

			// RegisterStreamConsumer
			rec := doRequest(t, h, "RegisterStreamConsumer", map[string]any{
				"StreamARN":    streamARN,
				"ConsumerName": tt.consumerName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var regResp struct {
				Consumer struct {
					ConsumerName   string `json:"ConsumerName"`
					ConsumerARN    string `json:"ConsumerARN"`
					ConsumerStatus string `json:"ConsumerStatus"`
					StreamARN      string `json:"StreamARN"`
				} `json:"Consumer"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))
			assert.Equal(t, tt.consumerName, regResp.Consumer.ConsumerName)
			assert.Equal(t, tt.expectedStatus, regResp.Consumer.ConsumerStatus)
			assert.Equal(t, streamARN, regResp.Consumer.StreamARN)
			assert.NotEmpty(t, regResp.Consumer.ConsumerARN)
			assert.Contains(t, regResp.Consumer.ConsumerARN, tt.consumerName)

			consumerARN := regResp.Consumer.ConsumerARN

			// DescribeStreamConsumer by ConsumerARN
			rec = doRequest(t, h, "DescribeStreamConsumer", map[string]any{
				"ConsumerARN": consumerARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp struct {
				ConsumerDescription struct {
					ConsumerName string `json:"ConsumerName"`
					ConsumerARN  string `json:"ConsumerARN"`
				} `json:"ConsumerDescription"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.consumerName, descResp.ConsumerDescription.ConsumerName)
			assert.Equal(t, consumerARN, descResp.ConsumerDescription.ConsumerARN)

			// DescribeStreamConsumer by StreamARN + ConsumerName
			rec = doRequest(t, h, "DescribeStreamConsumer", map[string]any{
				"StreamARN":    streamARN,
				"ConsumerName": tt.consumerName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// ListStreamConsumers
			rec = doRequest(t, h, "ListStreamConsumers", map[string]any{
				"StreamARN": streamARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp struct {
				Consumers []struct {
					ConsumerName string `json:"ConsumerName"`
				} `json:"Consumers"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			require.Len(t, listResp.Consumers, 1)
			assert.Equal(t, tt.consumerName, listResp.Consumers[0].ConsumerName)

			// DeregisterStreamConsumer by ConsumerARN
			rec = doRequest(t, h, "DeregisterStreamConsumer", map[string]any{
				"ConsumerARN": consumerARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify gone
			rec = doRequest(t, h, "ListStreamConsumers", map[string]any{
				"StreamARN": streamARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp2 struct {
				Consumers []any `json:"Consumers"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp2))
			assert.Empty(t, listResp2.Consumers)
		})
	}
}

func TestConsumerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		action      string
		wantErrType string
		wantCode    int
	}{
		{
			name:   "RegisterConsumer_StreamNotFound",
			action: "RegisterStreamConsumer",
			body: map[string]any{
				"StreamARN":    "arn:aws:kinesis:us-east-1:123:stream/no-such-stream",
				"ConsumerName": "c1",
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "DescribeConsumer_NotFound",
			action:      "DescribeStreamConsumer",
			body:        map[string]any{"ConsumerARN": "arn:aws:kinesis:us-east-1:123:stream/x/consumer/y:0"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "ListStreamConsumers_StreamNotFound",
			action:      "ListStreamConsumers",
			body:        map[string]any{"StreamARN": "arn:aws:kinesis:us-east-1:123:stream/no-such"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "DeregisterConsumer_NotFound",
			action:      "DeregisterStreamConsumer",
			body:        map[string]any{"ConsumerARN": "arn:aws:kinesis:us-east-1:123:stream/x/consumer/y:0"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
		})
	}
}

func TestRegisterConsumerDuplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	streamARN := createStreamAndGetARN(t, h, "dup-stream")

	// First register should succeed.
	rec := doRequest(t, h, "RegisterStreamConsumer", map[string]any{
		"StreamARN":    streamARN,
		"ConsumerName": "c1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Second register of the same name should fail.
	rec = doRequest(t, h, "RegisterStreamConsumer", map[string]any{
		"StreamARN":    streamARN,
		"ConsumerName": "c1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceInUseException", errResp.Type)
}

func TestSubscribeToShard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	streamName := "sub-stream"
	consumerName := "sub-consumer"

	streamARN := createStreamAndGetARN(t, h, streamName)

	// Get shard ID.
	rec := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)
	shardID := descResp.StreamDescription.Shards[0].ShardID

	// Put a record.
	doRequest(t, h, "PutRecord", map[string]any{
		"StreamName":   streamName,
		"PartitionKey": "pk",
		"Data":         []byte("event-data"),
	})

	// Register consumer.
	rec = doRequest(t, h, "RegisterStreamConsumer", map[string]any{
		"StreamARN":    streamARN,
		"ConsumerName": consumerName,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var regResp struct {
		Consumer struct {
			ConsumerARN string `json:"ConsumerARN"`
		} `json:"Consumer"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))
	consumerARN := regResp.Consumer.ConsumerARN

	// SubscribeToShard.
	e := echo.New()
	bodyBytes, err := json.Marshal(map[string]any{
		"ConsumerARN": consumerARN,
		"ShardId":     shardID,
		"StartingPosition": map[string]any{
			"Type": "TRIM_HORIZON",
		},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.SubscribeToShard")

	rec2 := httptest.NewRecorder()
	c := e.NewContext(req, rec2)
	err = h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec2.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec2.Body.Bytes())
}

func TestDeregisterConsumerByStreamARNAndName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	streamARN := createStreamAndGetARN(t, h, "dereg-stream")

	// Register consumer.
	rec := doRequest(t, h, "RegisterStreamConsumer", map[string]any{
		"StreamARN":    streamARN,
		"ConsumerName": "to-remove",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Deregister by StreamARN + ConsumerName (not ARN).
	rec = doRequest(t, h, "DeregisterStreamConsumer", map[string]any{
		"StreamARN":    streamARN,
		"ConsumerName": "to-remove",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify gone.
	rec = doRequest(t, h, "DescribeStreamConsumer", map[string]any{
		"StreamARN":    streamARN,
		"ConsumerName": "to-remove",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
