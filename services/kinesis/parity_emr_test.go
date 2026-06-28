package kinesis_test

// parity_emr_test.go — parity tests for audit-kinesis fixes:
//
//  1. MD5-based partition-key → shard routing (AWS contract)
//  2. ExplicitHashKey overrides partition-key routing
//  3. Sequence-number monotonicity within a shard
//  4. GetShardIterator: all five iterator types
//  5. GetRecords: MillisBehindLatest correctness
//  6. SplitShard: parent closed, children receive new records
//  7. MergeShards: adjacency enforcement
//  8. Retention period: janitor evicts old records
//  9. Enhanced fan-out consumer lifecycle (register/describe/list/deregister/subscribe)
// 10. DescribeStreamSummary: OpenShardCount and ConsumerCount
// 11. UpdateStreamMode: provisioned ↔ on-demand
// 12. ListShards pagination with MaxResults

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newParityBackend(t *testing.T) *kinesis.InMemoryBackend {
	t.Helper()

	return kinesis.NewInMemoryBackend()
}

func createParityStream(t *testing.T, b *kinesis.InMemoryBackend, name string, shards int) {
	t.Helper()

	err := b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: name,
		ShardCount: shards,
	})
	require.NoError(t, err)
}

// shardHashRange returns the [start, end] big.Int hash range for shard i in an
// n-shard stream — mirrors the CreateStream shard-layout calculation exactly.
func shardHashRange(i, n int) (*big.Int, *big.Int) {
	maxHashKey := new(big.Int).Sub(
		new(big.Int).Lsh(big.NewInt(1), 128),
		big.NewInt(1),
	)
	shardRange := new(big.Int).Div(
		new(big.Int).Add(maxHashKey, big.NewInt(1)),
		big.NewInt(int64(n)),
	)

	start := new(big.Int).Mul(shardRange, big.NewInt(int64(i)))

	var end *big.Int
	if i == n-1 {
		end = maxHashKey
	} else {
		end = new(big.Int).Sub(
			new(big.Int).Mul(shardRange, big.NewInt(int64(i+1))),
			big.NewInt(1),
		)
	}

	return start, end
}

// expectedShardIndex returns which shard (0-based) pk lands on in an n-shard stream
// using MD5 routing, matching the AWS Kinesis contract.
func expectedShardIndex(pk string, n int) int {
	sum := md5.Sum([]byte(pk))
	h := new(big.Int).SetBytes(sum[:])

	for i := range n {
		start, end := shardHashRange(i, n)
		if h.Cmp(start) >= 0 && h.Cmp(end) <= 0 {
			return i
		}
	}

	return 0
}

// doParityRequest sends a JSON action request through the handler.
func doParityRequest(t *testing.T, h *kinesis.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Kinesis_20131202."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ---------------------------------------------------------------------------
// 1. MD5-based partition-key → shard routing
// ---------------------------------------------------------------------------

func TestParityEMR_HashRouting_MD5_MatchesExpectedShard(t *testing.T) {
	t.Parallel()

	t.Run("known partition keys land on MD5-predicted shard", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		const (
			streamName = "md5-routing"
			shardCount = 4
		)

		createParityStream(t, b, streamName, shardCount)

		partitionKeys := []string{"hello", "world", "foo", "bar", "kinesis", "test-key-99"}

		for _, pk := range partitionKeys {
			t.Run(pk, func(t *testing.T) {
				t.Parallel()

				out, putErr := b.PutRecord(ctx, &kinesis.PutRecordInput{
					StreamName:   streamName,
					PartitionKey: pk,
					Data:         []byte("payload"),
				})
				require.NoError(t, putErr)

				wantIdx := expectedShardIndex(pk, shardCount)
				wantShard := fmt.Sprintf("shardId-%012d", wantIdx)

				assert.Equal(t, wantShard, out.ShardID,
					"partition key %q should route to shard %d via MD5", pk, wantIdx)
			})
		}
	})

	t.Run("multi-shard distribution: records spread across shards", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		createParityStream(t, b, "md5-spread", 2)

		shardCounts := map[string]int{}

		for i := range 40 {
			pk := fmt.Sprintf("key-%03d", i)
			out, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
				StreamName:   "md5-spread",
				PartitionKey: pk,
				Data:         []byte("x"),
			})
			require.NoError(t, err)
			shardCounts[out.ShardID]++
		}

		assert.Len(t, shardCounts, 2,
			"40 records over 2 shards should land on both shards")
	})
}

// ---------------------------------------------------------------------------
// 2. ExplicitHashKey overrides partition-key routing
// ---------------------------------------------------------------------------

func TestParityEMR_ExplicitHashKey_OverridesPartitionKey(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "explicit-key", 2)

	out0, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:      "explicit-key",
		PartitionKey:    "anything",
		ExplicitHashKey: "0",
		Data:            []byte("to-shard-0"),
	})
	require.NoError(t, err)
	assert.Equal(t, "shardId-000000000000", out0.ShardID)

	// shard 1 start = 2^127.
	shard1Start := new(big.Int).Lsh(big.NewInt(1), 127)

	out1, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:      "explicit-key",
		PartitionKey:    "anything",
		ExplicitHashKey: shard1Start.String(),
		Data:            []byte("to-shard-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "shardId-000000000001", out1.ShardID)
}

// ---------------------------------------------------------------------------
// 3. Sequence-number monotonicity within a shard
// ---------------------------------------------------------------------------

func TestParityEMR_SequenceNumber_MonotonicWithinShard(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "seq-mono", 1)

	const recordCount = 10
	seqs := make([]string, 0, recordCount)

	for i := range recordCount {
		out, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   "seq-mono",
			PartitionKey: "pk",
			Data:         fmt.Appendf(nil, "data-%d", i),
		})
		require.NoError(t, err)
		seqs = append(seqs, out.SequenceNumber)
	}

	for i := 1; i < recordCount; i++ {
		assert.Greater(t, seqs[i], seqs[i-1],
			"sequence numbers must be strictly increasing: seqs[%d]=%s seqs[%d]=%s",
			i, seqs[i], i-1, seqs[i-1])
	}
}

// ---------------------------------------------------------------------------
// 4. GetShardIterator: all five iterator types
// ---------------------------------------------------------------------------

func TestParityEMR_GetShardIterator_AllTypes(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "iter-types", 1)

	seqs := make([]string, 0, 5)
	timestamps := make([]time.Time, 0, 5)

	for i := range 5 {
		time.Sleep(time.Millisecond)
		out, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   "iter-types",
			PartitionKey: "pk",
			Data:         fmt.Appendf(nil, "r%d", i),
		})
		require.NoError(t, err)
		seqs = append(seqs, out.SequenceNumber)
		timestamps = append(timestamps, time.Now())
	}

	shardID := "shardId-000000000000"

	t.Run("TRIM_HORIZON reads from offset 0", func(t *testing.T) {
		t.Parallel()

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        "iter-types",
			ShardID:           shardID,
			ShardIteratorType: "TRIM_HORIZON",
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
		require.NoError(t, err)
		require.Len(t, rOut.Records, 5)
		assert.Equal(t, seqs[0], rOut.Records[0].SequenceNumber)
	})

	t.Run("LATEST returns no records", func(t *testing.T) {
		t.Parallel()

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        "iter-types",
			ShardID:           shardID,
			ShardIteratorType: "LATEST",
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
		require.NoError(t, err)
		assert.Empty(t, rOut.Records)
	})

	t.Run("AT_SEQUENCE_NUMBER starts at the given seq", func(t *testing.T) {
		t.Parallel()

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:             "iter-types",
			ShardID:                shardID,
			ShardIteratorType:      "AT_SEQUENCE_NUMBER",
			StartingSequenceNumber: seqs[2],
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
		require.NoError(t, err)
		require.NotEmpty(t, rOut.Records)
		assert.Equal(t, seqs[2], rOut.Records[0].SequenceNumber)
	})

	t.Run("AFTER_SEQUENCE_NUMBER starts after the given seq", func(t *testing.T) {
		t.Parallel()

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:             "iter-types",
			ShardID:                shardID,
			ShardIteratorType:      "AFTER_SEQUENCE_NUMBER",
			StartingSequenceNumber: seqs[2],
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
		require.NoError(t, err)
		require.NotEmpty(t, rOut.Records)
		assert.Equal(t, seqs[3], rOut.Records[0].SequenceNumber)
	})

	t.Run("AT_TIMESTAMP returns records at or after timestamp", func(t *testing.T) {
		t.Parallel()

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        "iter-types",
			ShardID:           shardID,
			ShardIteratorType: "AT_TIMESTAMP",
			Timestamp:         timestamps[3],
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
		require.NoError(t, err)
		assert.NotEmpty(t, rOut.Records)
	})
}

// ---------------------------------------------------------------------------
// 5. GetRecords: MillisBehindLatest
// ---------------------------------------------------------------------------

func TestParityEMR_GetRecords_MillisBehindLatest(t *testing.T) {
	t.Parallel()

	t.Run("non-zero when unread records remain", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		createParityStream(t, b, "mbl-nonzero", 1)

		// Push 5 records backdated 10 seconds so time.Since() resolves to > 0ms.
		for range 5 {
			err := b.PushOldRecordForTest("mbl-nonzero", 0, 10*time.Second)
			require.NoError(t, err)
		}

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        "mbl-nonzero",
			ShardID:           "shardId-000000000000",
			ShardIteratorType: "TRIM_HORIZON",
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: itOut.ShardIterator,
			Limit:         2,
		})
		require.NoError(t, err)
		assert.Len(t, rOut.Records, 2)
		assert.Positive(t, rOut.MillisBehindLatest,
			"MillisBehindLatest must be > 0 when there are unread records in the shard")
	})

	t.Run("zero when fully caught up", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		createParityStream(t, b, "mbl-zero", 1)

		_, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   "mbl-zero",
			PartitionKey: "pk",
			Data:         []byte("x"),
		})
		require.NoError(t, err)

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        "mbl-zero",
			ShardID:           "shardId-000000000000",
			ShardIteratorType: "TRIM_HORIZON",
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
		require.NoError(t, err)
		assert.Len(t, rOut.Records, 1)
		assert.Equal(t, int64(0), rOut.MillisBehindLatest)
	})
}

// ---------------------------------------------------------------------------
// 6. SplitShard: parent closed, children receive new records
// ---------------------------------------------------------------------------

func TestParityEMR_SplitShard_ParentClosedChildrenAcceptRecords(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "split-test", 1)

	_, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   "split-test",
		PartitionKey: "pre-split",
		Data:         []byte("before"),
	})
	require.NoError(t, err)

	mid := new(big.Int).Lsh(big.NewInt(1), 127)

	err = b.SplitShard(ctx, &kinesis.SplitShardInput{
		StreamName:         "split-test",
		ShardToSplit:       "shardId-000000000000",
		NewStartingHashKey: mid.String(),
	})
	require.NoError(t, err)

	desc, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "split-test"})
	require.NoError(t, err)

	var parent *kinesis.ShardDescription
	var children []*kinesis.ShardDescription

	for i := range desc.Shards {
		s := &desc.Shards[i]
		if s.ShardID == "shardId-000000000000" {
			parent = s
		} else {
			children = append(children, s)
		}
	}

	require.NotNil(t, parent)
	assert.True(t, parent.Closed, "parent shard must be closed after SplitShard")
	assert.Len(t, children, 2, "SplitShard must produce exactly 2 child shards")

	putOut, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   "split-test",
		PartitionKey: "post-split",
		Data:         []byte("after"),
	})
	require.NoError(t, err)
	assert.NotEqual(t, "shardId-000000000000", putOut.ShardID,
		"new record must land in a child shard, not the closed parent")

	// Parent records still readable.
	itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        "split-test",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
	require.NoError(t, err)
	assert.Len(t, rOut.Records, 1, "pre-split record must still be readable from the parent shard")
	assert.Empty(t, rOut.NextShardIterator,
		"closed shard with all records consumed must return empty NextShardIterator")
}

// ---------------------------------------------------------------------------
// 7. MergeShards: adjacency enforcement
// ---------------------------------------------------------------------------

func TestParityEMR_MergeShards_AdjacencyRequired(t *testing.T) {
	t.Parallel()

	t.Run("adjacent shards merge successfully", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		createParityStream(t, b, "merge-ok", 3)

		err := b.MergeShards(ctx, &kinesis.MergeShardsInput{
			StreamName:           "merge-ok",
			ShardToMerge:         "shardId-000000000000",
			AdjacentShardToMerge: "shardId-000000000001",
		})
		require.NoError(t, err)

		desc, descErr := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "merge-ok"})
		require.NoError(t, descErr)

		closed, open := 0, 0
		for _, s := range desc.Shards {
			if s.Closed {
				closed++
			} else {
				open++
			}
		}

		assert.Equal(t, 2, closed)
		assert.Equal(t, 2, open)
	})

	t.Run("non-adjacent shards are rejected", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		createParityStream(t, b, "merge-fail", 3)

		err := b.MergeShards(ctx, &kinesis.MergeShardsInput{
			StreamName:           "merge-fail",
			ShardToMerge:         "shardId-000000000000",
			AdjacentShardToMerge: "shardId-000000000002",
		})
		assert.Error(t, err)
	})

	t.Run("merged shard spans combined hash range", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		createParityStream(t, b, "merge-range", 2)

		desc0, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "merge-range"})
		require.NoError(t, err)

		s0Start := desc0.Shards[0].HashKeyRangeStart
		s1End := desc0.Shards[1].HashKeyRangeEnd

		err = b.MergeShards(ctx, &kinesis.MergeShardsInput{
			StreamName:           "merge-range",
			ShardToMerge:         "shardId-000000000000",
			AdjacentShardToMerge: "shardId-000000000001",
		})
		require.NoError(t, err)

		desc1, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "merge-range"})
		require.NoError(t, err)

		var merged *kinesis.ShardDescription
		for i := range desc1.Shards {
			if !desc1.Shards[i].Closed {
				merged = &desc1.Shards[i]

				break
			}
		}

		require.NotNil(t, merged)
		assert.Equal(t, s0Start, merged.HashKeyRangeStart)
		assert.Equal(t, s1End, merged.HashKeyRangeEnd)
	})
}

// ---------------------------------------------------------------------------
// 8. Retention period: janitor evicts old records
// ---------------------------------------------------------------------------

func TestParityEMR_RetentionPeriod_JanitorEvictsOldRecords(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "retention-test", 1)

	err := b.SetRetentionPeriodForTest("retention-test", 1)
	require.NoError(t, err)

	err = b.PushOldRecordForTest("retention-test", 0, 2*time.Hour)
	require.NoError(t, err)

	_, err = b.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   "retention-test",
		PartitionKey: "pk",
		Data:         []byte("fresh"),
	})
	require.NoError(t, err)

	itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        "retention-test",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rBefore, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
	require.NoError(t, err)
	assert.Len(t, rBefore.Records, 2)

	j := kinesis.NewJanitorForTest(b, time.Minute)
	j.SweepOnceForTest(ctx)

	itOut2, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        "retention-test",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rAfter, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut2.ShardIterator})
	require.NoError(t, err)
	assert.Len(t, rAfter.Records, 1, "old record must be evicted after janitor sweep")
	assert.Contains(t, string(rAfter.Records[0].Data), "fresh")
}

// ---------------------------------------------------------------------------
// 9. Enhanced fan-out consumer lifecycle (sequential — shares backend state)
// ---------------------------------------------------------------------------

func TestParityEMR_Consumer_Lifecycle(t *testing.T) {
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

// ---------------------------------------------------------------------------
// 10. DescribeStreamSummary: OpenShardCount and ConsumerCount via handler
// ---------------------------------------------------------------------------

func TestParityEMR_DescribeStreamSummary_OpenShardCount(t *testing.T) {
	t.Parallel()

	h := kinesis.NewHandler(kinesis.NewInMemoryBackend())

	rec := doParityRequest(t, h, "CreateStream",
		map[string]any{"StreamName": "summary-test", "ShardCount": 3})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doParityRequest(t, h, "DescribeStream",
		map[string]any{"StreamName": "summary-test"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		StreamDescription struct {
			StreamARN string `json:"StreamARN"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.NewDecoder(strings.NewReader(descRec.Body.String())).Decode(&descResp))

	regRec := doParityRequest(t, h, "RegisterStreamConsumer", map[string]any{
		"StreamARN":    descResp.StreamDescription.StreamARN,
		"ConsumerName": "c1",
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	sumRec := doParityRequest(t, h, "DescribeStreamSummary",
		map[string]any{"StreamName": "summary-test"})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var sumResp struct {
		StreamDescriptionSummary struct {
			StreamStatus   string `json:"StreamStatus"`
			OpenShardCount int    `json:"OpenShardCount"`
			ConsumerCount  int    `json:"ConsumerCount"`
		} `json:"StreamDescriptionSummary"`
	}

	require.NoError(t, json.NewDecoder(strings.NewReader(sumRec.Body.String())).Decode(&sumResp))

	assert.Equal(t, 3, sumResp.StreamDescriptionSummary.OpenShardCount)
	assert.Equal(t, 1, sumResp.StreamDescriptionSummary.ConsumerCount)
	assert.Equal(t, "ACTIVE", sumResp.StreamDescriptionSummary.StreamStatus)
}

// ---------------------------------------------------------------------------
// 11. UpdateStreamMode: provisioned ↔ on-demand
// ---------------------------------------------------------------------------

func TestParityEMR_UpdateStreamMode_ProvisionedToOnDemand(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "mode-test", 2)

	desc0, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "mode-test"})
	require.NoError(t, err)
	assert.Equal(t, "PROVISIONED", desc0.StreamMode)

	err = b.UpdateStreamMode(ctx, &kinesis.UpdateStreamModeInput{
		StreamARN: desc0.StreamARN,
		StreamModeDetails: kinesis.StreamModeDetails{
			StreamMode: "ON_DEMAND",
		},
	})
	require.NoError(t, err)

	desc1, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "mode-test"})
	require.NoError(t, err)
	assert.Equal(t, "ON_DEMAND", desc1.StreamMode)

	_, err = b.UpdateShardCount(ctx, &kinesis.UpdateShardCountInput{
		StreamName:       "mode-test",
		TargetShardCount: 4,
	})
	require.Error(t, err, "UpdateShardCount must fail for ON_DEMAND streams")

	err = b.UpdateStreamMode(ctx, &kinesis.UpdateStreamModeInput{
		StreamARN: desc1.StreamARN,
		StreamModeDetails: kinesis.StreamModeDetails{
			StreamMode: "PROVISIONED",
		},
	})
	require.NoError(t, err)

	desc2, err := b.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: "mode-test"})
	require.NoError(t, err)
	assert.Equal(t, "PROVISIONED", desc2.StreamMode)
}

// ---------------------------------------------------------------------------
// 12. ListShards pagination with MaxResults
// ---------------------------------------------------------------------------

func TestParityEMR_ListShards_Pagination_Complete(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "page-shards", 7)

	var allShards []kinesis.ShardDescription
	var nextToken string

	for {
		out, err := b.ListShards(ctx, &kinesis.ListShardsInput{
			StreamName: "page-shards",
			MaxResults: 3,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		allShards = append(allShards, out.Shards...)

		if out.NextToken == "" {
			break
		}

		nextToken = out.NextToken
	}

	assert.Len(t, allShards, 7)

	seen := make(map[string]bool)
	for _, s := range allShards {
		assert.False(t, seen[s.ShardID], "duplicate shard %s in paginated results", s.ShardID)
		seen[s.ShardID] = true
	}
}
