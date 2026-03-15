package kinesis_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestJanitor_RetentionSweep verifies that the janitor removes records older than
// the stream's configured retention period and leaves newer records intact.
func TestJanitor_RetentionSweep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		retentionHrs  int
		recordAge     time.Duration
		wantRemaining int
	}{
		{
			name:          "old_records_evicted",
			retentionHrs:  24,
			recordAge:     25 * time.Hour,
			wantRemaining: 0,
		},
		{
			name:          "new_records_kept",
			retentionHrs:  24,
			recordAge:     1 * time.Hour,
			wantRemaining: 1,
		},
		{
			name:          "boundary_record_kept",
			retentionHrs:  1,
			recordAge:     30 * time.Minute,
			wantRemaining: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := kinesis.NewInMemoryBackend()
			require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "sweep-stream"}))
			require.NoError(t, bk.SetRetentionPeriodForTest("sweep-stream", tt.retentionHrs))

			// Push a record with an artificial past arrival time.
			require.NoError(t, bk.PushOldRecordForTest("sweep-stream", 0, tt.recordAge))

			j := kinesis.NewJanitorForTest(bk, time.Minute)
			j.SweepOnceForTest(t.Context())

			assert.Equal(t, tt.wantRemaining, bk.ShardRecordCountForTest("sweep-stream", 0))
		})
	}
}

// TestJanitor_MultiStreamSweep checks that the janitor sweeps multiple streams
// independently with their own retention periods.
func TestJanitor_MultiStreamSweep(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()

	// Stream A: 24-hour retention, record is 30 hours old (should be evicted).
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "stream-a"}))
	require.NoError(t, bk.SetRetentionPeriodForTest("stream-a", 24))
	require.NoError(t, bk.PushOldRecordForTest("stream-a", 0, 30*time.Hour))

	// Stream B: 48-hour retention, record is 30 hours old (should be kept).
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "stream-b"}))
	require.NoError(t, bk.SetRetentionPeriodForTest("stream-b", 48))
	require.NoError(t, bk.PushOldRecordForTest("stream-b", 0, 30*time.Hour))

	j := kinesis.NewJanitorForTest(bk, time.Minute)
	j.SweepOnceForTest(t.Context())

	assert.Equal(t, 0, bk.ShardRecordCountForTest("stream-a", 0), "stream-a: old record should be evicted")
	assert.Equal(t, 1, bk.ShardRecordCountForTest("stream-b", 0), "stream-b: recent record should remain")
}

// TestJanitor_EmptyStream verifies a sweep over an empty stream is a no-op.
func TestJanitor_EmptyStream(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "empty"}))

	j := kinesis.NewJanitorForTest(bk, time.Minute)

	require.NotPanics(t, func() {
		j.SweepOnceForTest(t.Context())
	})

	assert.Equal(t, 0, bk.ShardRecordCountForTest("empty", 0))
}

// TestJanitor_Run_Cancel verifies that the janitor goroutine exits when ctx is cancelled.
func TestJanitor_Run_Cancel(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	j := kinesis.NewJanitorForTest(bk, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		j.Run(ctx)
		close(done)
	}()

	// Allow at least one tick.
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "janitor did not exit after context cancellation")
	}
}

// TestDeleteStream_CleansFaultEntry verifies that deleting a stream also removes any
// pending FIS fault entry to prevent unbounded map growth.
func TestDeleteStream_CleansFaultEntry(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "fault-stream"}))

	// Inject a fault for the stream.
	bk.InjectFaultForTest("fault-stream")
	assert.True(t, bk.HasFaultForTest("fault-stream"), "fault should be present before delete")

	require.NoError(t, bk.DeleteStream(&kinesis.DeleteStreamInput{StreamName: "fault-stream"}))

	assert.False(t, bk.HasFaultForTest("fault-stream"), "fault entry should be removed after delete")
}

// TestRingBuffer_WrapAround checks that pushing more than maxRecordsPerShard records
// into a shard evicts the oldest entries correctly (ring buffer semantics).
func TestRingBuffer_WrapAround(t *testing.T) {
	t.Parallel()

	const maxCap = 10000
	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "ring-stream"}))

	desc, err := bk.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "ring-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	// Push maxCap+5 records; the first 5 should be evicted.
	var lastSeq string

	for range maxCap + 5 {
		out, putErr := bk.PutRecord(&kinesis.PutRecordInput{
			StreamName:   "ring-stream",
			PartitionKey: "pk",
			Data:         []byte("data"),
		})
		require.NoError(t, putErr)

		lastSeq = out.SequenceNumber
	}

	// The shard should hold exactly maxCap records.
	assert.Equal(t, maxCap, bk.ShardRecordCountForTest("ring-stream", 0))

	// The last pushed record must be readable.
	iterOut, err := bk.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:             "ring-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AT_SEQUENCE_NUMBER",
		StartingSequenceNumber: lastSeq,
	})
	require.NoError(t, err)

	recs, err := bk.GetRecords(&kinesis.GetRecordsInput{ShardIterator: iterOut.ShardIterator, Limit: 1})
	require.NoError(t, err)
	require.Len(t, recs.Records, 1)
	assert.Equal(t, lastSeq, recs.Records[0].SequenceNumber)
}

// TestBinarySearch_FindSequencePosition verifies that GetShardIterator correctly
// locates sequence numbers using the binary-search path.
func TestBinarySearch_FindSequencePosition(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "bsearch-stream"}))

	desc, err := bk.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "bsearch-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	// Push 100 records.
	seqs := make([]string, 100)

	for i := range 100 {
		out, putErr := bk.PutRecord(&kinesis.PutRecordInput{
			StreamName:   "bsearch-stream",
			PartitionKey: "pk",
			Data:         []byte("data"),
		})
		require.NoError(t, putErr)
		seqs[i] = out.SequenceNumber
	}

	tests := []struct {
		name      string
		iterType  string
		seqNum    string
		wantFirst string
		wantCount int
	}{
		{
			name:      "at_first",
			iterType:  "AT_SEQUENCE_NUMBER",
			seqNum:    seqs[0],
			wantFirst: seqs[0],
			wantCount: 100,
		},
		{
			name:      "at_middle",
			iterType:  "AT_SEQUENCE_NUMBER",
			seqNum:    seqs[49],
			wantFirst: seqs[49],
			wantCount: 51,
		},
		{
			name:      "after_middle",
			iterType:  "AFTER_SEQUENCE_NUMBER",
			seqNum:    seqs[49],
			wantFirst: seqs[50],
			wantCount: 50,
		},
		{
			name:      "at_last",
			iterType:  "AT_SEQUENCE_NUMBER",
			seqNum:    seqs[99],
			wantFirst: seqs[99],
			wantCount: 1,
		},
		{
			name:      "after_last_returns_empty",
			iterType:  "AFTER_SEQUENCE_NUMBER",
			seqNum:    seqs[99],
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			iterOut, iterErr := bk.GetShardIterator(&kinesis.GetShardIteratorInput{
				StreamName:             "bsearch-stream",
				ShardID:                shardID,
				ShardIteratorType:      tt.iterType,
				StartingSequenceNumber: tt.seqNum,
			})
			require.NoError(t, iterErr)

			recs, recsErr := bk.GetRecords(&kinesis.GetRecordsInput{
				ShardIterator: iterOut.ShardIterator,
				Limit:         10000,
			})
			require.NoError(t, recsErr)
			assert.Len(t, recs.Records, tt.wantCount)

			if tt.wantFirst != "" && len(recs.Records) > 0 {
				assert.Equal(t, tt.wantFirst, recs.Records[0].SequenceNumber)
			}
		})
	}
}
