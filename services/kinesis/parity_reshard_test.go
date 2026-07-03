package kinesis_test

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// midHashKey returns the decimal hash key exactly midway between a shard's
// starting and ending hash keys — a value strictly interior to the range.
func midHashKey(t *testing.T, shard kinesis.ShardDescription) string {
	t.Helper()

	start, ok := new(big.Int).SetString(shard.HashKeyRangeStart, 10)
	require.True(t, ok, "invalid starting hash key %q", shard.HashKeyRangeStart)
	end, ok := new(big.Int).SetString(shard.HashKeyRangeEnd, 10)
	require.True(t, ok, "invalid ending hash key %q", shard.HashKeyRangeEnd)

	mid := new(big.Int).Add(start, end)
	mid.Div(mid, big.NewInt(2))

	// Guard: ensure strict interiority for degenerate tiny ranges.
	if mid.Cmp(start) <= 0 {
		mid = new(big.Int).Add(start, big.NewInt(1))
	}

	return mid.String()
}

// openShards returns only the OPEN (non-closed) shard descriptions for a stream.
func openShards(t *testing.T, b *kinesis.InMemoryBackend, name string) []kinesis.ShardDescription {
	t.Helper()

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: name},
	)
	require.NoError(t, err)

	var open []kinesis.ShardDescription
	for _, s := range out.Shards {
		if !s.Closed {
			open = append(open, s)
		}
	}

	return open
}

// allShards returns every shard description (open and closed) for a stream.
func allShards(t *testing.T, b *kinesis.InMemoryBackend, name string) []kinesis.ShardDescription {
	t.Helper()

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: name},
	)
	require.NoError(t, err)

	return out.Shards
}

// TestParityReshard_ScalingCap asserts that UpdateShardCount enforces the AWS
// per-call scaling window: the target shard count may not exceed double, nor
// drop below half, of the current OPEN shard count.
func TestParityReshard_ScalingCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialShards int
		targetShards  int
		wantErr       bool
	}{
		{name: "scale_up_exactly_double", initialShards: 2, targetShards: 4, wantErr: false},
		{name: "scale_up_over_double", initialShards: 2, targetShards: 5, wantErr: true},
		{name: "scale_up_from_one_over_double", initialShards: 1, targetShards: 3, wantErr: true},
		{name: "scale_up_from_five_to_ten", initialShards: 5, targetShards: 10, wantErr: false},
		{name: "scale_up_from_five_over", initialShards: 5, targetShards: 11, wantErr: true},
		{name: "scale_down_exactly_half", initialShards: 4, targetShards: 2, wantErr: false},
		{name: "scale_down_below_half", initialShards: 4, targetShards: 1, wantErr: true},
		{name: "scale_down_six_to_three", initialShards: 6, targetShards: 3, wantErr: false},
		{name: "scale_down_six_to_two", initialShards: 6, targetShards: 2, wantErr: true},
		{name: "within_window_up", initialShards: 3, targetShards: 4, wantErr: false},
		{name: "within_window_down", initialShards: 3, targetShards: 2, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			name := "cap-" + tt.name
			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: name,
				ShardCount: tt.initialShards,
			}))

			out, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
				StreamName:       name,
				TargetShardCount: tt.targetShards,
				ScalingType:      "UNIFORM_SCALING",
			})

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, kinesis.ErrShardCountScaling)
				// Rejected calls must not mutate the open shard count.
				assert.Len(t, openShards(t, b, name), tt.initialShards)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.initialShards, out.CurrentShardCount)
			assert.Equal(t, tt.targetShards, out.TargetShardCount)
			assert.Len(t, openShards(t, b, name), tt.targetShards)
		})
	}
}

// TestParityReshard_ScalingCap_HandlerValidationException asserts the scaling
// cap surfaces to the wire as ValidationException (AWS-accurate error type).
func TestParityReshard_ScalingCap_HandlerValidationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantType     string
		initialShard int
		targetShard  int
		wantCode     int
	}{
		{
			name:         "over_double_is_validation",
			initialShard: 2,
			targetShard:  8,
			wantType:     "ValidationException",
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "below_half_is_validation",
			initialShard: 4,
			targetShard:  1,
			wantType:     "ValidationException",
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "within_window_ok",
			initialShard: 2,
			targetShard:  4,
			wantType:     "",
			wantCode:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			name := "cap-h-" + tt.name

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": name,
				"ShardCount": tt.initialShard,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "UpdateShardCount", map[string]any{
				"StreamName":       name,
				"TargetShardCount": tt.targetShard,
				"ScalingType":      "UNIFORM_SCALING",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp struct {
					Type    string `json:"__type"`
					Message string `json:"message"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp.Type)
				assert.NotEmpty(t, resp.Message)
			}
		})
	}
}

// TestParityReshard_ChildLineage asserts that UpdateShardCount stamps each new
// OPEN child shard with a ParentShardID referencing an overlapping (now CLOSED)
// old shard — matching AWS resharding lineage.
func TestParityReshard_ChildLineage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialShards int
		targetShards  int
	}{
		{name: "split_2_to_4", initialShards: 2, targetShards: 4},
		{name: "merge_4_to_2", initialShards: 4, targetShards: 2},
		{name: "grow_3_to_6", initialShards: 3, targetShards: 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			name := "lineage-" + tt.name
			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: name,
				ShardCount: tt.initialShards,
			}))

			_, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
				StreamName:       name,
				TargetShardCount: tt.targetShards,
				ScalingType:      "UNIFORM_SCALING",
			})
			require.NoError(t, err)

			all := allShards(t, b, name)
			byID := make(map[string]kinesis.ShardDescription, len(all))
			for _, s := range all {
				byID[s.ShardID] = s
			}

			open := openShards(t, b, name)
			require.Len(t, open, tt.targetShards)

			for _, child := range open {
				require.NotEmpty(t, child.ParentShardID,
					"open child %q must record its parent lineage", child.ShardID)

				parent, ok := byID[child.ParentShardID]
				require.True(t, ok, "parent %q of child %q must exist", child.ParentShardID, child.ShardID)
				assert.True(t, parent.Closed,
					"parent %q of child %q must be CLOSED after resharding", parent.ShardID, child.ShardID)
			}
		})
	}
}

// TestParityReshard_SequenceNumbersShardScopedAndOrdered asserts that sequence
// numbers encode the shard (so numbers are shard-scoped) and are monotonically
// ordered within a shard — like AWS, not a flat global counter.
func TestParityReshard_SequenceNumbersShardScopedAndOrdered(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	name := "seq-scope-stream"
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: name,
		ShardCount: 2,
	}))

	open := openShards(t, b, name)
	require.Len(t, open, 2)

	type putResult struct {
		shardID string
		seq     string
	}

	// Route two records to each shard using the shard's own starting hash key.
	results := make([]putResult, 0, 2*len(open))
	for _, shard := range open {
		for range 2 {
			out, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
				StreamName:      name,
				PartitionKey:    "pk-" + shard.ShardID,
				ExplicitHashKey: shard.HashKeyRangeStart,
				Data:            []byte("payload"),
			})
			require.NoError(t, err)
			results = append(results, putResult{shardID: out.ShardID, seq: out.SequenceNumber})
		}
	}
	require.Len(t, results, 4)

	// Group sequence numbers by the shard they landed on.
	bySeq := map[string][]string{}
	for _, r := range results {
		require.GreaterOrEqual(t, len(r.seq), 40, "AWS-style sequence number is 40+ chars")
		assert.Equal(t, "49", r.seq[:2], "AWS sequence numbers begin with the 49 version prefix")
		bySeq[r.shardID] = append(bySeq[r.shardID], r.seq)
	}
	require.Len(t, bySeq, 2, "records must be shard-scoped across the two shards")

	// Within each shard, sequence numbers strictly increase.
	for shardID, seqs := range bySeq {
		require.Len(t, seqs, 2)
		assert.Less(t, seqs[0], seqs[1],
			"sequence numbers within shard %q must be monotonically ordered", shardID)
	}

	// The encoded shard-index segment differs across the two shards, proving
	// the sequence number is shard-scoped rather than a flat global counter.
	shardIDs := make([]string, 0, len(bySeq))
	for id := range bySeq {
		shardIDs = append(shardIDs, id)
	}
	segA := bySeq[shardIDs[0]][0][16:20]
	segB := bySeq[shardIDs[1]][0][16:20]
	assert.NotEqual(t, segA, segB, "sequence numbers must encode a per-shard segment")
}

// TestParityReshard_MergeRequiresOpenShards asserts MergeShards verifies both
// parents are OPEN before merging — a CLOSED parent is rejected.
func TestParityReshard_MergeRequiresOpenShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		closeFirst bool
	}{
		{name: "both_open_merges", closeFirst: false},
		{name: "closed_parent_rejected", closeFirst: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			name := "merge-open-" + tt.name
			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: name,
				ShardCount: 2,
			}))

			open := openShards(t, b, name)
			require.Len(t, open, 2)
			s0, s1 := open[0].ShardID, open[1].ShardID

			if tt.closeFirst {
				// Split s0 to close it, leaving s1 open but s0 CLOSED.
				mid := midHashKey(t, open[0])
				require.NoError(t, b.SplitShard(context.Background(), &kinesis.SplitShardInput{
					StreamName:         name,
					ShardToSplit:       s0,
					NewStartingHashKey: mid,
				}))

				err := b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
					StreamName:           name,
					ShardToMerge:         s0,
					AdjacentShardToMerge: s1,
				})
				require.Error(t, err, "merging a CLOSED parent must be rejected")
				require.ErrorIs(t, err, kinesis.ErrInvalidArgument)

				return
			}

			err := b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
				StreamName:           name,
				ShardToMerge:         s0,
				AdjacentShardToMerge: s1,
			})
			require.NoError(t, err)

			// The merged child records both parents in its lineage.
			for _, s := range openShards(t, b, name) {
				if s.ParentShardID != "" {
					assert.Equal(t, s0, s.ParentShardID)
					assert.Equal(t, s1, s.AdjacentParentShardID)
				}
			}
		})
	}
}

// splitKeyKind selects which hash key a split case targets, resolved per
// isolated stream so subtests stay independent and parallel-safe.
type splitKeyKind int

const (
	splitKeyStart splitKeyKind = iota
	splitKeyEnd
	splitKeyMid
)

// TestParityReshard_SplitStrictInterior asserts SplitShard requires a new
// starting hash key strictly interior to the parent range (matching AWS).
func TestParityReshard_SplitStrictInterior(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     splitKeyKind
		wantErr bool
	}{
		{name: "start_boundary_rejected", key: splitKeyStart, wantErr: true},
		{name: "end_boundary_rejected", key: splitKeyEnd, wantErr: true},
		{name: "strict_interior_ok", key: splitKeyMid, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			name := "split-interior-" + tt.name
			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: name,
				ShardCount: 1,
			}))

			open := openShards(t, b, name)
			require.Len(t, open, 1)
			shard := open[0]

			var hashKey string
			switch tt.key {
			case splitKeyStart:
				hashKey = shard.HashKeyRangeStart
			case splitKeyEnd:
				hashKey = shard.HashKeyRangeEnd
			case splitKeyMid:
				hashKey = midHashKey(t, shard)
			}

			err := b.SplitShard(context.Background(), &kinesis.SplitShardInput{
				StreamName:         name,
				ShardToSplit:       shard.ShardID,
				NewStartingHashKey: hashKey,
			})
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, kinesis.ErrInvalidArgument)

				return
			}
			require.NoError(t, err)
		})
	}
}
