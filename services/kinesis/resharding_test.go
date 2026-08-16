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

// TestScalingCap asserts that UpdateShardCount enforces the AWS
// per-call scaling window: the target shard count may not exceed double, nor
// drop below half, of the current OPEN shard count.
func TestScalingCap(t *testing.T) {
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

// TestScalingCap_HandlerValidationException asserts the scaling
// cap surfaces to the wire as ValidationException (AWS-accurate error type).
func TestScalingCap_HandlerValidationException(t *testing.T) {
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

// TestChildLineage asserts that UpdateShardCount stamps each new
// OPEN child shard with a ParentShardID referencing an overlapping (now CLOSED)
// old shard — matching AWS resharding lineage.
func TestChildLineage(t *testing.T) {
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

// TestSequenceNumbersShardScopedAndOrdered asserts that sequence
// numbers encode the shard (so numbers are shard-scoped) and are monotonically
// ordered within a shard — like AWS, not a flat global counter.
func TestSequenceNumbersShardScopedAndOrdered(t *testing.T) {
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

// TestMergeRequiresOpenShards asserts MergeShards verifies both
// parents are OPEN before merging — a CLOSED parent is rejected.
func TestMergeRequiresOpenShards(t *testing.T) {
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

// TestSplitStrictInterior asserts SplitShard requires a new
// starting hash key strictly interior to the parent range (matching AWS).
func TestSplitStrictInterior(t *testing.T) {
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

// TestMergeShards_ARNSupport verifies MergeShards accepts StreamARN.
func TestMergeShards_ARNSupport(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "arn-merge-stream",
		"ShardCount": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get ARN and shard IDs.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "arn-merge-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			StreamARN string `json:"StreamARN"`
			Shards    []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 2)

	streamARN := descResp.StreamDescription.StreamARN
	shard0 := descResp.StreamDescription.Shards[0].ShardID
	shard1 := descResp.StreamDescription.Shards[1].ShardID

	// Merge using ARN (no StreamName).
	rec = doRequest(t, h, "MergeShards", map[string]any{
		"StreamARN":            streamARN,
		"ShardToMerge":         shard0,
		"AdjacentShardToMerge": shard1,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestSplitShard_ARNSupport verifies SplitShard accepts StreamARN.
func TestSplitShard_ARNSupport(t *testing.T) {
	t.Parallel()

	const splitKey = "170141183460469231731687303715884105728"

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "arn-split-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "arn-split-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			StreamARN string `json:"StreamARN"`
			Shards    []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)

	rec = doRequest(t, h, "SplitShard", map[string]any{
		"StreamARN":          descResp.StreamDescription.StreamARN,
		"ShardToSplit":       descResp.StreamDescription.Shards[0].ShardID,
		"NewStartingHashKey": splitKey,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestMergeShards_OnDemandRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{
		"StreamName":        "on-demand-merge",
		"ShardCount":        1,
		"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
	})

	rec := doRequest(t, h, "MergeShards", map[string]any{
		"StreamName":           "on-demand-merge",
		"ShardToMerge":         "shardId-000000000000",
		"AdjacentShardToMerge": "shardId-000000000001",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidArgumentException", resp.Type)
}

func TestSplitShard_OnDemandRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{
		"StreamName":        "on-demand-split",
		"ShardCount":        1,
		"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
	})

	rec := doRequest(t, h, "SplitShard", map[string]any{
		"StreamName":         "on-demand-split",
		"ShardToSplit":       "shardId-000000000000",
		"NewStartingHashKey": "170141183460469231731687303715884105728",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidArgumentException", resp.Type)
}

func TestMergeShards_ProvisionedAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "prov-merge", "ShardCount": 2})

	b := h.Backend.(*kinesis.InMemoryBackend)
	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "prov-merge"})
	require.NoError(t, err)
	require.Len(t, out.Shards, 2)

	rec := doRequest(t, h, "MergeShards", map[string]any{
		"StreamName":           "prov-merge",
		"ShardToMerge":         out.Shards[0].ShardID,
		"AdjacentShardToMerge": out.Shards[1].ShardID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestMergeShards verifies that two adjacent shards can be merged into one.
func TestMergeShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 2 shards.
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "merge-stream",
		"ShardCount": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get the shard IDs.
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "merge-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 2)

	shard0 := descResp.StreamDescription.Shards[0].ShardID
	shard1 := descResp.StreamDescription.Shards[1].ShardID

	// Merge the two shards.
	rec = doRequest(t, h, "MergeShards", map[string]any{
		"StreamName":           "merge-stream",
		"ShardToMerge":         shard0,
		"AdjacentShardToMerge": shard1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// AWS DescribeStream returns ALL shards including closed parent shards.
	// After merging 2 → 1, expect 3 total: 2 closed parents + 1 open merged.
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "merge-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp2 struct {
		StreamDescription struct {
			Shards []struct {
				ShardID             string `json:"ShardId"`
				SequenceNumberRange struct {
					EndingSequenceNumber string `json:"EndingSequenceNumber"`
				} `json:"SequenceNumberRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp2))
	assert.Len(t, descResp2.StreamDescription.Shards, 3)
}

// TestMergeAndSplitShardIDs verifies that shard IDs remain unique after merge+split operations.
func TestMergeAndSplitShardIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 4 shards (IDs 0-3).
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "id-check-stream",
		"ShardCount": 4,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	type shardEntry struct {
		ShardID string `json:"ShardId"`
	}

	// getAllShards returns all shards (open + closed) from DescribeStream.
	getAllShards := func() []shardEntry {
		r := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "id-check-stream"})
		require.Equal(t, http.StatusOK, r.Code)

		var d struct {
			StreamDescription struct {
				Shards []shardEntry `json:"Shards"`
			} `json:"StreamDescription"`
		}

		require.NoError(t, json.Unmarshal(r.Body.Bytes(), &d))

		return d.StreamDescription.Shards
	}

	// getOpenShards returns only open (non-closed) shards via ListShards.
	getOpenShards := func() []shardEntry {
		r := doRequest(t, h, "ListShards", map[string]any{"StreamName": "id-check-stream"})
		require.Equal(t, http.StatusOK, r.Code)

		var d struct {
			Shards []shardEntry `json:"Shards"`
		}

		require.NoError(t, json.Unmarshal(r.Body.Bytes(), &d))

		return d.Shards
	}

	all := getAllShards()
	require.Len(t, all, 4)

	// Merge shards 0 and 1 → should produce shard with a new unique ID (4).
	rec = doRequest(t, h, "MergeShards", map[string]any{
		"StreamName":           "id-check-stream",
		"ShardToMerge":         all[0].ShardID,
		"AdjacentShardToMerge": all[1].ShardID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeStream returns all shards (2 closed parents + 3 open = 5 total).
	all = getAllShards()
	require.Len(t, all, 5)

	// Verify all IDs are unique.
	seen := map[string]struct{}{}
	for _, s := range all {
		assert.NotContains(t, seen, s.ShardID, "duplicate shard ID %q detected", s.ShardID)
		seen[s.ShardID] = struct{}{}
	}

	// Split one of the open shards. Use a key strictly inside shard 2's range
	// (170141183460469231731687303715884105728 to 255211775190703847598956918694523764991).
	const splitKey = "200000000000000000000000000000000000000"
	openShards := getOpenShards()
	require.NotEmpty(t, openShards)

	// splitKey 200000000000000000000000000000000000000 falls in shard 2's range
	// (170141183460469231731687303715884105728..255211775190703847597592248818726428671).
	// openShards[0] is shard 2 (first open shard after merge).
	rec = doRequest(t, h, "SplitShard", map[string]any{
		"StreamName":         "id-check-stream",
		"ShardToSplit":       openShards[0].ShardID,
		"NewStartingHashKey": splitKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// After split: 5 previous + 1 newly closed (parent of split) + 2 children = 7 total.
	all = getAllShards()
	require.Len(t, all, 7)

	// Verify all IDs are still unique after the split.
	seen = map[string]struct{}{}
	for _, s := range all {
		assert.NotContains(t, seen, s.ShardID, "duplicate shard ID %q detected after split", s.ShardID)
		seen[s.ShardID] = struct{}{}
	}
}

// TestMergeShards_Errors verifies error cases for MergeShards.
func TestMergeShards_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		wantErrType string
		wantCode    int
	}{
		{
			name: "stream_not_found",
			body: map[string]any{
				"StreamName":           "no-such-stream",
				"ShardToMerge":         "s1",
				"AdjacentShardToMerge": "s2",
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name: "shard_not_found",
			body: map[string]any{
				"StreamName":           "merge-err-stream",
				"ShardToMerge":         "bad-shard",
				"AdjacentShardToMerge": "bad-shard2",
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidArgumentException",
		},
	}

	h := newTestHandler(t)
	// Create the stream used in shard_not_found test.
	setup := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "merge-err-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, setup.Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doRequest(t, h, "MergeShards", tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}

				require.NoError(t, json.Unmarshal(got.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
		})
	}
}

// TestSplitShard verifies that a shard can be split into two at a given hash key.
func TestSplitShard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 1 shard.
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "split-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get shard details.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "split-stream"})
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

	// Split at midpoint (half of 2^128).
	const midKey = "170141183460469231731687303715884105728"

	rec = doRequest(t, h, "SplitShard", map[string]any{
		"StreamName":         "split-stream",
		"ShardToSplit":       shardID,
		"NewStartingHashKey": midKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// AWS DescribeStream returns ALL shards including closed parent.
	// After splitting 1 → 2, expect 3 total: 1 closed parent + 2 open children.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "split-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 3)
}

// TestSplitShard_Errors verifies error cases for SplitShard.
func TestSplitShard_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    any
		name    string
		errType string
		code    int
	}{
		{
			name: "stream_not_found",
			body: map[string]any{
				"StreamName":         "no-stream",
				"ShardToSplit":       "s1",
				"NewStartingHashKey": "100",
			},
			code:    http.StatusBadRequest,
			errType: "ResourceNotFoundException",
		},
		{
			name: "shard_not_found",
			body: map[string]any{
				"StreamName":         "split-err-stream",
				"ShardToSplit":       "bad-id",
				"NewStartingHashKey": "100",
			},
			code:    http.StatusBadRequest,
			errType: "InvalidArgumentException",
		},
	}

	h := newTestHandler(t)
	setup := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "split-err-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, setup.Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doRequest(t, h, "SplitShard", tt.body)
			assert.Equal(t, tt.code, got.Code)

			if tt.errType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}

				require.NoError(t, json.Unmarshal(got.Body.Bytes(), &errResp))
				assert.Equal(t, tt.errType, errResp.Type)
			}
		})
	}
}

func TestSplitShard_Basic(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "split-stream", ShardCount: 1}),
	)

	// Get the initial shard list.
	listOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "split-stream"})
	require.NoError(t, err)
	require.Len(t, listOut.Shards, 1)

	parentID := listOut.Shards[0].ShardID
	// Split at a midpoint well inside the shard range.
	splitKey := "170141183460469231731687303715884105728" // 2^127 / 1

	err = bk.SplitShard(context.Background(), &kinesis.SplitShardInput{
		StreamName:         "split-stream",
		ShardToSplit:       parentID,
		NewStartingHashKey: splitKey,
	})
	require.NoError(t, err)

	// Default list (open shards only) should now have 2 shards.
	listOut, err = bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "split-stream"})
	require.NoError(t, err)
	assert.Len(t, listOut.Shards, 2, "split should produce 2 open child shards")

	// Both child shards reference the parent.
	for _, s := range listOut.Shards {
		assert.Equal(t, parentID, s.ParentShardID)
	}

	// Full list includes the closed parent + 2 children.
	fullOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "split-stream",
		ShardFilter: "FROM_TRIM_HORIZON",
	})
	require.NoError(t, err)
	assert.Len(t, fullOut.Shards, 3, "FROM_TRIM_HORIZON should include closed parent")
}

func TestMergeShards_Basic(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "merge-stream", ShardCount: 2}),
	)

	listOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "merge-stream"})
	require.NoError(t, err)
	require.Len(t, listOut.Shards, 2)

	shard1 := listOut.Shards[0].ShardID
	shard2 := listOut.Shards[1].ShardID

	err = bk.MergeShards(context.Background(), &kinesis.MergeShardsInput{
		StreamName:           "merge-stream",
		ShardToMerge:         shard1,
		AdjacentShardToMerge: shard2,
	})
	require.NoError(t, err)

	// Only 1 open shard (the merged one).
	openOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "merge-stream"})
	require.NoError(t, err)
	assert.Len(t, openOut.Shards, 1)

	merged := openOut.Shards[0]
	assert.Equal(t, shard1, merged.ParentShardID)
	assert.Equal(t, shard2, merged.AdjacentParentShardID)

	// Full list: 2 closed parents + 1 open merged = 3.
	fullOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "merge-stream",
		ShardFilter: "FROM_TRIM_HORIZON",
	})
	require.NoError(t, err)
	assert.Len(t, fullOut.Shards, 3)
}

func TestSplitShard_ParentClosedChildrenAcceptRecords(t *testing.T) {
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

func TestMergeShards_AdjacencyRequired(t *testing.T) {
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

func TestMergeShards_KeepsClosedShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		streamName      string
		shardCount      int
		wantTotalShards int
		wantOpenShards  int
	}{
		{
			name:            "after_merge",
			streamName:      "merge-closed-stream-1",
			shardCount:      2,
			wantTotalShards: 3,
			wantOpenShards:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: tt.shardCount,
			}))

			out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)
			require.Len(t, out.Shards, 2)

			require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
				StreamName:           tt.streamName,
				ShardToMerge:         out.Shards[0].ShardID,
				AdjacentShardToMerge: out.Shards[1].ShardID,
			}))

			out2, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			assert.Len(t, out2.Shards, tt.wantTotalShards)

			openCount := 0
			for _, s := range out2.Shards {
				if !s.Closed {
					openCount++
				}
			}
			assert.Equal(t, tt.wantOpenShards, openCount)
		})
	}
}

func TestSplitShard_KeepsClosedShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		streamName      string
		wantTotalShards int
		wantOpenShards  int
	}{
		{
			name:            "after_split",
			streamName:      "split-closed-stream-1",
			wantTotalShards: 3,
			wantOpenShards:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: 1,
			}))

			out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)
			require.Len(t, out.Shards, 1)

			newHashKey := "170141183460469231731687303715884105727"

			require.NoError(t, b.SplitShard(context.Background(), &kinesis.SplitShardInput{
				StreamName:         tt.streamName,
				ShardToSplit:       out.Shards[0].ShardID,
				NewStartingHashKey: newHashKey,
			}))

			out2, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			assert.Len(t, out2.Shards, tt.wantTotalShards)

			openCount := 0
			for _, s := range out2.Shards {
				if !s.Closed {
					openCount++
				}
			}
			assert.Equal(t, tt.wantOpenShards, openCount)
		})
	}
}
