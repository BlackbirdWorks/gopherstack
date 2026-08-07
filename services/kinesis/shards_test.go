package kinesis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListShards_NextTokenStreamNameMutuallyExclusive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "mutex-shards-stream", "ShardCount": 2})

	// First page to get a valid NextToken.
	rec := doRequest(t, h, "ListShards", map[string]any{
		"StreamName": "mutex-shards-stream",
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	require.NotEmpty(t, page1.NextToken, "expected NextToken for pagination")

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "next_token_with_stream_name",
			body: map[string]any{
				"NextToken":  page1.NextToken,
				"StreamName": "mutex-shards-stream",
			},
		},
		{
			name: "next_token_with_exclusive_start_shard_id",
			body: map[string]any{
				"NextToken":             page1.NextToken,
				"StreamName":            "mutex-shards-stream",
				"ExclusiveStartShardId": "shardId-000000000000",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec2 := doRequest(t, h, "ListShards", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec2.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errResp))
			assert.Equal(t, "ValidationException", errResp.Type)
		})
	}
}

func TestListShards_NextTokenAlone_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "next-token-only-stream", "ShardCount": 3})

	// Get NextToken from first page.
	rec := doRequest(t, h, "ListShards", map[string]any{
		"StreamName": "next-token-only-stream",
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken string `json:"NextToken"`
		Shards    []struct {
			ShardID string `json:"ShardId"`
		} `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	require.NotEmpty(t, page1.NextToken)

	// Use NextToken alone (no StreamName) — should succeed because the token embeds stream context.
	rec2 := doRequest(t, h, "ListShards", map[string]any{
		"NextToken":  page1.NextToken,
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Shards []struct {
			ShardID string `json:"ShardId"`
		} `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	assert.NotEmpty(t, page2.Shards)
}

func TestListShards_AfterShardID(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "filter-stream", ShardCount: 4}),
	)

	// List all 4 shards.
	allOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "filter-stream"})
	require.NoError(t, err)
	require.Len(t, allOut.Shards, 4)

	// Use ExclusiveStartShardID to skip first two.
	filtOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:            "filter-stream",
		ExclusiveStartShardID: allOut.Shards[1].ShardID,
	})
	require.NoError(t, err)
	assert.Len(t, filtOut.Shards, 2, "should return shards after the second shard")
}

func TestHashRouting_MD5_MatchesExpectedShard(t *testing.T) {
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

func TestSequenceNumber_MonotonicWithinShard(t *testing.T) {
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

func TestListShards_Pagination_Complete(t *testing.T) {
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

func TestCountOpenShards_ExcludesClosedShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
		shardCount int
		wantCount  int
		doMerge    bool
	}{
		{name: "no_merge", streamName: "count-stream-1", shardCount: 2, doMerge: false, wantCount: 2},
		{name: "after_merge", streamName: "count-stream-2", shardCount: 2, doMerge: true, wantCount: 1},
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

			if tt.doMerge {
				out, err := b.DescribeStream(
					context.Background(),
					&kinesis.DescribeStreamInput{StreamName: tt.streamName},
				)
				require.NoError(t, err)
				require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
					StreamName:           tt.streamName,
					ShardToMerge:         out.Shards[0].ShardID,
					AdjacentShardToMerge: out.Shards[1].ShardID,
				}))
			}

			assert.Equal(t, tt.wantCount, b.CountOpenShards(context.Background()))
		})
	}
}

func TestListShards_ExclusiveStartShardId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		exclusiveStartShardID string
		shardCount            int
		wantCount             int
	}{
		{name: "no_filter", shardCount: 3, exclusiveStartShardID: "", wantCount: 3},
		{name: "skip_first", shardCount: 3, exclusiveStartShardID: "shardId-000000000000", wantCount: 2},
		{name: "skip_first_two", shardCount: 3, exclusiveStartShardID: "shardId-000000000001", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: "list-shards-excl-" + tt.name,
				ShardCount: tt.shardCount,
			}))

			out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
				StreamName:            "list-shards-excl-" + tt.name,
				ExclusiveStartShardID: tt.exclusiveStartShardID,
			})
			require.NoError(t, err)
			assert.Len(t, out.Shards, tt.wantCount)
		})
	}
}

func TestListShards_IncludesClosedShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		streamName      string
		shardCount      int
		wantTotalShards int
	}{
		{name: "after_merge_shows_all", streamName: "list-closed-stream-1", shardCount: 2, wantTotalShards: 3},
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

			ds, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
				StreamName:           tt.streamName,
				ShardToMerge:         ds.Shards[0].ShardID,
				AdjacentShardToMerge: ds.Shards[1].ShardID,
			}))

			// Use FROM_TRIM_HORIZON filter to retrieve all shards including closed ones.
			out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
				StreamName:  tt.streamName,
				ShardFilter: "FROM_TRIM_HORIZON",
			})
			require.NoError(t, err)
			assert.Len(t, out.Shards, tt.wantTotalShards)

			// Without a filter, only open shards are returned (matching AWS default behavior).
			openOut, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: tt.streamName})
			require.NoError(t, err)
			assert.Len(t, openOut.Shards, 1, "expected only the 1 open (merged) shard without filter")
		})
	}
}

func TestShardDescription_ParentShardId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
	}{
		{name: "merged_shard_has_parents", streamName: "parent-shard-stream-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: 2,
			}))

			ds, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			shard0ID := ds.Shards[0].ShardID
			shard1ID := ds.Shards[1].ShardID

			require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
				StreamName:           tt.streamName,
				ShardToMerge:         shard0ID,
				AdjacentShardToMerge: shard1ID,
			}))

			ds2, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: tt.streamName})
			require.NoError(t, err)

			var mergedShard *kinesis.ShardDescription
			for i := range ds2.Shards {
				if !ds2.Shards[i].Closed {
					mergedShard = &ds2.Shards[i]

					break
				}
			}
			require.NotNil(t, mergedShard)
			assert.Equal(t, shard0ID, mergedShard.ParentShardID)
			assert.Equal(t, shard1ID, mergedShard.AdjacentParentShardID)
		})
	}
}

func TestNextSeq_Serialized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		streamName string
	}{
		{name: "seq_preserved_after_snapshot_restore", streamName: "seq-stream-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: tt.streamName,
				ShardCount: 1,
			}))

			out, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
				StreamName:   tt.streamName,
				PartitionKey: "key",
				Data:         []byte("data"),
			})
			require.NoError(t, err)
			firstSeq := out.SequenceNumber

			snapshot := b.Snapshot(t.Context())
			require.NotNil(t, snapshot)

			b2 := kinesis.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), snapshot))

			out2, err := b2.PutRecord(context.Background(), &kinesis.PutRecordInput{
				StreamName:   tt.streamName,
				PartitionKey: "key2",
				Data:         []byte("data2"),
			})
			require.NoError(t, err)
			assert.NotEqual(t, firstSeq, out2.SequenceNumber)
		})
	}
}

func TestListShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateStream with 3 shards
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "shards-stream",
		"ShardCount": 3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListShards
	rec = doRequest(t, h, "ListShards", map[string]any{
		"StreamName": "shards-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listShardsResp struct {
		Shards []struct {
			ShardID string `json:"ShardId"`
		} `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listShardsResp))
	assert.Len(t, listShardsResp.Shards, 3)
}

func TestListShards_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "list-shards-paginated",
		ShardCount: 5,
	}))

	// Request only 2 shards.
	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName: "list-shards-paginated",
		MaxResults: 2,
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 2)
	assert.NotEmpty(t, out.NextToken, "should have a next token for remaining shards")
}

func TestListShards_NextToken_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "list-shards-nexttoken",
		ShardCount: 5,
	}))

	var allShards []kinesis.ShardDescription
	var nextToken string

	for {
		out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
			StreamName: "list-shards-nexttoken",
			MaxResults: 2,
			NextToken:  nextToken,
		})
		require.NoError(t, err)
		allShards = append(allShards, out.Shards...)
		nextToken = out.NextToken
		if nextToken == "" {
			break
		}
	}

	assert.Len(t, allShards, 5, "should get all 5 shards via pagination")

	// Verify no duplicates.
	seen := make(map[string]struct{})
	for _, s := range allShards {
		assert.NotContains(t, seen, s.ShardID, "duplicate shard %q in pagination", s.ShardID)
		seen[s.ShardID] = struct{}{}
	}
}

func TestListShards_NoMaxResults_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "list-shards-nomax",
		ShardCount: 4,
	}))

	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "list-shards-nomax"})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 4)
	assert.Empty(t, out.NextToken)
}

func TestListShards_MaxResults_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "list-shards-handler-paged",
		"ShardCount": 4,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListShards", map[string]any{
		"StreamName": "list-shards-handler-paged",
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken string `json:"NextToken"`
		Shards    []any  `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Shards, 2)
	assert.NotEmpty(t, resp.NextToken)

	// Second page — NextToken encodes stream context; StreamName must be omitted (AWS contract).
	rec = doRequest(t, h, "ListShards", map[string]any{
		"MaxResults": 2,
		"NextToken":  resp.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp2 struct {
		NextToken string `json:"NextToken"`
		Shards    []any  `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp2))
	assert.Len(t, resp2.Shards, 2)
	assert.Empty(t, resp2.NextToken, "no more pages")
}

func TestListShards_MaxResults_ExactlyFits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "list-shards-exact",
		ShardCount: 3,
	}))

	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName: "list-shards-exact",
		MaxResults: 3,
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 3)
	assert.Empty(t, out.NextToken, "should not emit next token when all fit in one page")
}

func TestListShards_WithMaxResults_PlusClosedShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-closed-paged",
		ShardCount: 2,
	}))

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "listshards-closed-paged"},
	)
	require.NoError(t, err)

	// Merge to produce 1 open + 2 closed = 3 total.
	require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
		StreamName:           "listshards-closed-paged",
		ShardToMerge:         out.Shards[0].ShardID,
		AdjacentShardToMerge: out.Shards[1].ShardID,
	}))

	// FROM_TRIM_HORIZON includes all shards; MaxResults=2 → page 1 of 2.
	list, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "listshards-closed-paged",
		ShardFilter: "FROM_TRIM_HORIZON",
		MaxResults:  2,
	})
	require.NoError(t, err)
	assert.Len(t, list.Shards, 2)
	assert.NotEmpty(t, list.NextToken)

	// Page 2.
	list2, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "listshards-closed-paged",
		ShardFilter: "FROM_TRIM_HORIZON",
		MaxResults:  2,
		NextToken:   list.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, list2.Shards, 1)
	assert.Empty(t, list2.NextToken)
}

func TestListShards_NextToken_SinglePage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-single-page",
		ShardCount: 2,
	}))

	// MaxResults > total shards → single page, no NextToken.
	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName: "listshards-single-page",
		MaxResults: 10,
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 2)
	assert.Empty(t, out.NextToken)
}

func TestListShards_NextToken_OddPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-odd-page",
		ShardCount: 7,
	}))

	var all []kinesis.ShardDescription
	nextToken := ""

	for {
		out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
			StreamName: "listshards-odd-page",
			MaxResults: 3,
			NextToken:  nextToken,
		})
		require.NoError(t, err)
		all = append(all, out.Shards...)
		nextToken = out.NextToken
		if nextToken == "" {
			break
		}
	}

	assert.Len(t, all, 7)
}

func TestListShards_ClosedShards_IncludedWithFilter(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-closed-filter",
		ShardCount: 2,
	}))

	ds, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "listshards-closed-filter"},
	)
	require.NoError(t, err)

	require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
		StreamName:           "listshards-closed-filter",
		ShardToMerge:         ds.Shards[0].ShardID,
		AdjacentShardToMerge: ds.Shards[1].ShardID,
	}))

	// Default: only open shards.
	open, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "listshards-closed-filter"})
	require.NoError(t, err)
	assert.Len(t, open.Shards, 1)

	// FROM_TRIM_HORIZON: all shards.
	all, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "listshards-closed-filter",
		ShardFilter: "FROM_TRIM_HORIZON",
	})
	require.NoError(t, err)
	assert.Len(t, all.Shards, 3)
}

func TestListShards_ExclusiveStart_WithMaxResults(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-start-max",
		ShardCount: 5,
	}))

	// Start after shard 1 (exclusive), take 2.
	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:            "listshards-start-max",
		ExclusiveStartShardID: "shardId-000000000001",
		MaxResults:            2,
	})
	require.NoError(t, err)
	require.Len(t, out.Shards, 2)
	// Should start from shard 2.
	assert.Equal(t, "shardId-000000000002", out.Shards[0].ShardID)
	assert.Equal(t, "shardId-000000000003", out.Shards[1].ShardID)
	assert.NotEmpty(t, out.NextToken)
}

// TestListShards_ShardFilterType_AfterShardID verifies the real AWS
// AFTER_SHARD_ID ShardFilter (previously gopherstack invented a nonexistent
// "AT_SHARD_ID" type with unrelated lineage-matching semantics): it acts as
// an exclusive-start cursor over ALL shards (open and closed), unlike the
// default/AT_LATEST filter which only ever considers open shards.
func TestListShards_ShardFilterType_AfterShardID(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	ctx := context.Background()
	require.NoError(t, b.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: "after-shard-id-stream",
		ShardCount: 4,
	}))

	all, err := b.ListShards(ctx, &kinesis.ListShardsInput{StreamName: "after-shard-id-stream"})
	require.NoError(t, err)
	require.Len(t, all.Shards, 4)

	out, err := b.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName:         "after-shard-id-stream",
		ShardFilterType:    "AFTER_SHARD_ID",
		ShardFilterShardID: all.Shards[0].ShardID,
	})
	require.NoError(t, err)
	require.Len(t, out.Shards, 3, "shards after shard 0")
	assert.Equal(t, all.Shards[1].ShardID, out.Shards[0].ShardID)

	// Now close a shard via merge and confirm AFTER_SHARD_ID surfaces it too
	// (includeAll), where the AT_LATEST default would not.
	require.NoError(t, b.MergeShards(ctx, &kinesis.MergeShardsInput{
		StreamName:           "after-shard-id-stream",
		ShardToMerge:         all.Shards[0].ShardID,
		AdjacentShardToMerge: all.Shards[1].ShardID,
	}))

	afterAll, err := b.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName:         "after-shard-id-stream",
		ShardFilterType:    "AFTER_SHARD_ID",
		ShardFilterShardID: all.Shards[0].ShardID,
	})
	require.NoError(t, err)
	// shards[1] (closed), shards[2] (open), shards[3] (open), plus the merged shard.
	assert.Len(t, afterAll.Shards, 4)

	defaultOut, err := b.ListShards(ctx, &kinesis.ListShardsInput{StreamName: "after-shard-id-stream"})
	require.NoError(t, err)
	// Default (open-only) excludes the two merge parents, keeping only the
	// still-open originals plus the new merged shard.
	assert.Len(t, defaultOut.Shards, 3)
}

// TestListShards_ShardFilterType_TimestampRequired verifies AT_TIMESTAMP and
// FROM_TIMESTAMP reject a request with no ShardFilterTimestamp, mirroring
// GetShardIterator's AT_TIMESTAMP requirement (see shard_iterators_test.go).
func TestListShards_ShardFilterType_TimestampRequired(t *testing.T) {
	t.Parallel()

	tests := []string{"AT_TIMESTAMP", "FROM_TIMESTAMP"}

	for _, filterType := range tests {
		t.Run(filterType, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			streamName := "ts-required-" + filterType
			doRequest(t, h, "CreateStream", map[string]any{"StreamName": streamName, "ShardCount": 1})

			rec := doRequest(t, h, "ListShards", map[string]any{
				"StreamName": streamName,
				"ShardFilter": map[string]any{
					"Type": filterType,
					// Timestamp deliberately omitted.
				},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidArgumentException", errResp.Type)
		})
	}
}

// TestListShards_ShardFilterType_AtTimestamp verifies AT_TIMESTAMP returns
// only shards that were open at the given instant: a shard closed before the
// query timestamp is excluded, a shard not yet started is excluded, and a
// shard open across the timestamp (or still open) is included.
// TestListShards_ShardFilterType_AtTimestamp, _FromTimestamp, and
// _AtTrimHorizon live in whitebox_test.go: they need direct access to the
// unexported shard StartedAt/ClosedAt fields for deterministic timestamps.

// TestListShards_ShardFilterType_UnrecognizedRejected verifies an
// unrecognized ShardFilter.Type is rejected rather than silently falling
// back to some default behavior.
func TestListShards_ShardFilterType_UnrecognizedRejected(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "bad-filter-stream",
		ShardCount: 1,
	}))

	_, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:      "bad-filter-stream",
		ShardFilterType: "NOT_A_REAL_FILTER_TYPE",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, kinesis.ErrValidation)
}
