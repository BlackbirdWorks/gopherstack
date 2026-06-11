package kinesis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// ---------------------------------------------------------------------------
// Constraint 1: Tag key/value length validation (AddTagsToStream, TagResource)
// ---------------------------------------------------------------------------

func TestAudit2_AddTagsToStream_KeyTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "tag-kv-stream", "ShardCount": 1})

	longKey := strings.Repeat("k", 129)
	rec := doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "tag-kv-stream",
		"Tags":       map[string]string{longKey: "value"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidArgumentException", resp.Type)
}

func TestAudit2_AddTagsToStream_EmptyKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "tag-emptykey-stream", "ShardCount": 1})

	rec := doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "tag-emptykey-stream",
		"Tags":       map[string]string{"": "value"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidArgumentException", resp.Type)
}

func TestAudit2_AddTagsToStream_ValueTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "tag-val-stream", "ShardCount": 1})

	longVal := strings.Repeat("v", 257)
	rec := doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "tag-val-stream",
		"Tags":       map[string]string{"validkey": longVal},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidArgumentException", resp.Type)
}

func TestAudit2_AddTagsToStream_ValidBoundaryLengths(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "tag-boundary-stream", "ShardCount": 1})

	maxKey := strings.Repeat("k", 128)
	maxVal := strings.Repeat("v", 256)
	rec := doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "tag-boundary-stream",
		"Tags":       map[string]string{maxKey: maxVal, "empty-val": ""},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit2_AddTagsToStream_StreamNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "nonexistent-stream",
		"Tags":       map[string]string{"k": "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
}

func TestAudit2_TagResource_KeyTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "tagres-kv-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "tagres-kv-stream"})
	require.NoError(t, err)

	longKey := strings.Repeat("k", 129)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": desc.StreamARN,
		"Tags":        map[string]string{longKey: "value"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidArgumentException", resp.Type)
}

func TestAudit2_TagResource_ValueTooLong(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "tagres-val-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "tagres-val-stream"})
	require.NoError(t, err)

	longVal := strings.Repeat("v", 257)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": desc.StreamARN,
		"Tags":        map[string]string{"validkey": longVal},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidArgumentException", resp.Type)
}

// ---------------------------------------------------------------------------
// Constraint 2: Stream existence for legacy tag operations
// ---------------------------------------------------------------------------

func TestAudit2_RemoveTagsFromStream_StreamNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "RemoveTagsFromStream", map[string]any{
		"StreamName": "ghost-stream",
		"TagKeys":    []string{"k"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
}

func TestAudit2_ListTagsForStream_StreamNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListTagsForStream", map[string]any{
		"StreamName": "ghost-stream",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
}

// ---------------------------------------------------------------------------
// Constraint 3: ListTagsForStream Limit parameter
// ---------------------------------------------------------------------------

func TestAudit2_ListTagsForStream_LimitRespected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "limit-tags-stream", "ShardCount": 1})

	// Add 20 tags.
	tags := make(map[string]string, 20)
	for i := range 20 {
		tags[strings.Repeat("a", 1)+string(rune('a'+i))] = "v"
	}
	doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "limit-tags-stream",
		"Tags":       tags,
	})

	// Request 5 tags.
	rec := doRequest(t, h, "ListTagsForStream", map[string]any{
		"StreamName": "limit-tags-stream",
		"Limit":      5,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key string `json:"Key"`
		} `json:"Tags"`
		HasMoreTags bool `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Tags, 5)
	assert.True(t, resp.HasMoreTags)
}

func TestAudit2_ListTagsForStream_LimitDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "default-limit-stream", "ShardCount": 1})

	// Add 15 tags.
	tags := make(map[string]string, 15)
	for i := range 15 {
		tags[strings.Repeat("b", 1)+string(rune('a'+i))] = "v"
	}
	doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "default-limit-stream",
		"Tags":       tags,
	})

	// No Limit → default 10.
	rec := doRequest(t, h, "ListTagsForStream", map[string]any{
		"StreamName": "default-limit-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key string `json:"Key"`
		} `json:"Tags"`
		HasMoreTags bool `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Tags, 10)
	assert.True(t, resp.HasMoreTags)
}

// ---------------------------------------------------------------------------
// Constraint 4: MergeShards/SplitShard reject ON_DEMAND streams
// ---------------------------------------------------------------------------

func TestAudit2_MergeShards_OnDemandRejected(t *testing.T) {
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

func TestAudit2_SplitShard_OnDemandRejected(t *testing.T) {
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

func TestAudit2_MergeShards_ProvisionedAllowed(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Constraint 5: RegisterStreamConsumer name validation
// ---------------------------------------------------------------------------

func TestAudit2_RegisterStreamConsumer_InvalidName(t *testing.T) {
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

func TestAudit2_RegisterStreamConsumer_ValidNames(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Constraint 6: ListStreamConsumers MaxResults pagination
// ---------------------------------------------------------------------------

func TestAudit2_ListStreamConsumers_MaxResultsPagination(t *testing.T) {
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

func TestAudit2_ListStreamConsumers_NoMaxResults_ReturnsAll(t *testing.T) {
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
