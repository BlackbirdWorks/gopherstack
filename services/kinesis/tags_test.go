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

func createKinesisStream(t *testing.T, h *kinesis.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": name,
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestKinesis_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "tag-stream")

	// Get ARN first
	rec := doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "tag-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// TagResource (using stream name as resource)
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/tag-stream",
		"Tags":        map[string]string{"env": "test"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/tag-stream",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UntagResource
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/tag-stream",
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListTagsForResource_SortedOutput verifies sorted tag output.
func TestListTagsForResource_SortedOutput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "sorted-tags-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	arn := createStreamAndGetARN(t, h, "sorted-tags-target")

	// AddTagsToStream and ListTagsForResource share one backing store
	// (stream.Tags), so tags applied via the legacy AddTagsToStream API must be
	// visible — and sorted by key — via the ARN-based ListTagsForResource API.
	doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "sorted-tags-target",
		"Tags":       map[string]string{"zebra": "z", "apple": "a", "mango": "m"},
	})

	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tags, 3)
	assert.Equal(t, "apple", resp.Tags[0].Key)
	assert.Equal(t, "a", resp.Tags[0].Value)
	assert.Equal(t, "mango", resp.Tags[1].Key)
	assert.Equal(t, "zebra", resp.Tags[2].Key)
}

func TestAddTagsToStream_KeyTooLong(t *testing.T) {
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

func TestAddTagsToStream_EmptyKey(t *testing.T) {
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

func TestAddTagsToStream_ValueTooLong(t *testing.T) {
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

func TestAddTagsToStream_ValidBoundaryLengths(t *testing.T) {
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

func TestAddTagsToStream_StreamNotFound(t *testing.T) {
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

func TestTagResource_KeyTooLong(t *testing.T) {
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

func TestTagResource_ValueTooLong(t *testing.T) {
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

// TestInMemoryBackend_TaggedStreams covers the enumeration method cli.go's
// wireTaggingKinesis registers with the Resource Groups Tagging API (gopherstack-3xne):
// every stream with at least one tag must be visible, and untagged streams must not
// appear at all.
func TestInMemoryBackend_TaggedStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, h *kinesis.Handler) map[string]map[string]string
		name  string
	}{
		{
			name: "multiple_tagged_streams",
			setup: func(t *testing.T, h *kinesis.Handler) map[string]map[string]string {
				t.Helper()

				arn1 := createStreamAndGetARN(t, h, "tagged-stream-1")
				arn2 := createStreamAndGetARN(t, h, "tagged-stream-2")

				b := h.Backend.(*kinesis.InMemoryBackend)
				require.NoError(t, b.TagResource(context.Background(), &kinesis.TagResourceInput{
					ResourceARN: arn1, Tags: map[string]string{"env": "prod"},
				}))
				require.NoError(t, b.TagResource(context.Background(), &kinesis.TagResourceInput{
					ResourceARN: arn2, Tags: map[string]string{"team": "platform"},
				}))

				return map[string]map[string]string{
					arn1: {"env": "prod"},
					arn2: {"team": "platform"},
				}
			},
		},
		{
			name: "untagged_stream_excluded",
			setup: func(t *testing.T, h *kinesis.Handler) map[string]map[string]string {
				t.Helper()

				createStreamAndGetARN(t, h, "untagged-stream")

				return map[string]map[string]string{}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			want := tt.setup(t, h)

			b := h.Backend.(*kinesis.InMemoryBackend)
			got := b.TaggedStreams()
			gotMap := make(map[string]map[string]string, len(got))

			for _, e := range got {
				gotMap[e.ARN] = e.Tags
			}

			assert.Equal(t, want, gotMap)
		})
	}
}

func TestRemoveTagsFromStream_StreamNotFound(t *testing.T) {
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

func TestListTagsForStream_StreamNotFound(t *testing.T) {
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

func TestListTagsForStream_LimitRespected(t *testing.T) {
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

func TestListTagsForStream_LimitDefault(t *testing.T) {
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

// TestListTagsForResource verifies that tags on a stream are retrievable by ARN.
func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "existing_stream_empty_tags",
			wantCode: http.StatusOK,
		},
		{
			name:     "stream_not_found",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.wantCode == http.StatusOK {
				arn = createStreamAndGetARN(t, h, "tag-resource-stream")
			} else {
				arn = "arn:aws:kinesis:us-east-1:123:stream/no-stream"
			}

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp struct {
					Tags []struct {
						Key   string `json:"Key"`
						Value string `json:"Value"`
					} `json:"Tags"`
				}

				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp.Tags)
			}
		})
	}
}

func TestHandleAddTagsToStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a stream first
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "tag-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Add tags
	rec = doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "tag-stream",
		"Tags":       map[string]string{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags
	rec = doRequest(t, h, "ListTagsForStream", map[string]any{
		"StreamName": "tag-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
		HasMoreTags bool `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.False(t, listResp.HasMoreTags)
	assert.Len(t, listResp.Tags, 2)

	tagMap := make(map[string]string)
	for _, tag := range listResp.Tags {
		tagMap[tag.Key] = tag.Value
	}
	assert.Equal(t, "prod", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])
}

func TestHandleRemoveTagsFromStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "rm-tag-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Add tags
	rec = doRequest(t, h, "AddTagsToStream", map[string]any{
		"StreamName": "rm-tag-stream",
		"Tags":       map[string]string{"env": "prod", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Remove one tag
	rec = doRequest(t, h, "RemoveTagsFromStream", map[string]any{
		"StreamName": "rm-tag-stream",
		"TagKeys":    []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify remaining tags
	rec = doRequest(t, h, "ListTagsForStream", map[string]any{
		"StreamName": "rm-tag-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Tags, 1)
	assert.Equal(t, "team", listResp.Tags[0].Key)
}

func TestHandleListTagsForStreamEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "no-tags-stream", "ShardCount": 1})

	rec := doRequest(t, h, "ListTagsForStream", map[string]any{
		"StreamName": "no-tags-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
		HasMoreTags bool `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Tags)
	assert.False(t, listResp.HasMoreTags)
}

// TestListTagsForResource_TagsFromCreate verifies that tags supplied
// inline on CreateStream are visible via ListTagsForResource. Both paths write
// through to the backend's persisted stream.Tags store (see
// TestTags_SurvivePersistenceRestore for the restart case).
func TestListTagsForResource_TagsFromCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]any
		name       string
		streamName string
		tagKey     string
		tagValue   string
	}{
		{
			name:       "tags_from_create",
			streamName: "ltfr-stream-1",
			tags:       map[string]any{"project": "gopherstack"},
			tagKey:     "project",
			tagValue:   "gopherstack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
				"Tags":       tt.tags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeStream", map[string]any{
				"StreamName": tt.streamName,
			})
			var descResp struct {
				StreamDescription struct {
					StreamARN string `json:"StreamARN"`
				} `json:"StreamDescription"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))

			rec3 := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": descResp.StreamDescription.StreamARN,
			})
			require.Equal(t, http.StatusOK, rec3.Code)

			var tagsResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &tagsResp))

			found := false
			for _, kv := range tagsResp.Tags {
				if kv.Key == tt.tagKey {
					assert.Equal(t, tt.tagValue, kv.Value)
					found = true
				}
			}
			assert.True(t, found, "expected tag key %q not found", tt.tagKey)
		})
	}
}
