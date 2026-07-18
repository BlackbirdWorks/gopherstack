package firehose_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// indexOf returns the byte offset of substr in s, or -1 if not found.
func indexOf(s, substr string) int {
	if len(substr) > len(s) {
		return -1
	}

	for i := range len(s) - len(substr) + 1 {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}

	return -1
}

func TestFirehoseHandler_TagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		tags       []map[string]string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
				"Tags":               tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestFirehoseHandler_UntagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		tagKeys    []string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			tagKeys:    []string{"env"},
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
				doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
					"DeliveryStreamName": "my-stream",
					"Tags":               []map[string]string{{"Key": "env", "Value": "prod"}},
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			tagKeys:    []string{"env"},
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "UntagDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
				"TagKeys":            tt.tagKeys,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestUntagDeliveryStream_RemovesSpecificKeys verifies that only the requested tag keys
// are removed, leaving the rest intact.
func TestUntagDeliveryStream_RemovesSpecificKeys(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "untag-stream")

	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "untag-stream",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
			{"Key": "version", "Value": "1"},
		},
	})

	rec := doFirehoseRequest(t, h, "UntagDeliveryStream", map[string]any{
		"DeliveryStreamName": "untag-stream",
		"TagKeys":            []string{"env", "version"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	list := doFirehoseRequest(t, h, "ListTagsForDeliveryStream",
		map[string]any{"DeliveryStreamName": "untag-stream"})
	require.Equal(t, http.StatusOK, list.Code)

	var out struct {
		Tags []map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &out))
	require.Len(t, out.Tags, 1)
	assert.Equal(t, "team", out.Tags[0]["Key"])
}

func TestUntagDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "UntagDeliveryStream", map[string]any{
		"DeliveryStreamName": "no-such-stream",
		"TagKeys":            []string{"k"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFirehoseHandler_ListTagsForDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *firehose.Handler)
		name         string
		streamName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "empty_tags",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"Tags", "HasMoreTags"},
		},
		{
			name:       "with_tags",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
				doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
					"DeliveryStreamName": "my-stream",
					"Tags":               []map[string]string{{"Key": "env", "Value": "prod"}},
				})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"env", "prod"},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.streamName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestListTagsForDeliveryStream_Sorted verifies that tags are returned sorted
// alphabetically by key.
func TestListTagsForDeliveryStream_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "tagged-stream"})
	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
		"Tags": []map[string]any{
			{"Key": "zebra", "Value": "z"},
			{"Key": "alpha", "Value": "a"},
			{"Key": "middle", "Value": "m"},
		},
	})

	rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	alphaIdx := indexOf(body, "alpha")
	middleIdx := indexOf(body, "middle")
	zebraIdx := indexOf(body, "zebra")

	assert.Less(t, alphaIdx, middleIdx, "alpha should come before middle")
	assert.Less(t, middleIdx, zebraIdx, "middle should come before zebra")
}

// TestListTagsForDeliveryStream_Limit verifies the Limit parameter and HasMoreTags flag.
func TestListTagsForDeliveryStream_Limit(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "tagged-stream")

	tags := make([]map[string]string, 10)
	for i := range tags {
		tags[i] = map[string]string{"Key": string(rune('a'+i)) + "-key", "Value": "v"}
	}

	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
		"Tags":               tags,
	})

	rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
		"Limit":              3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Tags        []map[string]string `json:"Tags"`
		HasMoreTags bool                `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Tags, 3)
	assert.True(t, out.HasMoreTags)
}

// TestListTagsForDeliveryStream_ExclusiveStart verifies the ExclusiveStartTagKey cursor.
func TestListTagsForDeliveryStream_ExclusiveStart(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "tagged-stream")

	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
		"Tags": []map[string]string{
			{"Key": "a-key", "Value": "1"},
			{"Key": "b-key", "Value": "2"},
			{"Key": "c-key", "Value": "3"},
		},
	})

	rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName":   "tagged-stream",
		"ExclusiveStartTagKey": "a-key",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Tags, 2)
	assert.Equal(t, "b-key", out.Tags[0].Key)
	assert.Equal(t, "c-key", out.Tags[1].Key)
}

// TestTagValidation_TooManyTags verifies that more than 50 tags on
// CreateDeliveryStream is rejected.
func TestTagValidation_TooManyTags(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	tags := make([]map[string]string, 51)
	for i := range tags {
		tags[i] = map[string]string{"Key": string(rune('A'+i%26)) + "-key", "Value": "v"}
	}

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-overflow",
		"Tags":               tags,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestTagValidation_KeyTooLong verifies that a tag key over 128 characters is rejected.
func TestTagValidation_KeyTooLong(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "tag-key-len")

	longKey := make([]byte, 129)
	for i := range longKey {
		longKey[i] = 'x'
	}

	rec := doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-key-len",
		"Tags":               []map[string]string{{"Key": string(longKey), "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestTagValidation_ValueTooLong verifies that a tag value over 256 characters is
// rejected.
func TestTagValidation_ValueTooLong(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "tag-val-len")

	longVal := make([]byte, 257)
	for i := range longVal {
		longVal[i] = 'v'
	}

	rec := doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-val-len",
		"Tags":               []map[string]string{{"Key": "k", "Value": string(longVal)}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestTagsLifecycle verifies add-at-create, add, list (with ExclusiveStartTagKey
// pagination), and untag end-to-end via the handler.
func TestTagsLifecycle(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	// Create with tags.
	auditCreateStream(t, h, "tag-stream", map[string]any{
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
		},
	})

	// Add more tags.
	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-stream",
		"Tags": []map[string]string{
			{"Key": "region", "Value": "us-east-1"},
			{"Key": "team", "Value": "platform"},
			{"Key": "version", "Value": "2"},
		},
	})

	// List all tags (sorted alphabetically by key).
	listRec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream",
		map[string]any{"DeliveryStreamName": "tag-stream"})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Tags        []map[string]string `json:"Tags"`
		HasMoreTags bool                `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Tags, 4)
	assert.False(t, listOut.HasMoreTags)
	// Keys must be sorted: env, region, team, version.
	assert.Equal(t, "env", listOut.Tags[0]["Key"])
	assert.Equal(t, "region", listOut.Tags[1]["Key"])

	// Paginate: list first 2 only.
	page1Rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-stream",
		"Limit":              2,
	})
	var page1 struct {
		Tags        []map[string]string `json:"Tags"`
		HasMoreTags bool                `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(page1Rec.Body.Bytes(), &page1))
	require.Len(t, page1.Tags, 2)
	assert.True(t, page1.HasMoreTags)
	assert.Equal(t, "env", page1.Tags[0]["Key"])
	lastKey := page1.Tags[1]["Key"] // "region"

	// Page 2 using ExclusiveStartTagKey cursor.
	page2Rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName":   "tag-stream",
		"Limit":                2,
		"ExclusiveStartTagKey": lastKey,
	})
	var page2 struct {
		Tags        []map[string]string `json:"Tags"`
		HasMoreTags bool                `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(page2Rec.Body.Bytes(), &page2))
	require.Len(t, page2.Tags, 2)
	assert.False(t, page2.HasMoreTags)
	assert.Equal(t, "team", page2.Tags[0]["Key"])
	assert.Equal(t, "version", page2.Tags[1]["Key"])

	// Untag two keys.
	doFirehoseRequest(t, h, "UntagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-stream",
		"TagKeys":            []string{"env", "team"},
	})

	// Verify only 2 remain.
	finalRec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream",
		map[string]any{"DeliveryStreamName": "tag-stream"})
	var finalOut struct {
		Tags []map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(finalRec.Body.Bytes(), &finalOut))
	require.Len(t, finalOut.Tags, 2)

	keys := map[string]bool{}
	for _, tag := range finalOut.Tags {
		keys[tag["Key"]] = true
	}
	assert.True(t, keys["region"])
	assert.True(t, keys["version"])
	assert.False(t, keys["env"])
	assert.False(t, keys["team"])
}
