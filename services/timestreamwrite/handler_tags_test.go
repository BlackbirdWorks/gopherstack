package timestreamwrite_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TagResource_UntagResource_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create the database first so that the ARN is known to the backend.
	dbRec := doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "my-db"})
	require.Equal(t, http.StatusOK, dbRec.Code)

	arn := "arn:aws:timestream:us-east-1:000000000000:database/my-db"

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].([]any)
	assert.True(t, ok)
	assert.Len(t, tags, 2)

	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"team"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, ok = listResp["Tags"].([]any)
	assert.True(t, ok)
	assert.Len(t, tags, 1)
}

func TestHandler_TagResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "",
		"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "",
		"TagKeys":     []string{"k"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListTagsForResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ListTagsForResource_Sorted verifies tags are sorted by key.
func TestHandler_ListTagsForResource_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]any{"DatabaseName": "sort-db"})

	descRec := doRequest(t, h, "DescribeDatabase", map[string]any{"DatabaseName": "sort-db"})
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	arn := descOut["Database"].(map[string]any)["Arn"].(string)

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]string{
			{"Key": "zzz", "Value": "last"},
			{"Key": "aaa", "Value": "first"},
			{"Key": "mmm", "Value": "middle"},
		},
	})

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tags := tagsOut["Tags"].([]any)
	require.Len(t, tags, 3)
	assert.Equal(t, "aaa", tags[0].(map[string]any)["Key"])
	assert.Equal(t, "mmm", tags[1].(map[string]any)["Key"])
	assert.Equal(t, "zzz", tags[2].(map[string]any)["Key"])
}

// TestHandler_TagResource_KeyValidation verifies that TagResource validates
// tag keys and values.
func TestHandler_TagResource_KeyValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "tagres-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/tagres-db"

	tests := []struct {
		name       string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name:       "valid tags",
			tags:       []map[string]string{{"Key": "team", "Value": "platform"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty key rejected",
			tags:       []map[string]string{{"Key": "", "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "oversized key rejected",
			tags:       []map[string]string{{"Key": strings.Repeat("x", 129), "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "oversized value rejected",
			tags:       []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 257)}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceARN": arn,
				"Tags":        tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_TagResource_HTTP400OnUnknownARN verifies the handler returns
// 400 (ResourceNotFoundException) when the ARN does not exist.
func TestHandler_TagResource_HTTP400OnUnknownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "arn:aws:timestream:us-east-1:000000000000:database/ghost-db",
		"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestHandler_TagResource_ScheduledQueryARN verifies that scheduled-query
// ARNs can be tagged via the write service's unified tag store.
func TestHandler_TagResource_ScheduledQueryARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sqARN := "arn:aws:timestream:us-east-1:000000000000:scheduled-query/my-query"

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": sqARN,
		"Tags":        []map[string]string{{"Key": "owner", "Value": "alice"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": sqARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))

	tags := out["Tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "owner", tag["Key"])
	assert.Equal(t, "alice", tag["Value"])
}

// TestHandler_UntagResource_Idempotent verifies that UntagResource succeeds
// even when the tag key does not exist on the resource.
func TestHandler_UntagResource_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "untag-idem-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/untag-idem-db"

	// Untag a key that was never tagged — should succeed without error.
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"nonexistent-key"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_ListTagsForResource_EmptyWhenNoTags verifies that
// ListTagsForResource returns an empty list (not an error) for an ARN that
// has no tags stored.
func TestHandler_ListTagsForResource_EmptyWhenNoTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "empty-tags-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/empty-tags-db"

	rec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Tags field should be present and empty (not absent).
	tags, ok := out["Tags"]
	require.True(t, ok, "Tags field should be present even when empty")
	assert.Empty(t, tags, "Tags list should be empty for a resource with no tags")
}

// TestHandler_UntagResource_FromKnownARN verifies that tagging then untagging
// works correctly and leaves no residual tags.
func TestHandler_UntagResource_FromKnownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "untag-known-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/untag-known-db"

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]string{
			{"Key": "alpha", "Value": "1"},
			{"Key": "beta", "Value": "2"},
		},
	})

	// Untag one key.
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"alpha"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List remaining tags.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))

	tags := out["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "beta", tags[0].(map[string]any)["Key"])
}

// TestHandler_TagResource_CumulativeUpdates verifies that calling TagResource
// multiple times accumulates tags on the resource (existing tags are
// preserved and new tags are added).
func TestHandler_TagResource_CumulativeUpdates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateDatabase", map[string]string{"DatabaseName": "cumtag-db"})

	arn := "arn:aws:timestream:us-east-1:000000000000:database/cumtag-db"

	// First TagResource call.
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        []map[string]string{{"Key": "env", "Value": "staging"}},
	})

	// Second TagResource call adds a new tag.
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        []map[string]string{{"Key": "team", "Value": "platform"}},
	})

	// Third TagResource call updates an existing tag.
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
	})

	listRec := doRequest(t, h, "ListTagsForResource", map[string]string{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))

	tags := out["Tags"].([]any)
	require.Len(t, tags, 2, "should have two distinct tags")

	tagMap := make(map[string]string, 2)
	for _, rawTag := range tags {
		tag := rawTag.(map[string]any)
		tagMap[tag["Key"].(string)] = tag["Value"].(string)
	}

	assert.Equal(t, "prod", tagMap["env"], "env tag should be updated to prod")
	assert.Equal(t, "platform", tagMap["team"], "team tag should be present")
}
