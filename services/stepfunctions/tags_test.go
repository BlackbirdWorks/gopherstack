package stepfunctions_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     string
		wantCode int
	}{
		{
			name:     "tags state machine successfully",
			tags:     `{"env":"prod","team":"infra"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			arn := createSM(ctx, t, h, e, "tag-sm")
			rec := sfnPost(ctx, t, h, e, "TagResource",
				`{"resourceArn":"`+arn+`","tags":`+tt.tags+`}`)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "returns tags for tagged resource",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			arn := createSM(ctx, t, h, e, "list-tag-sm")
			sfnPost(ctx, t, h, e, "TagResource", `{"resourceArn":"`+arn+`","tags":{"env":"prod"}}`)

			rec := sfnPost(ctx, t, h, e, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tags := resp["tags"].([]any)
			assert.NotEmpty(t, tags)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tagKeys      string
		wantTagKey   string
		wantTagValue string
		wantCode     int
		wantTagCount int
	}{
		{
			name:         "removes specified tag and leaves remaining tags",
			tagKeys:      `["team"]`,
			wantCode:     http.StatusOK,
			wantTagCount: 1,
			wantTagKey:   "env",
			wantTagValue: "prod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)

			arn := createSM(ctx, t, h, e, "untag-sm")
			sfnPost(ctx, t, h, e, "TagResource",
				`{"resourceArn":"`+arn+`","tags":{"env":"prod","team":"infra"}}`)

			rec := sfnPost(ctx, t, h, e, "UntagResource",
				`{"resourceArn":"`+arn+`","tagKeys":`+tt.tagKeys+`}`)
			assert.Equal(t, tt.wantCode, rec.Code)

			listRec := sfnPost(ctx, t, h, e, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			tags := resp["tags"].([]any)
			assert.Len(t, tags, tt.wantTagCount)

			tag := tags[0].(map[string]any)
			assert.Equal(t, tt.wantTagKey, tag["key"])
			assert.Equal(t, tt.wantTagValue, tag["value"])
		})
	}
}

func TestTags_CreateWithInlineTags(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)

	body := map[string]any{
		"name":       "tagged-sm",
		"definition": validPassDef,
		"roleArn":    validRoleARN,
		"tags": []map[string]any{
			{"key": "env", "value": "prod"},
			{"key": "team", "value": "core"},
		},
	}

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(raw))
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	arnStr := out["stateMachineArn"].(string)
	listBody, err := json.Marshal(map[string]any{"resourceArn": arnStr})
	require.NoError(t, err)

	listRec := sfnPost(ctx, t, h, e, "ListTagsForResource", string(listBody))
	require.Equal(t, http.StatusOK, listRec.Code)

	var tags map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tags))

	tagList, _ := tags["tags"].([]any)
	assert.Len(t, tagList, 2)
}

func TestTags_TagAndUntag(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	arnStr := createSM(ctx, t, h, e, "tag-sm")

	// Tag — this mock expects tags as a JSON object {"key":"value"}, not an AWS-style array.
	tagBody, err := json.Marshal(map[string]any{
		"resourceArn": arnStr,
		"tags":        map[string]string{"k1": "v1"},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(tagBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify
	listBody, err := json.Marshal(map[string]any{"resourceArn": arnStr})
	require.NoError(t, err)

	listRec := sfnPost(ctx, t, h, e, "ListTagsForResource", string(listBody))
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagOut))

	tagList, _ := tagOut["tags"].([]any)
	assert.Len(t, tagList, 1)

	// Untag
	untagBody, err := json.Marshal(map[string]any{
		"resourceArn": arnStr,
		"tagKeys":     []string{"k1"},
	})
	require.NoError(t, err)

	untagRec := sfnPost(ctx, t, h, e, "UntagResource", string(untagBody))
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec2 := sfnPost(ctx, t, h, e, "ListTagsForResource", string(listBody))
	require.Equal(t, http.StatusOK, listRec2.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &after))

	tagList2, _ := after["tags"].([]any)
	assert.Empty(t, tagList2)
}

// ─── Logging / Tracing / Encryption configs ───────────────────────────────────

func TestTagResource_KeyTooLong_Error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "tag-key-len-sm")

	longKey := strings.Repeat("k", 129)
	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        map[string]string{longKey: "val"},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TagPolicyViolation", resp["__type"])
}

func TestTagResource_EmptyKey_Error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "tag-empty-key-sm")

	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        map[string]string{"": "val"},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TagPolicyViolation", resp["__type"])
}

func TestTagResource_ValueTooLong_Error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "tag-val-len-sm")

	longVal := strings.Repeat("v", 257)
	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        map[string]string{"mykey": longVal},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TagPolicyViolation", resp["__type"])
}

func TestTagResource_MaxTagsExceeded_Error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	smARN := createSM(ctx, t, h, e, "tag-max-count-sm")

	// Add 50 tags in batches of 10.
	for i := range 5 {
		batch := make(map[string]string, 10)
		for j := range 10 {
			batch["key-"+string(rune('a'+i))+string(rune('0'+j))] = "val"
		}
		body, err := json.Marshal(map[string]any{
			"resourceArn": smARN,
			"tags":        batch,
		})
		require.NoError(t, err)
		rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Adding one more tag should fail.
	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        map[string]string{"overflow-key": "val"},
	})
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// AWS: TagResource's own error switch models TooManyTags for exceeding
	// the per-resource tag limit -- "TagPolicyViolation" names no type
	// anywhere in this SDK.
	assert.Equal(t, "TooManyTags", resp["__type"])
}

func TestTagResource_ValidTagsAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "max_key_length", key: strings.Repeat("k", 128), value: "val"},
		{name: "max_value_length", key: "k", value: strings.Repeat("v", 256)},
		{name: "empty_value_ok", key: "kk", value: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "tag-valid-sm-"+tt.name[:min(len(tt.name), 15)])

			body, err := json.Marshal(map[string]any{
				"resourceArn": smARN,
				"tags":        map[string]string{tt.key: tt.value},
			})
			require.NoError(t, err)

			rec := sfnPost(ctx, t, h, e, "TagResource", string(body))
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// ─── ListExecutions Sort Order ────────────────────────────────────────────────

func TestHandler_GetTags_EmptyAndNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupTags   bool
		wantTagsLen int
	}{
		{
			name:        "no_tags_returns_empty_map",
			setupTags:   false,
			wantTagsLen: 0,
		},
		{
			name:        "with_tags_returns_them",
			setupTags:   true,
			wantTagsLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h, e := newSFNHandler(t)
			smARN := createSM(ctx, t, h, e, "tags-sm-"+tt.name)

			if tt.setupTags {
				rec := sfnPost(ctx, t, h, e, "TagResource",
					`{"resourceArn":"`+smARN+`","tags":{"mykey":"myval"}}`)
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			rec := sfnPost(ctx, t, h, e, "ListTagsForResource",
				`{"resourceArn":"`+smARN+`"}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			tags := resp["tags"].([]any)
			assert.Len(t, tags, tt.wantTagsLen)
		})
	}
}

// ---- ExtractResource edge cases ----
