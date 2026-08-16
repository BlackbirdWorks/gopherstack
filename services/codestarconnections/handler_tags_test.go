package codestarconnections_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

func TestHandler_TagResource_Connection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection(context.Background(), "tagged-conn", "GitHub", "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": conn.ConnectionArn})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	tags, ok := out["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "prod", tag["Value"])
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:codestar-connections:us-east-1:000000000000:connection/nonexistent",
		"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection(
		context.Background(),
		"untag-conn",
		"GitHub",
		"",
		map[string]string{"env": "prod", "team": "ops"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tags, err := h.Backend.ListTagsForResource(context.Background(), conn.ConnectionArn)
	require.NoError(t, err)
	assert.NotContains(t, tags, "env")
	assert.Contains(t, tags, "team")
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection(
		context.Background(),
		"list-tags-conn",
		"GitHub",
		"",
		map[string]string{"k1": "v1"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": conn.ConnectionArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	tags, ok := out["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "k1", tag["Key"])
	assert.Equal(t, "v1", tag["Value"])
}

// TestSortedTags verifies ListTagsForResource returns tags sorted by key.
func TestSortedTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "tagged-conn",
		"ProviderType":   "GitHub",
		"Tags": []map[string]any{
			{"Key": "z-tag", "Value": "z"},
			{"Key": "a-tag", "Value": "a"},
			{"Key": "m-tag", "Value": "m"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	arn := resp["ConnectionArn"].(string)

	recTags := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, recTags.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recTags.Body.Bytes(), &out))
	tags := out["Tags"].([]any)

	keys := make([]string, len(tags))
	for i, tag := range tags {
		keys[i] = tag.(map[string]any)["Key"].(string)
	}

	assert.Equal(t, []string{"a-tag", "m-tag", "z-tag"}, keys)
}

// TestTagsDeepCopy verifies modifying returned connection doesn't affect stored data.
func TestTagsDeepCopy(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")

	conn, err := b.CreateConnection(context.Background(), "dc-conn", "GitHub", "", map[string]string{"k": "v1"})
	require.NoError(t, err)

	// Modify the returned copy's tags.
	conn.Tags["k"] = "mutated"

	// Original stored conn must be unaffected.
	got, err := b.GetConnection(context.Background(), conn.ConnectionArn)
	require.NoError(t, err)
	assert.Equal(t, "v1", got.Tags["k"])
}

// TestTagResourceOnHost verifies TagResource works on hosts.
func TestTagResourceOnHost(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "tag-host",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	hostArn := parseResp(t, rec)["HostArn"].(string)

	recTag := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": hostArn,
		"Tags":        []map[string]any{{"Key": "purpose", "Value": "ci"}},
	})
	require.Equal(t, http.StatusOK, recTag.Code)

	recList := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	tags := out["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "ci", tags[0].(map[string]any)["Value"].(string))
}

// TestUntagResourceRemovesKeys verifies UntagResource removes specified keys.
func TestUntagResourceRemovesKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "untag-conn",
		"ProviderType":   "GitHub",
		"Tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn := parseResp(t, rec)["ConnectionArn"].(string)

	recUntag := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []string{"remove"},
	})
	require.Equal(t, http.StatusOK, recUntag.Code)

	recList := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	tags := out["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "keep", tags[0].(map[string]any)["Key"].(string))
}

func TestTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name:       "valid single tag",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty value is valid",
			tags:       []map[string]string{{"Key": "flag", "Value": ""}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag key too long (129 chars)",
			tags:       []map[string]string{{"Key": repeatStr("k", 129), "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag key max length (128 chars) is valid",
			tags:       []map[string]string{{"Key": repeatStr("k", 128), "Value": "v"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag value too long (257 chars)",
			tags:       []map[string]string{{"Key": "k", "Value": repeatStr("v", 257)}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag value max length (256 chars) is valid",
			tags:       []map[string]string{{"Key": "k", "Value": repeatStr("v", 256)}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty tag key",
			tags:       []map[string]string{{"Key": "", "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"ConnectionName": "tag-conn-" + string(rune('a'+i)),
				"ProviderType":   "GitHub",
				"Tags":           tt.tags,
			}

			rec := doRequest(t, h, "CreateConnection", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "test %q", tt.name)
		})
	}
}

func repeatStr(s string, n int) string {
	result := make([]byte, n*len(s))
	for i := range n {
		copy(result[i*len(s):], s)
	}

	return string(result)
}

func TestTagResource_CountLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection(context.Background(), "limit-conn", "GitHub", "", nil)
	require.NoError(t, err)

	// Apply 199 tags (under limit).
	tags1 := make([]map[string]string, 199)
	for i := range 199 {
		tags1[i] = map[string]string{"Key": "k" + string(rune('A'+i%26)) + string(rune('0'+i/26)), "Value": "v"}
	}

	rec1 := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"Tags":        tags1,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Add 1 more tag (total 200 - at limit, OK).
	rec2 := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"Tags":        []map[string]string{{"Key": "boundary-tag", "Value": "ok"}},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Try to add 1 more (total 201 - over limit). Real TagResource's complete
	// error list is [ResourceNotFoundException, LimitExceededException]
	// (botocore codestar-connections/2019-12-01/service-2.json) -- there is
	// no InvalidInputException for this op at all, so exceeding the tag
	// count must map to LimitExceededException specifically.
	rec3 := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"Tags":        []map[string]string{{"Key": "over-limit", "Value": "nope"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)

	resp3 := parseResp(t, rec3)
	assert.Equal(t, "LimitExceededException", resp3["__type"])
}

// TestCreateConnection_TagLimitExceeded verifies CreateConnection maps
// too-many-initial-tags to LimitExceededException, the only error
// CreateHost's real error list documents at all, and one of CreateConnection's
// three real errors (botocore codestar-connections/2019-12-01/service-2.json).
func TestCreateConnection_TagLimitExceeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tags := make([]map[string]string, 201)
	for i := range tags {
		tags[i] = map[string]string{"Key": "k" + string(rune('A'+i%26)) + string(rune('0'+i/26)), "Value": "v"}
	}

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "over-tag-conn",
		"ProviderType":   "GitHub",
		"Tags":           tags,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	resp := parseResp(t, rec)
	assert.Equal(t, "LimitExceededException", resp["__type"])
}

// TestRepositoryLink_Tags verifies CreateRepositoryLink accepts a Tags
// member (real CreateRepositoryLinkInput has a `Tags []types.Tag` field,
// confirmed against aws-sdk-go-v2's generated api_op_CreateRepositoryLink.go)
// and that the created repository link is taggable via
// TagResource/UntagResource/ListTagsForResource by its RepositoryLinkArn,
// exactly like connections and hosts.
func TestRepositoryLink_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "rl-tags-conn", "GitHub")

	rec := doRequest(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":  connArn,
		"OwnerId":        "rl-tags-owner",
		"RepositoryName": "rl-tags-repo",
		"Tags":           []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	info := resp["RepositoryLinkInfo"].(map[string]any)
	// RepositoryLinkInfo itself has no Tags member in the real wire shape --
	// tags set at creation are only visible via ListTagsForResource.
	_, hasTags := info["Tags"]
	assert.False(t, hasTags, "RepositoryLinkInfo must not include a Tags field")
	linkArn := info["RepositoryLinkArn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
	require.Equal(t, http.StatusOK, listRec.Code)
	listResp := parseResp(t, listRec)
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])
	assert.Equal(t, "prod", tags[0].(map[string]any)["Value"])

	// TagResource/UntagResource also operate on the repository link ARN.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": linkArn,
		"Tags":        []map[string]string{{"Key": "team", "Value": "platform"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec2 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
	require.Equal(t, http.StatusOK, listRec2.Code)
	tags2 := parseResp(t, listRec2)["Tags"].([]any)
	require.Len(t, tags2, 2)

	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": linkArn,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec3 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
	require.Equal(t, http.StatusOK, listRec3.Code)
	tags3 := parseResp(t, listRec3)["Tags"].([]any)
	require.Len(t, tags3, 1)
	assert.Equal(t, "team", tags3[0].(map[string]any)["Key"])
}

// TestTags_NonNilOnList verifies Tags field is always non-nil (empty slice not null).
func TestTags_NonNilOnList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "empty_tags_not_null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "tags-test-conn", "GitHub")

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok, "Tags must be an array, not null")
			assert.Empty(t, tags)
		})
	}
}

// TestTagResource_OnHost verifies tags can be managed on hosts.
func TestTagResource_OnHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag_untag_host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			hostArn := createCSCHost(t, h, "tag-host", "GitHubEnterpriseServer", "https://ghe.example.com")

			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": hostArn,
				"Tags":        []map[string]string{{"Key": "Env", "Value": "prod"}, {"Key": "Owner", "Value": "ops"}},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := parseResp(t, listRec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, 2)

			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": hostArn,
				"TagKeys":     []string{"Owner"},
			})
			require.Equal(t, http.StatusOK, untagRec.Code)

			listRec2 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
			require.Equal(t, http.StatusOK, listRec2.Code)
			resp2 := parseResp(t, listRec2)
			tags2, ok := resp2["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags2, 1)
		})
	}
}
