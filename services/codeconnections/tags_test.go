package codeconnections_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestTagResource exercises the TagResource handler.
func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codeconnections.Handler) string
		name       string
		inputTags  []map[string]string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "conn", "GitHub")
			},
			inputTags:  []map[string]string{{"Key": "Team", "Value": "platform"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:connection/missing"
			},
			inputTags:  []map[string]string{{"Key": "k", "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)

			rec := doJSON(t, h, "TagResource", map[string]any{
				"ResourceArn": connArn,
				"Tags":        tt.inputTags,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestUntagResource exercises the UntagResource handler.
func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *codeconnections.Handler) string
		name          string
		tagsBefore    []map[string]string
		keysToRemove  []string
		wantStatus    int
		wantTagsAfter int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "conn", "GitHub")
			},
			tagsBefore: []map[string]string{
				{"Key": "Team", "Value": "p"},
				{"Key": "Env", "Value": "prod"},
			},
			keysToRemove:  []string{"Team"},
			wantStatus:    http.StatusOK,
			wantTagsAfter: 1,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:connection/missing"
			},
			keysToRemove: []string{"k"},
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)

			if len(tt.tagsBefore) > 0 {
				tagRec := doJSON(t, h, "TagResource", map[string]any{
					"ResourceArn": connArn,
					"Tags":        tt.tagsBefore,
				})
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			rec := doJSON(t, h, "UntagResource", map[string]any{
				"ResourceArn": connArn,
				"TagKeys":     tt.keysToRemove,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				listRec := doJSON(
					t,
					h,
					"ListTagsForResource",
					map[string]any{"ResourceArn": connArn},
				)
				resp := parseResp(t, listRec)
				tags, ok := resp["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, tt.wantTagsAfter)
			}
		})
	}
}

// TestListTagsForResource exercises the ListTagsForResource handler.
func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codeconnections.Handler) string
		name       string
		tagsToAdd  []map[string]string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success_with_tags",
			setup: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createConn(t, h, "conn", "GitHub")
			},
			tagsToAdd:  []map[string]string{{"Key": "A", "Value": "1"}, {"Key": "B", "Value": "2"}},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "not_found",
			setup: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:connection/missing"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := tt.setup(t, h)

			if len(tt.tagsToAdd) > 0 {
				tagRec := doJSON(t, h, "TagResource", map[string]any{
					"ResourceArn": connArn,
					"Tags":        tt.tagsToAdd,
				})
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			rec := doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				tags, ok := resp["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, tt.wantCount)
			}
		})
	}
}

// TestTagsOnHosts verifies that tags can be managed on hosts.
func TestTagsOnHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag_untag_list_on_host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			hostArn := createHost(
				t,
				h,
				"tagged-host",
				"GitLabSelfManaged",
				"https://gitlab.example.com",
			)

			tagRec := doJSON(t, h, "TagResource", map[string]any{
				"ResourceArn": hostArn,
				"Tags": []map[string]string{
					{"Key": "Env", "Value": "prod"},
					{"Key": "Team", "Value": "infra"},
				},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := parseResp(t, listRec)
			tagArr, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tagArr, 2)

			untagRec := doJSON(t, h, "UntagResource", map[string]any{
				"ResourceArn": hostArn,
				"TagKeys":     []string{"Env"},
			})
			require.Equal(t, http.StatusOK, untagRec.Code)

			listRec2 := doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
			require.Equal(t, http.StatusOK, listRec2.Code)
			resp2 := parseResp(t, listRec2)
			tagArr2, ok := resp2["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tagArr2, 1)
		})
	}
}

// TestSortedTagsOutput verifies that ListTagsForResource returns sorted tags.
func TestSortedTagsOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inputTags []map[string]string
		wantKeys  []string
	}{
		{
			name: "sorted_alpha",
			inputTags: []map[string]string{
				{"Key": "Zebra", "Value": "z"},
				{"Key": "Apple", "Value": "a"},
				{"Key": "Mango", "Value": "m"},
			},
			wantKeys: []string{"Apple", "Mango", "Zebra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			connArn := createConn(t, h, "sorted-tag-conn", "GitHub")

			rec := doJSON(t, h, "TagResource", map[string]any{
				"ResourceArn": connArn,
				"Tags":        tt.inputTags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			listRec := doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := parseResp(t, listRec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)

			gotKeys := make([]string, 0, len(tags))
			for _, tagItem := range tags {
				tmap, isMap := tagItem.(map[string]any)
				require.True(t, isMap)
				gotKeys = append(gotKeys, tmap["Key"].(string))
			}

			assert.Equal(t, tt.wantKeys, gotKeys)
		})
	}
}

// TestTagRoundTrip exercises the TagResource/ListTagsForResource round trip.
func TestTagRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerFixedAccount(t)

	rec := doJSON(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "tag-conn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	connArn := parseResp(t, rec)["ConnectionArn"].(string)

	rec = doJSON(t, h, "TagResource", map[string]any{
		"ResourceArn": connArn,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	tags, _ := m["Tags"].([]any)
	assert.Len(t, tags, 2)

	tag0, _ := tags[0].(map[string]any)
	assert.NotEmpty(t, tag0["Key"])
	assert.NotEmpty(t, tag0["Value"])
}

// TestUntagResourceRoundTrip exercises the TagResource/UntagResource round trip.
func TestUntagResourceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerFixedAccount(t)

	rec := doJSON(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "untag-conn",
		"ProviderType":   "GitHub",
		"Tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	connArn := parseResp(t, rec)["ConnectionArn"].(string)

	rec = doJSON(t, h, "UntagResource", map[string]any{
		"ResourceArn": connArn,
		"TagKeys":     []string{"remove"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	tags, _ := m["Tags"].([]any)
	assert.Len(t, tags, 1)

	remaining, _ := tags[0].(map[string]any)
	assert.Equal(t, "keep", remaining["Key"])
}

// TestRepositoryLinkTagsRoundTrip verifies that tags on a repository link are stored,
// returned via ListTagsForResource, and removable via UntagResource.
// Real AWS CodeConnections supports tagging repository links.
func TestRepositoryLinkTagsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		initTags  []map[string]string
		wantCount int
	}{
		{
			name: "tags_on_create_returned",
			initTags: []map[string]string{
				{"Key": "Env", "Value": "prod"},
				{"Key": "Team", "Value": "platform"},
			},
			wantCount: 2,
		},
		{
			name:      "no_tags_returns_empty",
			initTags:  nil,
			wantCount: 0,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "rl-tag-conn-"+strconv.Itoa(i), "GitHub")

			body := map[string]any{
				"ConnectionArn":  connArn,
				"OwnerId":        "my-org",
				"RepositoryName": "my-repo",
			}
			if tt.initTags != nil {
				body["Tags"] = tt.initTags
			}

			rec := doJSON(t, h, "CreateRepositoryLink", body)
			require.Equal(t, http.StatusOK, rec.Code)

			info := parseResp(t, rec)["RepositoryLinkInfo"].(map[string]any)
			linkArn, _ := info["RepositoryLinkArn"].(string)
			require.NotEmpty(t, linkArn)

			rec = doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			tags, _ := resp["Tags"].([]any)
			assert.Len(t, tags, tt.wantCount)
		})
	}
}

// TestRepositoryLinkTagResource verifies TagResource works on repository links.
func TestRepositoryLinkTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag_then_list_then_untag"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "rl-tagres-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			// Get the ARN from GetRepositoryLink
			rec := doJSON(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
			require.Equal(t, http.StatusOK, rec.Code)
			info := parseResp(t, rec)["RepositoryLinkInfo"].(map[string]any)
			linkArn := info["RepositoryLinkArn"].(string)

			// TagResource
			rec = doJSON(t, h, "TagResource", map[string]any{
				"ResourceArn": linkArn,
				"Tags":        []map[string]string{{"Key": "added", "Value": "yes"}},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify tag present
			rec = doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
			require.Equal(t, http.StatusOK, rec.Code)
			tags, _ := parseResp(t, rec)["Tags"].([]any)
			assert.Len(t, tags, 1)

			// UntagResource
			rec = doJSON(t, h, "UntagResource", map[string]any{
				"ResourceArn": linkArn,
				"TagKeys":     []string{"added"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Tag gone
			rec = doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
			require.Equal(t, http.StatusOK, rec.Code)
			tags, _ = parseResp(t, rec)["Tags"].([]any)
			assert.Empty(t, tags)
		})
	}
}
