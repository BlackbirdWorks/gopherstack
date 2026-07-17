package efs_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestTagOperations exercises TagResource and ListTagsForResource.
func TestTagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *efs.Handler)
		name string
	}{
		{
			name: "tag_and_list_file_system",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "tag-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				// Tag the resource. Real aws-sdk-go-v2 sends TagResource to
				// "/2015-02-01/resource-tags/{ResourceId}", not "/2015-02-01/tags/...".
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/resource-tags/"+fsID, map[string]any{
					"Tags": []map[string]string{{"Key": "Env", "Value": "prod"}},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				// List tags.
				rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/resource-tags/"+fsID, nil)
				assert.Equal(t, http.StatusOK, rec3.Code)
				resp := parseResp(t, rec3)
				tagsList := resp["Tags"].([]any)
				assert.NotEmpty(t, tagsList)
			},
		},
		{
			name: "list_tags_non_existent_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/2015-02-01/resource-tags/fs-notexist", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "tag_non_existent_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/resource-tags/fs-notexist", map[string]any{
					"Tags": []map[string]string{{"Key": "k", "Value": "v"}},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestEFSHandler()
			tt.ops(t, h)
		})
	}
}

// TestTagResourceByARN tests tagging via ARN instead of ID.
func TestTagResourceByARN(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "arn-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := parseResp(t, rec)
	fsARN := resp["FileSystemArn"].(string)
	require.NotEmpty(t, fsARN)

	// Tag via ARN.
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/resource-tags/"+fsARN, map[string]any{
		"Tags": []map[string]string{{"Key": "tagged", "Value": "true"}},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List tags via ARN.
	rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/resource-tags/"+fsARN, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestTagAccessPointByARN tests tagging access points via ARN.
func TestTagAccessPointByARN(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "ap-arn-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	fsID := parseResp(t, rec)["FileSystemId"].(string)

	// Create access point.
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	apARN := parseResp(t, rec2)["AccessPointArn"].(string)
	require.NotEmpty(t, apARN)

	// Tag via ARN.
	rec3 := doREST(t, h, http.MethodPost, "/2015-02-01/resource-tags/"+apARN, map[string]any{
		"Tags": []map[string]string{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestTagResourceByPercentEncodedARN tests tagging with a percent-encoded ARN.
func TestTagResourceByPercentEncodedARN(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "pct-arn-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := parseResp(t, rec)
	fsARN := resp["FileSystemArn"].(string)
	require.NotEmpty(t, fsARN)

	// Tag via percent-encoded ARN in path (simulating SDK/Terraform behaviour).
	encodedARN := url.PathEscape(fsARN)
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/resource-tags/"+encodedARN, map[string]any{
		"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List tags via percent-encoded ARN.
	rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/resource-tags/"+encodedARN, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	tagsResp := parseResp(t, rec3)
	tagsRaw, ok := tagsResp["Tags"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, tagsRaw)
}

// TestListTagsForAccessPointByARN tests that ListTagsForResource works with an access point ARN.
func TestListTagsForAccessPointByARN(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	// Create file system.
	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "lt-ap-arn-token",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	fsID := parseResp(t, rec)["FileSystemId"].(string)

	// Create access point.
	rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
		"FileSystemId": fsID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	apResp := parseResp(t, rec2)
	apARN := apResp["AccessPointArn"].(string)
	require.NotEmpty(t, apARN)

	// Tag the access point via its ARN.
	rec3 := doREST(t, h, http.MethodPost, "/2015-02-01/resource-tags/"+apARN, map[string]any{
		"Tags": []map[string]string{{"Key": "purpose", "Value": "e2e"}},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	// List tags for the access point via ARN.
	rec4 := doREST(t, h, http.MethodGet, "/2015-02-01/resource-tags/"+apARN, nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
	tagsResp := parseResp(t, rec4)
	tagsRaw, ok := tagsResp["Tags"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, tagsRaw)
}

// TestLegacyTagOperations exercises CreateTags and DeleteTags (legacy operations).
func TestLegacyTagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *efs.Handler)
		name string
	}{
		{
			name: "create_and_delete_tags",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "legacy-tag-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				// CreateTags.
				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/create-tags/"+fsID, map[string]any{
					"Tags": []map[string]string{
						{"Key": "Env", "Value": "prod"},
						{"Key": "Team", "Value": "platform"},
					},
				})
				assert.Equal(t, http.StatusNoContent, rec2.Code)

				// Verify tags were applied via ListTagsForResource.
				rec3 := doREST(t, h, http.MethodGet, "/2015-02-01/tags/"+fsID, nil)
				assert.Equal(t, http.StatusOK, rec3.Code)
				tagsRaw := parseResp(t, rec3)["Tags"].([]any)
				assert.Len(t, tagsRaw, 2)

				// DeleteTags.
				rec4 := doREST(t, h, http.MethodPost, "/2015-02-01/delete-tags/"+fsID, map[string]any{
					"TagKeys": []string{"Env"},
				})
				assert.Equal(t, http.StatusNoContent, rec4.Code)

				// Only 1 tag should remain.
				rec5 := doREST(t, h, http.MethodGet, "/2015-02-01/tags/"+fsID, nil)
				assert.Equal(t, http.StatusOK, rec5.Code)
				tagsRaw2 := parseResp(t, rec5)["Tags"].([]any)
				assert.Len(t, tagsRaw2, 1)
			},
		},
		{
			name: "create_tags_on_missing_fs_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodPost, "/2015-02-01/create-tags/fs-notexist", map[string]any{
					"Tags": []map[string]string{{"Key": "k", "Value": "v"}},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_tags_on_missing_fs_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodPost, "/2015-02-01/delete-tags/fs-notexist", map[string]any{
					"TagKeys": []string{"k"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestEFSHandler()
			tt.ops(t, h)
		})
	}
}

// TestTagsToEntries_Sorted verifies tag entries are sorted alphabetically in the response.
func TestTagsToEntries_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     map[string]any
		wantKeys []string
	}{
		{
			name:     "tags_sorted_alphabetically",
			tags:     map[string]any{"zebra": "z", "apple": "a", "mango": "m"},
			wantKeys: []string{"apple", "mango", "zebra"},
		},
		{
			name:     "single_tag_no_order_issue",
			tags:     map[string]any{"only": "one"},
			wantKeys: []string{"only"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			body := map[string]any{
				"CreationToken": "tok-tags-sort-" + tt.name,
				"Tags":          []map[string]any{},
			}
			tagEntries := make([]map[string]any, 0)
			for k, v := range tt.tags {
				tagEntries = append(tagEntries, map[string]any{"Key": k, "Value": v})
			}
			body["Tags"] = tagEntries

			rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			resp := parseResp(t, rec)
			rawTags, ok := resp["Tags"].([]any)
			require.True(t, ok)

			keys := make([]string, 0, len(rawTags))
			for _, rt := range rawTags {
				m := rt.(map[string]any)
				keys = append(keys, m["Key"].(string))
			}

			for i := 1; i < len(keys); i++ {
				assert.LessOrEqual(t, keys[i-1], keys[i], "tags not sorted at index %d", i)
			}
		})
	}
}
