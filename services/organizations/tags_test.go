package organizations_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/organizations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackend_TagOperations tests tagging resources.
func TestBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "tag_and_untag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			org, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			// TagResource.
			tags := []organizations.Tag{{Key: "env", Value: "test"}}
			err = b.TagResource(org.ID, tags)
			require.NoError(t, err)

			// ListTagsForResource.
			listed, err := b.ListTagsForResource(org.ID)
			require.NoError(t, err)
			assert.Len(t, listed, 1)
			assert.Equal(t, "env", listed[0].Key)
			assert.Equal(t, "test", listed[0].Value)

			// UntagResource.
			err = b.UntagResource(org.ID, []string{"env"})
			require.NoError(t, err)

			// After untag, tags should be empty.
			listed, err = b.ListTagsForResource(org.ID)
			require.NoError(t, err)
			assert.Empty(t, listed)
		})
	}
}

// TestTagOperations_MultiResource tests tagging multiple resource types.
func TestTagOperations_MultiResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceKind string // "account", "ou", "policy"
	}{
		{name: "tag_account", resourceKind: "account"},
		{name: "tag_ou", resourceKind: "ou"},
		{name: "tag_policy", resourceKind: "policy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)

			var resourceID string

			switch tt.resourceKind {
			case "account":
				s, err := b.CreateAccount("tagged", "tagged@example.com", "", "", nil)
				require.NoError(t, err)
				resourceID = s.AccountID
			case "ou":
				ou, err := b.CreateOrganizationalUnit(rootID, "tagged-ou", nil)
				require.NoError(t, err)
				resourceID = ou.ID
			case "policy":
				p, err := b.CreatePolicy("tagged-p", "", `{}`, "SERVICE_CONTROL_POLICY", nil)
				require.NoError(t, err)
				resourceID = p.PolicySummary.ID
			}

			tags := []organizations.Tag{
				{Key: "team", Value: "platform"},
				{Key: "env", Value: "staging"},
			}

			require.NoError(t, b.TagResource(resourceID, tags))

			listed, err := b.ListTagsForResource(resourceID)
			require.NoError(t, err)
			assert.Len(t, listed, 2)

			require.NoError(t, b.UntagResource(resourceID, []string{"team"}))

			listed, err = b.ListTagsForResource(resourceID)
			require.NoError(t, err)
			assert.Len(t, listed, 1)
			assert.Equal(t, "env", listed[0].Key)
		})
	}
}

// TestTagResource_ExistenceValidation verifies that TagResource/UntagResource/
// ListTagsForResource validate that the resource exists.
func TestTagResource_ExistenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resourceFn func(b *organizations.InMemoryBackend, rootID string) string
		name       string
		wantErr    bool
	}{
		{
			name: "root_id_valid",
			resourceFn: func(_ *organizations.InMemoryBackend, rootID string) string {
				return rootID
			},
			wantErr: false,
		},
		{
			name: "account_id_valid",
			resourceFn: func(b *organizations.InMemoryBackend, _ string) string {
				s, err := b.CreateAccount("tag-acct", "tag@example.com", "", "", nil)
				if err != nil {
					panic(err)
				}

				return s.AccountID
			},
			wantErr: false,
		},
		{
			name: "ou_id_valid",
			resourceFn: func(b *organizations.InMemoryBackend, rootID string) string {
				ou, err := b.CreateOrganizationalUnit(rootID, "TagOU", nil)
				if err != nil {
					panic(err)
				}

				return ou.ID
			},
			wantErr: false,
		},
		{
			name: "policy_id_valid",
			resourceFn: func(b *organizations.InMemoryBackend, _ string) string {
				p, err := b.CreatePolicy("tag-pol", "", `{}`, "TAG_POLICY", nil)
				if err != nil {
					panic(err)
				}

				return p.PolicySummary.ID
			},
			wantErr: false,
		},
		{
			name: "unknown_id_rejected",
			resourceFn: func(_ *organizations.InMemoryBackend, _ string) string {
				return "ou-nonexistent-00000000"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rootID := newOrgBackend(t)
			resourceID := tt.resourceFn(b, rootID)

			// Test TagResource.
			err := b.TagResource(resourceID, []organizations.Tag{{Key: "k", Value: "v"}})
			if tt.wantErr {
				require.Error(t, err, "TagResource should fail for unknown resource")
			} else {
				require.NoError(t, err, "TagResource should succeed")
			}

			// Test UntagResource.
			err = b.UntagResource(resourceID, []string{"k"})
			if tt.wantErr {
				require.Error(t, err, "UntagResource should fail for unknown resource")
			} else {
				require.NoError(t, err, "UntagResource should succeed")
			}

			// Test ListTagsForResource.
			_, err = b.ListTagsForResource(resourceID)
			if tt.wantErr {
				require.Error(t, err, "ListTagsForResource should fail for unknown resource")
			} else {
				require.NoError(t, err, "ListTagsForResource should succeed")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 13: RegisterDelegatedAdministrator service access check
// ---------------------------------------------------------------------------

// TestTagResource_InvalidResource_ViaHandler tests tag resource validation via handler.
func TestTagResource_InvalidResource_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Tag a non-existent resource.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceId": "ou-nonexistent-12345678",
		"Tags":       []map[string]string{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestTagOps_NonExistentResource_TargetNotFoundException verifies that tag
// operations on a resource ID that does not exist return TargetNotFoundException (not
// InvalidInputException).  Real AWS raises TargetNotFoundException for unknown resource
// IDs in all three tag operations.
func TestTagOps_NonExistentResource_TargetNotFoundException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(b *organizations.InMemoryBackend) error
		name string
	}{
		{
			name: "TagResource",
			fn: func(b *organizations.InMemoryBackend) error {
				return b.TagResource("ou-xxxx-nonexistent", []organizations.Tag{{Key: "k", Value: "v"}})
			},
		},
		{
			name: "UntagResource",
			fn: func(b *organizations.InMemoryBackend) error {
				return b.UntagResource("ou-xxxx-nonexistent", []string{"k"})
			},
		},
		{
			name: "ListTagsForResource",
			fn: func(b *organizations.InMemoryBackend) error {
				_, err := b.ListTagsForResource("ou-xxxx-nonexistent")

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			err := tt.fn(b)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "TargetNotFoundException",
				"%s on unknown resource must return TargetNotFoundException, got: %v", tt.name, err)
		})
	}
}

// TestTagOps_NonExistentResource_ViaHandler verifies the HTTP response includes
// TargetNotFoundException (not InvalidInputException) when tagging a non-existent resource.
func TestTagOps_NonExistentResource_ViaHandler(t *testing.T) {
	t.Parallel()

	const bogusID = "ou-xxxx-nonexistent1"

	tests := []struct {
		op   string
		body map[string]any
		name string
	}{
		{
			name: "TagResource",
			op:   "TagResource",
			body: map[string]any{
				"ResourceId": bogusID,
				"Tags":       []map[string]string{{"Key": "k", "Value": "v"}},
			},
		},
		{
			name: "UntagResource",
			op:   "UntagResource",
			body: map[string]any{
				"ResourceId": bogusID,
				"TagKeys":    []string{"k"},
			},
		},
		{
			name: "ListTagsForResource",
			op:   "ListTagsForResource",
			body: map[string]any{"ResourceId": bogusID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp map[string]string
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, "TargetNotFoundException", errResp["__type"],
				"%s on unknown resource must return TargetNotFoundException", tt.name)
		})
	}
}

// TestHandler_TagOperations tests tag CRUD operations via handler.
func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "tag_untag_list",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			// Get org ID via DescribeOrganization.
			rec := doRequest(t, h, "DescribeOrganization", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&descResp))
			org := descResp["Organization"].(map[string]any)
			orgID := org["Id"].(string)

			// TagResource.
			rec = doRequest(t, h, "TagResource", map[string]any{
				"ResourceId": orgID,
				"Tags":       []map[string]string{{"Key": "env", "Value": "test"}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// ListTagsForResource.
			rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceId": orgID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&tagsResp))
			tags, ok := tagsResp["Tags"].([]any)
			require.True(t, ok)
			assert.NotEmpty(t, tags)

			// UntagResource.
			rec = doRequest(t, h, "UntagResource", map[string]any{
				"ResourceId": orgID,
				"TagKeys":    []string{"env"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestListTagsForResource_Sorted verifies sorted output by key.
func TestListTagsForResource_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tags []organizations.Tag
	}{
		{
			name: "three_tags_sorted_by_key",
			tags: []organizations.Tag{
				{Key: "zzz", Value: "1"},
				{Key: "aaa", Value: "2"},
				{Key: "mmm", Value: "3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			createOrgOn(t, b)

			status, err := b.CreateAccount("tagged-acct", "t@example.com", "", "", tt.tags)
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(status.AccountID)
			require.NoError(t, err)

			for i := 1; i < len(tags); i++ {
				assert.LessOrEqual(t, tags[i-1].Key, tags[i].Key, "tags should be sorted by key")
			}
		})
	}
}
