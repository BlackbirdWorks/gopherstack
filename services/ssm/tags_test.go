package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestTagsForNonParameterResources verifies miscResourceTags.
func TestTagsForNonParameterResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		resourceID   string
		tags         []ssm.Tag
		wantTagCount int
	}{
		{
			name:         "tags_for_managed_instance",
			resourceType: "ManagedInstance",
			resourceID:   "mi-abc123",
			tags:         []ssm.Tag{{Key: "Env", Value: "prod"}, {Key: "Team", Value: "ops"}},
			wantTagCount: 2,
		},
		{
			name:         "tags_for_activation",
			resourceType: "Activation",
			resourceID:   "act-abc123",
			tags:         []ssm.Tag{{Key: "Purpose", Value: "test"}},
			wantTagCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			addBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
				"Tags":         tt.tags,
			})

			addRec := doRequest(t, h, "AddTagsToResource", string(addBody))
			require.Equal(t, http.StatusOK, addRec.Code)

			listBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
			})

			listRec := doRequest(t, h, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp ssm.ListTagsForResourceOutput
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.TagList, tt.wantTagCount)
		})
	}
}

// TestRemoveTagsForNonParameter verifies RemoveTagsFromResource for non-parameter resources.
func TestRemoveTagsForNonParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		resourceID   string
		addTags      []ssm.Tag
		removeKeys   []string
		wantRemain   int
	}{
		{
			name:         "remove_one_tag",
			resourceType: "ManagedInstance",
			resourceID:   "mi-xyz",
			addTags:      []ssm.Tag{{Key: "A", Value: "1"}, {Key: "B", Value: "2"}},
			removeKeys:   []string{"A"},
			wantRemain:   1,
		},
		{
			name:         "remove_all_tags",
			resourceType: "Activation",
			resourceID:   "act-xyz",
			addTags:      []ssm.Tag{{Key: "X", Value: "1"}},
			removeKeys:   []string{"X"},
			wantRemain:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			addBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
				"Tags":         tt.addTags,
			})
			require.Equal(t, http.StatusOK, doRequest(t, h, "AddTagsToResource", string(addBody)).Code)

			removeBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
				"TagKeys":      tt.removeKeys,
			})
			require.Equal(t, http.StatusOK, doRequest(t, h, "RemoveTagsFromResource", string(removeBody)).Code)

			listBody, _ := json.Marshal(map[string]any{
				"ResourceType": tt.resourceType,
				"ResourceId":   tt.resourceID,
			})
			listRec := doRequest(t, h, "ListTagsForResource", string(listBody))
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp ssm.ListTagsForResourceOutput
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			assert.Len(t, resp.TagList, tt.wantRemain)
		})
	}
}
func TestFull_ParameterStore_Tags(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "PutParameter", map[string]any{"Name": "/tag/p", "Type": "String", "Value": "v"})

	code, _ := postJSON(t, h, "AddTagsToResource", map[string]any{
		"ResourceType": "Parameter",
		"ResourceId":   "/tag/p",
		"Tags":         []map[string]any{{"Key": "env", "Value": "prod"}},
	})
	assert.Equal(t, http.StatusOK, code)

	code, out := postJSON(t, h, "ListTagsForResource", map[string]any{
		"ResourceType": "Parameter",
		"ResourceId":   "/tag/p",
	})
	assert.Equal(t, http.StatusOK, code)
	tags := out["TagList"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])

	code, _ = postJSON(t, h, "RemoveTagsFromResource", map[string]any{
		"ResourceType": "Parameter",
		"ResourceId":   "/tag/p",
		"TagKeys":      []string{"env"},
	})
	assert.Equal(t, http.StatusOK, code)

	code, out = postJSON(t, h, "ListTagsForResource", map[string]any{
		"ResourceType": "Parameter",
		"ResourceId":   "/tag/p",
	})
	assert.Equal(t, http.StatusOK, code)
	remaining, _ := out["TagList"].([]any)
	assert.Empty(t, remaining)
}

// TestAddTagsToResource_NonParameter exercises tag ops for non-parameter resources.
func TestAddTagsToResource_NonParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		wantErr      bool
	}{
		{
			name:         "maintenance_window_tags",
			resourceType: "MaintenanceWindow",
			wantErr:      false,
		},
		{
			name:         "empty_resource_type_parameter_path",
			resourceType: "Parameter",
			wantErr:      true, // because param doesn't exist
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			err := b.AddTagsToResource(context.TODO(), &ssm.AddTagsToResourceInput{
				ResourceType: tt.resourceType,
				ResourceID:   "my-resource",
				Tags:         []ssm.Tag{{Key: "k", Value: "v"}},
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Verify list tags works for non-parameter
				out, err2 := b.ListTagsForResource(context.TODO(), &ssm.ListTagsForResourceInput{
					ResourceType: tt.resourceType,
					ResourceID:   "my-resource",
				})
				require.NoError(t, err2)
				assert.NotEmpty(t, out.TagList)
			}
		})
	}
}

// TestRemoveTagsFromResource_NonParameter exercises remove tags for non-parameter.
func TestRemoveTagsFromResource_NonParameter(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	// Add tags first
	err := b.AddTagsToResource(context.TODO(), &ssm.AddTagsToResourceInput{
		ResourceType: "MaintenanceWindow",
		ResourceID:   "mw-1234",
		Tags:         []ssm.Tag{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}},
	})
	require.NoError(t, err)

	// Remove one tag
	err = b.RemoveTagsFromResource(context.TODO(), &ssm.RemoveTagsFromResourceInput{
		ResourceType: "MaintenanceWindow",
		ResourceID:   "mw-1234",
		TagKeys:      []string{"k1"},
	})
	require.NoError(t, err)

	// Remove from non-existent resource (should not error)
	err = b.RemoveTagsFromResource(context.TODO(), &ssm.RemoveTagsFromResourceInput{
		ResourceType: "MaintenanceWindow",
		ResourceID:   "mw-nonexistent",
		TagKeys:      []string{"k1"},
	})
	require.NoError(t, err)
}
