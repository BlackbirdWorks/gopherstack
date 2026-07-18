package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResourceTags tests GetResourceTags, TagResource, UntagResource.
func TestResourceTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags       map[string]string
		wantTagsAfter map[string]string
		name          string
		initialTags   string
		removeTags    []string
	}{
		{
			name:        "tag_and_untag",
			initialTags: `{"env":"test"}`,
			addTags:     map[string]string{"version": "2"},
			removeTags:  []string{"env"},
			wantTagsAfter: map[string]string{
				"version": "2",
			},
		},
		{
			name:        "tag_only",
			initialTags: `{}`,
			addTags:     map[string]string{"team": "backend"},
			wantTagsAfter: map[string]string{
				"team": "backend",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			// Create API with tags
			rec := postWithHandler(t, handler, e, "CreateRestApi",
				fmt.Sprintf(`{"name":"tagged-api","tags":%s}`, tt.initialTags))
			require.Equal(t, http.StatusCreated, rec.Code)
			var apiResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiResp))
			apiID := apiResp["id"].(string)

			arn := fmt.Sprintf("arn:aws:apigateway:us-east-1::/restapis/%s", apiID)

			// TagResource
			tagsJSON, err := json.Marshal(tt.addTags)
			require.NoError(t, err)
			tagRec := postWithHandler(t, handler, e, "TagResource",
				fmt.Sprintf(`{"resourceArn":%q,"tags":%s}`, arn, tagsJSON))
			assert.Equal(t, http.StatusNoContent, tagRec.Code)

			// UntagResource (if applicable)
			if len(tt.removeTags) > 0 {
				keysJSON, err2 := json.Marshal(tt.removeTags)
				require.NoError(t, err2)
				untagRec := postWithHandler(t, handler, e, "UntagResource",
					fmt.Sprintf(`{"resourceArn":%q,"tagKeys":%s}`, arn, keysJSON))
				assert.Equal(t, http.StatusNoContent, untagRec.Code)
			}

			// GetTags
			getRec := postWithHandler(t, handler, e, "GetTags",
				fmt.Sprintf(`{"resourceArn":%q}`, arn))
			assert.Equal(t, http.StatusOK, getRec.Code)
			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tagsResp))
			tagsMap, _ := tagsResp["tags"].(map[string]any)
			for k, v := range tt.wantTagsAfter {
				assert.Equal(t, v, tagsMap[k])
			}
			for _, removedKey := range tt.removeTags {
				_, exists := tagsMap[removedKey]
				assert.False(t, exists, "key %q should have been removed", removedKey)
			}
		})
	}
}
