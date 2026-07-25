package pipes_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTags_CreateAndDescribe verifies tags are stored on create and returned on describe.
func TestTags_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "no_tags", tags: nil},
		{name: "single_tag", tags: map[string]string{"env": "prod"}},
		{name: "many_tags", tags: map[string]string{"env": "prod", "owner": "team-a", "cost-center": "123"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			body := map[string]any{
				"RoleArn":      "arn:aws:iam::123456789012:role/r",
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
			}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}

			created := auditCreate(t, h, tt.name+"-pipe", body)
			described := auditDescribe(t, h, tt.name+"-pipe")

			if tt.tags != nil {
				createdTags, _ := created["Tags"].(map[string]any)
				for k, v := range tt.tags {
					assert.Equal(t, v, createdTags[k])
				}
				describedTags, _ := described["Tags"].(map[string]any)
				for k, v := range tt.tags {
					assert.Equal(t, v, describedTags[k])
				}
			}
		})
	}
}

// TestTags_UpdateViaTagResource verifies TagResource adds tags.
func TestTags_UpdateViaTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		newTags  map[string]string
		wantTags map[string]string
		name     string
	}{
		{
			name:     "add_new_tag",
			newTags:  map[string]string{"stage": "prod"},
			wantTags: map[string]string{"stage": "prod"},
		},
		{
			name:     "overwrite_existing_tag",
			newTags:  map[string]string{"env": "staging"},
			wantTags: map[string]string{"env": "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := auditNewHandler(t)
			created := auditCreate(t, h, tt.name+"-pipe", map[string]any{
				"RoleArn":      "arn:aws:iam::123456789012:role/r",
				"Source":       "arn:aws:sqs:us-west-2:123456789012:q",
				"Target":       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				"DesiredState": "RUNNING",
				"Tags":         map[string]string{"env": "dev"},
			})
			pipeARN := created["Arn"].(string)

			rec := auditDo(t, h, http.MethodPost, "/tags/"+pipeARN, map[string]any{
				"Tags": tt.newTags,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			tagsRec := auditDo(t, h, http.MethodGet, "/tags/"+pipeARN, nil)
			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(tagsRec.Body.Bytes(), &tagsResp))
			tags, _ := tagsResp["Tags"].(map[string]any)

			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}
