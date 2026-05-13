package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestECS_DescribeTaskDefinitionInclude verifies that registration tags supplied
// via RegisterTaskDefinition are persisted and surfaced through
// DescribeTaskDefinition only when the AWS-defined include=["TAGS"] option
// is present, matching the real ECS contract.
func TestECS_DescribeTaskDefinitionInclude(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		include     []string
		wantTagKeys []string
	}{
		{
			name:    "no_include_returns_no_tags",
			include: nil,
		},
		{
			name:        "include_tags_returns_tags",
			include:     []string{"TAGS"},
			wantTagKeys: []string{"env", "team"},
		},
		{
			name:        "include_tags_case_insensitive",
			include:     []string{"tags"},
			wantTagKeys: []string{"env", "team"},
		},
		{
			name:    "include_unknown_option_returns_no_tags",
			include: []string{"OTHER"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family": "tagged-fam",
				"containerDefinitions": []map[string]any{
					{"name": "c", "image": "nginx", "essential": true},
				},
				"tags": []map[string]string{
					{"key": "env", "value": "prod"},
					{"key": "team", "value": "platform"},
				},
			})

			req := map[string]any{"taskDefinition": "tagged-fam"}
			if tt.include != nil {
				req["include"] = tt.include
			}

			rec := doECSRequest(t, h, "DescribeTaskDefinition", req)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Tags []ecs.Tag `json:"tags"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			gotKeys := make([]string, 0, len(resp.Tags))
			for _, tag := range resp.Tags {
				gotKeys = append(gotKeys, tag.Key)
			}

			assert.ElementsMatch(t, tt.wantTagKeys, gotKeys)
		})
	}
}
