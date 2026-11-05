package ecs_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// TestECS_CreateCluster_TagsAndCapacityProviders proves that CreateCluster
// accepts capacityProviders, defaultCapacityProviderStrategy, and tags at
// creation time, matching the real ecs.CreateClusterInput wire shape (all
// three are real request fields; previously the handler silently dropped
// them, forcing a separate PutClusterCapacityProviders/TagResource call that
// Terraform-style "create with tags" flows never make).
func TestECS_CreateCluster_TagsAndCapacityProviders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		wantCapacityProvider string
		wantTagValue         string
	}{
		{
			name:                 "fargate default strategy and env tag",
			wantCapacityProvider: "FARGATE",
			wantTagValue:         "prod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doECSRequest(t, h, "CreateCluster", map[string]any{
				"clusterName":       "tagged-cluster",
				"capacityProviders": []string{tt.wantCapacityProvider},
				"defaultCapacityProviderStrategy": []map[string]any{
					{"capacityProvider": tt.wantCapacityProvider, "weight": 1},
				},
				"tags": []map[string]string{
					{"key": "env", "value": tt.wantTagValue},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp, jsonx.V1()))

			cluster := createResp["cluster"].(map[string]any)

			caps, ok := cluster["capacityProviders"].([]any)
			require.True(t, ok)
			require.Len(t, caps, 1)
			assert.Equal(t, tt.wantCapacityProvider, caps[0])

			strategy, ok := cluster["defaultCapacityProviderStrategy"].([]any)
			require.True(t, ok)
			require.Len(t, strategy, 1)
			assert.Equal(t, tt.wantCapacityProvider, strategy[0].(map[string]any)["capacityProvider"])

			// CreateCluster echoes back the tags it was just given (no
			// `include` gating on create).
			tags, ok := cluster["tags"].([]any)
			require.True(t, ok)
			require.Len(t, tags, 1)
			assert.Equal(t, tt.wantTagValue, tags[0].(map[string]any)["value"])

			// DescribeClusters omits tags unless include=["TAGS"] is set.
			rec = doECSRequest(t, h, "DescribeClusters", map[string]any{
				"clusters": []string{"tagged-cluster"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var describeResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &describeResp, jsonx.V1()))

			described := describeResp["clusters"].([]any)[0].(map[string]any)
			assert.Nil(t, described["tags"], "tags must be omitted when include=TAGS is not requested")

			// With include=["TAGS"], DescribeClusters must surface them.
			rec = doECSRequest(t, h, "DescribeClusters", map[string]any{
				"clusters": []string{"tagged-cluster"},
				"include":  []string{"TAGS"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var describeTaggedResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &describeTaggedResp, jsonx.V1()))

			describedTagged := describeTaggedResp["clusters"].([]any)[0].(map[string]any)
			taggedTags, ok := describedTagged["tags"].([]any)
			require.True(t, ok)
			require.Len(t, taggedTags, 1)
			assert.Equal(t, tt.wantTagValue, taggedTags[0].(map[string]any)["value"])
		})
	}
}
