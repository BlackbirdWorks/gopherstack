package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestInMemoryBackend_TaggedResources covers the enumeration method cli.go's
// wireTaggingECS registers with the Resource Groups Tagging API (gopherstack-3xne):
// every tagged resource ARN, spanning every ECS resource kind that shares the flat
// resourceTags side map, must be visible -- and untagged/emptied-out ARNs must not
// appear at all.
func TestInMemoryBackend_TaggedResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *ecs.InMemoryBackend)
		want  map[string]map[string]string
		name  string
	}{
		{
			name: "multiple_tagged_resource_kinds",
			setup: func(t *testing.T, b *ecs.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.TagResource(
					"arn:aws:ecs:us-east-1:000000000000:cluster/c1",
					[]ecs.Tag{{Key: "env", Value: "prod"}},
				))
				require.NoError(t, b.TagResource(
					"arn:aws:ecs:us-east-1:000000000000:service/c1/svc1",
					[]ecs.Tag{{Key: "team", Value: "platform"}},
				))
			},
			want: map[string]map[string]string{
				"arn:aws:ecs:us-east-1:000000000000:cluster/c1":      {"env": "prod"},
				"arn:aws:ecs:us-east-1:000000000000:service/c1/svc1": {"team": "platform"},
			},
		},
		{
			name: "untagged_after_untag_all_excluded",
			setup: func(t *testing.T, b *ecs.InMemoryBackend) {
				t.Helper()

				const arn = "arn:aws:ecs:us-east-1:000000000000:cluster/emptied"
				require.NoError(t, b.TagResource(arn, []ecs.Tag{{Key: "env", Value: "prod"}}))
				require.NoError(t, b.UntagResource(arn, []string{"env"}))
			},
			want: map[string]map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())
			tt.setup(t, b)

			got := b.TaggedResources()
			gotMap := make(map[string]map[string]string, len(got))

			for _, e := range got {
				gotMap[e.ARN] = e.Tags
			}

			assert.Equal(t, tt.want, gotMap)
		})
	}
}

func TestTagResource_Service(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "tag-svc-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "tag-svc-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	svcResp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "tag-svc-cluster",
		"serviceName":    "tag-svc",
		"taskDefinition": "tag-svc-task",
		"desiredCount":   1,
	})
	var svcOut map[string]any
	require.NoError(t, json.Unmarshal(svcResp.Body.Bytes(), &svcOut))
	svcArn := svcOut["service"].(map[string]any)["serviceArn"].(string)

	// Tag
	tagResp := doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": svcArn,
		"tags": []any{
			map[string]any{"key": "env", "value": "prod"},
			map[string]any{"key": "team", "value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, tagResp.Code)

	// List tags
	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": svcArn,
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	tags := listOut["tags"].([]any)
	assert.Len(t, tags, 2)
}

func TestTagResource_Overwrite(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	fakeArn := "arn:aws:ecs:us-east-1:000000000000:cluster/overwrite-cluster"

	doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": fakeArn,
		"tags":        []any{map[string]any{"key": "env", "value": "dev"}},
	})
	doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": fakeArn,
		"tags":        []any{map[string]any{"key": "env", "value": "prod"}},
	})

	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": fakeArn,
	})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	tags := listOut["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "prod", tags[0].(map[string]any)["value"])
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	fakeArn := "arn:aws:ecs:us-east-1:000000000000:cluster/untag-cluster"

	doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": fakeArn,
		"tags": []any{
			map[string]any{"key": "env", "value": "prod"},
			map[string]any{"key": "team", "value": "platform"},
			map[string]any{"key": "version", "value": "1.0"},
		},
	})

	doECSRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": fakeArn,
		"tagKeys":     []string{"env", "version"},
	})

	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": fakeArn,
	})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	tags := listOut["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "team", tags[0].(map[string]any)["key"])
}

func TestTagResource_Cluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "tagged-cluster"})

	clusterArn := "arn:aws:ecs:us-east-1:000000000000:cluster/tagged-cluster"
	doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": clusterArn,
		"tags":        []any{map[string]any{"key": "owner", "value": "ops-team"}},
	})

	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": clusterArn,
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &out))
	tags := out["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "owner", tags[0].(map[string]any)["key"])
}

// TestECS_Tagging verifies TagResource, UntagResource, ListTagsForResource.
func TestECS_Tagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		tags        []map[string]any
		removeKeys  []string
		wantLen     int
	}{
		{
			name:        "tag and list",
			resourceArn: "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster",
			tags: []map[string]any{
				{"key": "env", "value": "prod"},
				{"key": "team", "value": "platform"},
			},
			wantLen: 2,
		},
		{
			name:        "tag then untag one",
			resourceArn: "arn:aws:ecs:us-east-1:000000000000:cluster/other",
			tags: []map[string]any{
				{"key": "env", "value": "prod"},
				{"key": "team", "value": "platform"},
			},
			removeKeys: []string{"env"},
			wantLen:    1,
		},
		{
			name:        "empty tags",
			resourceArn: "arn:aws:ecs:us-east-1:000000000000:cluster/empty",
			tags:        []map[string]any{},
			wantLen:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doECSRequest(t, h, "TagResource", map[string]any{
				"resourceArn": tt.resourceArn,
				"tags":        tt.tags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if len(tt.removeKeys) > 0 {
				rec2 := doECSRequest(t, h, "UntagResource", map[string]any{
					"resourceArn": tt.resourceArn,
					"tagKeys":     tt.removeKeys,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
			}

			rec3 := doECSRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": tt.resourceArn,
			})
			require.Equal(t, http.StatusOK, rec3.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp))

			tagList, ok := resp["tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tagList, tt.wantLen)
		})
	}
}
