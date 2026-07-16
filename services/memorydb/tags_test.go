package memorydb_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags        map[string]string
		wantTags    map[string]string
		name        string
		clusterName string
		removedKeys []string
	}{
		{
			name:        "tag_and_untag",
			clusterName: "tagged-cluster",
			tags:        map[string]string{"Env": "test", "Team": "ops"},
			removedKeys: []string{"Team"},
			wantTags:    map[string]string{"Env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			req := &memorydb.ExportedCreateClusterRequest{
				ClusterName: tt.clusterName,
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			}

			c, err := b.CreateCluster(context.Background(), req)
			require.NoError(t, err)

			err = b.TagResource(context.Background(), c.ARN, tt.tags)
			require.NoError(t, err)

			got, err := b.ListTags(context.Background(), c.ARN)
			require.NoError(t, err)
			assert.Equal(t, "test", got["Env"])
			assert.Equal(t, "ops", got["Team"])

			err = b.UntagResource(context.Background(), c.ARN, tt.removedKeys)
			require.NoError(t, err)

			got, err = b.ListTags(context.Background(), c.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

// TestRefinement3_ARNIndex_NewOps verifies the ARN index is properly maintained for new operations.
func TestARNIndexTracksNewResourceTypes(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	initialSize := memorydb.ARNIndexSize(b)

	b.AddClusterInternal("idx-cluster", "db.r6g.large")
	assert.Equal(t, initialSize+1, memorydb.ARNIndexSize(b))

	b.AddSnapshotInternal("idx-snap", "idx-cluster")
	assert.Equal(t, initialSize+2, memorydb.ARNIndexSize(b))
}

// TestRefinement1_ARNIndexSize verifies the ARN index grows correctly.
func TestARNIndexSize(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	initialSize := memorydb.ARNIndexSize(b) // open-access

	b.AddClusterInternal("c1", "db.r6g.large")
	b.AddSnapshotInternal("s1", "c1")

	assert.Equal(t, initialSize+2, memorydb.ARNIndexSize(b))
}

// TestRefinement1_TagResourceReturnsTagList verifies TagResource returns resulting tags.
func TestTagResourceReturnsTagList(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	cluster := b.AddClusterInternal("tagtest", "db.r6g.large")
	h := memorydb.NewHandler(b)

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": cluster.ARN,
		"Tags":        []map[string]any{{"Key": "Env", "Value": "test"}},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tagList, ok := resp["TagList"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, tagList)
}

// TestRefinement1_UntagResourceReturnsTagList verifies UntagResource returns remaining tags.
func TestUntagResourceReturnsTagList(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	cluster := b.AddClusterInternal("untagtest", "db.r6g.large")
	h := memorydb.NewHandler(b)

	// Add a tag first
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": cluster.ARN,
		"Tags":        []map[string]any{{"Key": "K1", "Value": "V1"}, {"Key": "K2", "Value": "V2"}},
	})

	// Remove one tag
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": cluster.ARN,
		"TagKeys":     []string{"K1"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tagList := resp["TagList"].([]any)
	assert.Len(t, tagList, 1)
}
