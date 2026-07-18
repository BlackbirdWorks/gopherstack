package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/emr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEMR_AddAndRemoveTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "adds and removes tags on existing cluster",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "tag-cluster"})
			require.Equal(t, http.StatusOK, rec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

			addRec := doEMRRequest(t, h, "AddTags", map[string]any{
				"ResourceId": createOut.JobFlowID,
				"Tags":       []map[string]any{{"Key": "env", "Value": "dev"}},
			})
			require.Equal(t, tt.wantCode, addRec.Code)

			removeRec := doEMRRequest(t, h, "RemoveTags", map[string]any{
				"ResourceId": createOut.JobFlowID,
				"TagKeys":    []string{"env"},
			})
			require.Equal(t, tt.wantCode, removeRec.Code)
		})
	}
}

func TestEMR_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*emr.Handler) string
		checkTags func(*testing.T, []emr.Tag)
		name      string
		wantCode  int
	}{
		{
			name: "lists tags on existing cluster",
			setup: func(h *emr.Handler) string {
				rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
					"Name": "list-tags-cluster",
					"Tags": []map[string]any{{"Key": "env", "Value": "prod"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut struct {
					JobFlowID string `json:"JobFlowId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

				return createOut.JobFlowID
			},
			wantCode: http.StatusOK,
			checkTags: func(t *testing.T, tags []emr.Tag) {
				t.Helper()
				require.Len(t, tags, 1)
				assert.Equal(t, "env", tags[0].Key)
				assert.Equal(t, "prod", tags[0].Value)
			},
		},
		{
			name: "lists empty tags on cluster without tags",
			setup: func(h *emr.Handler) string {
				rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "no-tag-cluster"})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut struct {
					JobFlowID string `json:"JobFlowId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))

				return createOut.JobFlowID
			},
			wantCode: http.StatusOK,
			checkTags: func(t *testing.T, tags []emr.Tag) {
				t.Helper()
				assert.Empty(t, tags)
			},
		},
		{
			name: "returns error for non-existent resource",
			setup: func(_ *emr.Handler) string {
				return "j-NOTEXIST"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceID := tt.setup(h)

			listRec := doEMRRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceId": resourceID,
			})
			require.Equal(t, tt.wantCode, listRec.Code)

			if tt.checkTags != nil {
				var tagOut struct {
					Tags []emr.Tag `json:"Tags"`
				}
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagOut))
				tt.checkTags(t, tagOut.Tags)
			}
		})
	}
}

func TestEMR_AddTags_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "AddTags", map[string]any{
		"ResourceId": "j-NOTEXIST",
		"Tags":       []map[string]any{{"Key": "k", "Value": "v"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_RemoveTags_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "RemoveTags", map[string]any{
		"ResourceId": "j-NOTEXIST",
		"TagKeys":    []string{"env"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEMR_Backend_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		wantTags   []emr.Tag
		wantErr    bool
	}{
		{
			name:       "existing cluster by ID",
			resourceID: "",
			wantErr:    false,
			wantTags:   []emr.Tag{{Key: "env", Value: "test"}},
		},
		{
			name:       "not found",
			resourceID: "j-NOTEXIST",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emr.NewInMemoryBackend(testAccountID, testRegion)
			cluster, err := b.RunJobFlow(context.Background(), emr.RunJobFlowParams{
				Name: "test-cluster", ReleaseLabel: "emr-6.0.0",
				Tags: []emr.Tag{{Key: "env", Value: "test"}},
			})
			require.NoError(t, err)

			resourceID := tt.resourceID
			if resourceID == "" {
				resourceID = cluster.ID
			}

			tags, err := b.ListTagsForResource(context.Background(), resourceID)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}

func TestEMR_Backend_ListTagsForResourceByARN(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(context.Background(), emr.RunJobFlowParams{
		Name: "test-cluster", ReleaseLabel: "emr-6.0.0",
		Tags: []emr.Tag{{Key: "key", Value: "val"}},
	})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), cluster.ARN)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, "key", tags[0].Key)
	assert.Equal(t, "val", tags[0].Value)
}

// TestSortedListTagsForResource verifies tags are returned sorted by key.
func TestSortedListTagsForResource(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "tag-cluster", ReleaseLabel: "emr-6.0.0", Tags: []emr.Tag{
			{Key: "zzz", Value: "last"},
			{Key: "aaa", Value: "first"},
			{Key: "mmm", Value: "middle"},
		}},
	)
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), cluster.ID)
	require.NoError(t, err)
	require.Len(t, tags, 3)
	assert.Equal(t, "aaa", tags[0].Key)
	assert.Equal(t, "mmm", tags[1].Key)
	assert.Equal(t, "zzz", tags[2].Key)
}

// TestListTagsForResource_ReturnsList verifies HTTP response returns a list, not a map.
func TestListTagsForResource_ReturnsList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "tag-list-cluster",
		"Tags": []map[string]any{
			{"Key": "b", "Value": "2"},
			{"Key": "a", "Value": "1"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	listRec := doEMRRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceId": createOut.JobFlowID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		Tags []emr.Tag `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.Tags, 2)
	// Verify sorted order.
	assert.Equal(t, "a", out.Tags[0].Key)
	assert.Equal(t, "b", out.Tags[1].Key)
}
