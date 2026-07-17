package mwaa_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagsToAdd    map[string]string
		wantTags     map[string]string
		name         string
		envName      string
		keysToRemove []string
		wantErr      bool
	}{
		{
			name:      "tag_and_list",
			envName:   "tag-env",
			tagsToAdd: map[string]string{"env": "test", "owner": "team"},
			wantTags:  map[string]string{"env": "test", "owner": "team"},
		},
		{
			name:         "tag_and_untag",
			envName:      "untag-env",
			tagsToAdd:    map[string]string{"env": "test", "owner": "team"},
			keysToRemove: []string{"owner"},
			wantTags:     map[string]string{"env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			env, err := b.CreateEnvironment(context.Background(), tt.envName, newCreateReq())
			require.NoError(t, err)

			err = b.TagResource(context.Background(), env.ARN, tt.tagsToAdd)
			require.NoError(t, err)

			if len(tt.keysToRemove) > 0 {
				err = b.UntagResource(context.Background(), env.ARN, tt.keysToRemove)
				require.NoError(t, err)
			}

			tags, err := b.ListTagsForResource(context.Background(), env.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}

func TestUntagResource_NotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	err := b.UntagResource(
		context.Background(),
		"arn:aws:airflow:us-east-1:123456789012:environment/ghost",
		[]string{"k"},
	)
	require.Error(t, err)
}

func TestUntagResource_MultipleKeys(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	env, err := b.CreateEnvironment(context.Background(), "multi-untag-env", newCreateReq())
	require.NoError(t, err)

	err = b.TagResource(context.Background(), env.ARN, map[string]string{"a": "1", "b": "2", "c": "3"})
	require.NoError(t, err)

	err = b.UntagResource(context.Background(), env.ARN, []string{"a", "c"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), env.ARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, tags)
}

// ─────────────────────────────────────────────────────────────
// 14. EnvironmentCount helper mirrors backend state
// ─────────────────────────────────────────────────────────────

func TestTagLimit_CreateEnvironment_Exceeds(t *testing.T) {
	t.Parallel()

	tags := make(map[string]any, 51)
	for i := range 51 {
		tags[strings.Repeat("k", i+1)] = "v"
	}

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()

	tagMap := make(map[string]string, 51)
	for k, v := range tags {
		tagMap[k] = v.(string)
	}

	req.Tags = tagMap

	_, err := b.CreateEnvironment(context.Background(), "tag-env", req)
	require.Error(t, err)
}

func TestTagLimit_CreateEnvironment_AtLimit(t *testing.T) {
	t.Parallel()

	tags := make(map[string]string, 50)
	for i := range 50 {
		tags[strings.Repeat("k", i+1)] = "v"
	}

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.Tags = tags

	_, err := b.CreateEnvironment(context.Background(), "tag-at-limit", req)
	require.NoError(t, err)
}

func TestTagLimit_TagResource_Exceeds(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Create with 48 tags.
	initialTags := make(map[string]string, 48)
	for i := range 48 {
		initialTags[strings.Repeat("k", i+1)] = "v"
	}

	req := newCreateReq()
	req.Tags = initialTags

	env, err := b.CreateEnvironment(context.Background(), "tag-resource-env", req)
	require.NoError(t, err)

	// Adding 3 new tags should exceed the 50-tag limit.
	err = b.TagResource(context.Background(), env.ARN, map[string]string{"new1": "v", "new2": "v", "new3": "v"})
	require.Error(t, err)
}

func TestTagLimit_TagResource_UpdateExistingTagsOK(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Create with 50 tags.
	initialTags := make(map[string]string, 50)
	for i := range 50 {
		initialTags[strings.Repeat("k", i+1)] = "v"
	}

	req := newCreateReq()
	req.Tags = initialTags

	env, err := b.CreateEnvironment(context.Background(), "tag-update-ok", req)
	require.NoError(t, err)

	// Updating an existing tag (same key) does not increase count — should succeed.
	firstKey := strings.Repeat("k", 1)
	err = b.TagResource(context.Background(), env.ARN, map[string]string{firstKey: "updated"})
	require.NoError(t, err)
}

func TestTagLimit_TagResource_AddToFull(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// Create with 50 tags.
	initialTags := make(map[string]string, 50)
	for i := range 50 {
		initialTags[strings.Repeat("k", i+1)] = "v"
	}

	req := newCreateReq()
	req.Tags = initialTags

	env, err := b.CreateEnvironment(context.Background(), "tag-full-env", req)
	require.NoError(t, err)

	// Adding even one genuinely new tag must fail.
	err = b.TagResource(context.Background(), env.ARN, map[string]string{"brand-new-key": "v"})
	require.Error(t, err)
}

func TestTagLimit_Create_ExactlyAtLimit_ThenOneMore(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	// 50 tags at create — OK.
	tags50 := make(map[string]string, 50)
	for i := range 50 {
		tags50[strings.Repeat("x", i+1)] = "v"
	}

	req := newCreateReq()
	req.Tags = tags50
	env, err := b.CreateEnvironment(context.Background(), "tag-boundary-env", req)
	require.NoError(t, err)

	// One more new tag — must fail.
	err = b.TagResource(context.Background(), env.ARN, map[string]string{"brand-new": "v"})
	require.Error(t, err)
}

func TestTags_AtCreate_PersistedInGet(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.Tags = map[string]string{
		"env":   "production",
		"owner": "platform-team",
		"cost":  "cc-1234",
	}

	_, err := b.CreateEnvironment(context.Background(), "tagged-env", req)
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "tagged-env")
	env, err := b.GetEnvironment(context.Background(), "tagged-env")
	require.NoError(t, err)
	assert.Equal(t, "production", env.Tags["env"])
	assert.Equal(t, "platform-team", env.Tags["owner"])
	assert.Equal(t, "cc-1234", env.Tags["cost"])
}

func TestTags_Update_DoesNotTouchExistingTags(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.Tags = map[string]string{"keep": "this"}

	_, err := b.CreateEnvironment(context.Background(), "tags-upd-env", req)
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "tags-upd-env") // promote CREATING → AVAILABLE

	// Update the environment without touching tags.
	_, err = b.UpdateEnvironment(context.Background(), "tags-upd-env", &mwaa.ExportedUpdateEnvironmentRequest{
		DagS3Path: "new-dags/",
	})
	require.NoError(t, err)

	b.GetEnvironment(context.Background(), "tags-upd-env")
	env, err := b.GetEnvironment(context.Background(), "tags-upd-env")
	require.NoError(t, err)
	assert.Equal(t, "this", env.Tags["keep"])
}

func TestTags_NotLeakedBetweenEnvironments(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	reqA := newCreateReq()
	reqA.Tags = map[string]string{"env": "alpha"}
	envA, err := b.CreateEnvironment(context.Background(), "tag-leak-a", reqA)
	require.NoError(t, err)

	reqB := newCreateReq()
	reqB.Tags = map[string]string{"env": "beta"}
	_, err = b.CreateEnvironment(context.Background(), "tag-leak-b", reqB)
	require.NoError(t, err)

	// Add a tag to A's ARN.
	err = b.TagResource(context.Background(), envA.ARN, map[string]string{"extra": "from-a"})
	require.NoError(t, err)

	// Fetch B — should not have A's extra tag.
	b.GetEnvironment(context.Background(), "tag-leak-b")
	gotB, err := b.GetEnvironment(context.Background(), "tag-leak-b")
	require.NoError(t, err)
	assert.NotContains(t, gotB.Tags, "extra")
	assert.Equal(t, "beta", gotB.Tags["env"])
}

func TestBackend_ARNIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		envNames   []string
		wantTagsOK bool
	}{
		{
			name:       "tag_resource_uses_arn_index",
			envNames:   []string{"arn-env-1"},
			wantTagsOK: true,
		},
		{
			name:       "tag_resource_not_found",
			envNames:   []string{},
			wantTagsOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			var envARN string
			for _, n := range tt.envNames {
				rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+n, map[string]any{
					"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				envARN = resp["Arn"]
			}

			if !tt.wantTagsOK {
				envARN = "arn:aws:airflow:us-east-1:123456789012:environment/nonexistent"
			}

			tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{
				"Tags": map[string]string{"key": "val"},
			})

			if tt.wantTagsOK {
				assert.Equal(t, http.StatusOK, tagRec.Code)
			} else {
				assert.Equal(t, http.StatusNotFound, tagRec.Code)
			}
		})
	}
}

func TestARNIndex_GrowsOnCreate(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	assert.Equal(t, 0, mwaa.ARNIndexSize(b))

	for i := range 3 {
		_, err := b.CreateEnvironment(context.Background(),
			fmt.Sprintf("arn-env-%d", i), newCreateReq())
		require.NoError(t, err)
	}

	assert.Equal(t, 3, mwaa.ARNIndexSize(b))
}

func TestARNIndex_ShrinksOnDelete(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	_, err := b.CreateEnvironment(context.Background(), "arn-del-env", newCreateReq())
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.ARNIndexSize(b))

	_, err = b.DeleteEnvironment(context.Background(), "arn-del-env")
	require.NoError(t, err)
	assert.Equal(t, 0, mwaa.ARNIndexSize(b))
}
