package ecr_test

// tags_test.go — verifies tags.go: TagResource (upsert), UntagResource
// (selective key removal), and ListTagsForResource, both at the backend and
// HTTP-handler level, including tags supplied at CreateRepository time.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestECR_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a repo first so we have an ARN.
	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "tag-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	repo := createResp["repository"].(map[string]any)
	arn := repo["repositoryArn"].(string)

	rec = doECRRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	tags, ok := out["tags"]
	require.True(t, ok, "response should contain 'tags' field")
	assert.IsType(t, []any{}, tags, "tags should be an array")
}

func TestECR_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "TagResource", map[string]any{
		"resourceArn": "arn:aws:ecr:us-east-1:000000000000:repository/my-repo",
		"tags":        []map[string]any{{"Key": "Env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestECR_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": "arn:aws:ecr:us-east-1:000000000000:repository/my-repo",
		"tagKeys":     []string{"Env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestTagResource_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		arn     string
		wantLen int
	}{
		{
			name:    "single_tag",
			arn:     "arn:aws:ecr:us-east-1:123456789012:repository/my-repo",
			tags:    map[string]string{"env": "prod"},
			wantLen: 1,
		},
		{
			name:    "multiple_tags",
			arn:     "arn:aws:ecr:us-east-1:123456789012:repository/other-repo",
			tags:    map[string]string{"env": "dev", "team": "platform"},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
			err := b.TagResource(context.Background(), tt.arn, tt.tags)
			require.NoError(t, err)

			got, err := b.ListTagsForResource(context.Background(), tt.arn)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)

			for k, v := range tt.tags {
				assert.Equal(t, v, got[k])
			}
		})
	}
}

// TestInMemoryBackend_TaggedResources covers the enumeration method cli.go's
// wireTaggingECR registers with the Resource Groups Tagging API (gopherstack-3xne):
// every tagged repository ARN must be visible, and untagged/empty-tag ARNs must not
// appear at all.
func TestInMemoryBackend_TaggedResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *ecr.InMemoryBackend)
		want  map[string]map[string]string
		name  string
	}{
		{
			name: "multiple_tagged_repositories",
			setup: func(t *testing.T, b *ecr.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.TagResource(
					context.Background(), "arn:aws:ecr:us-east-1:123456789012:repository/repo1",
					map[string]string{"env": "prod"},
				))
				require.NoError(t, b.TagResource(
					context.Background(), "arn:aws:ecr:us-east-1:123456789012:repository/repo2",
					map[string]string{"team": "platform"},
				))
			},
			want: map[string]map[string]string{
				"arn:aws:ecr:us-east-1:123456789012:repository/repo1": {"env": "prod"},
				"arn:aws:ecr:us-east-1:123456789012:repository/repo2": {"team": "platform"},
			},
		},
		{
			name: "untagged_repository_excluded",
			setup: func(t *testing.T, b *ecr.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateRepository(context.Background(), "untagged-repo", "", false, "", "")
				require.NoError(t, err)
			},
			want: map[string]map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
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

func TestUntagResource_RemovesSpecificKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialTags map[string]string
		name        string
		arn         string
		removeKeys  []string
		wantLen     int
	}{
		{
			name:        "remove_one_tag",
			arn:         "arn:aws:ecr:us-east-1:123456789012:repository/repo1",
			initialTags: map[string]string{"env": "prod", "team": "platform"},
			removeKeys:  []string{"env"},
			wantLen:     1,
		},
		{
			name:        "remove_all_tags",
			arn:         "arn:aws:ecr:us-east-1:123456789012:repository/repo2",
			initialTags: map[string]string{"a": "1", "b": "2"},
			removeKeys:  []string{"a", "b"},
			wantLen:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
			require.NoError(t, b.TagResource(context.Background(), tt.arn, tt.initialTags))
			require.NoError(t, b.UntagResource(context.Background(), tt.arn, tt.removeKeys))

			got, err := b.ListTagsForResource(context.Background(), tt.arn)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)
		})
	}
}

func TestListTagsForResource_HandlerSortsKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     []map[string]any
		name     string
		arn      string
		wantKeys []string
	}{
		{
			name: "sorted_tags",
			arn:  "arn:aws:ecr:us-east-1:123456789012:repository/my-repo",
			tags: []map[string]any{
				{"Key": "zebra", "Value": "z"},
				{"Key": "alpha", "Value": "a"},
				{"Key": "mango", "Value": "m"},
			},
			wantKeys: []string{"alpha", "mango", "zebra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()

			doAccuracy(t, h, "TagResource", map[string]any{
				"resourceArn": tt.arn,
				"tags":        tt.tags,
			})

			rec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": tt.arn,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			gotKeys := make([]string, 0, len(resp.Tags))
			for _, tag := range resp.Tags {
				gotKeys = append(gotKeys, tag.Key)
			}

			assert.Equal(t, tt.wantKeys, gotKeys)
		})
	}
}

func TestCreateRepository_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		repoName string
		tags     []map[string]any
		wantLen  int
	}{
		{
			name:     "tags_on_create_and_list",
			repoName: "tagged-repo",
			tags:     []map[string]any{{"Key": "env", "Value": "test"}},
			wantLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()

			rec := doAccuracy(t, h, "CreateRepository", map[string]any{
				"repositoryName": tt.repoName,
				"tags":           tt.tags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp struct {
				Repository struct {
					RepositoryARN string `json:"repositoryArn"`
				} `json:"repository"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			arn := createResp.Repository.RepositoryARN

			listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": arn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp struct {
				Tags []any `json:"tags"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Tags, tt.wantLen)
		})
	}
}

func TestTags_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	repo, err := b.CreateRepository(context.Background(), "tagged-repo", "", false, "", "")
	require.NoError(t, err)

	// Add initial tags via TagResource.
	err = b.TagResource(context.Background(), repo.RepositoryARN, map[string]string{"env": "prod", "team": "platform"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), repo.RepositoryARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	err = b.TagResource(context.Background(), repo.RepositoryARN, map[string]string{"owner": "alice"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(context.Background(), repo.RepositoryARN)
	require.NoError(t, err)
	assert.Equal(t, "alice", tags["owner"])

	err = b.UntagResource(context.Background(), repo.RepositoryARN, []string{"env"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(context.Background(), repo.RepositoryARN)
	require.NoError(t, err)
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv, "untagged key must be absent")
	assert.Equal(t, "platform", tags["team"])
}

func TestCreateRepository_Tags_Persisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "tagged-repo-persist",
		"tags": []map[string]any{
			{"Key": "Env", "Value": "prod"},
			{"Key": "Team", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	arn := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	out := parseAccuracy(t, listRec)
	tags, _ := out["tags"].([]any)
	require.Len(t, tags, 2, "tags provided during CreateRepository must be stored")

	tagMap := map[string]string{}
	for _, item := range tags {
		tag := item.(map[string]any)
		tagMap[tag["Key"].(string)] = tag["Value"].(string)
	}
	assert.Equal(t, "prod", tagMap["Env"])
	assert.Equal(t, "platform", tagMap["Team"])
}

func TestUntagResource_RemovesSpecificKeys_HTTP(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "untag-repo",
		"tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	doAccuracy(t, h, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"remove"},
	})

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{"resourceArn": arn})
	out := parseAccuracy(t, listRec)
	tags, _ := out["tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "keep", tag["Key"])
}

func TestCreateRepository_Tags_Applied_On_Create(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "tagged-on-create",
		"tags": []map[string]any{
			{"Key": "Project", "Value": "gopherstack"},
			{"Key": "Cost", "Value": "engineering"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	listOut := parseAccuracy(t, listRec)
	tags, _ := listOut["tags"].([]any)
	assert.Len(t, tags, 2)
}

func TestTagResource_And_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "tag-ops",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	doAccuracy(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": []map[string]any{
			{"Key": "k1", "Value": "v1"},
			{"Key": "k2", "Value": "v2"},
		},
	})

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	out := parseAccuracy(t, listRec)
	tags, _ := out["tags"].([]any)
	assert.Len(t, tags, 2)

	tagMap := map[string]string{}
	for _, item := range tags {
		tag := item.(map[string]any)
		tagMap[tag["Key"].(string)] = tag["Value"].(string)
	}
	assert.Equal(t, "v1", tagMap["k1"])
	assert.Equal(t, "v2", tagMap["k2"])
}

func TestUntagResource_RemovesKeyOnly(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "untag-key",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	doAccuracy(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
			{"Key": "also-keep", "Value": "sure"},
		},
	})

	doAccuracy(t, h, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"remove"},
	})

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	tags, _ := parseAccuracy(t, listRec)["tags"].([]any)
	assert.Len(t, tags, 2)
}

func TestTagResource_Upsert(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "tag-upsert",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	doAccuracy(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        []map[string]any{{"Key": "env", "Value": "staging"}},
	})
	doAccuracy(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags":        []map[string]any{{"Key": "env", "Value": "prod"}},
	})

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	tags, _ := parseAccuracy(t, listRec)["tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "prod", tag["Value"],
		"TagResource must upsert (overwrite existing key)")
}

func TestListTagsForResource_Empty_ReturnsEmptyList(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "no-tags",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn, _ := parseAccuracy(t, rec)["repository"].(map[string]any)["repositoryArn"].(string)

	listRec := doAccuracy(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	out := parseAccuracy(t, listRec)
	tags, hasKey := out["tags"]
	if hasKey && tags != nil {
		tagList, _ := tags.([]any)
		assert.Empty(t, tagList)
	}
}
