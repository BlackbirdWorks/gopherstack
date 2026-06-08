package ecr_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// newRefinementBackend creates a fresh InMemoryBackend for testing.
func newRefinementBackend() *ecr.InMemoryBackend {
	return ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
}

// newRefinementHandler creates a fresh Handler backed by a fresh InMemoryBackend.
func newRefinementHandler() (*ecr.Handler, *ecr.InMemoryBackend) {
	b := newRefinementBackend()

	return ecr.NewHandler(b, nil), b
}

// doRefinementRequest fires a synthetic ECR control-plane request against h.
func doRefinementRequest(t *testing.T, h *ecr.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+action)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

// TestRefinement1_Reset verifies that Reset clears all backend state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		populate func(b *ecr.InMemoryBackend)
		name     string
		wantZero bool
	}{
		{
			name:     "empty_backend",
			wantZero: true,
		},
		{
			name: "populated_backend",
			populate: func(b *ecr.InMemoryBackend) {
				b.AddRepositoryInternal(ecr.Repository{
					RepositoryName: "repo1",
					RepositoryARN:  "arn:aws:ecr:us-east-1:123456789012:repository/repo1",
				})
				b.AddLifecyclePolicyInternal("repo1", `{"rules":[]}`)
			},
			wantZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			if tt.populate != nil {
				tt.populate(b)
			}

			b.Reset()

			assert.Equal(t, tt.wantZero, b.RepositoryCount() == 0)
			assert.Equal(t, tt.wantZero, b.LifecyclePolicyCount() == 0)
		})
	}
}

// TestRefinement1_MultipleResetCycle verifies that Reset can be called multiple times.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cycles int
	}{
		{name: "single_reset", cycles: 1},
		{name: "triple_reset", cycles: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			b.AddRepositoryInternal(ecr.Repository{RepositoryName: "r1", RepositoryARN: "arn:r1"})

			for i := range tt.cycles {
				b.Reset()
				assert.Equal(t, 0, b.RepositoryCount(), "cycle %d", i)
			}
		})
	}
}

// TestRefinement1_HandlerReset verifies that Handler.Reset() clears backend state.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		repo string
	}{
		{name: "resets_after_create", repo: "to-reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefinementHandler()
			rec := doRefinementRequest(t, h, "CreateRepository", map[string]any{"repositoryName": tt.repo})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, 1, b.RepositoryCount())

			h.Reset()
			assert.Equal(t, 0, b.RepositoryCount())
		})
	}
}

// TestRefinement1_ProviderInit_NilCtx verifies that Init returns ErrNilAppContext when ctx is nil.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{name: "nil_ctx_returns_err", wantErr: ecr.ErrNilAppContext},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &ecr.Provider{}
			_, err := p.Init(nil)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestRefinement1_HandlerOpsPreBuilt verifies the ops map is pre-built (dispatch works without rebuild).
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:       "describe_repositories",
			action:     "DescribeRepositories",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_auth_token",
			action:     "GetAuthorizationToken",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefinementHandler()
			rec := doRefinementRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_SortedDescribeRepositories verifies DescribeRepositories returns sorted results.
func TestRefinement1_SortedDescribeRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		repos     []string
		wantOrder []string
	}{
		{
			name:      "alphabetical_order",
			repos:     []string{"zebra", "alpha", "mango"},
			wantOrder: []string{"alpha", "mango", "zebra"},
		},
		{
			name:      "single_repo",
			repos:     []string{"only-one"},
			wantOrder: []string{"only-one"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			for _, name := range tt.repos {
				b.AddRepositoryInternal(ecr.Repository{
					RepositoryName: name,
					RepositoryARN:  "arn:aws:ecr:us-east-1:123456789012:repository/" + name,
				})
			}

			repos, err := b.DescribeRepositories(context.Background(), nil)
			require.NoError(t, err)

			got := make([]string, 0, len(repos))
			for _, r := range repos {
				got = append(got, r.RepositoryName)
			}

			assert.Equal(t, tt.wantOrder, got)
		})
	}
}

// TestRefinement1_TagResource verifies TagResource stores tags.
func TestRefinement1_TagResource(t *testing.T) {
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

			b := newRefinementBackend()
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

// TestRefinement1_UntagResource verifies UntagResource removes specific tags.
func TestRefinement1_UntagResource(t *testing.T) {
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

			b := newRefinementBackend()
			require.NoError(t, b.TagResource(context.Background(), tt.arn, tt.initialTags))
			require.NoError(t, b.UntagResource(context.Background(), tt.arn, tt.removeKeys))

			got, err := b.ListTagsForResource(context.Background(), tt.arn)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)
		})
	}
}

// TestRefinement1_ListTagsForResource verifies the handler returns sorted tags.
func TestRefinement1_ListTagsForResource(t *testing.T) {
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

			h, _ := newRefinementHandler()

			doRefinementRequest(t, h, "TagResource", map[string]any{
				"resourceArn": tt.arn,
				"tags":        tt.tags,
			})

			rec := doRefinementRequest(t, h, "ListTagsForResource", map[string]any{
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

// TestRefinement1_TagsRoundTrip verifies tags survive a create-tag-list cycle via HTTP.
func TestRefinement1_TagsRoundTrip(t *testing.T) {
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

			h, _ := newRefinementHandler()

			rec := doRefinementRequest(t, h, "CreateRepository", map[string]any{
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

			listRec := doRefinementRequest(t, h, "ListTagsForResource", map[string]any{
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

// TestRefinement1_DeleteRepository_Cascade verifies that deleting a repo cleans up lifecycle policies and tags.
func TestRefinement1_DeleteRepository_Cascade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		repoName string
		arn      string
	}{
		{
			name:     "cascade_delete",
			repoName: "cascade-repo",
			arn:      "arn:aws:ecr:us-east-1:123456789012:repository/cascade-repo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			b.AddRepositoryInternal(ecr.Repository{
				RepositoryName: tt.repoName,
				RepositoryARN:  tt.arn,
			})
			b.AddLifecyclePolicyInternal(tt.repoName, `{"rules":[]}`)
			require.NoError(t, b.TagResource(context.Background(), tt.arn, map[string]string{"env": "prod"}))

			assert.Equal(t, 1, b.LifecyclePolicyCount())

			_, err := b.DeleteRepository(context.Background(), tt.repoName)
			require.NoError(t, err)

			assert.Equal(t, 0, b.RepositoryCount())
			assert.Equal(t, 0, b.LifecyclePolicyCount())

			tags, err := b.ListTagsForResource(context.Background(), tt.arn)
			require.NoError(t, err)
			assert.Empty(t, tags)
		})
	}
}

// TestRefinement1_PutLifecyclePolicy verifies PutLifecyclePolicy via HTTP.
func TestRefinement1_PutLifecyclePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		policy     string
		wantStatus int
	}{
		{
			name:       "creates_policy",
			repoName:   "policy-repo",
			policy:     `{"rules":[{"rulePriority":1,"description":"test"}]}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "repo_not_found",
			repoName:   "no-such-repo",
			policy:     `{}`,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefinementHandler()

			if tt.wantStatus == http.StatusOK {
				doRefinementRequest(t, h, "CreateRepository", map[string]any{"repositoryName": tt.repoName})
			}

			rec := doRefinementRequest(t, h, "PutLifecyclePolicy", map[string]any{
				"repositoryName":      tt.repoName,
				"lifecyclePolicyText": tt.policy,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_DeleteLifecyclePolicy_NotFound verifies that deleting a non-existent policy returns 404.
func TestRefinement1_DeleteLifecyclePolicy_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		wantStatus int
	}{
		{
			name:       "repo_exists_but_no_policy",
			repoName:   "no-policy-repo",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "repo_not_found",
			repoName:   "ghost-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefinementHandler()

			if tt.repoName == "no-policy-repo" {
				doRefinementRequest(t, h, "CreateRepository", map[string]any{"repositoryName": tt.repoName})
			}

			rec := doRefinementRequest(t, h, "DeleteLifecyclePolicy", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_PutRegistryPolicy verifies PutRegistryPolicy via HTTP.
func TestRefinement1_PutRegistryPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		policy      string
		wantStatus  int
		wantStatus2 int
	}{
		{
			name:        "creates_then_replaces",
			policy:      `{"Version":"2012-10-17","Statement":[]}`,
			wantStatus:  http.StatusOK,
			wantStatus2: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefinementHandler()

			rec1 := doRefinementRequest(t, h, "PutRegistryPolicy", map[string]any{"policyText": tt.policy})
			assert.Equal(t, tt.wantStatus, rec1.Code)

			rec2 := doRefinementRequest(t, h, "PutRegistryPolicy", map[string]any{"policyText": tt.policy})
			assert.Equal(t, tt.wantStatus2, rec2.Code)
		})
	}
}

// TestRefinement1_DeleteRegistryPolicyAfterPut verifies delete succeeds after a put.
func TestRefinement1_DeleteRegistryPolicyAfterPut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		wantStatus int
	}{
		{
			name:       "delete_after_put",
			policy:     `{"Version":"2012-10-17","Statement":[]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefinementHandler()

			doRefinementRequest(t, h, "PutRegistryPolicy", map[string]any{"policyText": tt.policy})

			rec := doRefinementRequest(t, h, "DeleteRegistryPolicy", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_RepositoryCreationTemplate_DuplicatePrefixRejected verifies duplicate prefix returns 400.
func TestRefinement1_RepositoryCreationTemplate_DuplicatePrefixRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prefix     string
		wantFirst  int
		wantSecond int
	}{
		{
			name:       "duplicate_prefix",
			prefix:     "my-prefix",
			wantFirst:  http.StatusOK,
			wantSecond: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefinementHandler()

			rec1 := doRefinementRequest(t, h, "CreateRepositoryCreationTemplate", map[string]any{
				"prefix": tt.prefix,
			})
			assert.Equal(t, tt.wantFirst, rec1.Code)

			rec2 := doRefinementRequest(t, h, "CreateRepositoryCreationTemplate", map[string]any{
				"prefix": tt.prefix,
			})
			assert.Equal(t, tt.wantSecond, rec2.Code)
		})
	}
}

// TestRefinement1_Repository_ImageTagMutability verifies imageTagMutability is stored and returned.
func TestRefinement1_Repository_ImageTagMutability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		repoName           string
		imageTagMutability string
		wantMutability     string
	}{
		{
			name:               "explicit_immutable",
			repoName:           "immutable-repo",
			imageTagMutability: "IMMUTABLE",
			wantMutability:     "IMMUTABLE",
		},
		{
			name:               "default_mutable",
			repoName:           "default-repo",
			imageTagMutability: "",
			wantMutability:     "MUTABLE",
		},
		{
			name:               "explicit_mutable",
			repoName:           "explicit-mutable",
			imageTagMutability: "MUTABLE",
			wantMutability:     "MUTABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			repo, err := b.CreateRepository(context.Background(), tt.repoName, tt.imageTagMutability, false, "", "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantMutability, repo.ImageTagMutability)
		})
	}
}

// TestRefinement1_SeedHelpers verifies AddRepositoryInternal and AddLifecyclePolicyInternal.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		repoName     string
		policyText   string
		wantRepos    int
		wantPolicies int
	}{
		{
			name:         "seed_repo_and_policy",
			repoName:     "seeded-repo",
			policyText:   `{"rules":[]}`,
			wantRepos:    1,
			wantPolicies: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			b.AddRepositoryInternal(ecr.Repository{
				RepositoryName: tt.repoName,
				RepositoryARN:  "arn:aws:ecr:us-east-1:123456789012:repository/" + tt.repoName,
			})
			b.AddLifecyclePolicyInternal(tt.repoName, tt.policyText)

			assert.Equal(t, tt.wantRepos, b.RepositoryCount())
			assert.Equal(t, tt.wantPolicies, b.LifecyclePolicyCount())
		})
	}
}

// TestRefinement1_ExportCountHelpers verifies RepositoryCount and UploadedLayerCount.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repos      int
		wantRepos  int
		wantLayers int
	}{
		{
			name:       "empty_backend",
			repos:      0,
			wantRepos:  0,
			wantLayers: 0,
		},
		{
			name:       "two_repos",
			repos:      2,
			wantRepos:  2,
			wantLayers: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			for i := range tt.repos {
				name := "repo" + strconv.Itoa(i)
				b.AddRepositoryInternal(ecr.Repository{
					RepositoryName: name,
					RepositoryARN:  "arn:r-" + name,
				})
			}

			assert.Equal(t, tt.wantRepos, b.RepositoryCount())
			assert.Equal(t, tt.wantLayers, b.UploadedLayerCount())
		})
	}
}

// TestRefinement1_PersistenceRoundTrip verifies that tags survive snapshot/restore.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
		arn  string
	}{
		{
			name: "tags_survive_snapshot_restore",
			arn:  "arn:aws:ecr:us-east-1:123456789012:repository/persist-repo",
			tags: map[string]string{"env": "prod", "owner": "team"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefinementBackend()
			require.NoError(t, b1.TagResource(context.Background(), tt.arn, tt.tags))

			snap := b1.Snapshot()
			require.NotNil(t, snap)

			b2 := newRefinementBackend()
			require.NoError(t, b2.Restore(snap))

			got, err := b2.ListTagsForResource(context.Background(), tt.arn)
			require.NoError(t, err)
			assert.Equal(t, tt.tags, got)
		})
	}
}

// TestRefinement1_ErrValidationMapping verifies error types map to correct HTTP status codes.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:       "create_repository_empty_name",
			action:     "CreateRepository",
			body:       map[string]any{"repositoryName": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_nonexistent_repository",
			action:     "DeleteRepository",
			body:       map[string]any{"repositoryName": "no-such-repo"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_lifecycle_policy_no_repo",
			action:     "DeleteLifecyclePolicy",
			body:       map[string]any{"repositoryName": "ghost"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_registry_policy_none_set",
			action:     "DeleteRegistryPolicy",
			body:       map[string]any{},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefinementHandler()
			rec := doRefinementRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_GetSupportedOperations_NewOps verifies the new operations are listed.
func TestRefinement1_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		wantOp string
	}{
		{name: "put_lifecycle_policy", wantOp: "PutLifecyclePolicy"},
		{name: "put_registry_policy", wantOp: "PutRegistryPolicy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefinementHandler()
			ops := h.GetSupportedOperations()
			assert.True(t, slices.Contains(ops, tt.wantOp), "expected %s in supported operations", tt.wantOp)
		})
	}
}
