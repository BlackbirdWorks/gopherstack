package codecommit_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input       map[string]any
		name        string
		wantRepoKey string
		wantStatus  int
	}{
		{
			name: "success",
			input: map[string]any{
				"repositoryName":        "my-repo",
				"repositoryDescription": "A test repository",
				"tags":                  map[string]string{"env": "test"},
			},
			wantStatus:  http.StatusOK,
			wantRepoKey: "my-repo",
		},
		{
			name: "missing_name",
			input: map[string]any{
				"repositoryName": "",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateRepository", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				meta, ok := resp["repositoryMetadata"].(map[string]any)
				require.True(t, ok, "repositoryMetadata should be present")
				assert.Equal(t, tt.wantRepoKey, meta["repositoryName"])
				assert.NotEmpty(t, meta["repositoryId"])
				assert.NotEmpty(t, meta["Arn"])
			}
		})
	}
}

func TestHandler_GetRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		wantStatus int
	}{
		{
			name:       "existing_repository",
			repoName:   "my-repo",
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_repository",
			repoName:   "missing-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Seed a repository for the existing case.
			if tt.repoName == "my-repo" {
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "my-repo",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "GetRepository", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		wantStatus int
	}{
		{
			name:       "existing_repository",
			repoName:   "to-delete",
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_repository",
			repoName:   "missing-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.repoName == "to-delete" {
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "to-delete",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteRepository", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		repos     []string
		wantCount int
	}{
		{
			name:      "empty",
			repos:     nil,
			wantCount: 0,
		},
		{
			name:      "two_repositories",
			repos:     []string{"repo-a", "repo-b"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, n := range tt.repos {
				rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": n})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListRepositories", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			repos, ok := resp["repositories"].([]any)
			require.True(t, ok)
			assert.Len(t, repos, tt.wantCount)
		})
	}
}

func TestHandler_DuplicateRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName": "dupe-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName": "dupe-repo",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_SortedListRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra", "alpha", "middle"} {
		rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListRepositories", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, ok := resp["repositories"].([]any)
	require.True(t, ok)
	require.Len(t, items, 3)

	names := make([]string, 0, 3)
	for _, item := range items {
		m := item.(map[string]any)
		names = append(names, m["repositoryName"].(string))
	}

	assert.Equal(t, []string{"alpha", "middle", "zebra"}, names)
}

func TestHandler_DeleteRepository_Cascade(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create repo + branch + commit
	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"commitMessage":  "init",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete the repo
	rec = doRequest(t, h, "DeleteRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Repo is gone
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// BatchGetCommits on deleted repo returns 404
	rec = doRequest(t, h, "BatchGetCommits", map[string]any{
		"repositoryName": "repo",
		"commitIds":      []string{"some-id"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_RepoMetadataTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	meta, ok := resp["repositoryMetadata"].(map[string]any)
	require.True(t, ok)

	assert.NotNil(t, meta["creationDate"], "creationDate should be present")
	assert.NotNil(t, meta["lastModifiedDate"], "lastModifiedDate should be present")
	assert.Greater(t, meta["creationDate"].(float64), float64(0))
}

func TestHandler_CreateRepository_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		wantStatus int
	}{
		{
			name:       "valid_name",
			repoName:   "my-repo",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_with_dots",
			repoName:   "my.repo.name",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_with_underscores",
			repoName:   "my_repo_name",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_name",
			repoName:   "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name_with_spaces",
			repoName:   "my repo",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name_with_slash",
			repoName:   "my/repo",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name_too_long",
			repoName:   string(make([]byte, 101)),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateRepository", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_RepoMetadata_DefaultBranchAndKmsKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Initially no defaultBranch or kmsKeyId in metadata.
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	meta := resp["repositoryMetadata"].(map[string]any)
	_, hasDefault := meta["defaultBranch"]
	_, hasKms := meta["kmsKeyId"]
	assert.False(t, hasDefault, "defaultBranch should not appear when unset")
	assert.False(t, hasKms, "kmsKeyId should not appear when unset")

	// Create a commit so the "main" branch exists before setting it as default.
	rec = doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"commitMessage":  "init",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set defaultBranch.
	rec = doRequest(t, h, "UpdateDefaultBranch", map[string]any{
		"repositoryName":    "repo",
		"defaultBranchName": "main",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Set kmsKeyId.
	rec = doRequest(t, h, "UpdateRepositoryEncryptionKey", map[string]any{
		"repositoryName": "repo",
		"kmsKeyId":       "arn:aws:kms:us-east-1:123456789012:key/my-key",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Now GetRepository should include both.
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	meta = resp["repositoryMetadata"].(map[string]any)
	assert.Equal(t, "main", meta["defaultBranch"])
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/my-key", meta["kmsKeyId"])
}

func TestHandler_BatchGetRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		seedRepos      []string
		requestRepos   []string
		wantFoundCount int
		wantMissCount  int
		wantStatus     int
	}{
		{
			name:           "all_found",
			seedRepos:      []string{"repo-a", "repo-b"},
			requestRepos:   []string{"repo-a", "repo-b"},
			wantFoundCount: 2,
			wantMissCount:  0,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "partial_found",
			seedRepos:      []string{"repo-a"},
			requestRepos:   []string{"repo-a", "missing-b"},
			wantFoundCount: 1,
			wantMissCount:  1,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "none_found",
			seedRepos:      nil,
			requestRepos:   []string{"missing-a"},
			wantFoundCount: 0,
			wantMissCount:  1,
			wantStatus:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.seedRepos {
				rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": name})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "BatchGetRepositories", map[string]any{
				"repositoryNames": tt.requestRepos,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			repos, _ := resp["repositories"].([]any)
			assert.Len(t, repos, tt.wantFoundCount)

			notFound, _ := resp["repositoriesNotFound"].([]any)
			assert.Len(t, notFound, tt.wantMissCount)
		})
	}
}

func TestHandler_BatchGetRepositories_MaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 26 repos.
	names := make([]string, 26)
	for i := range names {
		names[i] = fmt.Sprintf("repo-%02d", i)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": names[i]})
	}

	// Request 26 — should fail.
	rec := doRequest(t, h, "BatchGetRepositories", map[string]any{
		"repositoryNames": names,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "MaximumRepositoryNamesExceededException", errResp["__type"])

	// Request 25 — should succeed.
	rec = doRequest(t, h, "BatchGetRepositories", map[string]any{
		"repositoryNames": names[:25],
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_BatchGetRepositories_PartialFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "exists"})

	rec := doRequest(t, h, "BatchGetRepositories", map[string]any{
		"repositoryNames": []string{"exists", "missing1", "missing2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	repos := resp["repositories"].([]any)
	notFound := resp["repositoriesNotFound"].([]any)

	assert.Len(t, repos, 1)
	assert.Len(t, notFound, 2)
}

func TestHandler_GetRepository_FullMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName":        "full-meta-repo",
		"repositoryDescription": "desc here",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "full-meta-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	meta := resp["repositoryMetadata"].(map[string]any)

	assert.NotEmpty(t, meta["repositoryId"])
	assert.Equal(t, "full-meta-repo", meta["repositoryName"])
	assert.NotEmpty(t, meta["Arn"])
	assert.NotEmpty(t, meta["accountId"])
	assert.NotEmpty(t, meta["cloneUrlHttp"])
	assert.NotEmpty(t, meta["cloneUrlSsh"])
	assert.NotNil(t, meta["creationDate"])
	assert.NotNil(t, meta["lastModifiedDate"])
	assert.Equal(t, "desc here", meta["repositoryDescription"])
}

func TestHandler_UpdateRepositoryDescription_Reflected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	doRequest(t, h, "UpdateRepositoryDescription", map[string]any{
		"repositoryName":        "repo",
		"repositoryDescription": "new description",
	})

	rec := doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	meta := resp["repositoryMetadata"].(map[string]any)
	assert.Equal(t, "new description", meta["repositoryDescription"])
}

func TestHandler_CreateRepository_InvalidName_ErrorType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "bad name with spaces"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidRepositoryNameException", errResp["__type"])
}

func TestHandler_UpdateRepositoryDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo1"})

	rec := doRequest(t, h, "UpdateRepositoryDescription", map[string]any{
		"repositoryName":        "repo1",
		"repositoryDescription": "updated desc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Non-existent repo
	rec = doRequest(t, h, "UpdateRepositoryDescription", map[string]any{
		"repositoryName":        "no-such-repo",
		"repositoryDescription": "x",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateRepositoryName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "old-name"})

	rec := doRequest(t, h, "UpdateRepositoryName", map[string]any{
		"oldName": "old-name",
		"newName": "new-name",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// old name no longer exists
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "old-name"})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// new name exists
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "new-name"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateRepositoryEncryptionKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "enc-repo"})

	rec := doRequest(t, h, "UpdateRepositoryEncryptionKey", map[string]any{
		"repositoryName": "enc-repo",
		"kmsKeyId":       "arn:aws:kms:us-east-1:123456789012:key/abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// not found
	rec = doRequest(t, h, "UpdateRepositoryEncryptionKey", map[string]any{
		"repositoryName": "no-repo",
		"kmsKeyId":       "key",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateRepositoryName_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		oldName    string
		newName    string
		seed       bool
		wantStatus int
	}{
		{
			name:       "success",
			oldName:    "old",
			newName:    "new",
			seed:       true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "old_not_found",
			oldName:    "no-such",
			newName:    "new",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "new_already_exists",
			oldName:    "old",
			newName:    "existing",
			seed:       true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_old_name",
			oldName:    "",
			newName:    "new",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "old"})
			}
			if tt.newName == "existing" {
				doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "existing"})
			}

			rec := doRequest(t, h, "UpdateRepositoryName", map[string]any{
				"oldName": tt.oldName,
				"newName": tt.newName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Verify rename is reflected.
			if tt.wantStatus == http.StatusOK {
				rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": tt.newName})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": tt.oldName})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			}
		})
	}
}
