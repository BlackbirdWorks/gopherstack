package codecommit_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateBranch(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

		// Seed a real commit via CreateCommit.
		commitRec := doRequest(t, h, "CreateCommit", map[string]any{
			"repositoryName": "repo",
			"branchName":     "main",
			"authorName":     "test",
			"email":          "test@example.com",
			"commitMessage":  "init",
		})
		require.Equal(t, http.StatusOK, commitRec.Code)

		var commitResp map[string]any
		require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitResp))
		commitID := commitResp["commitId"].(string)

		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       commitID,
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("commit_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       "does-not-exist",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("repo_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "missing-repo",
			"branchName":     "feature",
			"commitId":       "abc123",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("missing_branch_name", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "",
			"commitId":       "abc123",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing_commit_id", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       "",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("duplicate_branch", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

		commitRec := doRequest(t, h, "CreateCommit", map[string]any{
			"repositoryName": "repo",
			"branchName":     "main",
			"authorName":     "test",
			"email":          "test@example.com",
			"commitMessage":  "init",
		})
		require.Equal(t, http.StatusOK, commitRec.Code)

		var commitResp map[string]any
		require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitResp))
		commitID := commitResp["commitId"].(string)

		doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       commitID,
		})
		rec := doRequest(t, h, "CreateBranch", map[string]any{
			"repositoryName": "repo",
			"branchName":     "feature",
			"commitId":       commitID,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_CreateBranch_CommitValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	// Create a real commit.
	commitRec := doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"authorName":     "test",
		"email":          "test@example.com",
		"commitMessage":  "init",
	})
	require.Equal(t, http.StatusOK, commitRec.Code)

	var commitResp map[string]any
	require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitResp))
	realCommitID := commitResp["commitId"].(string)

	tests := []struct {
		name       string
		commitID   string
		wantStatus int
	}{
		{
			name:       "valid_commit",
			commitID:   realCommitID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_commit",
			commitID:   "00000000-0000-0000-0000-000000000000",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hh := newTestHandler(t)
			doRequest(t, hh, "CreateRepository", map[string]any{"repositoryName": "repo"})

			// Seed the commit into this handler's backend if needed.
			if tt.commitID == realCommitID {
				// Create our own real commit for this subtest handler.
				cr := doRequest(t, hh, "CreateCommit", map[string]any{
					"repositoryName": "repo",
					"branchName":     "main",
					"authorName":     "test",
					"email":          "test@example.com",
					"commitMessage":  "init",
				})
				require.Equal(t, http.StatusOK, cr.Code)
				var cr2 map[string]any
				require.NoError(t, json.Unmarshal(cr.Body.Bytes(), &cr2))
				tt.commitID = cr2["commitId"].(string)
			}

			rec := doRequest(t, hh, "CreateBranch", map[string]any{
				"repositoryName": "repo",
				"branchName":     "feature",
				"commitId":       tt.commitID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateDefaultBranch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "br-repo"})
	// Create a commit so the "main" branch exists.
	doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "br-repo",
		"branchName":     "main",
		"commitMessage":  "init",
	})

	rec := doRequest(t, h, "UpdateDefaultBranch", map[string]any{
		"repositoryName":    "br-repo",
		"defaultBranchName": "main",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Branch not found in repo.
	rec = doRequest(t, h, "UpdateDefaultBranch", map[string]any{
		"repositoryName":    "br-repo",
		"defaultBranchName": "no-such-branch",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Repo not found.
	rec = doRequest(t, h, "UpdateDefaultBranch", map[string]any{
		"repositoryName":    "no-repo",
		"defaultBranchName": "main",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteBranch_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		branchName string
		seed       bool
		wantCode   int
	}{
		{name: "success", branchName: "main", seed: true, wantCode: http.StatusOK},
		// DeleteBranch is idempotent in real AWS: deleting a branch name that
		// does not exist succeeds (its own error switch has no
		// BranchDoesNotExistException case), unlike GetBranch.
		{name: "not_found", branchName: "no-branch", seed: true, wantCode: http.StatusOK},
		{name: "repo_not_found", branchName: "main", seed: false, wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				setupRepoAndBranch(t, h, "repo")
			}

			repoName := "repo"
			if !tt.seed {
				repoName = "no-repo"
			}

			rec := doRequest(t, h, "DeleteBranch", map[string]any{
				"repositoryName": repoName,
				"branchName":     tt.branchName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
