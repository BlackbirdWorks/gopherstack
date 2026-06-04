package codecommit_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetCommit_ParentsNotNull verifies that GetCommit always returns "parents" as a JSON
// array, never null. AWS always returns "parents": [] even for root commits.
func TestGetCommit_ParentsNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		commitIdx  int // 0 = first (root), 1 = second (has parent)
		wantParent bool
	}{
		{
			name:       "root_commit_parents_empty_array",
			commitIdx:  0,
			wantParent: false,
		},
		{
			name:       "second_commit_parents_has_entry",
			commitIdx:  1,
			wantParent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			// Create first commit (root — no parents).
			r1 := doRequest(t, h, "CreateCommit", map[string]any{
				"repositoryName": "repo",
				"branchName":     "main",
				"commitMessage":  "first",
			})
			require.Equal(t, http.StatusOK, r1.Code)

			var firstResp map[string]any
			require.NoError(t, json.Unmarshal(r1.Body.Bytes(), &firstResp))
			firstID := firstResp["commitId"].(string)

			// Create second commit (has first as parent).
			r2 := doRequest(t, h, "CreateCommit", map[string]any{
				"repositoryName": "repo",
				"branchName":     "main",
				"commitMessage":  "second",
			})
			require.Equal(t, http.StatusOK, r2.Code)

			var secondResp map[string]any
			require.NoError(t, json.Unmarshal(r2.Body.Bytes(), &secondResp))
			secondID := secondResp["commitId"].(string)

			ids := []string{firstID, secondID}
			target := ids[tt.commitIdx]

			rec := doRequest(t, h, "GetCommit", map[string]any{
				"repositoryName": "repo",
				"commitId":       target,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			commit, ok := resp["commit"].(map[string]any)
			require.True(t, ok, "commit field must be present")

			// parents must be a JSON array, never null.
			parents, ok := commit["parents"].([]any)
			require.True(t, ok, "parents must be a JSON array, not null")

			if tt.wantParent {
				require.Len(t, parents, 1)
				assert.Equal(t, firstID, parents[0].(string))
			} else {
				assert.Empty(t, parents, "root commit must have empty parents array")
			}
		})
	}
}

// TestGetFolder_AlwaysIncludesSubArrays verifies that GetFolder always returns
// "subFolders", "subModules", and "symbolicLinks" as JSON arrays (not absent/null),
// even when empty. AWS always includes these fields.
func TestGetFolder_AlwaysIncludesSubArrays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		folderPath string
		seedFiles  []string
		wantFiles  int
	}{
		{
			name:       "empty_repo_root",
			seedFiles:  nil,
			folderPath: "",
			wantFiles:  0,
		},
		{
			name:       "folder_with_one_file",
			seedFiles:  []string{"src/main.go"},
			folderPath: "src",
			wantFiles:  1,
		},
		{
			name:       "root_with_files",
			seedFiles:  []string{"README.md", "go.mod"},
			folderPath: "",
			wantFiles:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			for _, fp := range tt.seedFiles {
				doRequest(t, h, "PutFile", map[string]any{
					"repositoryName": "repo",
					"branchName":     "main",
					"filePath":       fp,
					"fileContent":    "aGVsbG8=", // "hello" base64
				})
			}

			rec := doRequest(t, h, "GetFolder", map[string]any{
				"repositoryName": "repo",
				"folderPath":     tt.folderPath,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			// files array must always be present and correct count.
			files, ok := resp["files"].([]any)
			require.True(t, ok, "files must be a JSON array")
			assert.Len(t, files, tt.wantFiles)

			// subFolders, subModules, symbolicLinks must be present arrays (not absent/null).
			subFolders, ok := resp["subFolders"].([]any)
			require.True(t, ok, "subFolders must be a JSON array, not null")
			assert.Empty(t, subFolders)

			subModules, ok := resp["subModules"].([]any)
			require.True(t, ok, "subModules must be a JSON array, not null")
			assert.Empty(t, subModules)

			symLinks, ok := resp["symbolicLinks"].([]any)
			require.True(t, ok, "symbolicLinks must be a JSON array, not null")
			assert.Empty(t, symLinks)
		})
	}
}

// TestListAssociatedApprovalRuleTemplatesForRepository_RepoNotFound verifies that
// listing templates for a nonexistent repository returns 404. AWS throws
// RepositoryDoesNotExistException instead of silently returning an empty list.
func TestListAssociatedApprovalRuleTemplatesForRepository_RepoNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoName   string
		seed       bool
		wantStatus int
	}{
		{
			name:       "repo_exists_no_templates",
			repoName:   "repo",
			seed:       true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "repo_not_found",
			repoName:   "no-such-repo",
			seed:       false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
			}

			rec := doRequest(t, h, "ListAssociatedApprovalRuleTemplatesForRepository", map[string]any{
				"repositoryName": tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				names, ok := resp["approvalRuleTemplateNames"].([]any)
				require.True(t, ok, "approvalRuleTemplateNames must be a JSON array")
				assert.Empty(t, names)
			} else {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "RepositoryDoesNotExistException", errResp["__type"])
			}
		})
	}
}

// TestListRepositoriesForApprovalRuleTemplate_TemplateNotFound verifies that listing
// repositories for a nonexistent template returns 404. AWS throws
// ApprovalRuleTemplateDoesNotExistException instead of silently returning empty.
func TestListRepositoriesForApprovalRuleTemplate_TemplateNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		templateName string
		seed         bool
		wantStatus   int
	}{
		{
			name:         "template_exists_no_repos",
			templateName: "tmpl",
			seed:         true,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "template_not_found",
			templateName: "no-such-tmpl",
			seed:         false,
			wantStatus:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName":    "tmpl",
					"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
				})
			}

			rec := doRequest(t, h, "ListRepositoriesForApprovalRuleTemplate", map[string]any{
				"approvalRuleTemplateName": tt.templateName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				repos, ok := resp["repositoryNames"].([]any)
				require.True(t, ok, "repositoryNames must be a JSON array")
				assert.Empty(t, repos)
			} else {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "ApprovalRuleTemplateDoesNotExistException", errResp["__type"])
			}
		})
	}
}

// TestGetCommit_FullFieldSet verifies GetCommit response matches commitToMap shape:
// commitId, treeId, message, parents (array), author, committer, additionalData.
func TestGetCommit_FullFieldSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	r := doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"authorName":     "Alice",
		"email":          "alice@example.com",
		"commitMessage":  "test commit",
	})
	require.Equal(t, http.StatusOK, r.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(r.Body.Bytes(), &createResp))
	commitID := createResp["commitId"].(string)

	rec := doRequest(t, h, "GetCommit", map[string]any{
		"repositoryName": "repo",
		"commitId":       commitID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	commit, ok := resp["commit"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, commitID, commit["commitId"])
	assert.NotEmpty(t, commit["treeId"])
	assert.Equal(t, "test commit", commit["message"])

	// parents must be [] not null for root commit.
	parents, ok := commit["parents"].([]any)
	require.True(t, ok, "parents must be JSON array, not null")
	assert.Empty(t, parents)

	// author and committer sub-objects must be present.
	author, ok := commit["author"].(map[string]any)
	require.True(t, ok, "author sub-object must be present")
	assert.Equal(t, "Alice", author["name"])
	assert.Equal(t, "alice@example.com", author["email"])

	committer, ok := commit["committer"].(map[string]any)
	require.True(t, ok, "committer sub-object must be present")
	assert.NotNil(t, committer["name"])
}
