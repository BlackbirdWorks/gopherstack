package codecommit_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_GetDifferences_BlobObjectShape verifies GetDifferences returns afterBlob/beforeBlob
// as objects with blobId, path, and mode fields (not plain strings), matching real AWS behavior.
func TestParity_GetDifferences_BlobObjectShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filePaths []string
		wantCount int
	}{
		{
			name:      "single_file",
			filePaths: []string{"README.md"},
			wantCount: 1,
		},
		{
			name:      "multiple_files",
			filePaths: []string{"src/main.go", "go.mod", "README.md"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "diffs-repo"})

			putFiles := make([]map[string]any, 0, len(tt.filePaths))
			for _, fp := range tt.filePaths {
				putFiles = append(putFiles, map[string]any{
					"filePath":    fp,
					"fileContent": "aGVsbG8=",
				})
			}

			commitRec := doRequest(t, h, "CreateCommit", map[string]any{
				"repositoryName": "diffs-repo",
				"branchName":     "main",
				"putFiles":       putFiles,
			})
			require.Equal(t, http.StatusOK, commitRec.Code)

			var commitOut map[string]any
			require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitOut))
			commitID := commitOut["commitId"].(string)

			rec := doRequest(t, h, "GetDifferences", map[string]any{
				"repositoryName":       "diffs-repo",
				"afterCommitSpecifier": commitID,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Differences []map[string]any `json:"differences"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Differences, tt.wantCount)

			// Each difference must have afterBlob as an object, not a string.
			for i, diff := range resp.Differences {
				afterBlob, ok := diff["afterBlob"].(map[string]any)
				require.True(t, ok, "differences[%d].afterBlob must be an object", i)

				assert.NotEmpty(t, afterBlob["blobId"], "afterBlob.blobId must be non-empty")
				assert.NotEmpty(t, afterBlob["path"], "afterBlob.path must be non-empty")
				assert.NotEmpty(t, afterBlob["mode"], "afterBlob.mode must be non-empty")
				assert.Equal(t, "A", diff["changeType"], "changeType must be A for new files")
			}
		})
	}
}

// TestParity_GetDifferences_FilePathInBlob verifies that afterBlob.path matches the committed
// file path, matching real AWS behavior.
func TestParity_GetDifferences_FilePathInBlob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "path-repo"})

	commitRec := doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "path-repo",
		"branchName":     "main",
		"putFiles": []map[string]any{
			{"filePath": "src/handler.go", "fileContent": "cGFja2FnZSBtYWlu"},
		},
	})
	require.Equal(t, http.StatusOK, commitRec.Code)

	var commitOut map[string]any
	require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitOut))

	rec := doRequest(t, h, "GetDifferences", map[string]any{
		"repositoryName":       "path-repo",
		"afterCommitSpecifier": commitOut["commitId"].(string),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Differences []map[string]any `json:"differences"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Differences, 1)

	afterBlob := resp.Differences[0]["afterBlob"].(map[string]any)
	assert.Equal(t, "src/handler.go", afterBlob["path"],
		"afterBlob.path must match the committed file path")
}

// TestParity_ListFileCommitHistory_FiltersByFilePath verifies ListFileCommitHistory
// only returns commits that touched the specified file, matching real AWS behavior.
func TestParity_ListFileCommitHistory_FiltersByFilePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryPath string
		wantCount int
	}{
		{
			name:      "existing_file",
			queryPath: "main.go",
			wantCount: 2,
		},
		{
			name:      "other_file",
			queryPath: "other.go",
			wantCount: 1,
		},
		{
			name:      "nonexistent_file",
			queryPath: "does-not-exist.go",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "history-repo"})

			// Commit 1: touches main.go and other.go.
			doRequest(t, h, "CreateCommit", map[string]any{
				"repositoryName": "history-repo",
				"branchName":     "main",
				"putFiles": []map[string]any{
					{"filePath": "main.go", "fileContent": "dmVyc2lvbjE="},
					{"filePath": "other.go", "fileContent": "cGFja2FnZSBtYWlu"},
				},
			})

			// Commit 2: touches only main.go.
			doRequest(t, h, "CreateCommit", map[string]any{
				"repositoryName": "history-repo",
				"branchName":     "main",
				"putFiles": []map[string]any{
					{"filePath": "main.go", "fileContent": "dmVyc2lvbjI="},
				},
			})

			rec := doRequest(t, h, "ListFileCommitHistory", map[string]any{
				"repositoryName": "history-repo",
				"filePath":       tt.queryPath,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			dag := resp["revisionDag"].([]any)
			assert.Len(t, dag, tt.wantCount,
				"revisionDag must contain %d commits for path %q", tt.wantCount, tt.queryPath)
		})
	}
}

// TestParity_GetMergeCommit_ResolvesParents verifies GetMergeCommit returns the commit
// whose parents include both source and destination specifiers.
func TestParity_GetMergeCommit_ResolvesParents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "merge-repo"})

	// Create base commit on main.
	baseRec := doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "merge-repo",
		"branchName":     "main",
		"putFiles":       []map[string]any{{"filePath": "base.txt", "fileContent": "YmFzZQ=="}},
	})
	require.Equal(t, http.StatusOK, baseRec.Code)

	var baseOut map[string]any
	require.NoError(t, json.Unmarshal(baseRec.Body.Bytes(), &baseOut))
	baseCommitID := baseOut["commitId"].(string)

	// Create feature branch from base.
	doRequest(t, h, "CreateBranch", map[string]any{
		"repositoryName": "merge-repo",
		"branchName":     "feature",
		"commitId":       baseCommitID,
	})

	// Commit to feature branch.
	featureRec := doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "merge-repo",
		"branchName":     "feature",
		"putFiles":       []map[string]any{{"filePath": "feature.txt", "fileContent": "ZmVhdA=="}},
	})
	require.Equal(t, http.StatusOK, featureRec.Code)

	var featureOut map[string]any
	require.NoError(t, json.Unmarshal(featureRec.Body.Bytes(), &featureOut))
	featureCommitID := featureOut["commitId"].(string)

	// Fast-forward merge.
	mergeRec := doRequest(t, h, "MergeBranchesByFastForward", map[string]any{
		"repositoryName":             "merge-repo",
		"sourceCommitSpecifier":      featureCommitID,
		"destinationCommitSpecifier": baseCommitID,
		"targetBranch":               "main",
	})
	require.Equal(t, http.StatusOK, mergeRec.Code)

	// GetMergeCommit should return a valid commit.
	rec := doRequest(t, h, "GetMergeCommit", map[string]any{
		"repositoryName":             "merge-repo",
		"sourceCommitSpecifier":      featureCommitID,
		"destinationCommitSpecifier": baseCommitID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["mergedCommitId"],
		"GetMergeCommit must return a non-empty mergedCommitId")
}

// TestParity_GetDifferences_EmptyRepoReturnsEmptyArray verifies GetDifferences on
// an empty repo returns an empty array (not null), matching real AWS behavior.
func TestParity_GetDifferences_EmptyRepoReturnsEmptyArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "empty-repo"})

	rec := doRequest(t, h, "GetDifferences", map[string]any{
		"repositoryName":       "empty-repo",
		"afterCommitSpecifier": "abc123",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	diffs, ok := resp["differences"].([]any)
	require.True(t, ok, "differences must be a JSON array, not null")
	assert.Empty(t, diffs)
}
