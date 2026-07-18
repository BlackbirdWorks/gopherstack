package codecommit_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutFile_GetFile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "file-repo"})

	rec := doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "file-repo",
		"branchName":     "main",
		"filePath":       "hello.txt",
		"fileContent":    "aGVsbG8=", // base64 "hello"
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	putBlobID, _ := putResp["blobId"].(string)
	assert.NotEmpty(t, putBlobID, "PutFileOutput.blobId is a required AWS field")

	rec = doRequest(t, h, "GetFile", map[string]any{
		"repositoryName":  "file-repo",
		"commitSpecifier": "main",
		"filePath":        "hello.txt",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "hello.txt", resp["filePath"])
	assert.Equal(t, putBlobID, resp["blobId"], "GetFile's blobId must match the blobId PutFile returned")

	// The blob ID PutFile returns must be usable with GetBlob (round trip).
	rec = doRequest(t, h, "GetBlob", map[string]any{
		"repositoryName": "file-repo",
		"blobId":         putBlobID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var blobResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &blobResp))
	assert.Equal(t, "aGVsbG8=", blobResp["content"])
}

func TestHandler_GetFolder(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "folder-repo"})
	doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "folder-repo",
		"filePath":       "src/main.go",
		"fileContent":    "cGFja2FnZSBtYWlu",
	})

	rec := doRequest(t, h, "GetFolder", map[string]any{
		"repositoryName": "folder-repo",
		"folderPath":     "src",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	files := resp["files"].([]any)
	assert.Len(t, files, 1)
}

func TestHandler_DeleteFile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "del-file-repo"})
	putRec := doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "del-file-repo",
		"branchName":     "main",
		"filePath":       "todelete.txt",
		"fileContent":    "dG9kZWxldGU=",
	})

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
	putBlobID, _ := putResp["blobId"].(string)
	require.NotEmpty(t, putBlobID)

	rec := doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "del-file-repo",
		"branchName":     "main",
		"filePath":       "todelete.txt",
		"parentCommitId": "abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["commitId"])
	assert.Equal(t, putBlobID, resp["blobId"],
		"DeleteFileOutput.blobId must report the blob that was removed from the tree")
}

// TestHandler_DeleteFile_NotFound verifies AWS's FileDoesNotExistException is returned
// (not a fabricated success) when deleting a path that was never added to the repository.
func TestHandler_DeleteFile_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "del-file-repo-2"})

	rec := doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "del-file-repo-2",
		"branchName":     "main",
		"filePath":       "never-existed.txt",
	})
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "FileDoesNotExistException", resp["__type"])
}

func TestHandler_GetBlob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "blob-repo"})

	rec := doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "blob-repo",
		"filePath":       "data.bin",
		"fileContent":    "aGVsbG8=", // "hello" in base64
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get the file to find blobId
	rec2 := doRequest(t, h, "GetFile", map[string]any{
		"repositoryName": "blob-repo",
		"filePath":       "data.bin",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var fr map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &fr))
	blobID := fr["blobId"].(string)

	rec = doRequest(t, h, "GetBlob", map[string]any{
		"repositoryName": "blob-repo",
		"blobId":         blobID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["content"])
}

func TestHandler_ListFileCommitHistory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupRepoAndBranch(t, h, "hist-repo")

	rec := doRequest(t, h, "ListFileCommitHistory", map[string]any{
		"repositoryName": "hist-repo",
		"filePath":       "main.go",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["revisionDag"])
}

func TestHandler_ListFileCommitHistory_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		seedCommits int
		wantCode    int
	}{
		{name: "no_commits", seedCommits: 0, wantCode: http.StatusOK},
		{name: "one_commit", seedCommits: 1, wantCode: http.StatusOK},
		{name: "three_commits", seedCommits: 3, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			for i := range tt.seedCommits {
				doRequest(t, h, "CreateCommit", map[string]any{
					"repositoryName": "repo",
					"branchName":     "main",
					"authorName":     "author",
					"email":          "a@b.com",
					"commitMessage":  fmt.Sprintf("commit %d", i),
					"putFiles": []map[string]any{
						{
							"filePath":    "main.go",
							"fileContent": "cGFja2FnZSBtYWlu", // "package main" base64
						},
					},
				})
			}

			rec := doRequest(t, h, "ListFileCommitHistory", map[string]any{
				"repositoryName": "repo",
				"filePath":       "main.go",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			dag := resp["revisionDag"].([]any)
			assert.Len(t, dag, tt.seedCommits)
		})
	}
}

func TestHandler_FileLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	// Put file.
	rec := doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"filePath":       "src/main.go",
		"fileContent":    "cGFja2FnZSBtYWlu", // "package main" base64
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get file.
	rec = doRequest(t, h, "GetFile", map[string]any{
		"repositoryName": "repo",
		"filePath":       "src/main.go",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "src/main.go", resp["filePath"])
	assert.NotEmpty(t, resp["blobId"])

	// Get folder.
	rec = doRequest(t, h, "GetFolder", map[string]any{
		"repositoryName": "repo",
		"folderPath":     "src",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	files := resp["files"].([]any)
	assert.Len(t, files, 1)

	// Get blob.
	blobID := resp["files"].([]any) // We need blobId from GetFile
	_ = blobID

	// Delete file.
	rec = doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"filePath":       "src/main.go",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// File should be gone.
	rec = doRequest(t, h, "GetFile", map[string]any{
		"repositoryName": "repo",
		"filePath":       "src/main.go",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
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

// TestListFileCommitHistory_FiltersByFilePath verifies ListFileCommitHistory
// only returns commits that touched the specified file, matching real AWS behavior.
func TestListFileCommitHistory_FiltersByFilePath(t *testing.T) {
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
