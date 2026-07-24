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

	// AWS's GetFileOutput.CommitId is "the full commit ID of the commit that
	// contains the content" — it must be the commit PutFile created, not the
	// branch name (a prior bug stored the branch name in File.CommitSpecifier).
	putCommitID, _ := putResp["commitId"].(string)
	require.NotEmpty(t, putCommitID)
	assert.NotEqual(t, "main", resp["commitId"], "GetFile's commitId must not be the branch name")
	assert.Equal(t, putCommitID, resp["commitId"], "GetFile's commitId must be the commit PutFile created")

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
	putCommitID, _ := putResp["commitId"].(string)
	require.NotEmpty(t, putCommitID)

	// parentCommitId must be the current branch tip (PutFile's commit) —
	// real AWS returns ParentCommitIdOutdatedException otherwise.
	rec := doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "del-file-repo",
		"branchName":     "main",
		"filePath":       "todelete.txt",
		"parentCommitId": putCommitID,
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

// TestHandler_DeleteFile_ParentCommitIdRequired verifies that AWS's
// ParentCommitId (a required DeleteFileInput field per
// aws-sdk-go-v2/service/codecommit's validators.go) is enforced: omitting it
// against a file that does exist must not silently succeed.
func TestHandler_DeleteFile_ParentCommitIdRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "del-file-repo-3"})
	doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "del-file-repo-3",
		"branchName":     "main",
		"filePath":       "keep.txt",
		"fileContent":    "a2VlcA==",
	})

	rec := doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "del-file-repo-3",
		"branchName":     "main",
		"filePath":       "keep.txt",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ParentCommitIdRequiredException", resp["__type"])
}

// TestHandler_DeleteFile_ParentCommitIdOutdated verifies that a
// parentCommitId not matching the branch's current tip is rejected with
// ParentCommitIdOutdatedException, the same way CreateCommit already
// enforces this for its own parentCommitId.
func TestHandler_DeleteFile_ParentCommitIdOutdated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "del-file-repo-4"})
	doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "del-file-repo-4",
		"branchName":     "main",
		"filePath":       "keep.txt",
		"fileContent":    "a2VlcA==",
	})

	rec := doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "del-file-repo-4",
		"branchName":     "main",
		"filePath":       "keep.txt",
		"parentCommitId": "not-the-real-tip",
	})
	// This service's errCodeLookup maps every client-fault CodeCommit
	// exception to 400 (see handler.go), including "conflict"-shaped ones
	// like ParentCommitIdOutdatedException — not 409.
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ParentCommitIdOutdatedException", resp["__type"])
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

// TestHandler_ListFileCommitHistory_RevisionDagShape locks AWS's FileVersion
// wire shape for each revisionDag entry (blobId/path/commit/revisionChildren)
// — a prior version of this handler returned raw Commit objects in
// revisionDag, which a real SDK client's FileVersion deserializer cannot
// read (it looks for a nested "commit" object, not flattened commit fields).
// It also locks revisionChildren linkage: an older entry must reference the
// commit ID of the newer entry that touched the same path, and the newest
// entry must have none.
func TestHandler_ListFileCommitHistory_RevisionDagShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "dag-repo"})

	firstRec := doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "dag-repo",
		"branchName":     "main",
		"putFiles": []map[string]any{
			{"filePath": "main.go", "fileContent": "dmVyc2lvbjE="},
		},
	})
	require.Equal(t, http.StatusOK, firstRec.Code)
	var firstResp map[string]any
	require.NoError(t, json.Unmarshal(firstRec.Body.Bytes(), &firstResp))
	firstCommitID, _ := firstResp["commitId"].(string)
	require.NotEmpty(t, firstCommitID)

	secondRec := doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": "dag-repo",
		"branchName":     "main",
		"putFiles": []map[string]any{
			{"filePath": "main.go", "fileContent": "dmVyc2lvbjI="},
		},
	})
	require.Equal(t, http.StatusOK, secondRec.Code)
	var secondResp map[string]any
	require.NoError(t, json.Unmarshal(secondRec.Body.Bytes(), &secondResp))
	secondCommitID, _ := secondResp["commitId"].(string)
	require.NotEmpty(t, secondCommitID)

	rec := doRequest(t, h, "ListFileCommitHistory", map[string]any{
		"repositoryName": "dag-repo",
		"filePath":       "main.go",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dag, ok := resp["revisionDag"].([]any)
	require.True(t, ok)
	require.Len(t, dag, 2)

	older, ok := dag[0].(map[string]any)
	require.True(t, ok)
	newer, ok := dag[1].(map[string]any)
	require.True(t, ok)

	for _, entry := range []map[string]any{older, newer} {
		assert.Contains(t, entry, "blobId")
		assert.Equal(t, "main.go", entry["path"])
		commitObj, commitOK := entry["commit"].(map[string]any)
		require.True(t, commitOK, "revisionDag entry's \"commit\" field must be a nested object")
		assert.NotEmpty(t, commitObj["commitId"])
		assert.NotEmpty(t, commitObj["treeId"])
	}

	olderCommit := older["commit"].(map[string]any)
	newerCommit := newer["commit"].(map[string]any)
	assert.Equal(t, firstCommitID, olderCommit["commitId"])
	assert.Equal(t, secondCommitID, newerCommit["commitId"])

	olderChildren, ok := older["revisionChildren"].([]any)
	require.True(t, ok)
	require.Len(t, olderChildren, 1)
	assert.Equal(t, secondCommitID, olderChildren[0],
		"the older revision's revisionChildren must point at the newer commit")

	newerChildren, ok := newer["revisionChildren"].([]any)
	require.True(t, ok)
	assert.Empty(t, newerChildren, "the newest revision has no more recent versions")
}

// TestHandler_ListFileCommitHistory_IncludesPutFileWrites verifies that a
// file written via the standalone PutFile op (not CreateCommit) shows up in
// ListFileCommitHistory. PutFile previously never recorded fileHistory at
// all, so single-file writes were invisible to this op even though AWS's
// ListFileCommitHistory doc says it returns every commit that changed the
// file regardless of which op created that commit.
func TestHandler_ListFileCommitHistory_IncludesPutFileWrites(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "putfile-hist-repo"})
	doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "putfile-hist-repo",
		"branchName":     "main",
		"filePath":       "solo.txt",
		"fileContent":    "c29sbw==",
	})

	rec := doRequest(t, h, "ListFileCommitHistory", map[string]any{
		"repositoryName": "putfile-hist-repo",
		"filePath":       "solo.txt",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dag, ok := resp["revisionDag"].([]any)
	require.True(t, ok)
	require.Len(t, dag, 1, "the PutFile commit must appear in the file's history")
}

// TestHandler_ListFileCommitHistory_Pagination verifies nextToken/maxResults
// paginate revisionDag and every commit surfaces exactly once across pages.
func TestHandler_ListFileCommitHistory_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "page-hist-repo"})

	commitIDs := make([]string, 0, 5)
	for i := range 5 {
		rec := doRequest(t, h, "CreateCommit", map[string]any{
			"repositoryName": "page-hist-repo",
			"branchName":     "main",
			"commitMessage":  fmt.Sprintf("commit %d", i),
			"putFiles": []map[string]any{
				{"filePath": "main.go", "fileContent": "dmVyc2lvbg=="},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		id, _ := resp["commitId"].(string)
		require.NotEmpty(t, id)
		commitIDs = append(commitIDs, id)
	}

	seen := map[string]bool{}
	nextToken := ""

	for range 10 {
		req := map[string]any{
			"repositoryName": "page-hist-repo",
			"filePath":       "main.go",
			"maxResults":     2,
		}
		if nextToken != "" {
			req["nextToken"] = nextToken
		}

		rec := doRequest(t, h, "ListFileCommitHistory", req)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		dag, ok := resp["revisionDag"].([]any)
		require.True(t, ok)
		assert.LessOrEqual(t, len(dag), 2, "each page must respect maxResults")

		for _, raw := range dag {
			entry, entryOK := raw.(map[string]any)
			require.True(t, entryOK)
			commitObj, commitOK := entry["commit"].(map[string]any)
			require.True(t, commitOK)
			id, _ := commitObj["commitId"].(string)
			require.NotEmpty(t, id)
			assert.False(t, seen[id], "commit %s must not repeat across pages", id)
			seen[id] = true
		}

		next, hasNext := resp["nextToken"].(string)
		if !hasNext || next == "" {
			break
		}
		nextToken = next
	}

	assert.Len(t, seen, len(commitIDs), "pagination must eventually surface every commit exactly once")
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
	putCommitID, _ := resp["commitId"].(string)
	require.NotEmpty(t, putCommitID, "GetFile's commitId must be the real commit ID PutFile created")

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

	// Delete file. parentCommitId must be the current branch tip (real AWS
	// requires it and rejects a stale value with ParentCommitIdOutdatedException).
	rec = doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"filePath":       "src/main.go",
		"parentCommitId": putCommitID,
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
