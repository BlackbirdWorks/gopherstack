package codecommit

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

func (h *Handler) handlePutFile(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
		FilePath       string `json:"filePath"`
		FileContent    string `json:"fileContent"` // base64 encoded
		FileMode       string `json:"fileMode"`
		Name           string `json:"name"`
		Email          string `json:"email"`
		CommitMessage  string `json:"commitMessage"`
		ParentCommitID string `json:"parentCommitId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.FilePath == "" {
		return nil, fmt.Errorf("%w: repositoryName and filePath are required", errInvalidRequest)
	}

	content, err := base64.StdEncoding.DecodeString(req.FileContent)
	if err != nil {
		// treat as raw bytes if not base64
		content = []byte(req.FileContent)
	}

	commit, blobID, err := h.Backend.PutFile(req.RepositoryName, req.BranchName, req.FilePath, content, PutFileMetadata{
		FileMode:       req.FileMode,
		AuthorName:     req.Name,
		AuthorEmail:    req.Email,
		CommitMessage:  req.CommitMessage,
		ParentCommitID: req.ParentCommitID,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
		keyBlobID:   blobID,
		"filesAdded": []any{
			map[string]any{keyFilePath: req.FilePath},
		},
	}, nil
}

func (h *Handler) handleGetFile(body []byte) (any, error) {
	var req struct {
		RepositoryName  string `json:"repositoryName"`
		CommitSpecifier string `json:"commitSpecifier"`
		FilePath        string `json:"filePath"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.FilePath == "" {
		return nil, fmt.Errorf("%w: repositoryName and filePath are required", errInvalidRequest)
	}

	f, err := h.Backend.GetFile(req.RepositoryName, req.CommitSpecifier, req.FilePath)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyBlobID:     f.BlobID,
		"commitId":    f.CommitSpecifier,
		keyFilePath:   f.FilePath,
		keyFileMode:   f.FileMode,
		"fileContent": base64.StdEncoding.EncodeToString(f.FileContent),
		"fileSize":    len(f.FileContent),
	}, nil
}

func (h *Handler) handleGetFolder(body []byte) (any, error) {
	var req struct {
		RepositoryName  string `json:"repositoryName"`
		CommitSpecifier string `json:"commitSpecifier"`
		FolderPath      string `json:"folderPath"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	view, err := h.Backend.GetFolderView(req.RepositoryName, req.CommitSpecifier, req.FolderPath)
	if err != nil {
		return nil, err
	}

	folder := strings.Trim(req.FolderPath, "/")
	files := make([]map[string]any, 0, len(view.Files))

	for _, f := range view.Files {
		fileMode := f.FileMode
		if fileMode == "" {
			fileMode = fileModeNormal
		}

		files = append(files, map[string]any{
			keyAbsolutePath: f.FilePath,
			"relativePath":  strings.TrimPrefix(strings.TrimPrefix(f.FilePath, folder), "/"),
			keyBlobID:       f.BlobID,
			keyFileMode:     fileMode,
		})
	}

	subFolders := make([]map[string]any, 0, len(view.SubFolders))

	for _, sub := range view.SubFolders {
		subFolders = append(subFolders, map[string]any{
			keyAbsolutePath: sub,
			"relativePath":  strings.TrimPrefix(strings.TrimPrefix(sub, folder), "/"),
			"treeId":        folderTreeID(view.CommitID, sub),
		})
	}

	folderPath := req.FolderPath
	if folderPath == "" {
		folderPath = "/"
	}

	out := map[string]any{
		"folderPath":    folderPath,
		"files":         files,
		"subFolders":    subFolders,
		"subModules":    []any{},
		"symbolicLinks": []any{},
	}
	if view.CommitID != "" {
		out["commitId"] = view.CommitID
		out["treeId"] = folderTreeID(view.CommitID, folder)
	}

	return out, nil
}

func folderTreeID(commitID, path string) string {
	sum := sha256.Sum256([]byte(commitID + "\x00" + path))

	return hex.EncodeToString(sum[:20])
}

func (h *Handler) handleDeleteFile(body []byte) (any, error) {
	var req struct {
		RepositoryName   string `json:"repositoryName"`
		BranchName       string `json:"branchName"`
		FilePath         string `json:"filePath"`
		ParentCommitID   string `json:"parentCommitId"`
		Name             string `json:"name"`
		Email            string `json:"email"`
		CommitMessage    string `json:"commitMessage"`
		KeepEmptyFolders bool   `json:"keepEmptyFolders"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.FilePath == "" {
		return nil, fmt.Errorf("%w: repositoryName and filePath are required", errInvalidRequest)
	}

	commit, blobID, err := h.Backend.DeleteFile(req.RepositoryName, req.BranchName, req.FilePath, DeleteFileMetadata{
		ParentCommitID:   req.ParentCommitID,
		AuthorName:       req.Name,
		AuthorEmail:      req.Email,
		CommitMessage:    req.CommitMessage,
		KeepEmptyFolders: req.KeepEmptyFolders,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
		keyBlobID:   blobID,
		keyFilePath: req.FilePath,
	}, nil
}

func (h *Handler) handleGetBlob(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BlobID         string `json:"blobId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.BlobID == "" {
		return nil, fmt.Errorf("%w: repositoryName and blobId are required", errInvalidRequest)
	}

	content, err := h.Backend.GetBlob(req.RepositoryName, req.BlobID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"content": base64.StdEncoding.EncodeToString(content),
	}, nil
}

func (h *Handler) handleListFileCommitHistory(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		FilePath       string `json:"filePath"`
		NextToken      string `json:"nextToken"`
		MaxResults     int    `json:"maxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	pg, err := h.Backend.ListFileCommitHistory(req.RepositoryName, req.FilePath, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	// revisionDag entries are AWS's FileVersion shape (blobId/commit/path/
	// revisionChildren) — a prior version of this handler returned raw
	// Commit objects here, which is the wrong wire shape entirely (a real
	// SDK client deserializing this field expects FileVersion, not Commit).
	items := make([]map[string]any, 0, len(pg.Data))
	for _, v := range pg.Data {
		items = append(items, map[string]any{
			keyBlobID:          v.BlobID,
			"path":             v.FilePath,
			"commit":           commitToMap(v.Commit),
			"revisionChildren": v.RevisionChildren,
		})
	}

	resp := map[string]any{
		"revisionDag": items,
	}
	if pg.Next != "" {
		resp["nextToken"] = pg.Next
	}

	return resp, nil
}
