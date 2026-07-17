package codecommit

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

func (h *Handler) handlePutFile(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
		FilePath       string `json:"filePath"`
		FileContent    string `json:"fileContent"` // base64 encoded
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

	commit, blobID, err := h.Backend.PutFile(req.RepositoryName, req.BranchName, req.FilePath, content)
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

	fileObjs, err := h.Backend.GetFolderFiles(req.RepositoryName, req.CommitSpecifier, req.FolderPath)
	if err != nil {
		return nil, err
	}

	files := make([]map[string]any, 0, len(fileObjs))
	for _, f := range fileObjs {
		fileMode := f.FileMode
		if fileMode == "" {
			fileMode = fileModeNormal
		}
		files = append(files, map[string]any{
			"absolutePath": f.FilePath,
			"relativePath": f.FilePath,
			"blobId":       f.BlobID,
			keyFileMode:    fileMode,
		})
	}

	return map[string]any{
		"commitId":      req.CommitSpecifier,
		"folderPath":    req.FolderPath,
		"files":         files,
		"subFolders":    []any{},
		"subModules":    []any{},
		"symbolicLinks": []any{},
	}, nil
}

func (h *Handler) handleDeleteFile(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
		FilePath       string `json:"filePath"`
		ParentCommitID string `json:"parentCommitId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.FilePath == "" {
		return nil, fmt.Errorf("%w: repositoryName and filePath are required", errInvalidRequest)
	}

	commit, blobID, err := h.Backend.DeleteFile(req.RepositoryName, req.BranchName, req.FilePath, req.ParentCommitID)
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
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	commits, err := h.Backend.ListFileCommitHistory(req.RepositoryName, req.FilePath)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(commits))
	for _, c := range commits {
		items = append(items, commitToMap(c))
	}

	return map[string]any{
		"revisionDag": items,
	}, nil
}
