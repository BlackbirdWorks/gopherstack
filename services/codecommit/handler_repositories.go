package codecommit

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type createRepositoryInput struct {
	Tags                  map[string]string `json:"tags"`
	RepositoryName        string            `json:"repositoryName"`
	RepositoryDescription string            `json:"repositoryDescription"`
	KmsKeyID              string            `json:"kmsKeyId"`
}

type getRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
}

type deleteRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
}

type batchGetRepositoriesInput struct {
	RepositoryNames []string `json:"repositoryNames"`
}

func repoMetadata(r *Repository) map[string]any {
	m := map[string]any{
		keyRepositoryID:     r.RepositoryID,
		keyRepositoryName:   r.RepositoryName,
		"Arn":               r.ARN,
		"accountId":         r.AccountID,
		"cloneUrlHttp":      r.CloneURLHTTP,
		"cloneUrlSsh":       r.CloneURLSSH,
		keyCreationDate:     r.CreationDate.Unix(),
		keyLastModifiedDate: r.LastModifiedDate.Unix(),
	}
	if r.Description != "" {
		m["repositoryDescription"] = r.Description
	}
	if r.DefaultBranch != "" {
		m["defaultBranch"] = r.DefaultBranch
	}
	if r.KmsKeyID != "" {
		m["kmsKeyId"] = r.KmsKeyID
	}

	return m
}

func (h *Handler) handleCreateRepository(body []byte) (any, error) {
	var in createRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	r, err := h.Backend.CreateRepository(in.RepositoryName, in.RepositoryDescription, in.KmsKeyID, in.Tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"repositoryMetadata": repoMetadata(r),
	}, nil
}

func (h *Handler) handleGetRepository(body []byte) (any, error) {
	var in getRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	r, err := h.Backend.GetRepository(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"repositoryMetadata": repoMetadata(r),
	}, nil
}

func (h *Handler) handleDeleteRepository(body []byte) (any, error) {
	var in deleteRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	r, err := h.Backend.DeleteRepository(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyRepositoryID: r.RepositoryID,
	}, nil
}

func (h *Handler) handleListRepositories(body []byte) (any, error) {
	var in struct {
		SortBy     string `json:"sortBy"` // "repositoryName" (default) or "lastModifiedDate"
		Order      string `json:"order"`  // "ASCENDING" (default) or "DESCENDING"
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}
	// Ignore parse errors — all fields are optional.
	_ = json.Unmarshal(body, &in)

	repos := h.Backend.ListRepositories()

	// Apply sort.
	switch in.SortBy {
	case "lastModifiedDate":
		sort.Slice(repos, func(i, j int) bool {
			if strings.EqualFold(in.Order, "DESCENDING") {
				return repos[i].LastModifiedDate.After(repos[j].LastModifiedDate)
			}

			return repos[i].LastModifiedDate.Before(repos[j].LastModifiedDate)
		})
	default:
		// Default: sort by repositoryName ascending (already sorted by backend).
		if strings.EqualFold(in.Order, "DESCENDING") {
			sort.Slice(repos, func(i, j int) bool {
				return repos[i].RepositoryName > repos[j].RepositoryName
			})
		}
	}

	// Apply pagination.
	start := 0
	if in.NextToken != "" {
		if idx, err := strconv.Atoi(in.NextToken); err == nil && idx >= 0 {
			start = idx
		}
	}
	if start > len(repos) {
		start = len(repos)
	}
	end := len(repos)
	if in.MaxResults > 0 && start+in.MaxResults < end {
		end = start + in.MaxResults
	}
	page := repos[start:end]

	items := make([]map[string]any, 0, len(page))
	for _, r := range page {
		items = append(items, map[string]any{
			keyRepositoryID:   r.RepositoryID,
			keyRepositoryName: r.RepositoryName,
		})
	}

	resp := map[string]any{
		"repositories": items,
	}
	if end < len(repos) {
		resp["nextToken"] = strconv.Itoa(end)
	}

	return resp, nil
}

func (h *Handler) handleBatchGetRepositories(body []byte) (any, error) {
	var in batchGetRepositoriesInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	found, notFound, err := h.Backend.BatchGetRepositories(in.RepositoryNames)
	if err != nil {
		return nil, err
	}

	repos := make([]map[string]any, 0, len(found))
	for _, r := range found {
		repos = append(repos, repoMetadata(r))
	}

	if notFound == nil {
		notFound = []string{}
	}

	return map[string]any{
		"repositories":         repos,
		"repositoriesNotFound": notFound,
	}, nil
}

func (h *Handler) handleUpdateRepositoryDescription(body []byte) (any, error) {
	var req struct {
		RepositoryName        string `json:"repositoryName"`
		RepositoryDescription string `json:"repositoryDescription"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdateRepositoryDescription(req.RepositoryName, req.RepositoryDescription)
}

func (h *Handler) handleUpdateRepositoryName(body []byte) (any, error) {
	var req struct {
		OldName string `json:"oldName"`
		NewName string `json:"newName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.OldName == "" || req.NewName == "" {
		return nil, fmt.Errorf("%w: oldName and newName are required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdateRepositoryName(req.OldName, req.NewName)
}

func (h *Handler) handleUpdateRepositoryEncryptionKey(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		KmsKeyID       string `json:"kmsKeyId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	repositoryID, originalKmsKeyID, err := h.Backend.UpdateRepositoryEncryptionKey(
		req.RepositoryName, req.KmsKeyID,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"repositoryId":     repositoryID,
		"kmsKeyId":         req.KmsKeyID,
		"originalKmsKeyId": originalKmsKeyID,
	}, nil
}
