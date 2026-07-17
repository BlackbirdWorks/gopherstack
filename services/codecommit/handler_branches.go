package codecommit

import (
	"encoding/json"
	"fmt"
)

type createBranchInput struct {
	RepositoryName string `json:"repositoryName"`
	BranchName     string `json:"branchName"`
	CommitID       string `json:"commitId"`
}

func (h *Handler) handleCreateBranch(body []byte) (any, error) {
	var in createBranchInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if in.BranchName == "" {
		return nil, fmt.Errorf("%w: branchName is required", errInvalidRequest)
	}

	if in.CommitID == "" {
		return nil, fmt.Errorf("%w: commitId is required", errInvalidRequest)
	}

	if err := h.Backend.CreateBranch(in.RepositoryName, in.BranchName, in.CommitID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleDeleteBranch(body []byte) (any, error) {
	var in struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" || in.BranchName == "" {
		return nil, fmt.Errorf("%w: repositoryName and branchName are required", errInvalidRequest)
	}

	br, err := h.Backend.DeleteBranch(in.RepositoryName, in.BranchName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"deletedBranch": map[string]any{
			"branchName": br.BranchName,
			keyCommitID:  br.CommitID,
		},
	}, nil
}

func (h *Handler) handleGetBranch(body []byte) (any, error) {
	var in struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	br, err := h.Backend.GetBranch(in.RepositoryName, in.BranchName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"branch": map[string]any{
			"branchName": br.BranchName,
			keyCommitID:  br.CommitID,
		},
	}, nil
}

func (h *Handler) handleListBranches(body []byte) (any, error) {
	var in struct {
		RepositoryName string `json:"repositoryName"`
		NextToken      string `json:"nextToken"`
		MaxResults     int    `json:"maxResults"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	branches, err := h.Backend.ListBranches(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	// Apply pagination.
	page, nextToken := paginateStrings(branches, in.NextToken, in.MaxResults)

	resp := map[string]any{
		"branches": page,
	}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, nil
}

func (h *Handler) handleUpdateDefaultBranch(body []byte) (any, error) {
	var req struct {
		RepositoryName    string `json:"repositoryName"`
		DefaultBranchName string `json:"defaultBranchName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdateDefaultBranch(req.RepositoryName, req.DefaultBranchName)
}
