package codecommit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

type batchDescribeMergeConflictsInput struct {
	RepositoryName             string   `json:"repositoryName"`
	DestinationCommitSpecifier string   `json:"destinationCommitSpecifier"`
	SourceCommitSpecifier      string   `json:"sourceCommitSpecifier"`
	MergeOption                string   `json:"mergeOption"`
	FilePaths                  []string `json:"filePaths"`
	mergeQueryWire
}

// validMergeOptions are the AWS-accepted values for the mergeOption parameter.
func isValidMergeOption(opt string) bool {
	switch opt {
	case "FAST_FORWARD_MERGE", "SQUASH_MERGE", "THREE_WAY_MERGE":
		return true
	}

	return false
}

func (h *Handler) handleBatchDescribeMergeConflicts(body []byte) (any, error) {
	var in batchDescribeMergeConflictsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if in.DestinationCommitSpecifier == "" {
		return nil, fmt.Errorf("%w: destinationCommitSpecifier is required", errInvalidRequest)
	}

	if in.SourceCommitSpecifier == "" {
		return nil, fmt.Errorf("%w: sourceCommitSpecifier is required", errInvalidRequest)
	}

	if in.MergeOption == "" {
		return nil, fmt.Errorf("%w: mergeOption is required", errInvalidRequest)
	}

	if !isValidMergeOption(in.MergeOption) {
		return nil, fmt.Errorf(
			"%w: mergeOption must be FAST_FORWARD_MERGE, SQUASH_MERGE, or THREE_WAY_MERGE",
			ErrInvalidMergeOption,
		)
	}

	q, err := in.query()
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.BatchDescribeMergeConflicts(
		in.RepositoryName,
		in.DestinationCommitSpecifier,
		in.SourceCommitSpecifier,
		in.MergeOption,
		in.FilePaths,
		q,
	)
	if err != nil {
		return nil, err
	}

	errs := result.Errors
	if errs == nil {
		errs = []ConflictError{}
	}

	out := map[string]any{
		"conflicts":       result.Conflicts,
		keyDestCommitID:   result.DestinationCommitID,
		keySourceCommitID: result.SourceCommitID,
		keyErrors:         errs,
	}
	setIfNotEmpty(out, keyBaseCommitID, result.BaseCommitID)
	setIfNotEmpty(out, "nextToken", result.NextToken)

	return out, nil
}

func setIfNotEmpty(m map[string]any, key, value string) {
	if value != "" {
		m[key] = value
	}
}

func (h *Handler) handleMergePullRequest(ctx context.Context, option string, body []byte) (any, error) {
	var req struct {
		PullRequestID  string `json:"pullRequestId"`
		RepositoryName string `json:"repositoryName"`
		SourceCommitID string `json:"sourceCommitId"`
		CommitMessage  string `json:"commitMessage"`
		AuthorName     string `json:"authorName"`
		Email          string `json:"email"`
		mergeSettingsWire
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	settings, err := req.settings()
	if err != nil {
		return nil, err
	}

	opts := MergePullRequestOptions{
		CommitMessage: req.CommitMessage,
		AuthorName:    req.AuthorName,
		Email:         req.Email,
		MergedBy:      awsmeta.CallerArn(ctx),
		Settings:      settings,
	}

	var pr *PullRequest

	switch option {
	case mergeOptionFastForward:
		pr, err = h.Backend.MergePullRequestByFastForward(
			req.PullRequestID,
			req.RepositoryName,
			req.SourceCommitID,
			opts,
		)
	case mergeOptionSquash:
		pr, err = h.Backend.MergePullRequestBySquash(req.PullRequestID, req.RepositoryName, req.SourceCommitID, opts)
	default:
		pr, err = h.Backend.MergePullRequestByThreeWay(req.PullRequestID, req.RepositoryName, req.SourceCommitID, opts)
	}
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleMergeBranchesByFastForward(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		TargetBranch               string `json:"targetBranch"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	commit, err := h.Backend.MergeBranchesByFastForward(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier, req.TargetBranch,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
	}, nil
}

func (h *Handler) handleGetMergeOptions(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		mergeSettingsWire
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	settings, err := req.settings()
	if err != nil {
		return nil, err
	}

	res, err := h.Backend.GetMergeOptions(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier, settings,
	)
	if err != nil {
		return nil, err
	}

	options := res.Options
	if options == nil {
		options = []string{}
	}

	out := map[string]any{
		"mergeOptions":    options,
		keySourceCommitID: res.SourceCommitID,
		keyDestCommitID:   res.DestinationCommitID,
	}
	setIfNotEmpty(out, keyBaseCommitID, res.BaseCommitID)

	return out, nil
}

func (h *Handler) handleCreateUnreferencedMergeCommit(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		MergeOption                string `json:"mergeOption"`
		AuthorName                 string `json:"authorName"`
		Email                      string `json:"email"`
		CommitMessage              string `json:"commitMessage"`
		mergeSettingsWire
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if req.MergeOption == "" {
		return nil, fmt.Errorf("%w: mergeOption is required", errInvalidRequest)
	}

	if !isValidMergeOption(req.MergeOption) {
		return nil, fmt.Errorf(
			"%w: mergeOption must be FAST_FORWARD_MERGE, SQUASH_MERGE, or THREE_WAY_MERGE",
			ErrInvalidMergeOption,
		)
	}

	settings, err := req.settings()
	if err != nil {
		return nil, err
	}

	commit, err := h.Backend.CreateUnreferencedMergeCommit(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier, req.MergeOption,
		req.AuthorName, req.Email, req.CommitMessage, settings,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
	}, nil
}

func (h *Handler) handleGetMergeCommit(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		mergeSettingsWire
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	settings, err := req.settings()
	if err != nil {
		return nil, err
	}

	res, err := h.Backend.GetMergeCommit(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier, settings,
	)
	if err != nil {
		return nil, err
	}

	out := map[string]any{keySourceCommitID: res.SourceCommitID, keyDestCommitID: res.DestinationCommitID}
	setIfNotEmpty(out, "mergedCommitId", res.MergedCommitID)
	setIfNotEmpty(out, keyBaseCommitID, res.BaseCommitID)

	return out, nil
}

func (h *Handler) handleGetMergeConflicts(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		MergeOption                string `json:"mergeOption"`
		mergeQueryWire
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if req.SourceCommitSpecifier == "" {
		return nil, fmt.Errorf("%w: sourceCommitSpecifier is required", errInvalidRequest)
	}

	if req.DestinationCommitSpecifier == "" {
		return nil, fmt.Errorf("%w: destinationCommitSpecifier is required", errInvalidRequest)
	}

	if req.MergeOption == "" {
		return nil, fmt.Errorf("%w: mergeOption is required", errInvalidRequest)
	}

	if !isValidMergeOption(req.MergeOption) {
		return nil, fmt.Errorf(
			"%w: mergeOption must be FAST_FORWARD_MERGE, SQUASH_MERGE, or THREE_WAY_MERGE",
			ErrInvalidMergeOption,
		)
	}

	q, err := req.query()
	if err != nil {
		return nil, err
	}

	res, err := h.Backend.GetMergeConflicts(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier, req.MergeOption, q,
	)
	if err != nil {
		return nil, err
	}

	conflicts := res.Conflicts
	if conflicts == nil {
		conflicts = []ConflictMetadata{}
	}

	out := map[string]any{
		"mergeable":            res.Mergeable,
		keySourceCommitID:      res.SourceCommitID,
		keyDestCommitID:        res.DestinationCommitID,
		"conflictMetadataList": conflicts,
	}
	setIfNotEmpty(out, keyBaseCommitID, res.BaseCommitID)
	setIfNotEmpty(out, "nextToken", res.NextToken)

	return out, nil
}

// handleDescribeMergeConflicts describes one file's conflict, paging its hunks.
func (h *Handler) handleDescribeMergeConflicts(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		MergeOption                string `json:"mergeOption"`
		FilePath                   string `json:"filePath"`
		mergeQueryWire
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if req.DestinationCommitSpecifier == "" {
		return nil, fmt.Errorf("%w: destinationCommitSpecifier is required", errInvalidRequest)
	}

	if req.SourceCommitSpecifier == "" {
		return nil, fmt.Errorf("%w: sourceCommitSpecifier is required", errInvalidRequest)
	}

	if req.FilePath == "" {
		return nil, fmt.Errorf("%w: filePath is required", errInvalidRequest)
	}

	if req.MergeOption == "" {
		return nil, fmt.Errorf("%w: mergeOption is required", errInvalidRequest)
	}

	if !isValidMergeOption(req.MergeOption) {
		return nil, fmt.Errorf(
			"%w: mergeOption must be FAST_FORWARD_MERGE, SQUASH_MERGE, or THREE_WAY_MERGE",
			ErrInvalidMergeOption,
		)
	}

	q, err := req.query()
	if err != nil {
		return nil, err
	}

	res, err := h.Backend.DescribeMergeConflicts(
		req.RepositoryName, req.DestinationCommitSpecifier, req.SourceCommitSpecifier,
		req.MergeOption, req.FilePath, q,
	)
	if err != nil {
		return nil, err
	}

	hunks := res.Hunks
	if hunks == nil {
		hunks = []MergeHunk{}
	}

	out := map[string]any{
		keyDestCommitID:    res.DestinationCommitID,
		keySourceCommitID:  res.SourceCommitID,
		"mergeHunks":       hunks,
		"conflictMetadata": res.Metadata,
	}
	setIfNotEmpty(out, keyBaseCommitID, res.BaseCommitID)
	setIfNotEmpty(out, "nextToken", res.NextToken)

	return out, nil
}

type mergeBranchesRequest struct {
	RepositoryName             string `json:"repositoryName"`
	SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
	DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
	TargetBranch               string `json:"targetBranch"`
	CommitMessage              string `json:"commitMessage"`
	AuthorName                 string `json:"authorName"`
	Email                      string `json:"email"`
	mergeSettingsWire
}

func (r mergeBranchesRequest) options() (MergeBranchesOptions, error) {
	settings, err := r.settings()

	return MergeBranchesOptions{
		TargetBranch:  r.TargetBranch,
		CommitMessage: r.CommitMessage,
		AuthorName:    r.AuthorName,
		Email:         r.Email,
		Settings:      settings,
	}, err
}

func (h *Handler) handleMergeBranchesBySquash(body []byte) (any, error) {
	var req mergeBranchesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	opts, err := req.options()
	if err != nil {
		return nil, err
	}

	commit, err := h.Backend.MergeBranchesBySquash(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier, opts,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
	}, nil
}

func (h *Handler) handleMergeBranchesByThreeWay(body []byte) (any, error) {
	var req mergeBranchesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	opts, err := req.options()
	if err != nil {
		return nil, err
	}

	commit, err := h.Backend.MergeBranchesByThreeWay(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier, opts,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
	}, nil
}
