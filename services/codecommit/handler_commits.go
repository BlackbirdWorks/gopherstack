package codecommit

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
)

type batchGetCommitsInput struct {
	RepositoryName string   `json:"repositoryName"`
	CommitIDs      []string `json:"commitIds"`
}

type createCommitPutFileEntry struct {
	FilePath    string `json:"filePath"`
	FileContent string `json:"fileContent"` // base64-encoded
	FileMode    string `json:"fileMode"`
}

type createCommitDeleteFileEntry struct {
	FilePath string `json:"filePath"`
}

type createCommitInput struct {
	RepositoryName string                        `json:"repositoryName"`
	BranchName     string                        `json:"branchName"`
	AuthorName     string                        `json:"authorName"`
	Email          string                        `json:"email"`
	CommitMessage  string                        `json:"commitMessage"`
	ParentCommitID string                        `json:"parentCommitId"`
	PutFiles       []createCommitPutFileEntry    `json:"putFiles"`
	DeleteFiles    []createCommitDeleteFileEntry `json:"deleteFiles"`
}

// commitToMap converts a Commit to the AWS-accurate JSON map representation.
// The author/committer date is returned as a Unix timestamp string, matching the real AWS API.
func commitToMap(c *Commit) map[string]any {
	parents := c.Parents
	if parents == nil {
		parents = []string{}
	}

	// AWS returns the commit date as a Unix epoch integer formatted as a decimal string.
	date := ""
	if !c.CreatedAt.IsZero() {
		date = strconv.FormatInt(c.CreatedAt.Unix(), 10)
	}

	return map[string]any{
		keyCommitID: c.CommitID,
		keyTreeID:   c.TreeID,
		keyMessage:  c.Message,
		"parents":   parents,
		"author": map[string]any{
			"name":  c.AuthorName,
			"email": c.AuthorEmail,
			"date":  date,
		},
		"committer": map[string]any{
			"name":  c.CommitterName,
			"email": c.CommitterEmail,
			"date":  date,
		},
		"additionalData": c.AdditionalData,
	}
}

func (h *Handler) handleBatchGetCommits(body []byte) (any, error) {
	var in batchGetCommitsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if len(in.CommitIDs) == 0 {
		return nil, fmt.Errorf("%w: commitIds must contain at least one commit ID", errInvalidRequest)
	}

	found, batchErrors, err := h.Backend.BatchGetCommits(in.RepositoryName, in.CommitIDs)
	if err != nil {
		return nil, err
	}

	commits := make([]map[string]any, 0, len(found))
	for _, c := range found {
		commits = append(commits, commitToMap(c))
	}

	return map[string]any{
		"commits": commits,
		keyErrors: batchErrors,
	}, nil
}

func (h *Handler) handleCreateCommit(body []byte) (any, error) {
	var in createCommitInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if in.BranchName == "" {
		return nil, fmt.Errorf("%w: branchName is required", errInvalidRequest)
	}

	// Decode putFiles entries.
	putFiles := make([]PutFileEntry, 0, len(in.PutFiles))
	fileModes := make(map[string]string, len(in.PutFiles))
	for _, pf := range in.PutFiles {
		content, err := base64.StdEncoding.DecodeString(pf.FileContent)
		if err != nil {
			content = []byte(pf.FileContent)
		}
		fileMode := pf.FileMode
		if fileMode == "" {
			fileMode = fileModeNormal
		}
		putFiles = append(putFiles, PutFileEntry{
			FilePath:    pf.FilePath,
			FileContent: content,
			FileMode:    fileMode,
		})
		fileModes[pf.FilePath] = fileMode
	}

	deleteFiles := make([]string, 0, len(in.DeleteFiles))
	for _, df := range in.DeleteFiles {
		deleteFiles = append(deleteFiles, df.FilePath)
	}

	commit, blobIDsAdded, blobIDsDeleted, err := h.Backend.CreateCommit(
		in.RepositoryName, in.BranchName,
		in.AuthorName, in.Email, in.CommitMessage,
		in.ParentCommitID, putFiles, deleteFiles,
	)
	if err != nil {
		return nil, err
	}

	// filesAdded is built from the backend's assigned blob IDs (not the
	// request order) so the response reflects the real blob per file — AWS's
	// CreateCommitOutput.filesAdded.blobId is a required field. FileMetadata
	// on the real wire has exactly three keys -- absolutePath, blobId,
	// fileMode (deserializers.go's awsAwsjson11_deserializeDocumentFileMetadata)
	// -- there is no "filePath".
	filesAdded := make([]any, 0, len(in.PutFiles))
	for _, pf := range in.PutFiles {
		filesAdded = append(filesAdded, map[string]any{
			keyAbsolutePath: pf.FilePath,
			keyBlobID:       blobIDsAdded[pf.FilePath],
			keyFileMode:     fileModes[pf.FilePath],
		})
	}

	// filesDeleted mirrors filesAdded: built from the backend's reported blob
	// IDs (the blob each deletion removed from the tree) rather than left
	// empty, matching the fix already applied to the standalone DeleteFile op.
	// Uses "absolutePath", not "filePath" -- see the filesAdded comment above;
	// a client reading FilesDeleted[i].AbsolutePath previously always saw "".
	filesDeleted := make([]any, 0, len(in.DeleteFiles))
	for _, df := range in.DeleteFiles {
		filesDeleted = append(filesDeleted, map[string]any{
			keyAbsolutePath: df.FilePath,
			keyBlobID:       blobIDsDeleted[df.FilePath],
		})
	}

	return map[string]any{
		keyCommitID:    commit.CommitID,
		keyTreeID:      commit.TreeID,
		"filesAdded":   filesAdded,
		"filesUpdated": []any{},
		"filesDeleted": filesDeleted,
	}, nil
}

func (h *Handler) handleGetCommit(body []byte) (any, error) {
	var in struct {
		RepositoryName string `json:"repositoryName"`
		CommitID       string `json:"commitId"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	c, err := h.Backend.GetCommit(in.RepositoryName, in.CommitID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"commit": commitToMap(c),
	}, nil
}

func (h *Handler) handleGetDifferences(body []byte) (any, error) {
	// GetDifferences is the one CodeCommit op whose pagination fields are
	// capitalized on the wire (MaxResults/NextToken, both request and
	// response) — verified against aws-sdk-go-v2/service/codecommit's
	// generated (de)serializers; every other List/Get op in this service uses
	// lowercase maxResults/nextToken.
	var req struct {
		RepositoryName        string `json:"repositoryName"`
		AfterCommitSpecifier  string `json:"afterCommitSpecifier"`
		BeforeCommitSpecifier string `json:"beforeCommitSpecifier"`
		NextToken             string `json:"NextToken"`
		MaxResults            int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	pg, err := h.Backend.GetDifferences(
		req.RepositoryName, req.AfterCommitSpecifier, req.BeforeCommitSpecifier, req.NextToken, req.MaxResults,
	)
	if err != nil {
		return nil, err
	}
	diffs := pg.Data

	if pg.Next != "" {
		return map[string]any{
			"differences": diffs,
			"NextToken":   pg.Next,
		}, nil
	}

	return map[string]any{
		"differences": diffs,
	}, nil
}
