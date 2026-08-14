package codecommit

import (
	"encoding/json"
	"fmt"
)

func commentToMap(c *Comment) map[string]any {
	return map[string]any{
		"commentId":         c.CommentID,
		"content":           c.Content,
		"authorArn":         c.AuthorARN,
		"creationDate":      c.CreationDate.Unix(),
		keyLastModifiedDate: c.LastModifiedDate.Unix(),
		"inReplyTo":         c.InReplyTo,
		"deleted":           c.Deleted,
	}
}

func (h *Handler) handlePostCommentForComparedCommit(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BeforeCommitID string `json:"beforeCommitId"`
		AfterCommitID  string `json:"afterCommitId"`
		Content        string `json:"content"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: repositoryName and content are required", errInvalidRequest)
	}

	c, err := h.Backend.PostCommentForComparedCommit(
		req.RepositoryName, req.BeforeCommitID, req.AfterCommitID, req.Content,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyComment:        commentToMap(c),
		keyRepositoryName: req.RepositoryName,
		keyAfterCommitID:  req.AfterCommitID,
		"beforeCommitId":  req.BeforeCommitID,
	}, nil
}

func (h *Handler) handlePostCommentForPullRequest(body []byte) (any, error) {
	var req struct {
		PullRequestID  string `json:"pullRequestId"`
		RepositoryName string `json:"repositoryName"`
		BeforeCommitID string `json:"beforeCommitId"`
		AfterCommitID  string `json:"afterCommitId"`
		Content        string `json:"content"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: pullRequestId and content are required", errInvalidRequest)
	}

	c, err := h.Backend.PostCommentForPullRequest(req.PullRequestID, req.RepositoryName, req.Content)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyComment:        commentToMap(c),
		keyPullRequestID:  req.PullRequestID,
		keyRepositoryName: req.RepositoryName,
		keyAfterCommitID:  req.AfterCommitID,
		"beforeCommitId":  req.BeforeCommitID,
	}, nil
}

func (h *Handler) handlePostCommentReply(body []byte) (any, error) {
	var req struct {
		InReplyTo string `json:"inReplyTo"`
		Content   string `json:"content"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.InReplyTo == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: inReplyTo and content are required", errInvalidRequest)
	}

	c, err := h.Backend.PostCommentReply(req.InReplyTo, req.Content)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyComment: commentToMap(c),
	}, nil
}

func (h *Handler) handleGetComment(body []byte) (any, error) {
	var req struct {
		CommentID string `json:"commentId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.CommentID == "" {
		return nil, fmt.Errorf("%w: commentId is required", errInvalidRequest)
	}

	c, err := h.Backend.GetComment(req.CommentID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyComment: commentToMap(c),
	}, nil
}

func (h *Handler) handleGetCommentsForComparedCommit(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		AfterCommitID  string `json:"afterCommitId"`
		BeforeCommitID string `json:"beforeCommitId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	comments, err := h.Backend.GetCommentsForComparedCommit(req.RepositoryName, req.AfterCommitID)
	if err != nil {
		return nil, err
	}

	data := []map[string]any{}
	if len(comments) > 0 {
		items := make([]map[string]any, 0, len(comments))
		for _, c := range comments {
			items = append(items, commentToMap(c))
		}
		group := map[string]any{
			keyRepositoryName: req.RepositoryName,
			keyAfterCommitID:  req.AfterCommitID,
			"comments":        items,
		}
		if req.BeforeCommitID != "" {
			group["beforeCommitId"] = req.BeforeCommitID
		}
		data = append(data, group)
	}

	return map[string]any{
		"commentsForComparedCommitData": data,
	}, nil
}

func (h *Handler) handleGetCommentsForPullRequest(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	comments, err := h.Backend.GetCommentsForPullRequest(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	data := []map[string]any{}
	if len(comments) > 0 {
		items := make([]map[string]any, 0, len(comments))
		for _, c := range comments {
			items = append(items, commentToMap(c))
		}
		group := map[string]any{
			keyPullRequestID: req.PullRequestID,
			"comments":       items,
		}
		if comments[0].RepoName != "" {
			group[keyRepositoryName] = comments[0].RepoName
		}
		data = append(data, group)
	}

	return map[string]any{
		"commentsForPullRequestData": data,
	}, nil
}

func (h *Handler) handleUpdateComment(body []byte) (any, error) {
	var req struct {
		CommentID string `json:"commentId"`
		Content   string `json:"content"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.CommentID == "" {
		return nil, fmt.Errorf("%w: commentId is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateComment(req.CommentID, req.Content); err != nil {
		return nil, err
	}

	c, err := h.Backend.GetComment(req.CommentID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyComment: commentToMap(c),
	}, nil
}

func (h *Handler) handleDeleteCommentContent(body []byte) (any, error) {
	var req struct {
		CommentID string `json:"commentId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.CommentID == "" {
		return nil, fmt.Errorf("%w: commentId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteCommentContent(req.CommentID); err != nil {
		return nil, err
	}

	c, err := h.Backend.GetComment(req.CommentID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyComment: commentToMap(c),
	}, nil
}
