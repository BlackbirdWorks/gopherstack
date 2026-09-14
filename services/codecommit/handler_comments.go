package codecommit

import (
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// getCommentsForComparedCommitDefaultMaxResults is the documented default
// page size (api_op_GetCommentsForComparedCommit.go: "The default is 100
// comments, but you can configure up to 500."); GetCommentsForPullRequest
// shares the same 100/500 default/max (api_op_GetCommentsForPullRequest.go).
const getCommentsForComparedCommitDefaultMaxResults = 100

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
		NextToken      string `json:"nextToken"`
		MaxResults     int    `json:"maxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}
	if err := page.ValidateToken(req.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid nextToken", ErrInvalidContinuationToken)
	}

	comments, err := h.Backend.GetCommentsForComparedCommit(req.RepositoryName, req.AfterCommitID)
	if err != nil {
		return nil, err
	}

	// This backend groups every comment for a commit pair into one
	// CommentsForComparedCommit entry (see PARITY.md -- no per-diff-position
	// grouping is modeled), so MaxResults/NextToken paginate the comments
	// nested inside that single group rather than the (always <=1) outer list.
	pg := page.New(comments, req.NextToken, req.MaxResults, getCommentsForComparedCommitDefaultMaxResults)

	data := []map[string]any{}
	if len(pg.Data) > 0 {
		items := make([]map[string]any, 0, len(pg.Data))
		for _, c := range pg.Data {
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

	out := map[string]any{"commentsForComparedCommitData": data}
	if pg.Next != "" {
		out["nextToken"] = pg.Next
	}

	return out, nil
}

func (h *Handler) handleGetCommentsForPullRequest(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
		NextToken     string `json:"nextToken"`
		MaxResults    int    `json:"maxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}
	if err := page.ValidateToken(req.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid nextToken", ErrInvalidContinuationToken)
	}

	comments, err := h.Backend.GetCommentsForPullRequest(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	// Same single-group simplification as GetCommentsForComparedCommit --
	// MaxResults/NextToken paginate the nested comments list.
	pg := page.New(comments, req.NextToken, req.MaxResults, getCommentsForComparedCommitDefaultMaxResults)

	data := []map[string]any{}
	if len(pg.Data) > 0 {
		items := make([]map[string]any, 0, len(pg.Data))
		for _, c := range pg.Data {
			items = append(items, commentToMap(c))
		}
		group := map[string]any{
			keyPullRequestID: req.PullRequestID,
			"comments":       items,
		}
		if pg.Data[0].RepoName != "" {
			group[keyRepositoryName] = pg.Data[0].RepoName
		}
		data = append(data, group)
	}

	out := map[string]any{"commentsForPullRequestData": data}
	if pg.Next != "" {
		out["nextToken"] = pg.Next
	}

	return out, nil
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
