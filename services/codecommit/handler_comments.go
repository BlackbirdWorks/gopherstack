package codecommit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
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

func (l *CommentLocation) toMap() map[string]any {
	out := map[string]any{}
	if l.FilePath != "" {
		out["filePath"] = l.FilePath
	}

	if l.FilePosition != 0 {
		out["filePosition"] = l.FilePosition
	}

	if l.RelativeFileVersion != "" {
		out["relativeFileVersion"] = l.RelativeFileVersion
	}

	return out
}

func (h *Handler) postComment(
	ctx context.Context, op, token string, cc CommentContext, req any, content string,
) (*Comment, error) {
	cc.AuthorARN = awsmeta.CallerArn(ctx)

	return replayCreate(
		h, op, token, req,
		func(c *Comment) string { return c.CommentID },
		h.Backend.GetComment,
		func() (*Comment, error) { return h.Backend.PostComment(cc, content) },
	)
}

func (h *Handler) handlePostCommentForComparedCommit(ctx context.Context, body []byte) (any, error) {
	var req struct {
		Location           *CommentLocation `json:"location"`
		RepositoryName     string           `json:"repositoryName"`
		BeforeCommitID     string           `json:"beforeCommitId"`
		AfterCommitID      string           `json:"afterCommitId"`
		Content            string           `json:"content"`
		ClientRequestToken string           `json:"clientRequestToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: repositoryName and content are required", errInvalidRequest)
	}

	c, err := h.postComment(ctx, "PostCommentForComparedCommit", req.ClientRequestToken, CommentContext{
		RepoName: req.RepositoryName, BeforeCommitID: req.BeforeCommitID,
		AfterCommitID: req.AfterCommitID, Location: req.Location,
	}, req, req.Content)
	if err != nil {
		return nil, err
	}

	out := map[string]any{
		keyComment:        commentToMap(c),
		keyRepositoryName: req.RepositoryName,
		keyAfterCommitID:  req.AfterCommitID,
		"beforeCommitId":  req.BeforeCommitID,
	}
	if c.Location != nil {
		out[keyLocation] = c.Location.toMap()
	}

	return out, nil
}

func (h *Handler) handlePostCommentForPullRequest(ctx context.Context, body []byte) (any, error) {
	var req struct {
		Location           *CommentLocation `json:"location"`
		PullRequestID      string           `json:"pullRequestId"`
		RepositoryName     string           `json:"repositoryName"`
		BeforeCommitID     string           `json:"beforeCommitId"`
		AfterCommitID      string           `json:"afterCommitId"`
		Content            string           `json:"content"`
		ClientRequestToken string           `json:"clientRequestToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: pullRequestId and content are required", errInvalidRequest)
	}

	c, err := h.postComment(ctx, "PostCommentForPullRequest", req.ClientRequestToken, CommentContext{
		PullRequestID: req.PullRequestID, RepoName: req.RepositoryName,
		BeforeCommitID: req.BeforeCommitID, AfterCommitID: req.AfterCommitID, Location: req.Location,
	}, req, req.Content)
	if err != nil {
		return nil, err
	}

	out := map[string]any{
		keyComment:        commentToMap(c),
		keyPullRequestID:  req.PullRequestID,
		keyRepositoryName: req.RepositoryName,
		keyAfterCommitID:  req.AfterCommitID,
		"beforeCommitId":  req.BeforeCommitID,
	}
	if c.Location != nil {
		out[keyLocation] = c.Location.toMap()
	}

	return out, nil
}

func (h *Handler) handlePostCommentReply(ctx context.Context, body []byte) (any, error) {
	var req struct {
		InReplyTo          string `json:"inReplyTo"`
		Content            string `json:"content"`
		ClientRequestToken string `json:"clientRequestToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.InReplyTo == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: inReplyTo and content are required", errInvalidRequest)
	}

	author := awsmeta.CallerArn(ctx)

	c, err := replayCreate(
		h, "PostCommentReply", req.ClientRequestToken, req,
		func(c *Comment) string { return c.CommentID },
		h.Backend.GetComment,
		func() (*Comment, error) { return h.Backend.PostCommentReply(req.InReplyTo, req.Content, author) },
	)
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

// groupComments splits a page of comments into one group per comment location, oldest first.
func groupComments(comments []*Comment, base func(*Comment) map[string]any) []map[string]any {
	var order []string

	items := map[string][]map[string]any{}
	heads := map[string]*Comment{}

	for _, c := range comments {
		key := ""
		if c.Location != nil {
			key = fmt.Sprintf("%s|%d|%s", c.Location.FilePath, c.Location.FilePosition, c.Location.RelativeFileVersion)
		}

		if _, seen := heads[key]; !seen {
			order = append(order, key)
			heads[key] = c
		}

		items[key] = append(items[key], commentToMap(c))
	}

	data := make([]map[string]any, 0, len(order))

	for _, key := range order {
		group := base(heads[key])
		group["comments"] = items[key]

		if heads[key].Location != nil {
			group[keyLocation] = heads[key].Location.toMap()
		}

		data = append(data, group)
	}

	return data
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

	comments, err := h.Backend.GetCommentsForComparedCommit(req.RepositoryName, req.AfterCommitID, req.BeforeCommitID)
	if err != nil {
		return nil, err
	}

	pg := page.New(comments, req.NextToken, req.MaxResults, getCommentsForComparedCommitDefaultMaxResults)

	data := groupComments(pg.Data, func(c *Comment) map[string]any {
		group := map[string]any{
			keyRepositoryName: req.RepositoryName,
			keyAfterCommitID:  req.AfterCommitID,
		}
		if c.BeforeCommitID != "" {
			group["beforeCommitId"] = c.BeforeCommitID
		}

		return group
	})

	out := map[string]any{"commentsForComparedCommitData": data}
	if pg.Next != "" {
		out["nextToken"] = pg.Next
	}

	return out, nil
}

func (h *Handler) handleGetCommentsForPullRequest(body []byte) (any, error) {
	var req struct {
		PullRequestID  string `json:"pullRequestId"`
		RepositoryName string `json:"repositoryName"`
		BeforeCommitID string `json:"beforeCommitId"`
		AfterCommitID  string `json:"afterCommitId"`
		NextToken      string `json:"nextToken"`
		MaxResults     int    `json:"maxResults"`
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

	comments, err := h.Backend.GetCommentsForPullRequest(
		req.PullRequestID, req.RepositoryName, req.BeforeCommitID, req.AfterCommitID,
	)
	if err != nil {
		return nil, err
	}

	pg := page.New(comments, req.NextToken, req.MaxResults, getCommentsForComparedCommitDefaultMaxResults)

	data := groupComments(pg.Data, func(c *Comment) map[string]any {
		group := map[string]any{keyPullRequestID: req.PullRequestID}
		if c.RepoName != "" {
			group[keyRepositoryName] = c.RepoName
		}

		if c.BeforeCommitID != "" {
			group["beforeCommitId"] = c.BeforeCommitID
		}

		if c.AfterCommitID != "" {
			group[keyAfterCommitID] = c.AfterCommitID
		}

		return group
	})

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
