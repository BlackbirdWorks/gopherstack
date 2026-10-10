package codecommit

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

func newCommentID() string {
	return uuid.NewString()
}

// CommentContext is the request context a new comment is anchored to.
type CommentContext struct {
	Location       *CommentLocation
	RepoName       string
	BeforeCommitID string
	AfterCommitID  string
	PullRequestID  string
	AuthorARN      string
}

func validateLocation(loc *CommentLocation) error {
	if loc == nil {
		return nil
	}

	switch loc.RelativeFileVersion {
	case "", "BEFORE", "AFTER":
		return nil
	default:
		return fmt.Errorf("%w: relativeFileVersion %q", ErrInvalidRelativeFileVersion, loc.RelativeFileVersion)
	}
}

// PostComment creates a comment on a compared commit or, when ctx.PullRequestID is set, a pull request.
func (b *InMemoryBackend) PostComment(cc CommentContext, content string) (*Comment, error) {
	if err := validateLocation(cc.Location); err != nil {
		return nil, err
	}

	b.mu.Lock("PostComment")
	defer b.mu.Unlock()

	if cc.PullRequestID != "" && !b.pullRequests.Has(cc.PullRequestID) {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, cc.PullRequestID)
	}

	checkRepo := cc.PullRequestID == "" || cc.RepoName != ""
	if checkRepo && !b.repositories.Has(cc.RepoName) {
		return nil, fmt.Errorf("%w: %s does not exist", ErrNotFound, cc.RepoName)
	}

	now := time.Now().UTC()
	c := &Comment{
		CommentID:        newCommentID(),
		Content:          content,
		AuthorARN:        cc.AuthorARN,
		CreationDate:     now,
		LastModifiedDate: now,
		PRid:             cc.PullRequestID,
		RepoName:         cc.RepoName,
		BeforeCommitID:   cc.BeforeCommitID,
		AfterCommitID:    cc.AfterCommitID,
	}

	if cc.Location != nil {
		loc := *cc.Location
		c.Location = &loc
	}

	b.comments.Put(c)
	cp := *c

	return &cp, nil
}

// PostCommentReply creates a reply that inherits its parent's commit or pull request context.
func (b *InMemoryBackend) PostCommentReply(inReplyTo, content, authorARN string) (*Comment, error) {
	b.mu.Lock("PostCommentReply")
	defer b.mu.Unlock()

	parent, ok := b.comments.Get(inReplyTo)
	if !ok {
		return nil, fmt.Errorf("%w: comment %s not found", ErrCommentNotFound, inReplyTo)
	}

	now := time.Now().UTC()
	c := &Comment{
		CommentID:        newCommentID(),
		Content:          content,
		AuthorARN:        authorARN,
		CreationDate:     now,
		LastModifiedDate: now,
		InReplyTo:        inReplyTo,
		PRid:             parent.PRid,
		RepoName:         parent.RepoName,
		BeforeCommitID:   parent.BeforeCommitID,
		AfterCommitID:    parent.AfterCommitID,
		Location:         parent.Location,
	}
	b.comments.Put(c)
	cp := *c

	return &cp, nil
}

// GetComment retrieves a comment by ID.
func (b *InMemoryBackend) GetComment(commentID string) (*Comment, error) {
	b.mu.RLock("GetComment")
	defer b.mu.RUnlock()

	c, ok := b.comments.Get(commentID)
	if !ok {
		return nil, fmt.Errorf("%w: comment %s not found", ErrCommentNotFound, commentID)
	}
	cp := *c

	return &cp, nil
}

// GetCommentsForComparedCommit returns comments on a commit pair, oldest first.
// An empty beforeCommitID matches any.
func (b *InMemoryBackend) GetCommentsForComparedCommit(
	repoName, afterCommitID, beforeCommitID string,
) ([]*Comment, error) {
	b.mu.RLock("GetCommentsForComparedCommit")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: %s does not exist", ErrNotFound, repoName)
	}

	return b.collectCommentsLocked(func(c *Comment) bool {
		return c.PRid == "" && c.RepoName == repoName && c.AfterCommitID == afterCommitID &&
			(beforeCommitID == "" || c.BeforeCommitID == beforeCommitID)
	}), nil
}

// GetCommentsForPullRequest returns comments on a pull request, oldest first, optionally
// narrowed to a repository and commit pair.
func (b *InMemoryBackend) GetCommentsForPullRequest(
	prID, repoName, beforeCommitID, afterCommitID string,
) ([]*Comment, error) {
	b.mu.RLock("GetCommentsForPullRequest")
	defer b.mu.RUnlock()

	if !b.pullRequests.Has(prID) {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	return b.collectCommentsLocked(func(c *Comment) bool {
		return c.PRid == prID &&
			(repoName == "" || c.RepoName == repoName) &&
			(beforeCommitID == "" || c.BeforeCommitID == beforeCommitID) &&
			(afterCommitID == "" || c.AfterCommitID == afterCommitID)
	}), nil
}

func (b *InMemoryBackend) collectCommentsLocked(keep func(*Comment) bool) []*Comment {
	var result []*Comment

	for _, c := range b.comments.All() {
		if keep(c) {
			cp := *c
			result = append(result, &cp)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreationDate.Equal(result[j].CreationDate) {
			return result[i].CreationDate.Before(result[j].CreationDate)
		}

		return result[i].CommentID < result[j].CommentID
	})

	return result
}

// UpdateComment updates the content of a comment.
func (b *InMemoryBackend) UpdateComment(commentID, content string) error {
	b.mu.Lock("UpdateComment")
	defer b.mu.Unlock()

	c, ok := b.comments.Get(commentID)
	if !ok {
		return fmt.Errorf("%w: comment %s not found", ErrCommentNotFound, commentID)
	}
	c.Content = content
	c.LastModifiedDate = time.Now().UTC()

	return nil
}

// DeleteCommentContent marks a comment as deleted and clears its content.
func (b *InMemoryBackend) DeleteCommentContent(commentID string) error {
	b.mu.Lock("DeleteCommentContent")
	defer b.mu.Unlock()

	c, ok := b.comments.Get(commentID)
	if !ok {
		return fmt.Errorf("%w: comment %s not found", ErrCommentNotFound, commentID)
	}
	c.Deleted = true
	c.Content = ""
	c.LastModifiedDate = time.Now().UTC()

	return nil
}
