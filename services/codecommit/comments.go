package codecommit

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

func newCommentID() string {
	return uuid.NewString()
}

// PostCommentForComparedCommit creates a comment on a compared commit.
func (b *InMemoryBackend) PostCommentForComparedCommit(repoName, _, afterCommitID, content string) (*Comment, error) {
	b.mu.Lock("PostCommentForComparedCommit")
	defer b.mu.Unlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	now := time.Now().UTC()
	c := &Comment{
		CommentID:        newCommentID(),
		Content:          content,
		CreationDate:     now,
		LastModifiedDate: now,
		RepoName:         repoName,
		AfterCommitID:    afterCommitID,
	}
	b.comments.Put(c)
	cp := *c

	return &cp, nil
}

// PostCommentForPullRequest creates a comment on a pull request.
func (b *InMemoryBackend) PostCommentForPullRequest(prID, repoName, content string) (*Comment, error) {
	b.mu.Lock("PostCommentForPullRequest")
	defer b.mu.Unlock()

	if !b.pullRequests.Has(prID) {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	now := time.Now().UTC()
	c := &Comment{
		CommentID:        newCommentID(),
		Content:          content,
		CreationDate:     now,
		LastModifiedDate: now,
		PRid:             prID,
		RepoName:         repoName,
	}
	b.comments.Put(c)
	cp := *c

	return &cp, nil
}

// PostCommentReply creates a reply to an existing comment.
func (b *InMemoryBackend) PostCommentReply(inReplyTo, content string) (*Comment, error) {
	b.mu.Lock("PostCommentReply")
	defer b.mu.Unlock()

	if !b.comments.Has(inReplyTo) {
		return nil, fmt.Errorf("%w: comment %s not found", ErrCommentNotFound, inReplyTo)
	}

	now := time.Now().UTC()
	c := &Comment{
		CommentID:        newCommentID(),
		Content:          content,
		CreationDate:     now,
		LastModifiedDate: now,
		InReplyTo:        inReplyTo,
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

// GetCommentsForComparedCommit returns comments for a compared commit.
func (b *InMemoryBackend) GetCommentsForComparedCommit(repoName, afterCommitID string) ([]*Comment, error) {
	b.mu.RLock("GetCommentsForComparedCommit")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	var result []*Comment
	for _, c := range b.comments.All() {
		if c.RepoName == repoName && c.AfterCommitID == afterCommitID {
			cp := *c
			result = append(result, &cp)
		}
	}

	return result, nil
}

// GetCommentsForPullRequest returns comments for a pull request.
func (b *InMemoryBackend) GetCommentsForPullRequest(prID string) ([]*Comment, error) {
	b.mu.RLock("GetCommentsForPullRequest")
	defer b.mu.RUnlock()

	if !b.pullRequests.Has(prID) {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}

	var result []*Comment
	for _, c := range b.comments.All() {
		if c.PRid == prID {
			cp := *c
			result = append(result, &cp)
		}
	}

	return result, nil
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
