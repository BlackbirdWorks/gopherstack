package codecommit

import "fmt"

// GetCommentReactions returns reactions for a comment.
func (b *InMemoryBackend) GetCommentReactions(commentID string) ([]Reaction, error) {
	b.mu.RLock("GetCommentReactions")
	defer b.mu.RUnlock()

	if !b.comments.Has(commentID) {
		return nil, fmt.Errorf("%w: comment %s not found", ErrCommentNotFound, commentID)
	}

	reactions := b.commentReactions[commentID]
	result := make([]Reaction, len(reactions))
	copy(result, reactions)

	return result, nil
}

// PutCommentReaction adds a reaction to a comment on behalf of userARN
// (the resolved caller identity -- PutCommentReactionInput carries no
// client-supplied ARN field, matching OverridePullRequestApprovalRules'
// same convention, see handler_pull_requests.go). Idempotent per
// (commentID, emoji, userARN): reacting twice with the same emoji does not
// duplicate the entry, matching a real "Put" operation's semantics.
func (b *InMemoryBackend) PutCommentReaction(commentID, emoji, userARN string) error {
	b.mu.Lock("PutCommentReaction")
	defer b.mu.Unlock()

	if !b.comments.Has(commentID) {
		return fmt.Errorf("%w: comment %s not found", ErrCommentNotFound, commentID)
	}

	for _, r := range b.commentReactions[commentID] {
		if r.Emoji == emoji && r.UserARN == userARN {
			return nil
		}
	}

	b.commentReactions[commentID] = append(b.commentReactions[commentID], Reaction{Emoji: emoji, UserARN: userARN})

	return nil
}
