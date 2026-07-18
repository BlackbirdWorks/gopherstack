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

// PutCommentReaction adds a reaction to a comment.
func (b *InMemoryBackend) PutCommentReaction(commentID, emoji string) error {
	b.mu.Lock("PutCommentReaction")
	defer b.mu.Unlock()

	if !b.comments.Has(commentID) {
		return fmt.Errorf("%w: comment %s not found", ErrCommentNotFound, commentID)
	}
	b.commentReactions[commentID] = append(b.commentReactions[commentID], Reaction{Emoji: emoji})

	return nil
}
