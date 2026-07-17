package codecommit

import (
	"encoding/json"
	"fmt"
)

func (h *Handler) handleGetCommentReactions(body []byte) (any, error) {
	var req struct {
		CommentID string `json:"commentId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.CommentID == "" {
		return nil, fmt.Errorf("%w: commentId is required", errInvalidRequest)
	}

	reactions, err := h.Backend.GetCommentReactions(req.CommentID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"reactionsForComment": reactions,
	}, nil
}

func (h *Handler) handlePutCommentReaction(body []byte) (any, error) {
	var req struct {
		CommentID     string `json:"commentId"`
		ReactionValue string `json:"reactionValue"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.CommentID == "" || req.ReactionValue == "" {
		return nil, fmt.Errorf("%w: commentId and reactionValue are required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.PutCommentReaction(req.CommentID, req.ReactionValue)
}
