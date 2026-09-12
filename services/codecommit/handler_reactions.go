package codecommit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
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
		"reactionsForComment": reactionsForCommentJSON(reactions),
	}, nil
}

// reactionsForCommentJSON converts this backend's flat []Reaction into the
// real ReactionForComment wire shape: one entry per distinct emoji, each
// nesting a ReactionValueFormats object under "reaction" and the reacting
// users' ARNs under "reactionUsers" (confirmed against
// codecommit@v1.36.4/types/types.go's ReactionForComment/ReactionValueFormats
// -- there is no flat emoji/userArn member on the real shape at all).
// ShortCode/Unicode are left unset: this backend never resolves a client's
// raw reactionValue into those two alternate representations.
func reactionsForCommentJSON(reactions []Reaction) []map[string]any {
	order := make([]string, 0, len(reactions))
	usersByEmoji := make(map[string][]string, len(reactions))

	for _, r := range reactions {
		if _, seen := usersByEmoji[r.Emoji]; !seen {
			order = append(order, r.Emoji)
		}

		if r.UserARN != "" {
			usersByEmoji[r.Emoji] = append(usersByEmoji[r.Emoji], r.UserARN)
		}
	}

	out := make([]map[string]any, 0, len(order))

	for _, emoji := range order {
		out = append(out, map[string]any{
			"reaction":      map[string]any{"emoji": emoji},
			"reactionUsers": usersByEmoji[emoji],
		})
	}

	return out
}

// handlePutCommentReaction records the resolved caller identity
// (awsmeta.CallerArn, set onto ctx by cli.go's global principalMiddleware
// before dispatch ever runs) as the reacting user -- PutCommentReactionInput
// has no client-supplied ARN field at all (codecommit@v1.36.4
// api_op_PutCommentReaction.go), matching
// handleOverridePullRequestApprovalRules' same convention.
func (h *Handler) handlePutCommentReaction(ctx context.Context, body []byte) (any, error) {
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

	actorARN := awsmeta.CallerArn(ctx)

	return map[string]any{}, h.Backend.PutCommentReaction(req.CommentID, req.ReactionValue, actorARN)
}
