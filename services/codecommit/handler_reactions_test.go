package codecommit_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetCommentReactions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "react-repo"})

	rec := doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
		"repositoryName": "react-repo",
		"afterCommitId":  "abc",
		"content":        "react to me",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	cID := cr["comment"].(map[string]any)["commentId"].(string)

	doRequest(t, h, "PutCommentReaction", map[string]any{
		"commentId":     cID,
		"reactionValue": ":+1:",
	})

	rec = doRequest(t, h, "GetCommentReactions", map[string]any{"commentId": cID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	reactions := resp["reactionsForComment"].([]any)
	assert.Len(t, reactions, 1)
}

func TestHandler_PutCommentReaction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "react2-repo"})

	rec := doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
		"repositoryName": "react2-repo",
		"afterCommitId":  "abc",
		"content":        "react2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	cID := cr["comment"].(map[string]any)["commentId"].(string)

	rec = doRequest(t, h, "PutCommentReaction", map[string]any{
		"commentId":     cID,
		"reactionValue": ":heart:",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
