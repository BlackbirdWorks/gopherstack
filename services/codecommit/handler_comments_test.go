package codecommit_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PostCommentForComparedCommit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "comment-repo-1"})

	rec := doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
		"repositoryName": "comment-repo-1",
		"beforeCommitId": "abc",
		"afterCommitId":  "def",
		"content":        "hello world",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	comment := resp["comment"].(map[string]any)
	assert.Equal(t, "hello world", comment["content"])
	assert.NotEmpty(t, comment["commentId"])
}

func TestHandler_PostCommentForPullRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-comment-repo")

	rec := doRequest(t, h, "PostCommentForPullRequest", map[string]any{
		"pullRequestId":  prID,
		"repositoryName": "pr-comment-repo",
		"content":        "PR comment",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	comment := resp["comment"].(map[string]any)
	assert.Equal(t, "PR comment", comment["content"])
}

func TestHandler_PostCommentReply(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "reply-repo"})

	rec := doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
		"repositoryName": "reply-repo",
		"afterCommitId":  "abc",
		"content":        "parent comment",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var parentResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &parentResp))
	parentID := parentResp["comment"].(map[string]any)["commentId"].(string)

	rec = doRequest(t, h, "PostCommentReply", map[string]any{
		"inReplyTo": parentID,
		"content":   "reply comment",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "reply comment", resp["comment"].(map[string]any)["content"])
}

func TestHandler_GetComment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "get-comment-repo"})

	rec := doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
		"repositoryName": "get-comment-repo",
		"afterCommitId":  "abc",
		"content":        "get me",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	cID := cr["comment"].(map[string]any)["commentId"].(string)

	rec = doRequest(t, h, "GetComment", map[string]any{"commentId": cID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// not found
	rec = doRequest(t, h, "GetComment", map[string]any{"commentId": "bad-id"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetCommentsForComparedCommit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "cmp-repo"})
	doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
		"repositoryName": "cmp-repo",
		"afterCommitId":  "commit123",
		"content":        "comment1",
	})

	rec := doRequest(t, h, "GetCommentsForComparedCommit", map[string]any{
		"repositoryName": "cmp-repo",
		"afterCommitId":  "commit123",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	comments := resp["commentsForComparedCommitData"].([]any)
	assert.Len(t, comments, 1)
}

func TestHandler_GetCommentsForComparedCommit_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		afterCommit  string
		commentCount int
		wantCode     int
	}{
		{name: "no_comments", commentCount: 0, afterCommit: "abc123", wantCode: http.StatusOK},
		{name: "two_comments", commentCount: 2, afterCommit: "commit-1", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			for i := range tt.commentCount {
				doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
					"repositoryName": "repo",
					"beforeCommitId": "before",
					"afterCommitId":  tt.afterCommit,
					"content":        fmt.Sprintf("comment %d", i),
				})
			}

			rec := doRequest(t, h, "GetCommentsForComparedCommit", map[string]any{
				"repositoryName": "repo",
				"afterCommitId":  tt.afterCommit,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items := resp["commentsForComparedCommitData"].([]any)
			assert.Len(t, items, tt.commentCount)
		})
	}
}

func TestHandler_GetCommentsForPullRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-comment-list-repo")

	doRequest(t, h, "PostCommentForPullRequest", map[string]any{
		"pullRequestId":  prID,
		"repositoryName": "pr-comment-list-repo",
		"content":        "pr comment",
	})

	rec := doRequest(t, h, "GetCommentsForPullRequest", map[string]any{
		"pullRequestId": prID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	comments := resp["commentsForPullRequestData"].([]any)
	assert.Len(t, comments, 1)
}

func TestHandler_GetCommentsForPullRequest_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		commentCount int
		wantCode     int
	}{
		{name: "no_comments", commentCount: 0, wantCode: http.StatusOK},
		{name: "one_comment", commentCount: 1, wantCode: http.StatusOK},
		{name: "three_comments", commentCount: 3, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			prID := setupPR(t, h, "repo")

			for i := range tt.commentCount {
				doRequest(t, h, "PostCommentForPullRequest", map[string]any{
					"pullRequestId":  prID,
					"repositoryName": "repo",
					"content":        fmt.Sprintf("comment %d", i),
				})
			}

			rec := doRequest(t, h, "GetCommentsForPullRequest", map[string]any{
				"pullRequestId": prID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items := resp["commentsForPullRequestData"].([]any)
			assert.Len(t, items, tt.commentCount)
		})
	}
}

func TestHandler_UpdateComment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "upd-comment-repo"})

	rec := doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
		"repositoryName": "upd-comment-repo",
		"afterCommitId":  "abc",
		"content":        "original",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	cID := cr["comment"].(map[string]any)["commentId"].(string)

	rec = doRequest(t, h, "UpdateComment", map[string]any{
		"commentId": cID,
		"content":   "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "updated", resp["comment"].(map[string]any)["content"])
}

func TestHandler_DeleteCommentContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "del-comment-repo"})

	rec := doRequest(t, h, "PostCommentForComparedCommit", map[string]any{
		"repositoryName": "del-comment-repo",
		"afterCommitId":  "abc",
		"content":        "delete me",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	cID := cr["comment"].(map[string]any)["commentId"].(string)

	rec = doRequest(t, h, "DeleteCommentContent", map[string]any{"commentId": cID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["comment"].(map[string]any)["deleted"])
}

func TestHandler_CommentLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "repo")

	// Post comment on PR.
	rec := doRequest(t, h, "PostCommentForPullRequest", map[string]any{
		"pullRequestId":  prID,
		"repositoryName": "repo",
		"content":        "initial comment",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	commentID := resp["comment"].(map[string]any)["commentId"].(string)

	// Update the comment.
	rec = doRequest(t, h, "UpdateComment", map[string]any{
		"commentId": commentID,
		"content":   "updated content",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "updated content", resp["comment"].(map[string]any)["content"])

	// Add reaction.
	rec = doRequest(t, h, "PutCommentReaction", map[string]any{
		"commentId":     commentID,
		"reactionValue": ":thumbsup:",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get reactions.
	rec = doRequest(t, h, "GetCommentReactions", map[string]any{"commentId": commentID})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	reactions := resp["reactionsForComment"].([]any)
	assert.Len(t, reactions, 1)

	// Post reply.
	rec = doRequest(t, h, "PostCommentReply", map[string]any{
		"inReplyTo": commentID,
		"content":   "reply here",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete comment content.
	rec = doRequest(t, h, "DeleteCommentContent", map[string]any{"commentId": commentID})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	c := resp["comment"].(map[string]any)
	assert.Equal(t, true, c["deleted"])
	assert.Empty(t, c["content"])
}
