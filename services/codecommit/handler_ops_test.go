package codecommit_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// --- helpers ---

func setupRepoAndBranch(t *testing.T, h *codecommit.Handler, repoName string) {
	t.Helper()

	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": repoName})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create a commit so branch can exist
	rec = doRequest(t, h, "CreateCommit", map[string]any{
		"repositoryName": repoName,
		"branchName":     "main",
		"authorName":     "test",
		"email":          "test@example.com",
		"commitMessage":  "initial",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func setupPR(t *testing.T, h *codecommit.Handler, repoName string) string {
	t.Helper()
	setupRepoAndBranch(t, h, repoName)
	rec := doRequest(t, h, "CreatePullRequest", map[string]any{
		"title": "Test PR",
		"targets": []map[string]any{
			{"repositoryName": repoName, "sourceReference": "refs/heads/feature"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	pr := resp["pullRequest"].(map[string]any)

	return pr["pullRequestId"].(string)
}

// --- Group 1: Repository CRUD edges ---

func TestHandler_UpdateRepositoryDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo1"})

	rec := doRequest(t, h, "UpdateRepositoryDescription", map[string]any{
		"repositoryName":        "repo1",
		"repositoryDescription": "updated desc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Non-existent repo
	rec = doRequest(t, h, "UpdateRepositoryDescription", map[string]any{
		"repositoryName":        "no-such-repo",
		"repositoryDescription": "x",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateRepositoryName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "old-name"})

	rec := doRequest(t, h, "UpdateRepositoryName", map[string]any{
		"oldName": "old-name",
		"newName": "new-name",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// old name no longer exists
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "old-name"})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// new name exists
	rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": "new-name"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateRepositoryEncryptionKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "enc-repo"})

	rec := doRequest(t, h, "UpdateRepositoryEncryptionKey", map[string]any{
		"repositoryName": "enc-repo",
		"kmsKeyId":       "arn:aws:kms:us-east-1:123456789012:key/abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// not found
	rec = doRequest(t, h, "UpdateRepositoryEncryptionKey", map[string]any{
		"repositoryName": "no-repo",
		"kmsKeyId":       "key",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateDefaultBranch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "br-repo"})

	rec := doRequest(t, h, "UpdateDefaultBranch", map[string]any{
		"repositoryName":    "br-repo",
		"defaultBranchName": "main",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// not found
	rec = doRequest(t, h, "UpdateDefaultBranch", map[string]any{
		"repositoryName":    "no-repo",
		"defaultBranchName": "main",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Group 2: Approval Rule Template CRUD ---

func TestHandler_DeleteApprovalRuleTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl1",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "DeleteApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// not found after deletion
	rec = doRequest(t, h, "DeleteApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl1",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetApprovalRuleTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-get",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl-get",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tmpl := resp["approvalRuleTemplate"].(map[string]any)
	assert.Equal(t, "tmpl-get", tmpl["approvalRuleTemplateName"])

	// not found
	rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "no-tmpl",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListApprovalRuleTemplates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-a",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-b",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "ListApprovalRuleTemplates", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	names := resp["approvalRuleTemplateNames"].([]any)
	assert.Len(t, names, 2)
}

func TestHandler_UpdateApprovalRuleTemplateContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-content",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	newContent := `{"Version":"2018-11-08","Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":1}]}`
	rec := doRequest(t, h, "UpdateApprovalRuleTemplateContent", map[string]any{
		"approvalRuleTemplateName": "tmpl-content",
		"newRuleContent":           newContent,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// not found
	rec = doRequest(t, h, "UpdateApprovalRuleTemplateContent", map[string]any{
		"approvalRuleTemplateName": "no-tmpl",
		"newRuleContent":           "{}",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateApprovalRuleTemplateDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-desc",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "UpdateApprovalRuleTemplateDescription", map[string]any{
		"approvalRuleTemplateName":        "tmpl-desc",
		"approvalRuleTemplateDescription": "new description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateApprovalRuleTemplateName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-old",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "UpdateApprovalRuleTemplateName", map[string]any{
		"oldApprovalRuleTemplateName": "tmpl-old",
		"newApprovalRuleTemplateName": "tmpl-new",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// old name no longer exists
	rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl-old",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// new name exists
	rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl-new",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Group 3: Template-Repository association edges ---

func TestHandler_ListAssociatedApprovalRuleTemplatesForRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "assoc-repo"})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "assoc-tmpl",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "assoc-tmpl",
		"repositoryName":           "assoc-repo",
	})

	rec := doRequest(t, h, "ListAssociatedApprovalRuleTemplatesForRepository", map[string]any{
		"repositoryName": "assoc-repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	names := resp["approvalRuleTemplateNames"].([]any)
	assert.Contains(t, names, "assoc-tmpl")
}

func TestHandler_ListRepositoriesForApprovalRuleTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo-for-tmpl"})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl-for-repo",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "tmpl-for-repo",
		"repositoryName":           "repo-for-tmpl",
	})

	rec := doRequest(t, h, "ListRepositoriesForApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl-for-repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	repos := resp["repositoryNames"].([]any)
	assert.Contains(t, repos, "repo-for-tmpl")
}

// --- Group 4: PullRequest operations ---

func TestHandler_GetPullRequestApprovalStates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-1")

	rec := doRequest(t, h, "GetPullRequestApprovalStates", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["approvals"])
}

func TestHandler_UpdatePullRequestApprovalState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-2")

	rec := doRequest(t, h, "UpdatePullRequestApprovalState", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev1",
		"approvalState": "APPROVE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetPullRequestOverrideState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-3")

	rec := doRequest(t, h, "GetPullRequestOverrideState", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, false, resp["overridden"])
}

func TestHandler_OverridePullRequestApprovalRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-4")

	rec := doRequest(t, h, "OverridePullRequestApprovalRules", map[string]any{
		"pullRequestId":  prID,
		"revisionId":     "rev1",
		"overrideStatus": "OVERRIDE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdatePullRequestDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-5")

	rec := doRequest(t, h, "UpdatePullRequestDescription", map[string]any{
		"pullRequestId": prID,
		"description":   "new description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)
	assert.Equal(t, "new description", pr["description"])
}

func TestHandler_UpdatePullRequestStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-6")

	rec := doRequest(t, h, "UpdatePullRequestStatus", map[string]any{
		"pullRequestId":     prID,
		"pullRequestStatus": "CLOSED",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)
	assert.Equal(t, "CLOSED", pr["pullRequestStatus"])
}

func TestHandler_UpdatePullRequestTitle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-7")

	rec := doRequest(t, h, "UpdatePullRequestTitle", map[string]any{
		"pullRequestId": prID,
		"title":         "updated title",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)
	assert.Equal(t, "updated title", pr["title"])
}

func TestHandler_CreatePullRequestApprovalRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-8")

	rec := doRequest(t, h, "CreatePullRequestApprovalRule", map[string]any{
		"pullRequestId":       prID,
		"approvalRuleName":    "my-rule",
		"approvalRuleContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule := resp["approvalRule"].(map[string]any)
	assert.Equal(t, "my-rule", rule["approvalRuleName"])
}

func TestHandler_DeletePullRequestApprovalRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-9")

	doRequest(t, h, "CreatePullRequestApprovalRule", map[string]any{
		"pullRequestId":       prID,
		"approvalRuleName":    "delete-me",
		"approvalRuleContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "DeletePullRequestApprovalRule", map[string]any{
		"pullRequestId":    prID,
		"approvalRuleName": "delete-me",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdatePullRequestApprovalRuleContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-10")

	doRequest(t, h, "CreatePullRequestApprovalRule", map[string]any{
		"pullRequestId":       prID,
		"approvalRuleName":    "update-rule",
		"approvalRuleContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "UpdatePullRequestApprovalRuleContent", map[string]any{
		"pullRequestId":    prID,
		"approvalRuleName": "update-rule",
		"newRuleContent":   `{"Version":"2018-11-08","Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":2}]}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribePullRequestEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-11")

	rec := doRequest(t, h, "DescribePullRequestEvents", map[string]any{
		"pullRequestId": prID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["pullRequestEvents"])
}

func TestHandler_EvaluatePullRequestApprovalRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "pr-repo-12")

	doRequest(t, h, "CreatePullRequestApprovalRule", map[string]any{
		"pullRequestId":       prID,
		"approvalRuleName":    "eval-rule",
		"approvalRuleContent": `{"Version":"2018-11-08","Statements":[]}`,
	})

	rec := doRequest(t, h, "EvaluatePullRequestApprovalRules", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	evals := resp["evaluationResults"].([]any)
	require.Len(t, evals, 1)
	eval := evals[0].(map[string]any)
	assert.Equal(t, "eval-rule", eval["approvalRuleName"])
	assert.Equal(t, true, eval["satisfied"])
}

// --- Group 5: Comments ---

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

// --- Group 6: File/Blob ops ---

func TestHandler_PutFile_GetFile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "file-repo"})

	rec := doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "file-repo",
		"branchName":     "main",
		"filePath":       "hello.txt",
		"fileContent":    "aGVsbG8=", // base64 "hello"
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetFile", map[string]any{
		"repositoryName":  "file-repo",
		"commitSpecifier": "main",
		"filePath":        "hello.txt",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "hello.txt", resp["filePath"])
}

func TestHandler_GetFolder(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "folder-repo"})
	doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "folder-repo",
		"filePath":       "src/main.go",
		"fileContent":    "cGFja2FnZSBtYWlu",
	})

	rec := doRequest(t, h, "GetFolder", map[string]any{
		"repositoryName": "folder-repo",
		"folderPath":     "src",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	files := resp["files"].([]any)
	assert.Len(t, files, 1)
}

func TestHandler_DeleteFile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "del-file-repo"})
	doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "del-file-repo",
		"branchName":     "main",
		"filePath":       "todelete.txt",
		"fileContent":    "dG9kZWxldGU=",
	})

	rec := doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "del-file-repo",
		"branchName":     "main",
		"filePath":       "todelete.txt",
		"parentCommitId": "abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["commitId"])
}

// --- Group 7: Triggers ---

func TestHandler_PutGetRepositoryTriggers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "trigger-repo"})

	rec := doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "trigger-repo",
		"triggers": []map[string]any{
			{
				"name":           "my-trigger",
				"destinationArn": "arn:aws:sns:us-east-1:123456789012:my-topic",
				"events":         []string{"all"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepositoryTriggers", map[string]any{
		"repositoryName": "trigger-repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	triggers := resp["triggers"].([]any)
	assert.Len(t, triggers, 1)
}

func TestHandler_TestRepositoryTriggers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "test-trigger-repo"})
	doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "test-trigger-repo",
		"triggers": []map[string]any{
			{
				"name":           "trigger1",
				"destinationArn": "arn:aws:sns:us-east-1:123456789012:topic1",
				"events":         []string{"all"},
			},
		},
	})

	rec := doRequest(t, h, "TestRepositoryTriggers", map[string]any{
		"repositoryName": "test-trigger-repo",
		"triggers":       []map[string]any{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	succeeded := resp["successfulExecutions"].([]any)
	assert.Len(t, succeeded, 1)
}

// --- Group 8: Merge ops ---

func TestHandler_MergePullRequestByFastForward(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "merge-ff-repo")

	rec := doRequest(t, h, "MergePullRequestByFastForward", map[string]any{
		"pullRequestId":  prID,
		"repositoryName": "merge-ff-repo",
		"sourceCommitId": "abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)
	assert.Equal(t, "MERGED", pr["pullRequestStatus"])
}

func TestHandler_MergePullRequestBySquash(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "merge-sq-repo")

	rec := doRequest(t, h, "MergePullRequestBySquash", map[string]any{
		"pullRequestId":  prID,
		"repositoryName": "merge-sq-repo",
		"sourceCommitId": "abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)
	assert.Equal(t, "MERGED", pr["pullRequestStatus"])
}

func TestHandler_MergePullRequestByThreeWay(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "merge-3w-repo")

	rec := doRequest(t, h, "MergePullRequestByThreeWay", map[string]any{
		"pullRequestId":  prID,
		"repositoryName": "merge-3w-repo",
		"sourceCommitId": "abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)
	assert.Equal(t, "MERGED", pr["pullRequestStatus"])
}

func TestHandler_MergeBranchesByFastForward(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "branch-merge-repo"})

	rec := doRequest(t, h, "MergeBranchesByFastForward", map[string]any{
		"repositoryName":             "branch-merge-repo",
		"sourceCommitSpecifier":      "feature",
		"destinationCommitSpecifier": "main",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["commitId"])
}

func TestHandler_GetMergeOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "merge-opts-repo"})

	rec := doRequest(t, h, "GetMergeOptions", map[string]any{
		"repositoryName":             "merge-opts-repo",
		"sourceCommitSpecifier":      "feature",
		"destinationCommitSpecifier": "main",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	opts := resp["mergeOptions"].([]any)
	assert.Len(t, opts, 3)
}

// --- Group 9: Misc ---

func TestHandler_GetBlob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "blob-repo"})

	rec := doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "blob-repo",
		"filePath":       "data.bin",
		"fileContent":    "aGVsbG8=", // "hello" in base64
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get the file to find blobId
	rec2 := doRequest(t, h, "GetFile", map[string]any{
		"repositoryName": "blob-repo",
		"filePath":       "data.bin",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var fr map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &fr))
	blobID := fr["blobId"].(string)

	rec = doRequest(t, h, "GetBlob", map[string]any{
		"repositoryName": "blob-repo",
		"blobId":         blobID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["content"])
}

func TestHandler_ListFileCommitHistory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupRepoAndBranch(t, h, "hist-repo")

	rec := doRequest(t, h, "ListFileCommitHistory", map[string]any{
		"repositoryName": "hist-repo",
		"filePath":       "main.go",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["revisionDag"])
}

func TestHandler_CreateUnreferencedMergeCommit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "unref-repo"})

	rec := doRequest(t, h, "CreateUnreferencedMergeCommit", map[string]any{
		"repositoryName":             "unref-repo",
		"sourceCommitSpecifier":      "abc",
		"destinationCommitSpecifier": "def",
		"mergeOption":                "FAST_FORWARD_MERGE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["commitId"])
}

func TestHandler_GetMergeCommit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupRepoAndBranch(t, h, "merge-commit-repo")

	rec := doRequest(t, h, "GetMergeCommit", map[string]any{
		"repositoryName":             "merge-commit-repo",
		"sourceCommitSpecifier":      "abc",
		"destinationCommitSpecifier": "def",
		"mergeOption":                "FAST_FORWARD_MERGE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["mergedCommitId"])
}

func TestHandler_GetMergeConflicts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "conflicts-repo"})

	rec := doRequest(t, h, "GetMergeConflicts", map[string]any{
		"repositoryName":             "conflicts-repo",
		"sourceCommitSpecifier":      "abc",
		"destinationCommitSpecifier": "def",
		"mergeOption":                "FAST_FORWARD_MERGE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, false, resp["mergeable"])
}

func TestHandler_GetDifferences(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "diffs-repo"})

	rec := doRequest(t, h, "GetDifferences", map[string]any{
		"repositoryName":       "diffs-repo",
		"afterCommitSpecifier": "abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["differences"])
}

func TestHandler_DisassociateApprovalRuleTemplateFromRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "disassoc-repo"})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "disassoc-tmpl",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "disassoc-tmpl",
		"repositoryName":           "disassoc-repo",
	})

	rec := doRequest(t, h, "DisassociateApprovalRuleTemplateFromRepository", map[string]any{
		"approvalRuleTemplateName": "disassoc-tmpl",
		"repositoryName":           "disassoc-repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeMergeConflicts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeMergeConflicts", map[string]any{
		"repositoryName":             "any-repo",
		"sourceCommitSpecifier":      "abc",
		"destinationCommitSpecifier": "def",
		"mergeOption":                "FAST_FORWARD_MERGE",
		"filePath":                   "main.go",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_MergeBranchesBySquash(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "sq-merge-repo"})

	rec := doRequest(t, h, "MergeBranchesBySquash", map[string]any{
		"repositoryName":             "sq-merge-repo",
		"sourceCommitSpecifier":      "feature",
		"destinationCommitSpecifier": "main",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_MergeBranchesByThreeWay(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "3w-merge-repo"})

	rec := doRequest(t, h, "MergeBranchesByThreeWay", map[string]any{
		"repositoryName":             "3w-merge-repo",
		"sourceCommitSpecifier":      "feature",
		"destinationCommitSpecifier": "main",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Additional parity tests from go-7lrz audit ---

func TestHandler_UpdatePullRequestStatus_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		status      string
		wantErrType string
		wantStatus  int
	}{
		{name: "close_pr", status: "CLOSED", wantStatus: http.StatusOK},
		{name: "reopen_pr", status: "OPEN", wantStatus: http.StatusOK},
		{
			name:        "merged_rejected",
			status:      "MERGED",
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidParameterException",
		},
		{
			name:        "empty_rejected",
			status:      "",
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidParameterException",
		},
		{
			name:        "bad_value_rejected",
			status:      "DONE",
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			prID := setupPR(t, h, "repo")

			rec := doRequest(t, h, "UpdatePullRequestStatus", map[string]any{
				"pullRequestId":     prID,
				"pullRequestStatus": tt.status,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrType, errResp["__type"])
			}
		})
	}
}

func TestHandler_UpdateApprovalRuleTemplateContent_Reflected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create template.
	rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl",
		"approvalRuleTemplateContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	newContent := `{"Version":"2018-11-08","Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":2}]}`
	rec = doRequest(t, h, "UpdateApprovalRuleTemplateContent", map[string]any{
		"approvalRuleTemplateName": "tmpl",
		"newRuleContent":           newContent,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tmpl := resp["approvalRuleTemplate"].(map[string]any)
	assert.Equal(t, newContent, tmpl["approvalRuleTemplateContent"])
}

func TestHandler_ListRepositoriesForApprovalRuleTemplate_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl",
		"approvalRuleTemplateContent": `{}`,
	})

	rec := doRequest(t, h, "ListRepositoriesForApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName": "tmpl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	repos := resp["repositoryNames"].([]any)
	assert.Empty(t, repos)
}

func TestHandler_ListAssociatedApprovalRuleTemplatesForRepository_AfterAssoc(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl1",
		"approvalRuleTemplateContent": `{}`,
	})
	doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
		"approvalRuleTemplateName":    "tmpl2",
		"approvalRuleTemplateContent": `{}`,
	})

	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "tmpl1",
		"repositoryName":           "repo",
	})
	doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
		"approvalRuleTemplateName": "tmpl2",
		"repositoryName":           "repo",
	})

	rec := doRequest(t, h, "ListAssociatedApprovalRuleTemplatesForRepository", map[string]any{
		"repositoryName": "repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	names := resp["approvalRuleTemplateNames"].([]any)
	assert.Len(t, names, 2)
}

func TestHandler_DisassociateApprovalRuleTemplateFromRepository_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tmplName   string
		repoName   string
		seedAssoc  bool
		wantStatus int
	}{
		{
			name:       "success",
			tmplName:   "tmpl",
			repoName:   "repo",
			seedAssoc:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "template_not_found",
			tmplName:   "no-tmpl",
			repoName:   "repo",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "repo_not_found",
			tmplName:   "tmpl",
			repoName:   "no-repo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
			doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
				"approvalRuleTemplateName":    "tmpl",
				"approvalRuleTemplateContent": `{}`,
			})

			if tt.seedAssoc {
				doRequest(t, h, "AssociateApprovalRuleTemplateWithRepository", map[string]any{
					"approvalRuleTemplateName": "tmpl",
					"repositoryName":           "repo",
				})
			}

			rec := doRequest(t, h, "DisassociateApprovalRuleTemplateFromRepository", map[string]any{
				"approvalRuleTemplateName": tt.tmplName,
				"repositoryName":           tt.repoName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_EvaluatePullRequestApprovalRules_WithRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "repo")

	// Add two approval rules.
	doRequest(t, h, "CreatePullRequestApprovalRule", map[string]any{
		"pullRequestId":       prID,
		"approvalRuleName":    "rule-a",
		"approvalRuleContent": `{"Version":"2018-11-08"}`,
	})
	doRequest(t, h, "CreatePullRequestApprovalRule", map[string]any{
		"pullRequestId":       prID,
		"approvalRuleName":    "rule-b",
		"approvalRuleContent": `{"Version":"2018-11-08"}`,
	})

	rec := doRequest(t, h, "EvaluatePullRequestApprovalRules", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results := resp["evaluationResults"].([]any)
	assert.Len(t, results, 2)
}

func TestHandler_GetPullRequestOverrideState_AfterOverride(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "repo")

	// Initially not overridden.
	rec := doRequest(t, h, "GetPullRequestOverrideState", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, false, resp["overridden"])

	// Override it.
	doRequest(t, h, "OverridePullRequestApprovalRules", map[string]any{
		"pullRequestId":  prID,
		"revisionId":     "rev-1",
		"overrideStatus": "OVERRIDE",
	})

	rec = doRequest(t, h, "GetPullRequestOverrideState", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["overridden"])
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

func TestHandler_TriggerLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	triggers := []map[string]any{
		{
			"name":           "trigger1",
			"destinationArn": "arn:aws:sns:us-east-1:123456789012:topic1",
			"events":         []string{"all"},
		},
		{
			"name":           "trigger2",
			"destinationArn": "arn:aws:sns:us-east-1:123456789012:topic2",
			"events":         []string{"updateReference"},
		},
	}

	rec := doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "repo",
		"triggers":       triggers,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepositoryTriggers", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got := resp["triggers"].([]any)
	assert.Len(t, got, 2)

	// Test triggers.
	rec = doRequest(t, h, "TestRepositoryTriggers", map[string]any{
		"repositoryName": "repo",
		"triggers":       triggers,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	succeeded := resp["successfulExecutions"].([]any)
	assert.Len(t, succeeded, 2)

	// Replace with empty triggers.
	rec = doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "repo",
		"triggers":       []map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepositoryTriggers", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got = resp["triggers"].([]any)
	assert.Empty(t, got)
}

func TestHandler_FileLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	// Put file.
	rec := doRequest(t, h, "PutFile", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"filePath":       "src/main.go",
		"fileContent":    "cGFja2FnZSBtYWlu", // "package main" base64
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get file.
	rec = doRequest(t, h, "GetFile", map[string]any{
		"repositoryName": "repo",
		"filePath":       "src/main.go",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "src/main.go", resp["filePath"])
	assert.NotEmpty(t, resp["blobId"])

	// Get folder.
	rec = doRequest(t, h, "GetFolder", map[string]any{
		"repositoryName": "repo",
		"folderPath":     "src",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	files := resp["files"].([]any)
	assert.Len(t, files, 1)

	// Get blob.
	blobID := resp["files"].([]any) // We need blobId from GetFile
	_ = blobID

	// Delete file.
	rec = doRequest(t, h, "DeleteFile", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
		"filePath":       "src/main.go",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// File should be gone.
	rec = doRequest(t, h, "GetFile", map[string]any{
		"repositoryName": "repo",
		"filePath":       "src/main.go",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_MergeOptions_AllStrategies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	rec := doRequest(t, h, "GetMergeOptions", map[string]any{
		"repositoryName":             "repo",
		"sourceCommitSpecifier":      "feat",
		"destinationCommitSpecifier": "main",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	opts := resp["mergeOptions"].([]any)
	assert.Len(t, opts, 3)

	optStrs := make([]string, len(opts))
	for i, o := range opts {
		optStrs[i] = o.(string)
	}
	assert.Contains(t, optStrs, "FAST_FORWARD_MERGE")
	assert.Contains(t, optStrs, "SQUASH_MERGE")
	assert.Contains(t, optStrs, "THREE_WAY_MERGE")
}

func TestHandler_BatchGetCommits_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantCode     int
		includeReal  bool
		includeFake  bool
		wantFoundLen int
		wantErrLen   int
	}{
		{
			name:         "real_commit_found",
			wantCode:     http.StatusOK,
			includeReal:  true,
			includeFake:  false,
			wantFoundLen: 1,
			wantErrLen:   0,
		},
		{
			name:         "fake_commit_in_errors",
			wantCode:     http.StatusOK,
			includeReal:  false,
			includeFake:  true,
			wantFoundLen: 0,
			wantErrLen:   1,
		},
		{
			name:         "mixed",
			wantCode:     http.StatusOK,
			includeReal:  true,
			includeFake:  true,
			wantFoundLen: 1,
			wantErrLen:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRepoAndBranch(t, h, "repo")

			// Get a real commit ID.
			branchRec := doRequest(t, h, "GetBranch", map[string]any{
				"repositoryName": "repo",
				"branchName":     "main",
			})
			require.Equal(t, http.StatusOK, branchRec.Code)

			var brResp map[string]any
			require.NoError(t, json.Unmarshal(branchRec.Body.Bytes(), &brResp))
			realCommitID := brResp["branch"].(map[string]any)["commitId"].(string)

			var ids []string
			if tt.includeReal {
				ids = append(ids, realCommitID)
			}
			if tt.includeFake {
				ids = append(ids, "00000000-0000-0000-0000-000000000000")
			}

			rec := doRequest(t, h, "BatchGetCommits", map[string]any{
				"repositoryName": "repo",
				"commitIds":      ids,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			commits := resp["commits"].([]any)
			errors := resp["errors"].([]any)
			assert.Len(t, commits, tt.wantFoundLen)
			assert.Len(t, errors, tt.wantErrLen)
		})
	}
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

func TestHandler_UpdateRepositoryName_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		oldName    string
		newName    string
		seed       bool
		wantStatus int
	}{
		{
			name:       "success",
			oldName:    "old",
			newName:    "new",
			seed:       true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "old_not_found",
			oldName:    "no-such",
			newName:    "new",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "new_already_exists",
			oldName:    "old",
			newName:    "existing",
			seed:       true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_old_name",
			oldName:    "",
			newName:    "new",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "old"})
			}
			if tt.newName == "existing" {
				doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "existing"})
			}

			rec := doRequest(t, h, "UpdateRepositoryName", map[string]any{
				"oldName": tt.oldName,
				"newName": tt.newName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Verify rename is reflected.
			if tt.wantStatus == http.StatusOK {
				rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": tt.newName})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "GetRepository", map[string]any{"repositoryName": tt.oldName})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			}
		})
	}
}

func TestHandler_ApprovalRuleTemplate_CRUD_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tmplName    string
		content     string
		description string
		wantCode    int
	}{
		{
			name:     "minimal_content",
			tmplName: "tmpl-min",
			content:  `{"Version":"2018-11-08","Statements":[]}`,
			wantCode: http.StatusOK,
		},
		{
			name:        "with_description",
			tmplName:    "tmpl-desc",
			content:     `{"Version":"2018-11-08","Statements":[]}`,
			description: "Requires 2 approvers",
			wantCode:    http.StatusOK,
		},
		{
			name:     "missing_name",
			tmplName: "",
			content:  `{}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_content",
			tmplName: "tmpl-no-content",
			content:  "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
				"approvalRuleTemplateName":        tt.tmplName,
				"approvalRuleTemplateContent":     tt.content,
				"approvalRuleTemplateDescription": tt.description,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tmpl := resp["approvalRuleTemplate"].(map[string]any)
				assert.Equal(t, tt.tmplName, tmpl["approvalRuleTemplateName"])
				assert.Equal(t, tt.content, tmpl["approvalRuleTemplateContent"])
				assert.NotEmpty(t, tmpl["approvalRuleTemplateId"])
				assert.NotEmpty(t, tmpl["approvalRuleTemplateArn"])
				assert.NotEmpty(t, tmpl["ruleContentSha256"])
				assert.NotNil(t, tmpl["creationDate"])
				assert.NotNil(t, tmpl["lastModifiedDate"])
				if tt.description != "" {
					assert.Equal(t, tt.description, tmpl["approvalRuleTemplateDescription"])
				}

				// Get it back.
				rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName": tt.tmplName,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Delete it.
				rec = doRequest(t, h, "DeleteApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName": tt.tmplName,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify deleted.
				rec = doRequest(t, h, "GetApprovalRuleTemplate", map[string]any{
					"approvalRuleTemplateName": tt.tmplName,
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			}
		})
	}
}

func TestHandler_BatchAssociateAndDisassociate_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		repoNames    []string
		wantAssocLen int
		wantErrLen   int
	}{
		{
			name:         "all_exist",
			repoNames:    []string{"repo1", "repo2", "repo3"},
			wantAssocLen: 3,
			wantErrLen:   0,
		},
		{
			name:         "some_missing",
			repoNames:    []string{"repo1", "no-such", "repo2"},
			wantAssocLen: 2,
			wantErrLen:   1,
		},
		{
			name:         "all_missing",
			repoNames:    []string{"no1", "no2"},
			wantAssocLen: 0,
			wantErrLen:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create only repo1 and repo2.
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo1"})
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo2"})
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo3"})
			doRequest(t, h, "CreateApprovalRuleTemplate", map[string]any{
				"approvalRuleTemplateName":    "tmpl",
				"approvalRuleTemplateContent": `{}`,
			})

			rec := doRequest(t, h, "BatchAssociateApprovalRuleTemplateWithRepositories", map[string]any{
				"approvalRuleTemplateName": "tmpl",
				"repositoryNames":          tt.repoNames,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assoc := resp["associatedRepositoryNames"].([]any)
			errs := resp["errors"].([]any)
			assert.Len(t, assoc, tt.wantAssocLen)
			assert.Len(t, errs, tt.wantErrLen)

			// Now disassociate.
			if tt.wantAssocLen > 0 {
				rec = doRequest(t, h, "BatchDisassociateApprovalRuleTemplateFromRepositories", map[string]any{
					"approvalRuleTemplateName": "tmpl",
					"repositoryNames":          tt.repoNames,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				disassoc := resp["disassociatedRepositoryNames"].([]any)
				assert.Len(t, disassoc, tt.wantAssocLen)
			}
		})
	}
}

func TestHandler_PullRequestApprovalRule_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "repo")

	// Create approval rule.
	rec := doRequest(t, h, "CreatePullRequestApprovalRule", map[string]any{
		"pullRequestId":       prID,
		"approvalRuleName":    "my-rule",
		"approvalRuleContent": `{"Version":"2018-11-08","Statements":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule := resp["approvalRule"].(map[string]any)
	assert.Equal(t, "my-rule", rule["approvalRuleName"])
	assert.NotEmpty(t, rule["approvalRuleId"])

	// Update content.
	rec = doRequest(t, h, "UpdatePullRequestApprovalRuleContent", map[string]any{
		"pullRequestId":    prID,
		"approvalRuleName": "my-rule",
		"newRuleContent":   `{"Version":"2018-11-08","Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":1}]}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Evaluate.
	rec = doRequest(t, h, "EvaluatePullRequestApprovalRules", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	evals := resp["evaluationResults"].([]any)
	assert.Len(t, evals, 1)

	// Delete rule.
	rec = doRequest(t, h, "DeletePullRequestApprovalRule", map[string]any{
		"pullRequestId":    prID,
		"approvalRuleName": "my-rule",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Evaluate — should be empty now.
	rec = doRequest(t, h, "EvaluatePullRequestApprovalRules", map[string]any{
		"pullRequestId": prID,
		"revisionId":    "rev-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	evals = resp["evaluationResults"].([]any)
	assert.Empty(t, evals)
}

func TestHandler_UpdatePullRequestTitle_Reflected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "repo")

	rec := doRequest(t, h, "UpdatePullRequestTitle", map[string]any{
		"pullRequestId": prID,
		"title":         "Updated Title",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)
	assert.Equal(t, "Updated Title", pr["title"])
	assert.Equal(t, prID, pr["pullRequestId"])
}

func TestHandler_UpdatePullRequestDescription_Reflected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	prID := setupPR(t, h, "repo")

	rec := doRequest(t, h, "UpdatePullRequestDescription", map[string]any{
		"pullRequestId": prID,
		"description":   "A much better description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pr := resp["pullRequest"].(map[string]any)
	assert.Equal(t, "A much better description", pr["description"])
}

func TestHandler_GetPullRequestApprovalStates_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		approveUsers int
		wantCode     int
	}{
		{name: "no_approvals", approveUsers: 0, wantCode: http.StatusOK},
		{name: "one_approval", approveUsers: 1, wantCode: http.StatusOK},
		{name: "three_approvals", approveUsers: 3, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			prID := setupPR(t, h, "repo")

			for range tt.approveUsers {
				doRequest(t, h, "UpdatePullRequestApprovalState", map[string]any{
					"pullRequestId": prID,
					"revisionId":    "rev-1",
					"approvalState": "APPROVE",
				})
			}

			rec := doRequest(t, h, "GetPullRequestApprovalStates", map[string]any{
				"pullRequestId": prID,
				"revisionId":    "rev-1",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			approvals := resp["approvals"].([]any)
			// Each UpdatePullRequestApprovalState with the same (empty) userARN just overwrites, so max 1.
			if tt.approveUsers > 0 {
				assert.NotEmpty(t, approvals)
			} else {
				assert.Empty(t, approvals)
			}
		})
	}
}

func TestHandler_DescribePullRequestEvents_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		doOverride bool
		wantEvents int
		wantCode   int
	}{
		{
			name:       "no_events",
			wantEvents: 0,
			wantCode:   http.StatusOK,
		},
		{
			name:       "with_override_event",
			doOverride: true,
			wantEvents: 1,
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			prID := setupPR(t, h, "repo")

			if tt.doOverride {
				doRequest(t, h, "OverridePullRequestApprovalRules", map[string]any{
					"pullRequestId":  prID,
					"revisionId":     "rev-1",
					"overrideStatus": "OVERRIDE",
				})
			}

			rec := doRequest(t, h, "DescribePullRequestEvents", map[string]any{
				"pullRequestId": prID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events := resp["pullRequestEvents"].([]any)
			assert.Len(t, events, tt.wantEvents)
		})
	}
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

func TestHandler_MergeBranches_AllStrategies(t *testing.T) {
	t.Parallel()

	strategies := []string{
		"MergeBranchesByFastForward",
		"MergeBranchesBySquash",
		"MergeBranchesByThreeWay",
	}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			rec := doRequest(t, h, strategy, map[string]any{
				"repositoryName":             "repo",
				"sourceCommitSpecifier":      "feature",
				"destinationCommitSpecifier": "main",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["commitId"])
			assert.NotEmpty(t, resp["treeId"])
		})
	}
}

func TestHandler_GetMergeCommit_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Repo with no commits.
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "empty-repo"})

	rec := doRequest(t, h, "GetMergeCommit", map[string]any{
		"repositoryName":             "empty-repo",
		"sourceCommitSpecifier":      "feat",
		"destinationCommitSpecifier": "main",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_CreateUnreferencedMergeCommit_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupRepoAndBranch(t, h, "repo")

	branchRec := doRequest(t, h, "GetBranch", map[string]any{
		"repositoryName": "repo",
		"branchName":     "main",
	})
	require.Equal(t, http.StatusOK, branchRec.Code)

	var brResp map[string]any
	require.NoError(t, json.Unmarshal(branchRec.Body.Bytes(), &brResp))
	commitID := brResp["branch"].(map[string]any)["commitId"].(string)

	rec := doRequest(t, h, "CreateUnreferencedMergeCommit", map[string]any{
		"repositoryName":             "repo",
		"sourceCommitSpecifier":      commitID,
		"destinationCommitSpecifier": commitID,
		"mergeOption":                "FAST_FORWARD_MERGE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["commitId"])
	assert.NotEmpty(t, resp["treeId"])
}

func TestHandler_ListFileCommitHistory_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		seedCommits int
		wantCode    int
	}{
		{name: "no_commits", seedCommits: 0, wantCode: http.StatusOK},
		{name: "one_commit", seedCommits: 1, wantCode: http.StatusOK},
		{name: "three_commits", seedCommits: 3, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			for i := range tt.seedCommits {
				doRequest(t, h, "CreateCommit", map[string]any{
					"repositoryName": "repo",
					"branchName":     "main",
					"authorName":     "author",
					"email":          "a@b.com",
					"commitMessage":  fmt.Sprintf("commit %d", i),
				})
			}

			rec := doRequest(t, h, "ListFileCommitHistory", map[string]any{
				"repositoryName": "repo",
				"filePath":       "main.go",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			dag := resp["revisionDag"].([]any)
			assert.Len(t, dag, tt.seedCommits)
		})
	}
}

func TestHandler_DeleteBranch_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		branchName string
		seed       bool
		wantCode   int
	}{
		{name: "success", branchName: "main", seed: true, wantCode: http.StatusOK},
		{name: "not_found", branchName: "no-branch", seed: true, wantCode: http.StatusNotFound},
		{name: "repo_not_found", branchName: "main", seed: false, wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.seed {
				setupRepoAndBranch(t, h, "repo")
			}

			repoName := "repo"
			if !tt.seed {
				repoName = "no-repo"
			}

			rec := doRequest(t, h, "DeleteBranch", map[string]any{
				"repositoryName": repoName,
				"branchName":     tt.branchName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetDifferences_RepoNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetDifferences", map[string]any{
		"repositoryName":       "no-repo",
		"afterCommitSpecifier": "abc",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetMergeConflicts_NoConflicts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	rec := doRequest(t, h, "GetMergeConflicts", map[string]any{
		"repositoryName":             "repo",
		"sourceCommitSpecifier":      "feat",
		"destinationCommitSpecifier": "main",
		"mergeOption":                "FAST_FORWARD_MERGE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// Always no conflicts in emulation.
	assert.Equal(t, false, resp["mergeable"])
}

func TestHandler_BatchDescribeMergeConflicts_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mergeOption   string
		filePaths     []string
		wantCode      int
		wantConflicts int
	}{
		{
			name:          "no_file_paths",
			mergeOption:   "FAST_FORWARD_MERGE",
			filePaths:     nil,
			wantCode:      http.StatusOK,
			wantConflicts: 0,
		},
		{
			name:          "two_file_paths",
			mergeOption:   "SQUASH_MERGE",
			filePaths:     []string{"a.go", "b.go"},
			wantCode:      http.StatusOK,
			wantConflicts: 2,
		},
		{
			name:        "invalid_merge_option",
			mergeOption: "INVALID_MERGE",
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

			body := map[string]any{
				"repositoryName":             "repo",
				"destinationCommitSpecifier": "main",
				"sourceCommitSpecifier":      "feat",
				"mergeOption":                tt.mergeOption,
			}
			if tt.filePaths != nil {
				body["filePaths"] = tt.filePaths
			}

			rec := doRequest(t, h, "BatchDescribeMergeConflicts", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				conflicts := resp["conflicts"].([]any)
				assert.Len(t, conflicts, tt.wantConflicts)
			}
		})
	}
}
