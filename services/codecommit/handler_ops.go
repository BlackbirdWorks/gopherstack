package codecommit

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// --- Group 1: Repository CRUD edges ---

func (h *Handler) handleUpdateRepositoryDescription(body []byte) (any, error) {
	var req struct {
		RepositoryName        string `json:"repositoryName"`
		RepositoryDescription string `json:"repositoryDescription"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdateRepositoryDescription(req.RepositoryName, req.RepositoryDescription)
}

func (h *Handler) handleUpdateRepositoryName(body []byte) (any, error) {
	var req struct {
		OldName string `json:"oldName"`
		NewName string `json:"newName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.OldName == "" || req.NewName == "" {
		return nil, fmt.Errorf("%w: oldName and newName are required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdateRepositoryName(req.OldName, req.NewName)
}

func (h *Handler) handleUpdateRepositoryEncryptionKey(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		KmsKeyID       string `json:"kmsKeyId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdateRepositoryEncryptionKey(req.RepositoryName, req.KmsKeyID)
}

func (h *Handler) handleUpdateDefaultBranch(body []byte) (any, error) {
	var req struct {
		RepositoryName    string `json:"repositoryName"`
		DefaultBranchName string `json:"defaultBranchName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdateDefaultBranch(req.RepositoryName, req.DefaultBranchName)
}

// --- Group 2: Approval Rule Template CRUD ---

func (h *Handler) handleDeleteApprovalRuleTemplate(body []byte) (any, error) {
	var req struct {
		ApprovalRuleTemplateName string `json:"approvalRuleTemplateName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.DeleteApprovalRuleTemplate(req.ApprovalRuleTemplateName)
}

func (h *Handler) handleGetApprovalRuleTemplate(body []byte) (any, error) {
	var req struct {
		ApprovalRuleTemplateName string `json:"approvalRuleTemplateName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	t, err := h.Backend.GetApprovalRuleTemplate(req.ApprovalRuleTemplateName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyApprovalRuleTmpl: approvalRuleTemplateToMap(t),
	}, nil
}

func (h *Handler) handleListApprovalRuleTemplates(_ []byte) (any, error) {
	templates := h.Backend.ListApprovalRuleTemplates()
	names := make([]string, 0, len(templates))
	for _, t := range templates {
		names = append(names, t.ApprovalRuleTemplateName)
	}

	return map[string]any{
		"approvalRuleTemplateNames": names,
	}, nil
}

func (h *Handler) handleUpdateApprovalRuleTemplateContent(body []byte) (any, error) {
	var req struct {
		ApprovalRuleTemplateName string `json:"approvalRuleTemplateName"`
		NewRuleContent           string `json:"newRuleContent"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateApprovalRuleTemplateContent(
		req.ApprovalRuleTemplateName,
		req.NewRuleContent,
	); err != nil {
		return nil, err
	}
	t, err := h.Backend.GetApprovalRuleTemplate(req.ApprovalRuleTemplateName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyApprovalRuleTmpl: approvalRuleTemplateToMap(t),
	}, nil
}

func (h *Handler) handleUpdateApprovalRuleTemplateDescription(body []byte) (any, error) {
	var req struct {
		ApprovalRuleTemplateName        string `json:"approvalRuleTemplateName"`
		ApprovalRuleTemplateDescription string `json:"approvalRuleTemplateDescription"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	err := h.Backend.UpdateApprovalRuleTemplateDescription(
		req.ApprovalRuleTemplateName, req.ApprovalRuleTemplateDescription,
	)
	if err != nil {
		return nil, err
	}
	t, err := h.Backend.GetApprovalRuleTemplate(req.ApprovalRuleTemplateName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyApprovalRuleTmpl: approvalRuleTemplateToMap(t),
	}, nil
}

func (h *Handler) handleUpdateApprovalRuleTemplateName(body []byte) (any, error) {
	var req struct {
		OldApprovalRuleTemplateName string `json:"oldApprovalRuleTemplateName"`
		NewApprovalRuleTemplateName string `json:"newApprovalRuleTemplateName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.OldApprovalRuleTemplateName == "" || req.NewApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf(
			"%w: oldApprovalRuleTemplateName and newApprovalRuleTemplateName are required",
			errInvalidRequest,
		)
	}

	if err := h.Backend.UpdateApprovalRuleTemplateName(
		req.OldApprovalRuleTemplateName, req.NewApprovalRuleTemplateName,
	); err != nil {
		return nil, err
	}
	t, err := h.Backend.GetApprovalRuleTemplate(req.NewApprovalRuleTemplateName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyApprovalRuleTmpl: approvalRuleTemplateToMap(t),
	}, nil
}

// --- Group 3: Template-Repository association edges ---

func (h *Handler) handleListAssociatedApprovalRuleTemplatesForRepository(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	names := h.Backend.ListAssociatedApprovalRuleTemplatesForRepository(req.RepositoryName)

	return map[string]any{
		"approvalRuleTemplateNames": names,
	}, nil
}

func (h *Handler) handleListRepositoriesForApprovalRuleTemplate(body []byte) (any, error) {
	var req struct {
		ApprovalRuleTemplateName string `json:"approvalRuleTemplateName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	repos := h.Backend.ListRepositoriesForApprovalRuleTemplate(req.ApprovalRuleTemplateName)

	return map[string]any{
		"repositoryNames": repos,
	}, nil
}

// --- Group 4: PullRequest operations ---

func (h *Handler) handleGetPullRequestApprovalStates(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
		RevisionID    string `json:"revisionId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	approvals, err := h.Backend.GetPullRequestApprovalStates(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"approvals": approvals,
	}, nil
}

func (h *Handler) handleGetPullRequestOverrideState(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
		RevisionID    string `json:"revisionId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	overridden, overrider, err := h.Backend.GetPullRequestOverrideState(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"overridden": overridden,
		"overrider":  overrider,
	}, nil
}

func (h *Handler) handleOverridePullRequestApprovalRules(body []byte) (any, error) {
	var req struct {
		PullRequestID  string `json:"pullRequestId"`
		RevisionID     string `json:"revisionId"`
		OverrideStatus string `json:"overrideStatus"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.OverridePullRequestApprovalRules(req.PullRequestID, req.OverrideStatus, "")
}

func (h *Handler) handleUpdatePullRequestApprovalState(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
		RevisionID    string `json:"revisionId"`
		ApprovalState string `json:"approvalState"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdatePullRequestApprovalState(req.PullRequestID, "", req.ApprovalState)
}

func (h *Handler) handleUpdatePullRequestDescription(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
		Description   string `json:"description"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	if err := h.Backend.UpdatePullRequestDescription(req.PullRequestID, req.Description); err != nil {
		return nil, err
	}

	pr, err := h.Backend.GetPullRequest(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleUpdatePullRequestStatus(body []byte) (any, error) {
	var req struct {
		PullRequestID     string `json:"pullRequestId"`
		PullRequestStatus string `json:"pullRequestStatus"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	if err := h.Backend.UpdatePullRequestStatus(req.PullRequestID, req.PullRequestStatus); err != nil {
		return nil, err
	}

	pr, err := h.Backend.GetPullRequest(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleUpdatePullRequestTitle(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
		Title         string `json:"title"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	if err := h.Backend.UpdatePullRequestTitle(req.PullRequestID, req.Title); err != nil {
		return nil, err
	}

	pr, err := h.Backend.GetPullRequest(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleCreatePullRequestApprovalRule(body []byte) (any, error) {
	var req struct {
		PullRequestID       string `json:"pullRequestId"`
		ApprovalRuleName    string `json:"approvalRuleName"`
		ApprovalRuleContent string `json:"approvalRuleContent"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" || req.ApprovalRuleName == "" {
		return nil, fmt.Errorf("%w: pullRequestId and approvalRuleName are required", errInvalidRequest)
	}

	rule, err := h.Backend.CreatePullRequestApprovalRule(
		req.PullRequestID,
		req.ApprovalRuleName,
		req.ApprovalRuleContent,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"approvalRule": map[string]any{
			"approvalRuleId":      rule.RuleID,
			"approvalRuleName":    rule.RuleName,
			"approvalRuleContent": rule.ApprovalRuleContent,
		},
	}, nil
}

func (h *Handler) handleDeletePullRequestApprovalRule(body []byte) (any, error) {
	var req struct {
		PullRequestID    string `json:"pullRequestId"`
		ApprovalRuleName string `json:"approvalRuleName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" || req.ApprovalRuleName == "" {
		return nil, fmt.Errorf("%w: pullRequestId and approvalRuleName are required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.DeletePullRequestApprovalRule(req.PullRequestID, req.ApprovalRuleName)
}

func (h *Handler) handleUpdatePullRequestApprovalRuleContent(body []byte) (any, error) {
	var req struct {
		PullRequestID    string `json:"pullRequestId"`
		ApprovalRuleName string `json:"approvalRuleName"`
		NewRuleContent   string `json:"newRuleContent"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" || req.ApprovalRuleName == "" {
		return nil, fmt.Errorf("%w: pullRequestId and approvalRuleName are required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.UpdatePullRequestApprovalRuleContent(
		req.PullRequestID, req.ApprovalRuleName, req.NewRuleContent,
	)
}

func (h *Handler) handleDescribePullRequestEvents(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	events, err := h.Backend.DescribePullRequestEvents(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"pullRequestEvents": events,
	}, nil
}

func (h *Handler) handleEvaluatePullRequestApprovalRules(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
		RevisionID    string `json:"revisionId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	evals, err := h.Backend.EvaluatePullRequestApprovalRules(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"evaluationResults": evals,
	}, nil
}

// --- Group 5: Comments ---

func commentToMap(c *Comment) map[string]any {
	return map[string]any{
		"commentId":         c.CommentID,
		"content":           c.Content,
		"authorArn":         c.AuthorARN,
		"creationDate":      c.CreationDate,
		keyLastModifiedDate: c.LastModifiedDate,
		"inReplyTo":         c.InReplyTo,
		"deleted":           c.Deleted,
	}
}

func (h *Handler) handlePostCommentForComparedCommit(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BeforeCommitID string `json:"beforeCommitId"`
		AfterCommitID  string `json:"afterCommitId"`
		Content        string `json:"content"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: repositoryName and content are required", errInvalidRequest)
	}

	c, err := h.Backend.PostCommentForComparedCommit(
		req.RepositoryName, req.BeforeCommitID, req.AfterCommitID, req.Content,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyComment: commentToMap(c),
	}, nil
}

func (h *Handler) handlePostCommentForPullRequest(body []byte) (any, error) {
	var req struct {
		PullRequestID  string `json:"pullRequestId"`
		RepositoryName string `json:"repositoryName"`
		BeforeCommitID string `json:"beforeCommitId"`
		AfterCommitID  string `json:"afterCommitId"`
		Content        string `json:"content"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: pullRequestId and content are required", errInvalidRequest)
	}

	c, err := h.Backend.PostCommentForPullRequest(req.PullRequestID, req.RepositoryName, req.Content)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyComment: commentToMap(c),
	}, nil
}

func (h *Handler) handlePostCommentReply(body []byte) (any, error) {
	var req struct {
		InReplyTo string `json:"inReplyTo"`
		Content   string `json:"content"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.InReplyTo == "" || req.Content == "" {
		return nil, fmt.Errorf("%w: inReplyTo and content are required", errInvalidRequest)
	}

	c, err := h.Backend.PostCommentReply(req.InReplyTo, req.Content)
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

func (h *Handler) handleGetCommentsForComparedCommit(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		AfterCommitID  string `json:"afterCommitId"`
		BeforeCommitID string `json:"beforeCommitId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	comments, err := h.Backend.GetCommentsForComparedCommit(req.RepositoryName, req.AfterCommitID)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(comments))
	for _, c := range comments {
		items = append(items, commentToMap(c))
	}

	return map[string]any{
		"commentsForComparedCommitData": items,
	}, nil
}

func (h *Handler) handleGetCommentsForPullRequest(body []byte) (any, error) {
	var req struct {
		PullRequestID string `json:"pullRequestId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	comments, err := h.Backend.GetCommentsForPullRequest(req.PullRequestID)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(comments))
	for _, c := range comments {
		items = append(items, commentToMap(c))
	}

	return map[string]any{
		"commentsForPullRequestData": items,
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

// --- Group 6: File/Blob ops ---

func (h *Handler) handlePutFile(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
		FilePath       string `json:"filePath"`
		FileContent    string `json:"fileContent"` // base64 encoded
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.FilePath == "" {
		return nil, fmt.Errorf("%w: repositoryName and filePath are required", errInvalidRequest)
	}

	content, err := base64.StdEncoding.DecodeString(req.FileContent)
	if err != nil {
		// treat as raw bytes if not base64
		content = []byte(req.FileContent)
	}

	commit, err := h.Backend.PutFile(req.RepositoryName, req.BranchName, req.FilePath, content)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
		keyBlobID:   "",
		"filesAdded": []any{
			map[string]any{keyFilePath: req.FilePath},
		},
	}, nil
}

func (h *Handler) handleGetFile(body []byte) (any, error) {
	var req struct {
		RepositoryName  string `json:"repositoryName"`
		CommitSpecifier string `json:"commitSpecifier"`
		FilePath        string `json:"filePath"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.FilePath == "" {
		return nil, fmt.Errorf("%w: repositoryName and filePath are required", errInvalidRequest)
	}

	f, err := h.Backend.GetFile(req.RepositoryName, req.CommitSpecifier, req.FilePath)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyBlobID:     f.BlobID,
		"commitId":    f.CommitSpecifier,
		keyFilePath:   f.FilePath,
		"fileMode":    f.FileMode,
		"fileContent": base64.StdEncoding.EncodeToString(f.FileContent),
		"fileSize":    len(f.FileContent),
	}, nil
}

func (h *Handler) handleGetFolder(body []byte) (any, error) {
	var req struct {
		RepositoryName  string `json:"repositoryName"`
		CommitSpecifier string `json:"commitSpecifier"`
		FolderPath      string `json:"folderPath"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	paths, err := h.Backend.GetFolder(req.RepositoryName, req.CommitSpecifier, req.FolderPath)
	if err != nil {
		return nil, err
	}

	files := make([]map[string]any, 0, len(paths))
	for _, p := range paths {
		files = append(files, map[string]any{"absolutePath": p})
	}

	return map[string]any{
		"commitId":   req.CommitSpecifier,
		"folderPath": req.FolderPath,
		"files":      files,
	}, nil
}

func (h *Handler) handleDeleteFile(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
		FilePath       string `json:"filePath"`
		ParentCommitID string `json:"parentCommitId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.FilePath == "" {
		return nil, fmt.Errorf("%w: repositoryName and filePath are required", errInvalidRequest)
	}

	commit, err := h.Backend.DeleteFile(req.RepositoryName, req.BranchName, req.FilePath, req.ParentCommitID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
		keyBlobID:   "",
		keyFilePath: req.FilePath,
	}, nil
}

// --- Group 7: Triggers ---

func (h *Handler) handleGetRepositoryTriggers(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	triggers, err := h.Backend.GetRepositoryTriggers(req.RepositoryName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"triggers":        triggers,
		"configurationId": "",
	}, nil
}

func (h *Handler) handlePutRepositoryTriggers(body []byte) (any, error) {
	var req struct {
		RepositoryName string              `json:"repositoryName"`
		Triggers       []RepositoryTrigger `json:"triggers"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if err := h.Backend.PutRepositoryTriggers(req.RepositoryName, req.Triggers); err != nil {
		return nil, err
	}

	return map[string]any{
		"configurationId": "",
	}, nil
}

func (h *Handler) handleTestRepositoryTriggers(body []byte) (any, error) {
	var req struct {
		RepositoryName string              `json:"repositoryName"`
		Triggers       []RepositoryTrigger `json:"triggers"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	names, err := h.Backend.TestRepositoryTriggers(req.RepositoryName)
	if err != nil {
		return nil, err
	}

	succeeded := make([]map[string]any, 0, len(names))
	for _, n := range names {
		succeeded = append(succeeded, map[string]any{"triggerName": n})
	}

	return map[string]any{
		"successfulExecutions": succeeded,
		"failedExecutions":     []any{},
	}, nil
}

// --- Group 8: Merge ops ---

func (h *Handler) handleMergePullRequestByFastForward(body []byte) (any, error) {
	var req struct {
		PullRequestID  string `json:"pullRequestId"`
		RepositoryName string `json:"repositoryName"`
		SourceCommitID string `json:"sourceCommitId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	pr, err := h.Backend.MergePullRequestByFastForward(req.PullRequestID, req.RepositoryName, req.SourceCommitID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleMergePullRequestBySquash(body []byte) (any, error) {
	var req struct {
		PullRequestID  string `json:"pullRequestId"`
		RepositoryName string `json:"repositoryName"`
		SourceCommitID string `json:"sourceCommitId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	pr, err := h.Backend.MergePullRequestBySquash(req.PullRequestID, req.RepositoryName, req.SourceCommitID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleMergePullRequestByThreeWay(body []byte) (any, error) {
	var req struct {
		PullRequestID  string `json:"pullRequestId"`
		RepositoryName string `json:"repositoryName"`
		SourceCommitID string `json:"sourceCommitId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	pr, err := h.Backend.MergePullRequestByThreeWay(req.PullRequestID, req.RepositoryName, req.SourceCommitID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleMergeBranchesByFastForward(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	commit, err := h.Backend.MergeBranchesByFastForward(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
	}, nil
}

func (h *Handler) handleGetMergeOptions(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	options, err := h.Backend.GetMergeOptions(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"mergeOptions":    options,
		keySourceCommitID: req.SourceCommitSpecifier,
		keyDestCommitID:   req.DestinationCommitSpecifier,
	}, nil
}

// --- Group 9: Misc ---

func (h *Handler) handleGetBlob(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		BlobID         string `json:"blobId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" || req.BlobID == "" {
		return nil, fmt.Errorf("%w: repositoryName and blobId are required", errInvalidRequest)
	}

	content, err := h.Backend.GetBlob(req.RepositoryName, req.BlobID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"content": base64.StdEncoding.EncodeToString(content),
	}, nil
}

func (h *Handler) handleListFileCommitHistory(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
		FilePath       string `json:"filePath"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	commits, err := h.Backend.ListFileCommitHistory(req.RepositoryName, req.FilePath)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(commits))
	for _, c := range commits {
		items = append(items, commitToMap(c))
	}

	return map[string]any{
		"revisionDag": items,
	}, nil
}

func (h *Handler) handleCreateUnreferencedMergeCommit(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		MergeOption                string `json:"mergeOption"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	commit, err := h.Backend.CreateUnreferencedMergeCommit(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
	}, nil
}

func (h *Handler) handleGetMergeCommit(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		MergeOption                string `json:"mergeOption"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	commit, err := h.Backend.GetMergeCommit(
		req.RepositoryName,
		req.SourceCommitSpecifier,
		req.DestinationCommitSpecifier,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keySourceCommitID: req.SourceCommitSpecifier,
		keyDestCommitID:   req.DestinationCommitSpecifier,
		"mergedCommitId":  commit.CommitID,
	}, nil
}

func (h *Handler) handleGetMergeConflicts(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		MergeOption                string `json:"mergeOption"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	mergeable, err := h.Backend.GetMergeConflicts(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier, req.MergeOption,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"mergeable":       mergeable,
		keySourceCommitID: req.SourceCommitSpecifier,
		keyDestCommitID:   req.DestinationCommitSpecifier,
		"conflicts":       []any{},
	}, nil
}

func (h *Handler) handleGetDifferences(body []byte) (any, error) {
	var req struct {
		RepositoryName        string `json:"repositoryName"`
		AfterCommitSpecifier  string `json:"afterCommitSpecifier"`
		BeforeCommitSpecifier string `json:"beforeCommitSpecifier"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	diffs, err := h.Backend.GetDifferences(req.RepositoryName, req.AfterCommitSpecifier)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"differences": diffs,
	}, nil
}

// handleDisassociateApprovalRuleTemplateFromRepository delegates to the backend.
func (h *Handler) handleDisassociateApprovalRuleTemplateFromRepository(body []byte) (any, error) {
	var req struct {
		ApprovalRuleTemplateName string `json:"approvalRuleTemplateName"`
		RepositoryName           string `json:"repositoryName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.ApprovalRuleTemplateName == "" || req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName and repositoryName are required", errInvalidRequest)
	}

	return map[string]any{}, h.Backend.DisassociateApprovalRuleTemplateFromRepository(
		req.ApprovalRuleTemplateName, req.RepositoryName,
	)
}

// handleDescribeMergeConflicts is a stub that delegates to BatchDescribeMergeConflicts for a single file.
func (h *Handler) handleDescribeMergeConflicts(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		MergeOption                string `json:"mergeOption"`
		FilePath                   string `json:"filePath"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	return map[string]any{
		keyDestCommitID:   req.DestinationCommitSpecifier,
		keySourceCommitID: req.SourceCommitSpecifier,
		"mergeHunks":      []any{},
		"conflictMetadata": map[string]any{
			keyFilePath:         req.FilePath,
			"numberOfConflicts": 0,
			"contentConflict":   false,
		},
	}, nil
}

// MergeBranchesBySquash and MergeBranchesByThreeWay stub handlers.
func (h *Handler) handleMergeBranchesBySquash(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	commit, err := h.Backend.MergeBranchesByFastForward(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
	}, nil
}

func (h *Handler) handleMergeBranchesByThreeWay(body []byte) (any, error) {
	var req struct {
		RepositoryName             string `json:"repositoryName"`
		SourceCommitSpecifier      string `json:"sourceCommitSpecifier"`
		DestinationCommitSpecifier string `json:"destinationCommitSpecifier"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	commit, err := h.Backend.MergeBranchesByFastForward(
		req.RepositoryName, req.SourceCommitSpecifier, req.DestinationCommitSpecifier,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID: commit.CommitID,
		keyTreeID:   commit.TreeID,
	}, nil
}
