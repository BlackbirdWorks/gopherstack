package codecommit

import (
	"encoding/json"
	"fmt"
)

type createApprovalRuleTemplateInput struct {
	ApprovalRuleTemplateName        string `json:"approvalRuleTemplateName"`
	ApprovalRuleTemplateContent     string `json:"approvalRuleTemplateContent"`
	ApprovalRuleTemplateDescription string `json:"approvalRuleTemplateDescription"`
}

type associateApprovalRuleTemplateWithRepositoryInput struct {
	ApprovalRuleTemplateName string `json:"approvalRuleTemplateName"`
	RepositoryName           string `json:"repositoryName"`
}

type batchAssociateApprovalRuleTemplateInput struct {
	ApprovalRuleTemplateName string   `json:"approvalRuleTemplateName"`
	RepositoryNames          []string `json:"repositoryNames"`
}

type batchDisassociateApprovalRuleTemplateInput struct {
	ApprovalRuleTemplateName string   `json:"approvalRuleTemplateName"`
	RepositoryNames          []string `json:"repositoryNames"`
}

func approvalRuleTemplateToMap(t *ApprovalRuleTemplate) map[string]any {
	m := map[string]any{
		"approvalRuleTemplateId":          t.ApprovalRuleTemplateID,
		"approvalRuleTemplateName":        t.ApprovalRuleTemplateName,
		"approvalRuleTemplateArn":         t.ApprovalRuleTemplateARN,
		"approvalRuleTemplateContent":     t.ApprovalRuleTemplateContent,
		"approvalRuleTemplateDescription": t.ApprovalRuleTemplateDescription,
		keyCreationDate:                   t.CreationDate.Unix(),
		keyLastModifiedDate:               t.LastModifiedDate.Unix(),
		"ruleContentSha256":               t.RuleContentSha256,
	}
	if t.LastModifiedUser != "" {
		m["lastModifiedUser"] = t.LastModifiedUser
	}

	return m
}

func (h *Handler) handleCreateApprovalRuleTemplate(body []byte) (any, error) {
	var in createApprovalRuleTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	if in.ApprovalRuleTemplateContent == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateContent is required", errInvalidRequest)
	}

	t, err := h.Backend.CreateApprovalRuleTemplate(
		in.ApprovalRuleTemplateName,
		in.ApprovalRuleTemplateDescription,
		in.ApprovalRuleTemplateContent,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyApprovalRuleTmpl: approvalRuleTemplateToMap(t),
	}, nil
}

func (h *Handler) handleAssociateApprovalRuleTemplateWithRepository(body []byte) (any, error) {
	var in associateApprovalRuleTemplateWithRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateApprovalRuleTemplateWithRepository(
		in.ApprovalRuleTemplateName,
		in.RepositoryName,
	); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleBatchAssociateApprovalRuleTemplateWithRepositories(body []byte) (any, error) {
	var in batchAssociateApprovalRuleTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	associated, batchErrors := h.Backend.BatchAssociateApprovalRuleTemplateWithRepositories(
		in.ApprovalRuleTemplateName,
		in.RepositoryNames,
	)

	if associated == nil {
		associated = []string{}
	}

	if batchErrors == nil {
		batchErrors = []BatchAssociationError{}
	}

	return map[string]any{
		"associatedRepositoryNames": associated,
		keyErrors:                   batchErrors,
	}, nil
}

func (h *Handler) handleBatchDisassociateApprovalRuleTemplateFromRepositories(body []byte) (any, error) {
	var in batchDisassociateApprovalRuleTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	disassociated, batchErrors := h.Backend.BatchDisassociateApprovalRuleTemplateFromRepositories(
		in.ApprovalRuleTemplateName,
		in.RepositoryNames,
	)

	if disassociated == nil {
		disassociated = []string{}
	}

	if batchErrors == nil {
		batchErrors = []BatchAssociationError{}
	}

	return map[string]any{
		"disassociatedRepositoryNames": disassociated,
		keyErrors:                      batchErrors,
	}, nil
}

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

	templateID, err := h.Backend.DeleteApprovalRuleTemplate(req.ApprovalRuleTemplateName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"approvalRuleTemplateId": templateID,
	}, nil
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

	names, err := h.Backend.ListAssociatedApprovalRuleTemplatesForRepository(req.RepositoryName)
	if err != nil {
		return nil, err
	}

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

	repos, err := h.Backend.ListRepositoriesForApprovalRuleTemplate(req.ApprovalRuleTemplateName)
	if err != nil {
		return nil, err
	}
	if repos == nil {
		repos = []string{}
	}

	return map[string]any{
		"repositoryNames": repos,
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
