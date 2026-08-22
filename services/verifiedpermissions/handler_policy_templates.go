package verifiedpermissions

import (
	"context"
	"fmt"
)

type createPolicyTemplateInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	Description   string `json:"description"`
	Statement     string `json:"statement"`
	ClientToken   string `json:"clientToken,omitempty"`
}

type policyTemplateIDsOutput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
	CreatedDate      string `json:"createdDate"`
	LastUpdatedDate  string `json:"lastUpdatedDate"`
}

func (h *Handler) handleCreatePolicyTemplate(
	_ context.Context,
	in *createPolicyTemplateInput,
) (*policyTemplateIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.Statement == "" {
		return nil, fmt.Errorf("%w: statement is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	pt, err := h.Backend.CreatePolicyTemplate(resolvedID, in.Description, in.Statement, in.ClientToken)
	if err != nil {
		return nil, err
	}

	return &policyTemplateIDsOutput{
		PolicyStoreID:    pt.PolicyStoreID,
		PolicyTemplateID: pt.PolicyTemplateID,
		CreatedDate:      pt.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

type policyTemplateInput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
}

// policyTemplateView mirrors the real SDK's PolicyTemplateItem
// (ListPolicyTemplates): unlike GetPolicyTemplateOutput, it has no statement
// field -- the real type carries only createdDate/lastUpdatedDate/
// policyStoreId/policyTemplateId/description/name.
type policyTemplateView struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
	Description      string `json:"description"`
	CreatedDate      string `json:"createdDate"`
	LastUpdatedDate  string `json:"lastUpdatedDate"`
}

type getPolicyTemplateOutput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
	Description      string `json:"description"`
	Statement        string `json:"statement"`
	CreatedDate      string `json:"createdDate"`
	LastUpdatedDate  string `json:"lastUpdatedDate"`
}

func (h *Handler) handleGetPolicyTemplate(
	_ context.Context,
	in *policyTemplateInput,
) (*getPolicyTemplateOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyTemplateID == "" {
		return nil, fmt.Errorf("%w: policyTemplateId is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	pt, err := h.Backend.GetPolicyTemplate(resolvedID, in.PolicyTemplateID)
	if err != nil {
		return nil, err
	}

	return &getPolicyTemplateOutput{
		PolicyStoreID:    pt.PolicyStoreID,
		PolicyTemplateID: pt.PolicyTemplateID,
		Description:      pt.Description,
		Statement:        pt.Statement,
		CreatedDate:      pt.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

type listPolicyTemplatesInput struct {
	PolicyStoreID string `json:"policyStoreId"`
	NextToken     string `json:"nextToken,omitempty"`
	MaxResults    int    `json:"maxResults,omitempty"`
}

type listPolicyTemplatesOutput struct {
	NextToken       string               `json:"nextToken,omitempty"`
	PolicyTemplates []policyTemplateView `json:"policyTemplates"`
}

func (h *Handler) handleListPolicyTemplates(
	_ context.Context,
	in *listPolicyTemplatesInput,
) (*listPolicyTemplatesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	templates, nextToken, err := h.Backend.ListPolicyTemplates(resolvedID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]policyTemplateView, 0, len(templates))

	for i := range templates {
		pt := &templates[i]
		items = append(items, policyTemplateView{
			PolicyStoreID:    pt.PolicyStoreID,
			PolicyTemplateID: pt.PolicyTemplateID,
			Description:      pt.Description,
			CreatedDate:      pt.CreatedDate.UTC().Format(timeFormat),
			LastUpdatedDate:  pt.LastUpdated.UTC().Format(timeFormat),
		})
	}

	return &listPolicyTemplatesOutput{PolicyTemplates: items, NextToken: nextToken}, nil
}

type updatePolicyTemplateInput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	PolicyTemplateID string `json:"policyTemplateId"`
	Description      string `json:"description"`
	Statement        string `json:"statement"`
}

func (h *Handler) handleUpdatePolicyTemplate(
	_ context.Context,
	in *updatePolicyTemplateInput,
) (*policyTemplateIDsOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyTemplateID == "" {
		return nil, fmt.Errorf("%w: policyTemplateId is required", errInvalidRequest)
	}

	if in.Statement == "" {
		return nil, fmt.Errorf("%w: statement is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	pt, err := h.Backend.UpdatePolicyTemplate(resolvedID, in.PolicyTemplateID, in.Description, in.Statement)
	if err != nil {
		return nil, err
	}

	return &policyTemplateIDsOutput{
		PolicyStoreID:    pt.PolicyStoreID,
		PolicyTemplateID: pt.PolicyTemplateID,
		CreatedDate:      pt.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:  pt.LastUpdated.UTC().Format(timeFormat),
	}, nil
}

func (h *Handler) handleDeletePolicyTemplate(_ context.Context, in *policyTemplateInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.PolicyTemplateID == "" {
		return nil, fmt.Errorf("%w: policyTemplateId is required", errInvalidRequest)
	}

	resolvedID, err := h.resolvePolicyStoreID(in.PolicyStoreID)
	if err != nil {
		return nil, err
	}

	if err = h.Backend.DeletePolicyTemplate(resolvedID, in.PolicyTemplateID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
