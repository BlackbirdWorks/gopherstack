package workmail

import (
	"context"
)

// ---- Impersonation Roles ----

type impersonationRuleReq struct {
	ImpersonationRuleID string   `json:"ImpersonationRuleId"`
	Name                string   `json:"Name"`
	Description         string   `json:"Description"`
	Effect              string   `json:"Effect"`
	TargetUsers         []string `json:"TargetUsers"`
	NotTargetUsers      []string `json:"NotTargetUsers"`
}

type createImpersonationRoleReq struct {
	OrganizationID string                 `json:"OrganizationId"`
	Name           string                 `json:"Name"`
	Type           string                 `json:"Type"`
	Description    string                 `json:"Description"`
	Rules          []impersonationRuleReq `json:"Rules"`
}

type createImpersonationRoleResp struct {
	ImpersonationRoleID string `json:"ImpersonationRoleId"`
}

func (h *Handler) handleCreateImpersonationRole(
	_ context.Context,
	req *createImpersonationRoleReq,
) (*createImpersonationRoleResp, error) {
	rules := make([]ImpersonationRule, 0, len(req.Rules))
	for _, r := range req.Rules {
		rules = append(rules, ImpersonationRule{
			RuleID:         r.ImpersonationRuleID,
			Name:           r.Name,
			Description:    r.Description,
			Effect:         r.Effect,
			TargetUsers:    r.TargetUsers,
			NotTargetUsers: r.NotTargetUsers,
		})
	}

	role, err := h.Backend.CreateImpersonationRole(req.OrganizationID, req.Name, req.Type, req.Description, rules)
	if err != nil {
		return nil, err
	}

	return &createImpersonationRoleResp{ImpersonationRoleID: role.RoleID}, nil
}

type getImpersonationRoleReq struct {
	OrganizationID      string `json:"OrganizationId"`
	ImpersonationRoleID string `json:"ImpersonationRoleId"`
}

type impersonationRuleResp struct {
	ImpersonationRuleID string   `json:"ImpersonationRuleId"`
	Name                string   `json:"Name,omitempty"`
	Description         string   `json:"Description,omitempty"`
	Effect              string   `json:"Effect"`
	TargetUsers         []string `json:"TargetUsers,omitempty"`
	NotTargetUsers      []string `json:"NotTargetUsers,omitempty"`
}

type getImpersonationRoleResp struct {
	ImpersonationRoleID string                  `json:"ImpersonationRoleId"`
	Name                string                  `json:"Name"`
	Type                string                  `json:"Type"`
	Description         string                  `json:"Description,omitempty"`
	Rules               []impersonationRuleResp `json:"Rules,omitempty"`
	DateCreated         int64                   `json:"DateCreated"`
	DateModified        int64                   `json:"DateModified"`
}

func (h *Handler) handleGetImpersonationRole(
	_ context.Context,
	req *getImpersonationRoleReq,
) (*getImpersonationRoleResp, error) {
	role, err := h.Backend.GetImpersonationRole(req.OrganizationID, req.ImpersonationRoleID)
	if err != nil {
		return nil, err
	}

	rresps := make([]impersonationRuleResp, 0, len(role.Rules))
	for _, r := range role.Rules {
		rresps = append(rresps, impersonationRuleResp{
			ImpersonationRuleID: r.RuleID,
			Name:                r.Name,
			Description:         r.Description,
			Effect:              r.Effect,
			TargetUsers:         r.TargetUsers,
			NotTargetUsers:      r.NotTargetUsers,
		})
	}

	return &getImpersonationRoleResp{
		ImpersonationRoleID: role.RoleID,
		Name:                role.Name,
		Type:                role.RoleType,
		Description:         role.Description,
		Rules:               rresps,
		DateCreated:         role.DateCreated.Unix(),
		DateModified:        role.DateModified.Unix(),
	}, nil
}

type updateImpersonationRoleReq struct {
	OrganizationID      string                 `json:"OrganizationId"`
	ImpersonationRoleID string                 `json:"ImpersonationRoleId"`
	Name                string                 `json:"Name"`
	Type                string                 `json:"Type"`
	Description         string                 `json:"Description"`
	Rules               []impersonationRuleReq `json:"Rules"`
}

func (h *Handler) handleUpdateImpersonationRole(
	_ context.Context,
	req *updateImpersonationRoleReq,
) (*emptyResp, error) {
	rules := make([]ImpersonationRule, 0, len(req.Rules))
	for _, r := range req.Rules {
		rules = append(rules, ImpersonationRule{
			RuleID:         r.ImpersonationRuleID,
			Name:           r.Name,
			Description:    r.Description,
			Effect:         r.Effect,
			TargetUsers:    r.TargetUsers,
			NotTargetUsers: r.NotTargetUsers,
		})
	}

	if err := h.Backend.UpdateImpersonationRole(
		req.OrganizationID, req.ImpersonationRoleID, req.Name, req.Type, req.Description, rules,
	); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteImpersonationRoleReq struct {
	OrganizationID      string `json:"OrganizationId"`
	ImpersonationRoleID string `json:"ImpersonationRoleId"`
}

func (h *Handler) handleDeleteImpersonationRole(
	_ context.Context,
	req *deleteImpersonationRoleReq,
) (*emptyResp, error) {
	if err := h.Backend.DeleteImpersonationRole(req.OrganizationID, req.ImpersonationRoleID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listImpersonationRolesReq struct {
	OrganizationID string `json:"OrganizationId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type impersonationRoleSummaryResp struct {
	ImpersonationRoleID string `json:"ImpersonationRoleId"`
	Name                string `json:"Name"`
	Type                string `json:"Type"`
	DateCreated         int64  `json:"DateCreated"`
	DateModified        int64  `json:"DateModified"`
}

// listImpersonationRolesResp mirrors ListImpersonationRolesOutput: the field
// is "Roles", not "Items".
type listImpersonationRolesResp struct {
	NextToken string                         `json:"NextToken,omitempty"`
	Roles     []impersonationRoleSummaryResp `json:"Roles"`
}

func (h *Handler) handleListImpersonationRoles(
	_ context.Context,
	req *listImpersonationRolesReq,
) (*listImpersonationRolesResp, error) {
	roles, next, err := h.Backend.ListImpersonationRoles(req.OrganizationID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]impersonationRoleSummaryResp, 0, len(roles))
	for _, r := range roles {
		summaries = append(summaries, impersonationRoleSummaryResp{
			ImpersonationRoleID: r.RoleID,
			Name:                r.Name,
			Type:                r.RoleType,
			DateCreated:         r.DateCreated.Unix(),
			DateModified:        r.DateModified.Unix(),
		})
	}

	return &listImpersonationRolesResp{Roles: summaries, NextToken: next}, nil
}

// ---- Impersonation Role Effect ----

type getImpersonationRoleEffectReq struct {
	OrganizationID      string `json:"OrganizationId"`
	ImpersonationRoleId string `json:"ImpersonationRoleId"` //nolint:revive,staticcheck // existing issue.
	TargetUser          string `json:"TargetUser"`
}

type impersonationMatchedRuleJSON struct {
	ImpersonationRuleId string `json:"ImpersonationRuleId"` //nolint:revive,staticcheck // existing issue.
	Name                string `json:"Name,omitempty"`
}

type getImpersonationRoleEffectResp struct {
	Effect       string                         `json:"Effect,omitempty"`
	Type         string                         `json:"Type,omitempty"`
	MatchedRules []impersonationMatchedRuleJSON `json:"MatchedRules,omitempty"`
}

func (h *Handler) handleGetImpersonationRoleEffect(
	_ context.Context, req *getImpersonationRoleEffectReq,
) (*getImpersonationRoleEffectResp, error) {
	effect, roleType, matched, err := h.Backend.GetImpersonationRoleEffect(
		req.OrganizationID, req.ImpersonationRoleId, req.TargetUser,
	)
	if err != nil {
		return nil, err
	}
	matchedJSON := make([]impersonationMatchedRuleJSON, 0, len(matched))
	for _, m := range matched {
		matchedJSON = append(matchedJSON, impersonationMatchedRuleJSON{
			ImpersonationRuleId: m.RuleID,
			Name:                m.Name,
		})
	}

	return &getImpersonationRoleEffectResp{Effect: effect, Type: roleType, MatchedRules: matchedJSON}, nil
}

// ---- Assume Impersonation Role ----

type assumeImpersonationRoleReq struct {
	OrganizationID      string `json:"OrganizationId"`
	ImpersonationRoleId string `json:"ImpersonationRoleId"` //nolint:revive,staticcheck // existing issue.
}

type assumeImpersonationRoleResp struct {
	Token     string `json:"Token,omitempty"`
	ExpiresIn int64  `json:"ExpiresIn,omitempty"`
}

func (h *Handler) handleAssumeImpersonationRole(
	_ context.Context, req *assumeImpersonationRoleReq,
) (*assumeImpersonationRoleResp, error) {
	token, expiresIn, err := h.Backend.AssumeImpersonationRole(req.OrganizationID, req.ImpersonationRoleId)
	if err != nil {
		return nil, err
	}

	return &assumeImpersonationRoleResp{Token: token, ExpiresIn: expiresIn}, nil
}
