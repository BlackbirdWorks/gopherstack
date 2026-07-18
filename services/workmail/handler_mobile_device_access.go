package workmail

import (
	"context"
)

// ---- Mobile Device Access Rules ----

type createMobileDeviceAccessRuleReq struct {
	OrganizationID            string   `json:"OrganizationId"`
	Name                      string   `json:"Name"`
	Effect                    string   `json:"Effect"`
	Description               string   `json:"Description"`
	DeviceModels              []string `json:"DeviceModels"`
	NotDeviceModels           []string `json:"NotDeviceModels"`
	DeviceTypes               []string `json:"DeviceTypes"`
	NotDeviceTypes            []string `json:"NotDeviceTypes"`
	DeviceOperatingSystems    []string `json:"DeviceOperatingSystems"`
	NotDeviceOperatingSystems []string `json:"NotDeviceOperatingSystems"`
	DeviceUserAgents          []string `json:"DeviceUserAgents"`
	NotDeviceUserAgents       []string `json:"NotDeviceUserAgents"`
}

type createMobileDeviceAccessRuleResp struct {
	MobileDeviceAccessRuleId string `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateMobileDeviceAccessRule(
	_ context.Context, req *createMobileDeviceAccessRuleReq,
) (*createMobileDeviceAccessRuleResp, error) {
	rule, err := h.Backend.CreateMobileDeviceAccessRule(
		req.OrganizationID, req.Name, req.Effect, req.Description,
		req.DeviceModels, req.NotDeviceModels, req.DeviceTypes, req.NotDeviceTypes,
		req.DeviceOperatingSystems, req.NotDeviceOperatingSystems, req.DeviceUserAgents, req.NotDeviceUserAgents,
	)
	if err != nil {
		return nil, err
	}

	return &createMobileDeviceAccessRuleResp{MobileDeviceAccessRuleId: rule.RuleID}, nil
}

type deleteMobileDeviceAccessRuleReq struct {
	OrganizationID           string `json:"OrganizationId"`
	MobileDeviceAccessRuleId string `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteMobileDeviceAccessRule(
	_ context.Context, req *deleteMobileDeviceAccessRuleReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteMobileDeviceAccessRule(req.OrganizationID, req.MobileDeviceAccessRuleId)
}

type updateMobileDeviceAccessRuleReq struct {
	OrganizationID            string   `json:"OrganizationId"`
	MobileDeviceAccessRuleId  string   `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
	Name                      string   `json:"Name"`
	Effect                    string   `json:"Effect"`
	Description               string   `json:"Description"`
	DeviceModels              []string `json:"DeviceModels"`
	NotDeviceModels           []string `json:"NotDeviceModels"`
	DeviceTypes               []string `json:"DeviceTypes"`
	NotDeviceTypes            []string `json:"NotDeviceTypes"`
	DeviceOperatingSystems    []string `json:"DeviceOperatingSystems"`
	NotDeviceOperatingSystems []string `json:"NotDeviceOperatingSystems"`
	DeviceUserAgents          []string `json:"DeviceUserAgents"`
	NotDeviceUserAgents       []string `json:"NotDeviceUserAgents"`
}

func (h *Handler) handleUpdateMobileDeviceAccessRule(
	_ context.Context, req *updateMobileDeviceAccessRuleReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.UpdateMobileDeviceAccessRule(
		req.OrganizationID, req.MobileDeviceAccessRuleId, req.Name, req.Effect, req.Description,
		req.DeviceModels, req.NotDeviceModels, req.DeviceTypes, req.NotDeviceTypes,
		req.DeviceOperatingSystems, req.NotDeviceOperatingSystems, req.DeviceUserAgents, req.NotDeviceUserAgents,
	)
}

type listMobileDeviceAccessRulesReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type mobileDeviceAccessRuleJSON struct {
	MobileDeviceAccessRuleId  string   `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
	Name                      string   `json:"Name"`
	Effect                    string   `json:"Effect"`
	Description               string   `json:"Description,omitempty"`
	DeviceModels              []string `json:"DeviceModels,omitempty"`
	NotDeviceModels           []string `json:"NotDeviceModels,omitempty"`
	DeviceTypes               []string `json:"DeviceTypes,omitempty"`
	NotDeviceTypes            []string `json:"NotDeviceTypes,omitempty"`
	DeviceOperatingSystems    []string `json:"DeviceOperatingSystems,omitempty"`
	NotDeviceOperatingSystems []string `json:"NotDeviceOperatingSystems,omitempty"`
	DeviceUserAgents          []string `json:"DeviceUserAgents,omitempty"`
	NotDeviceUserAgents       []string `json:"NotDeviceUserAgents,omitempty"`
	DateCreated               int64    `json:"DateCreated"`
	DateModified              int64    `json:"DateModified"`
}

type listMobileDeviceAccessRulesResp struct {
	Rules []mobileDeviceAccessRuleJSON `json:"Rules"`
}

func mobileRuleToJSON(r *MobileDeviceAccessRule) mobileDeviceAccessRuleJSON {
	return mobileDeviceAccessRuleJSON{
		MobileDeviceAccessRuleId:  r.RuleID,
		Name:                      r.Name,
		Effect:                    r.Effect,
		Description:               r.Description,
		DeviceModels:              r.DeviceModels,
		NotDeviceModels:           r.NotDeviceModels,
		DeviceTypes:               r.DeviceTypes,
		NotDeviceTypes:            r.NotDeviceTypes,
		DeviceOperatingSystems:    r.DeviceOperatingSystems,
		NotDeviceOperatingSystems: r.NotDeviceOperatingSystems,
		DeviceUserAgents:          r.DeviceUserAgents,
		NotDeviceUserAgents:       r.NotDeviceUserAgents,
		DateCreated:               r.DateCreated.Unix(),
		DateModified:              r.DateModified.Unix(),
	}
}

func (h *Handler) handleListMobileDeviceAccessRules(
	_ context.Context, req *listMobileDeviceAccessRulesReq,
) (*listMobileDeviceAccessRulesResp, error) {
	rules, err := h.Backend.ListMobileDeviceAccessRules(req.OrganizationID)
	if err != nil {
		return nil, err
	}
	result := make([]mobileDeviceAccessRuleJSON, 0, len(rules))
	for _, r := range rules {
		result = append(result, mobileRuleToJSON(r))
	}

	return &listMobileDeviceAccessRulesResp{Rules: result}, nil
}

type getMobileDeviceAccessEffectReq struct {
	OrganizationID        string `json:"OrganizationId"`
	DeviceType            string `json:"DeviceType"`
	DeviceModel           string `json:"DeviceModel"`
	DeviceOperatingSystem string `json:"DeviceOperatingSystem"`
	DeviceUserAgent       string `json:"DeviceUserAgent"`
}

type mobileDeviceMatchedRuleJSON struct {
	MobileDeviceAccessRuleId string `json:"MobileDeviceAccessRuleId"` //nolint:revive,staticcheck // existing issue.
	Name                     string `json:"Name"`
}

type getMobileDeviceAccessEffectResp struct {
	Effect       string                        `json:"Effect"`
	MatchedRules []mobileDeviceMatchedRuleJSON `json:"MatchedRules"`
}

func (h *Handler) handleGetMobileDeviceAccessEffect(
	_ context.Context, req *getMobileDeviceAccessEffectReq,
) (*getMobileDeviceAccessEffectResp, error) {
	effect, matched, err := h.Backend.GetMobileDeviceAccessEffect(
		req.OrganizationID, req.DeviceType, req.DeviceModel, req.DeviceOperatingSystem, req.DeviceUserAgent,
	)
	if err != nil {
		return nil, err
	}
	matchedJSON := make([]mobileDeviceMatchedRuleJSON, 0, len(matched))
	for _, m := range matched {
		matchedJSON = append(matchedJSON, mobileDeviceMatchedRuleJSON{
			MobileDeviceAccessRuleId: m.RuleID,
			Name:                     m.Name,
		})
	}

	return &getMobileDeviceAccessEffectResp{Effect: effect, MatchedRules: matchedJSON}, nil
}

// ---- Mobile Device Access Overrides ----

type putMobileDeviceAccessOverrideReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId       string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
	Effect         string `json:"Effect"`
	Description    string `json:"Description"`
}

func (h *Handler) handlePutMobileDeviceAccessOverride(
	_ context.Context, req *putMobileDeviceAccessOverrideReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.PutMobileDeviceAccessOverride(
		req.OrganizationID, req.UserId, req.DeviceId, req.Effect, req.Description,
	)
}

type deleteMobileDeviceAccessOverrideReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId       string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteMobileDeviceAccessOverride(
	_ context.Context, req *deleteMobileDeviceAccessOverrideReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteMobileDeviceAccessOverride(
		req.OrganizationID, req.UserId, req.DeviceId,
	)
}

type getMobileDeviceAccessOverrideReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId       string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
}

type mobileDeviceAccessOverrideJSON struct {
	UserId       string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId     string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
	Effect       string `json:"Effect"`
	Description  string `json:"Description,omitempty"`
	DateCreated  int64  `json:"DateCreated"`
	DateModified int64  `json:"DateModified"`
}

func (h *Handler) handleGetMobileDeviceAccessOverride(
	_ context.Context, req *getMobileDeviceAccessOverrideReq,
) (*mobileDeviceAccessOverrideJSON, error) {
	ov, err := h.Backend.GetMobileDeviceAccessOverride(req.OrganizationID, req.UserId, req.DeviceId)
	if err != nil {
		return nil, err
	}

	return &mobileDeviceAccessOverrideJSON{
		UserId:       ov.UserID,
		DeviceId:     ov.DeviceID,
		Effect:       ov.Effect,
		Description:  ov.Description,
		DateCreated:  ov.DateCreated.Unix(),
		DateModified: ov.DateModified.Unix(),
	}, nil
}

type listMobileDeviceAccessOverridesReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"`   //nolint:revive,staticcheck // existing issue.
	DeviceId       string `json:"DeviceId"` //nolint:revive,staticcheck // existing issue.
	MaxResults     *int32 `json:"MaxResults"`
	NextToken      string `json:"NextToken"`
}

type listMobileDeviceAccessOverridesResp struct {
	NextToken string                           `json:"NextToken,omitempty"`
	Overrides []mobileDeviceAccessOverrideJSON `json:"Overrides"`
}

func (h *Handler) handleListMobileDeviceAccessOverrides(
	_ context.Context, req *listMobileDeviceAccessOverridesReq,
) (*listMobileDeviceAccessOverridesResp, error) {
	maxResults := int32(0)
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	overrides, next, err := h.Backend.ListMobileDeviceAccessOverrides(
		req.OrganizationID, req.UserId, req.DeviceId, maxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}
	result := make([]mobileDeviceAccessOverrideJSON, 0, len(overrides))
	for _, ov := range overrides {
		result = append(result, mobileDeviceAccessOverrideJSON{
			UserId:       ov.UserID,
			DeviceId:     ov.DeviceID,
			Effect:       ov.Effect,
			Description:  ov.Description,
			DateCreated:  ov.DateCreated.Unix(),
			DateModified: ov.DateModified.Unix(),
		})
	}

	return &listMobileDeviceAccessOverridesResp{Overrides: result, NextToken: next}, nil
}
