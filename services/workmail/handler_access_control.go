package workmail

import (
	"context"
)

// ---- Access Control Rules ----

type putACRReq struct {
	OrganizationID string   `json:"OrganizationId"`
	Name           string   `json:"Name"`
	Effect         string   `json:"Effect"`
	Description    string   `json:"Description"`
	IPRanges       []string `json:"IpRanges"`
	NotIPRanges    []string `json:"NotIpRanges"`
	Actions        []string `json:"Actions"`
	NotActions     []string `json:"NotActions"`
	UserIDs        []string `json:"UserIds"`
	NotUserIDs     []string `json:"NotUserIds"`
}

func (h *Handler) handlePutAccessControlRule(_ context.Context, req *putACRReq) (*emptyResp, error) {
	_, err := h.Backend.PutAccessControlRule(
		req.OrganizationID, req.Name, req.Effect, req.Description,
		req.IPRanges, req.NotIPRanges,
		req.Actions, req.NotActions,
		req.UserIDs, req.NotUserIDs,
	)
	if err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteACRReq struct {
	OrganizationID string `json:"OrganizationId"`
	Name           string `json:"Name"`
}

func (h *Handler) handleDeleteAccessControlRule(_ context.Context, req *deleteACRReq) (*emptyResp, error) {
	if err := h.Backend.DeleteAccessControlRule(req.OrganizationID, req.Name); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type getACEReq struct {
	OrganizationID string `json:"OrganizationId"`
	IPAddress      string `json:"IpAddress"`
	Action         string `json:"Action"`
	UserID         string `json:"UserId"`
}

type getACEResp struct {
	Effect       string   `json:"Effect"`
	MatchedRules []string `json:"MatchedRules"`
}

func (h *Handler) handleGetAccessControlEffect(_ context.Context, req *getACEReq) (*getACEResp, error) {
	effect, matched, err := h.Backend.GetAccessControlEffect(req.OrganizationID, req.IPAddress, req.Action, req.UserID)
	if err != nil {
		return nil, err
	}

	return &getACEResp{Effect: effect, MatchedRules: matched}, nil
}

type listACRReq struct {
	OrganizationID string `json:"OrganizationId"`
}

// acrResp mirrors aws-sdk-go-v2/service/workmail/types.AccessControlRule.
// The wire keys are IpRanges/NotIpRanges (lowercase "p"), NOT IPRanges/
// NotIPRanges -- a real SDK client deserializes case-sensitively and would
// silently see empty slices for these fields under the wrong casing.
type acrResp struct {
	Name         string   `json:"Name"`
	Effect       string   `json:"Effect"`
	Description  string   `json:"Description,omitempty"`
	IPRanges     []string `json:"IpRanges,omitempty"`
	NotIPRanges  []string `json:"NotIpRanges,omitempty"`
	Actions      []string `json:"Actions,omitempty"`
	NotActions   []string `json:"NotActions,omitempty"`
	UserIDs      []string `json:"UserIds,omitempty"`
	NotUserIDs   []string `json:"NotUserIds,omitempty"`
	DateCreated  int64    `json:"DateCreated,omitempty"`
	DateModified int64    `json:"DateModified,omitempty"`
}

type listACRResp struct {
	Rules []acrResp `json:"Rules"`
}

func (h *Handler) handleListAccessControlRules(_ context.Context, req *listACRReq) (*listACRResp, error) {
	rules, err := h.Backend.ListAccessControlRules(req.OrganizationID)
	if err != nil {
		return nil, err
	}

	rresps := make([]acrResp, 0, len(rules))
	for _, r := range rules {
		ar := acrResp{
			Name:        r.Name,
			Effect:      r.Effect,
			Description: r.Description,
			IPRanges:    r.IPRanges,
			NotIPRanges: r.NotIPRanges,
			Actions:     r.Actions,
			NotActions:  r.NotActions,
			UserIDs:     r.UserIDs,
			NotUserIDs:  r.NotUserIDs,
		}
		if !r.DateCreated.IsZero() {
			ar.DateCreated = r.DateCreated.Unix()
		}
		if !r.DateModified.IsZero() {
			ar.DateModified = r.DateModified.Unix()
		}
		rresps = append(rresps, ar)
	}

	return &listACRResp{Rules: rresps}, nil
}
