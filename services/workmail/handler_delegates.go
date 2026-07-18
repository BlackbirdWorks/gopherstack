package workmail

import (
	"context"
)

type associateDelegateReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
	EntityID       string `json:"EntityId"`
}

func (h *Handler) handleAssociateDelegateToResource(_ context.Context, req *associateDelegateReq) (*emptyResp, error) {
	if err := h.Backend.AssociateDelegateToResource(req.OrganizationID, req.ResourceID, req.EntityID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type disassociateDelegateReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
	EntityID       string `json:"EntityId"`
}

func (h *Handler) handleDisassociateDelegateFromResource(
	_ context.Context,
	req *disassociateDelegateReq,
) (*emptyResp, error) {
	if err := h.Backend.DisassociateDelegateFromResource(req.OrganizationID, req.ResourceID, req.EntityID); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listDelegatesReq struct {
	OrganizationID string `json:"OrganizationId"`
	ResourceID     string `json:"ResourceId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type delegateResp struct {
	ID   string `json:"Id"`
	Type string `json:"Type"`
}

type listDelegatesResp struct {
	NextToken string         `json:"NextToken,omitempty"`
	Delegates []delegateResp `json:"Delegates"`
}

func (h *Handler) handleListResourceDelegates(_ context.Context, req *listDelegatesReq) (*listDelegatesResp, error) {
	delegates, next, err := h.Backend.ListResourceDelegates(
		req.OrganizationID,
		req.ResourceID,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	dresps := make([]delegateResp, 0, len(delegates))
	for _, d := range delegates {
		dresps = append(dresps, delegateResp{ID: d.DelegateID, Type: d.DelegateType})
	}

	return &listDelegatesResp{Delegates: dresps, NextToken: next}, nil
}
