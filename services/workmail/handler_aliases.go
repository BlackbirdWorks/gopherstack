package workmail

import (
	"context"
)

// ---- Aliases ----

type createAliasReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	Alias          string `json:"Alias"`
}

func (h *Handler) handleCreateAlias(_ context.Context, req *createAliasReq) (*emptyResp, error) {
	if err := h.Backend.CreateAlias(req.OrganizationID, req.EntityID, req.Alias); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type deleteAliasReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	Alias          string `json:"Alias"`
}

func (h *Handler) handleDeleteAlias(_ context.Context, req *deleteAliasReq) (*emptyResp, error) {
	if err := h.Backend.DeleteAlias(req.OrganizationID, req.EntityID, req.Alias); err != nil {
		return nil, err
	}

	return &emptyResp{}, nil
}

type listAliasesReq struct {
	OrganizationID string `json:"OrganizationId"`
	EntityID       string `json:"EntityId"`
	NextToken      string `json:"NextToken"`
	MaxResults     int32  `json:"MaxResults"`
}

type listAliasesResp struct {
	NextToken string   `json:"NextToken,omitempty"`
	Aliases   []string `json:"Aliases"`
}

func (h *Handler) handleListAliases(_ context.Context, req *listAliasesReq) (*listAliasesResp, error) {
	aliases, next, err := h.Backend.ListAliases(req.OrganizationID, req.EntityID, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	return &listAliasesResp{Aliases: aliases, NextToken: next}, nil
}
