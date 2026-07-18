package workmail

import (
	"context"
)

// ---- Retention Policies ----

type folderConfigJSON struct {
	Period *int32 `json:"Period,omitempty"`
	Name   string `json:"Name"`
	Action string `json:"Action"`
}

type putRetentionPolicyReq struct {
	OrganizationID       string             `json:"OrganizationId"`
	ID                   string             `json:"Id"`
	Name                 string             `json:"Name"`
	Description          string             `json:"Description"`
	FolderConfigurations []folderConfigJSON `json:"FolderConfigurations"`
}

func (h *Handler) handlePutRetentionPolicy(
	_ context.Context, req *putRetentionPolicyReq,
) (*struct{}, error) {
	folderCfgs := make([]*FolderConfiguration, 0, len(req.FolderConfigurations))
	for _, fc := range req.FolderConfigurations {
		folderCfgs = append(folderCfgs, &FolderConfiguration{
			Name:   fc.Name,
			Action: fc.Action,
			Period: fc.Period,
		})
	}

	return &struct{}{}, h.Backend.PutRetentionPolicy(
		req.OrganizationID, req.ID, req.Name, req.Description, folderCfgs,
	)
}

type deleteRetentionPolicyReq struct {
	OrganizationID string `json:"OrganizationId"`
	Id             string `json:"Id"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteRetentionPolicy(
	_ context.Context, req *deleteRetentionPolicyReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteRetentionPolicy(req.OrganizationID, req.Id)
}

type getDefaultRetentionPolicyReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type getDefaultRetentionPolicyResp struct {
	Id                   string             `json:"Id,omitempty"` //nolint:revive,staticcheck // existing issue.
	Name                 string             `json:"Name,omitempty"`
	Description          string             `json:"Description,omitempty"`
	FolderConfigurations []folderConfigJSON `json:"FolderConfigurations"`
}

func (h *Handler) handleGetDefaultRetentionPolicy(
	_ context.Context, req *getDefaultRetentionPolicyReq,
) (*getDefaultRetentionPolicyResp, error) {
	pol, err := h.Backend.GetDefaultRetentionPolicy(req.OrganizationID)
	if err != nil {
		return nil, err
	}
	folderCfgs := make([]folderConfigJSON, 0, len(pol.FolderConfigurations))
	for _, fc := range pol.FolderConfigurations {
		folderCfgs = append(folderCfgs, folderConfigJSON{
			Name:   fc.Name,
			Action: fc.Action,
			Period: fc.Period,
		})
	}

	return &getDefaultRetentionPolicyResp{
		Id:                   pol.ID,
		Name:                 pol.Name,
		Description:          pol.Description,
		FolderConfigurations: folderCfgs,
	}, nil
}
