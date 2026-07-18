package workmail

import (
	"context"
)

// ---- Identity Center Applications ----

type createIdentityCenterApplicationReq struct {
	InstanceArn string `json:"InstanceArn"`
	Name        string `json:"Name"`
}

type createIdentityCenterApplicationResp struct {
	ApplicationArn string `json:"ApplicationArn"`
}

func (h *Handler) handleCreateIdentityCenterApplication(
	_ context.Context, req *createIdentityCenterApplicationReq,
) (*createIdentityCenterApplicationResp, error) {
	appARN, err := h.Backend.CreateIdentityCenterApplication(req.InstanceArn, req.Name)
	if err != nil {
		return nil, err
	}

	return &createIdentityCenterApplicationResp{ApplicationArn: appARN}, nil
}

type deleteIdentityCenterApplicationReq struct {
	ApplicationArn string `json:"ApplicationArn"`
}

func (h *Handler) handleDeleteIdentityCenterApplication(
	_ context.Context, req *deleteIdentityCenterApplicationReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteIdentityCenterApplication(req.ApplicationArn)
}

// ---- Identity Provider Configuration ----

type identityCenterConfigJSON struct {
	ApplicationArn string `json:"ApplicationArn"`
	InstanceArn    string `json:"InstanceArn"`
}

type personalAccessTokenConfigJSON struct {
	LifetimeInDays *int32 `json:"LifetimeInDays,omitempty"`
	Status         string `json:"Status"`
}

type putIdentityProviderConfigReq struct {
	IdentityCenterConfiguration      *identityCenterConfigJSON      `json:"IdentityCenterConfiguration"`
	PersonalAccessTokenConfiguration *personalAccessTokenConfigJSON `json:"PersonalAccessTokenConfiguration"`
	OrganizationID                   string                         `json:"OrganizationId"`
	AuthenticationMode               string                         `json:"AuthenticationMode"`
}

func (h *Handler) handlePutIdentityProviderConfiguration(
	_ context.Context, req *putIdentityProviderConfigReq,
) (*struct{}, error) {
	appARN, instanceARN, patStatus := "", "", ""
	var patLifetime int32
	if req.IdentityCenterConfiguration != nil {
		appARN = req.IdentityCenterConfiguration.ApplicationArn
		instanceARN = req.IdentityCenterConfiguration.InstanceArn
	}
	if req.PersonalAccessTokenConfiguration != nil {
		patStatus = req.PersonalAccessTokenConfiguration.Status
		if req.PersonalAccessTokenConfiguration.LifetimeInDays != nil {
			patLifetime = *req.PersonalAccessTokenConfiguration.LifetimeInDays
		}
	}

	return &struct{}{}, h.Backend.PutIdentityProviderConfiguration(
		req.OrganizationID, req.AuthenticationMode, appARN, instanceARN, patStatus, patLifetime,
	)
}

type deleteIdentityProviderConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
}

func (h *Handler) handleDeleteIdentityProviderConfiguration(
	_ context.Context, req *deleteIdentityProviderConfigReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteIdentityProviderConfiguration(req.OrganizationID)
}

type describeIdentityProviderConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type describeIdentityProviderConfigResp struct {
	IdentityCenterConfiguration      *identityCenterConfigJSON      `json:"IdentityCenterConfiguration,omitempty"`
	PersonalAccessTokenConfiguration *personalAccessTokenConfigJSON `json:"PersonalAccessTokenConfiguration,omitempty"`
	AuthenticationMode               string                         `json:"AuthenticationMode,omitempty"`
}

func (h *Handler) handleDescribeIdentityProviderConfiguration(
	_ context.Context, req *describeIdentityProviderConfigReq,
) (*describeIdentityProviderConfigResp, error) {
	cfg, err := h.Backend.DescribeIdentityProviderConfiguration(req.OrganizationID)
	if err != nil {
		return nil, err
	}
	resp := &describeIdentityProviderConfigResp{AuthenticationMode: cfg.AuthMode}
	if cfg.IdentityCenterAppARN != "" || cfg.IdentityCenterInstanceARN != "" {
		resp.IdentityCenterConfiguration = &identityCenterConfigJSON{
			ApplicationArn: cfg.IdentityCenterAppARN,
			InstanceArn:    cfg.IdentityCenterInstanceARN,
		}
	}
	if cfg.PATStatus != "" {
		resp.PersonalAccessTokenConfiguration = &personalAccessTokenConfigJSON{
			Status:         cfg.PATStatus,
			LifetimeInDays: cfg.PATLifetimeDays,
		}
	}

	return resp, nil
}
