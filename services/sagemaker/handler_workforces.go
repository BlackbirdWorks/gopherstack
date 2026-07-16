package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Workforce handlers
// ---------------------------------------------------------------------------

// oidcConfigRequest is the wire shape of OidcConfig on Create/UpdateWorkforce
// requests. Unlike the stored/returned OidcConfig, it includes ClientSecret.
type oidcConfigRequest struct {
	AuthenticationRequestExtraParams map[string]string `json:"AuthenticationRequestExtraParams"`
	ClientID                         string            `json:"ClientId"`
	ClientSecret                     string            `json:"ClientSecret"`
	Issuer                           string            `json:"Issuer"`
	AuthorizationEndpoint            string            `json:"AuthorizationEndpoint"`
	TokenEndpoint                    string            `json:"TokenEndpoint"`
	UserInfoEndpoint                 string            `json:"UserInfoEndpoint"`
	LogoutEndpoint                   string            `json:"LogoutEndpoint"`
	JwksURI                          string            `json:"JwksUri"`
	Scope                            string            `json:"Scope"`
}

func (r *oidcConfigRequest) toOidcConfig() *OidcConfig {
	if r == nil {
		return nil
	}

	return &OidcConfig{
		AuthenticationRequestExtraParams: r.AuthenticationRequestExtraParams,
		ClientID:                         r.ClientID,
		ClientSecret:                     r.ClientSecret,
		Issuer:                           r.Issuer,
		AuthorizationEndpoint:            r.AuthorizationEndpoint,
		TokenEndpoint:                    r.TokenEndpoint,
		UserInfoEndpoint:                 r.UserInfoEndpoint,
		LogoutEndpoint:                   r.LogoutEndpoint,
		JwksURI:                          r.JwksURI,
		Scope:                            r.Scope,
	}
}

// workforceResponseMap builds the AWS wire representation of a Workforce,
// converting timestamps to epoch seconds as required by the aws-json-1.1 protocol.
func workforceResponseMap(w *Workforce) map[string]any {
	resp := map[string]any{
		"WorkforceName":   w.WorkforceName,
		keyWorkforceArn:   w.WorkforceArn,
		keyStatus:         w.Status,
		"CreateDate":      epochSeconds(w.CreateDate),
		"LastUpdatedDate": epochSeconds(w.LastUpdatedDate),
	}

	if w.CognitoConfig != nil {
		resp["CognitoConfig"] = w.CognitoConfig
	}

	if w.OidcConfig != nil {
		resp["OidcConfig"] = w.OidcConfig
	}

	if w.SourceIPConfig != nil {
		resp["SourceIpConfig"] = w.SourceIPConfig
	}

	if w.WorkforceVpcConfig != nil {
		resp["WorkforceVpcConfig"] = w.WorkforceVpcConfig
	}

	if w.SubDomain != "" {
		resp["SubDomain"] = w.SubDomain
	}

	return resp
}

func (h *Handler) handleCreateWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CognitoConfig      *CognitoConfig      `json:"CognitoConfig"`
		OidcConfig         *oidcConfigRequest  `json:"OidcConfig"`
		SourceIPConfig     *SourceIPConfig     `json:"SourceIpConfig"`
		WorkforceVpcConfig *WorkforceVpcConfig `json:"WorkforceVpcConfig"`
		WorkforceName      string              `json:"WorkforceName"`
		Tags               []tagObject         `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateWorkforce(ctx, CreateWorkforceOptions{
		Name:               req.WorkforceName,
		CognitoConfig:      req.CognitoConfig,
		OidcConfig:         req.OidcConfig.toOidcConfig(),
		SourceIPConfig:     req.SourceIPConfig,
		WorkforceVpcConfig: req.WorkforceVpcConfig,
		Tags:               fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkforceArn: result.WorkforceArn})
}

func (h *Handler) handleDescribeWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkforceName string `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeWorkforce(ctx, req.WorkforceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": workforceResponseMap(result)})
}

func (h *Handler) handleUpdateWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		OidcConfig         *oidcConfigRequest  `json:"OidcConfig"`
		SourceIPConfig     *SourceIPConfig     `json:"SourceIpConfig"`
		WorkforceVpcConfig *WorkforceVpcConfig `json:"WorkforceVpcConfig"`
		WorkforceName      string              `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateWorkforce(ctx, UpdateWorkforceOptions{
		Name:               req.WorkforceName,
		OidcConfig:         req.OidcConfig.toOidcConfig(),
		SourceIPConfig:     req.SourceIPConfig,
		WorkforceVpcConfig: req.WorkforceVpcConfig,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": workforceResponseMap(result)})
}

func (h *Handler) handleDeleteWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkforceName string `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWorkforce(ctx, req.WorkforceName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListWorkforces(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListWorkforces(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, w := range items {
		summaries = append(summaries, workforceResponseMap(w))
	}

	resp := map[string]any{"Workforces": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}
