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

	if w.IPAddressType != "" {
		resp["IpAddressType"] = w.IPAddressType
	}

	return resp
}

// createWorkforceInput mirrors CreateWorkforceInput (api_op_CreateWorkforce.go:48-86).
type createWorkforceInput struct {
	CognitoConfig      *CognitoConfig      `json:"CognitoConfig"`
	OidcConfig         *oidcConfigRequest  `json:"OidcConfig"`
	SourceIPConfig     *SourceIPConfig     `json:"SourceIpConfig"`
	WorkforceVpcConfig *WorkforceVpcConfig `json:"WorkforceVpcConfig"`
	WorkforceName      string              `json:"WorkforceName"`
	IPAddressType      string              `json:"IpAddressType"`
	Tags               []tagObject         `json:"Tags"`
}

func (h *Handler) handleCreateWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req createWorkforceInput

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
		IPAddressType:      req.IPAddressType,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkforceArn: result.WorkforceArn})
}

// describeWorkforceInput mirrors DescribeWorkforceInput (api_op_DescribeWorkforce.go:34-42).
type describeWorkforceInput struct {
	WorkforceName string `json:"WorkforceName"`
}

func (h *Handler) handleDescribeWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req describeWorkforceInput

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

// updateWorkforceInput mirrors UpdateWorkforceInput (api_op_UpdateWorkforce.go:65-93).
type updateWorkforceInput struct {
	OidcConfig         *oidcConfigRequest  `json:"OidcConfig"`
	SourceIPConfig     *SourceIPConfig     `json:"SourceIpConfig"`
	WorkforceVpcConfig *WorkforceVpcConfig `json:"WorkforceVpcConfig"`
	WorkforceName      string              `json:"WorkforceName"`
	IPAddressType      string              `json:"IpAddressType"`
}

func (h *Handler) handleUpdateWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req updateWorkforceInput

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
		IPAddressType:      req.IPAddressType,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": workforceResponseMap(result)})
}

// deleteWorkforceInput mirrors DeleteWorkforceInput (api_op_DeleteWorkforce.go:39-44).
type deleteWorkforceInput struct {
	WorkforceName string `json:"WorkforceName"`
}

func (h *Handler) handleDeleteWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteWorkforceInput

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

// listWorkforcesInput mirrors ListWorkforcesInput (api_op_ListWorkforces.go:31-48).
type listWorkforcesInput struct {
	NextToken    string `json:"NextToken"`
	NameContains string `json:"NameContains"`
	SortBy       string `json:"SortBy"`
	SortOrder    string `json:"SortOrder"`
	MaxResults   int32  `json:"MaxResults"`
}

func (h *Handler) handleListWorkforces(ctx context.Context, body []byte) ([]byte, error) {
	var req listWorkforcesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListWorkforces(ctx, req.NextToken, ListWorkforcesFilter{
		NameContains: req.NameContains,
		SortBy:       req.SortBy,
		SortOrder:    req.SortOrder,
		MaxResults:   req.MaxResults,
	})

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
