package elasticsearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// vpcEndpointJSON is the JSON representation of a VPC endpoint.
type vpcEndpointJSON struct {
	VpcOptions       map[string]string `json:"VpcOptions"`
	VpcEndpointID    string            `json:"VpcEndpointId"`
	VpcEndpointOwner string            `json:"VpcEndpointOwner"`
	DomainArn        string            `json:"DomainArn"`
	Endpoint         string            `json:"Endpoint"`
	Status           string            `json:"Status"`
}

// createVpcEndpointRequest is the JSON body for CreateVpcEndpoint.
type createVpcEndpointRequest struct {
	VpcOptions map[string]string `json:"VpcOptions"`
	DomainArn  string            `json:"DomainArn"`
}

// createVpcEndpointOutput wraps the new VPC endpoint.
type createVpcEndpointOutput struct {
	VpcEndpoint vpcEndpointJSON `json:"VpcEndpoint"`
}

func (h *Handler) handleCreateVpcEndpoint(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createVpcEndpointRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	endpoint, createErr := h.Backend.CreateVpcEndpoint(h.reqContext(r), req.DomainArn, req.VpcOptions)
	if createErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

		return
	}

	h.writeJSON(r, w, createVpcEndpointOutput{VpcEndpoint: toVpcEndpointJSON(endpoint)})
}

func toVpcEndpointJSON(e *VpcEndpoint) vpcEndpointJSON {
	return vpcEndpointJSON{
		VpcEndpointID:    e.ID,
		VpcEndpointOwner: e.OwnerAccountID,
		DomainArn:        e.DomainARN,
		Endpoint:         e.Endpoint,
		Status:           e.Status,
		VpcOptions:       e.VpcOptions,
	}
}

// authorizeVpcEndpointAccessRequest is the JSON body for AuthorizeVpcEndpointAccess.
type authorizeVpcEndpointAccessRequest struct {
	Account string `json:"Account"`
}

// authorizedPrincipalJSON is the JSON representation of an authorized principal.
type authorizedPrincipalJSON struct {
	PrincipalType string `json:"PrincipalType"`
	Principal     string `json:"Principal"`
}

// authorizeVpcEndpointAccessOutput is the response for AuthorizeVpcEndpointAccess.
type authorizeVpcEndpointAccessOutput struct {
	AuthorizedPrincipal authorizedPrincipalJSON `json:"AuthorizedPrincipal"`
}

func (h *Handler) handleAuthorizeVpcEndpointAccess(w http.ResponseWriter, r *http.Request, domainName string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req authorizeVpcEndpointAccessRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if authErr := h.Backend.AuthorizeVpcEndpointAccess(h.reqContext(r), domainName, req.Account); authErr != nil {
		if errors.Is(authErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", authErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", authErr.Error())
		}

		return
	}

	h.writeJSON(r, w, authorizeVpcEndpointAccessOutput{
		AuthorizedPrincipal: authorizedPrincipalJSON{
			PrincipalType: "AWS_ACCOUNT",
			Principal:     req.Account,
		},
	})
}

func (h *Handler) handleDescribeVpcEndpoints(w http.ResponseWriter, r *http.Request) {
	var req struct {
		VpcEndpointIDs []string `json:"VpcEndpointIds"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	h.writeJSON(r, w, map[string]any{
		"VpcEndpoints":      toVpcEndpointsJSON(h.Backend.DescribeVpcEndpoints(h.reqContext(r), req.VpcEndpointIDs)),
		"VpcEndpointErrors": []any{},
	})
}

func (h *Handler) handleUpdateVpcEndpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		VpcOptions    map[string]string `json:"VpcOptions"`
		VpcEndpointID string            `json:"VpcEndpointId"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	endpoint, err := h.Backend.UpdateVpcEndpoint(h.reqContext(r), req.VpcEndpointID, req.VpcOptions)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"VpcEndpoint": toVpcEndpointJSON(endpoint)})
}

func (h *Handler) handleListVpcEndpoints(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(r, w, map[string]any{
		"VpcEndpointSummaryList": toVpcEndpointsJSON(h.Backend.ListVpcEndpoints(h.reqContext(r))),
	})
}

func (h *Handler) handleDeleteVpcEndpoint(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, elasticsearchVpcEndpoints+"/")
	endpoint, err := h.Backend.DeleteVpcEndpoint(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"VpcEndpointSummary": toVpcEndpointJSON(endpoint)})
}

func (h *Handler) handleListVpcEndpointAccess(w http.ResponseWriter, r *http.Request, domainName string) {
	accounts, err := h.Backend.ListVpcEndpointAccess(h.reqContext(r), domainName)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	principals := make([]authorizedPrincipalJSON, 0, len(accounts))
	for _, account := range accounts {
		principals = append(principals, authorizedPrincipalJSON{PrincipalType: "AWS_ACCOUNT", Principal: account})
	}

	h.writeJSON(r, w, map[string]any{"AuthorizedPrincipalList": principals})
}

func (h *Handler) handleListVpcEndpointsForDomain(w http.ResponseWriter, r *http.Request, domainName string) {
	h.writeJSON(r, w, map[string]any{
		"VpcEndpointSummaryList": toVpcEndpointsJSON(h.Backend.ListVpcEndpointsForDomain(h.reqContext(r), domainName)),
	})
}

func (h *Handler) handleRevokeVpcEndpointAccess(w http.ResponseWriter, r *http.Request, domainName string) {
	var req authorizeVpcEndpointAccessRequest
	if !h.decodeRequest(w, r, &req) {
		return
	}

	if err := h.Backend.RevokeVpcEndpointAccess(h.reqContext(r), domainName, req.Account); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{})
}

func toVpcEndpointsJSON(endpoints []*VpcEndpoint) []vpcEndpointJSON {
	result := make([]vpcEndpointJSON, 0, len(endpoints))
	for _, endpoint := range endpoints {
		result = append(result, toVpcEndpointJSON(endpoint))
	}

	return result
}
