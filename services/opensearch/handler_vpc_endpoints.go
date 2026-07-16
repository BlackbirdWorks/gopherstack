package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// authorizeVpcEndpointAccessRequest is the JSON request body for AuthorizeVpcEndpointAccess.
type authorizeVpcEndpointAccessRequest struct {
	Account string `json:"Account"`
	Service string `json:"Service"`
}

// authorizeVpcEndpointAccessOutput is the JSON response for AuthorizeVpcEndpointAccess.
type authorizeVpcEndpointAccessOutput struct {
	AuthorizedPrincipal authorizedPrincipalJSON `json:"AuthorizedPrincipal"`
}

// authorizedPrincipalJSON is the JSON representation of an authorized principal.
type authorizedPrincipalJSON struct {
	Principal     string `json:"Principal"`
	PrincipalType string `json:"PrincipalType"`
}

func (h *Handler) handleAuthorizeVpcEndpointAccess(
	w http.ResponseWriter,
	r *http.Request,
	domainName string,
) {
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

	principal, authErr := h.Backend.AuthorizeVpcEndpointAccess(domainName, req.Account, req.Service)
	if authErr != nil {
		if errors.Is(authErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", authErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", authErr.Error())
		}

		return
	}

	h.writeJSON(r, w, authorizeVpcEndpointAccessOutput{
		AuthorizedPrincipal: authorizedPrincipalJSON{
			Principal:     principal.Principal,
			PrincipalType: principal.PrincipalType,
		},
	})
}

// handleVpcEndpointsRoutes handles VPC endpoint routes.
func (h *Handler) handleVpcEndpointsRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchVpcEndpointsPath)

	switch {
	// POST /vpcEndpoints/describe → DescribeVpcEndpoints
	case rest == "/describe" && r.Method == http.MethodPost:
		body, _ := httputils.ReadBody(r)
		var req struct {
			VpcEndpointIDs []string `json:"VpcEndpointIds"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		endpoints, errs := h.Backend.DescribeVpcEndpoints(req.VpcEndpointIDs)
		h.writeJSON(r, w, map[string]any{"VpcEndpoints": endpoints, "VpcEndpointErrors": errs})
	// Root: Create/List.
	case rest == "" || rest == "/":
		h.handleVpcEndpointRootRoutes(w, r)
	// Per-ID: Delete/Update.
	case strings.HasPrefix(rest, "/"):
		h.handleVpcEndpointIDRoutes(w, r, strings.TrimPrefix(rest, "/"))
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleVpcEndpointRootRoutes handles /vpcEndpoints and /vpcEndpoints/ requests.
func (h *Handler) handleVpcEndpointRootRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			VpcOptions map[string]any `json:"VpcOptions"`
			DomainArn  string         `json:"DomainArn"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		ep, createErr := h.Backend.CreateVpcEndpoint(req.DomainArn, req.VpcOptions)
		if createErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"VpcEndpoint": ep})
	case http.MethodGet:
		endpoints := h.Backend.ListVpcEndpoints()
		if endpoints == nil {
			endpoints = []*VpcEndpoint{}
		}
		h.writeJSON(r, w, map[string]any{"VpcEndpoints": endpoints})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleVpcEndpointIDRoutes handles /vpcEndpoints/{id} requests.
func (h *Handler) handleVpcEndpointIDRoutes(
	w http.ResponseWriter,
	r *http.Request,
	endpointID string,
) {
	switch r.Method {
	case http.MethodDelete:
		ep, err := h.Backend.DeleteVpcEndpoint(endpointID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{
			"VpcEndpointSummary": map[string]any{
				jsonKeyVpcEndpointID: ep.VpcEndpointID,
				jsonKeyStatus:        ep.Status,
			},
		})
	case http.MethodPut:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			VpcOptions map[string]any `json:"VpcOptions"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		ep, updateErr := h.Backend.UpdateVpcEndpoint(endpointID, req.VpcOptions)
		if updateErr != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"VpcEndpoint": ep})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// dispatchDomainGetVpcRoutes handles VPC-related GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetVpcRoutes(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	switch {
	case strings.HasSuffix(trimmed, "/vpcEndpoints"):
		// ListVpcEndpointsForDomain
		domainName, _ := strings.CutSuffix(trimmed, "/vpcEndpoints")
		domain, descErr := h.Backend.DescribeDomain(domainName)
		var domainArn string
		if descErr == nil {
			domainArn = domain.ARN
		}
		endpoints := h.Backend.ListVpcEndpointsForDomain(domainArn)
		httputils.WriteJSON(
			r.Context(),
			w,
			http.StatusOK,
			map[string]any{"VpcEndpointSummaryList": endpoints},
		)
	case strings.HasSuffix(trimmed, "/listVpcEndpointAccess"):
		// ListVpcEndpointAccess
		domainName, _ := strings.CutSuffix(trimmed, "/listVpcEndpointAccess")
		principals, _ := h.Backend.ListVpcEndpointAccess(domainName)
		if principals == nil {
			principals = []AuthorizedPrincipal{}
		}
		httputils.WriteJSON(
			r.Context(),
			w,
			http.StatusOK,
			map[string]any{"AuthorizedPrincipalList": principals},
		)
	default:
		return false
	}

	return true
}
