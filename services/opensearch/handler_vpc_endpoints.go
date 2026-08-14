package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// vpcEndpointSummaryJSON mirrors types.VpcEndpointSummary: DomainArn, Status,
// VpcEndpointId, VpcEndpointOwner (opensearch@v1.75.4 types/types.go:3483-3498).
// No Endpoint, VpcOptions, or the internal-only StatusUntil clock field.
type vpcEndpointSummaryJSON struct {
	DomainArn        string `json:"DomainArn"`
	Status           string `json:"Status"`
	VpcEndpointID    string `json:"VpcEndpointId"`
	VpcEndpointOwner string `json:"VpcEndpointOwner"`
}

func toVpcEndpointSummary(ep *VpcEndpoint) vpcEndpointSummaryJSON {
	return vpcEndpointSummaryJSON{
		DomainArn:        ep.DomainArn,
		Status:           ep.Status,
		VpcEndpointID:    ep.VpcEndpointID,
		VpcEndpointOwner: ep.VpcEndpointOwner,
	}
}

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
	case rest == pathSuffixDescribe && r.Method == http.MethodPost:
		body, _ := httputils.ReadBody(r)
		var req struct {
			VpcEndpointIDs []string `json:"VpcEndpointIds"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		endpoints, errs := h.Backend.DescribeVpcEndpoints(req.VpcEndpointIDs)
		h.writeJSON(r, w, map[string]any{"VpcEndpoints": endpoints, "VpcEndpointErrors": errs})
	// POST /vpcEndpoints/update → UpdateVpcEndpoint. Real clients always POST here with
	// VpcEndpointId in the JSON body (api_op_UpdateVpcEndpoint.go, opensearch@v1.75.4:
	// no URL bindings at all -- the whole request travels in the body) -- gopherstack-l5ir.
	case rest == pathSuffixUpdate && r.Method == http.MethodPost:
		h.handleUpdateVpcEndpoint(w, r)
	// Root: Create/List.
	case rest == "" || rest == "/":
		h.handleVpcEndpointRootRoutes(w, r)
	// Per-ID: Delete.
	case strings.HasPrefix(rest, "/"):
		h.handleVpcEndpointIDRoutes(w, r, strings.TrimPrefix(rest, "/"))
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleUpdateVpcEndpoint handles UpdateVpcEndpoint: POST /vpcEndpoints/update,
// VpcEndpointId carried in the body (not the URL).
func (h *Handler) handleUpdateVpcEndpoint(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req struct {
		VpcOptions    map[string]any `json:"VpcOptions"`
		VpcEndpointID string         `json:"VpcEndpointId"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	ep, updateErr := h.Backend.UpdateVpcEndpoint(req.VpcEndpointID, req.VpcOptions)
	if updateErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{"VpcEndpoint": ep})
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
		summaries := make([]vpcEndpointSummaryJSON, 0, len(endpoints))
		for _, ep := range endpoints {
			summaries = append(summaries, toVpcEndpointSummary(ep))
		}
		// Real key is "VpcEndpointSummaryList", not "VpcEndpoints" -- verified
		// against ListVpcEndpointsOutput in api_op_ListVpcEndpoints.go
		// (opensearch@v1.75.4), matching the sibling ListVpcEndpointsForDomain.
		h.writeJSON(r, w, map[string]any{"VpcEndpointSummaryList": summaries})
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
				"DomainArn":          ep.DomainArn,
				"VpcEndpointOwner":   ep.VpcEndpointOwner,
			},
		})
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
		summaries := make([]vpcEndpointSummaryJSON, 0, len(endpoints))
		for _, ep := range endpoints {
			summaries = append(summaries, toVpcEndpointSummary(ep))
		}
		httputils.WriteJSON(
			r.Context(),
			w,
			http.StatusOK,
			map[string]any{"VpcEndpointSummaryList": summaries},
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
