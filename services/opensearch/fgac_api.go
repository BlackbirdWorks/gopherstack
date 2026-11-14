package opensearch

import (
	"encoding/json/v2"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	secStatusOK      = "OK"
	secStatusCreated = "CREATED"
)

func writeSecStatus(w http.ResponseWriter, status int, code, message string) {
	writeRawJSON(w, status, map[string]any{jsonKeyStatusLower: code, "message": message})
}

func writeSecError(w http.ResponseWriter, e *SecError) {
	if e.Code == jsonKeyError {
		body := map[string]any{jsonKeyStatusLower: jsonKeyError, jsonKeyReason: e.Message}
		maps.Copy(body, e.Extra)

		writeRawJSON(w, e.Status, body)

		return
	}

	writeSecStatus(w, e.Status, e.Code, e.Message)
}

// serveSecurityAPI serves the /_plugins/_security REST API of a domain.
func (h *Handler) serveSecurityAPI(
	w http.ResponseWriter, r *http.Request, domain string, perms EffectivePerms, sub string,
) {
	segs := strings.Split(sub, "/")

	switch {
	case sub == "authinfo" || sub == "whoami":
		h.writeAuthInfo(w, domain, perms)
	case sub == "health":
		writeRawJSON(w, http.StatusOK, map[string]any{"message": nil, "mode": "strict", jsonKeyStatusLower: "UP"})
	case sub == "api/account":
		h.serveAccount(w, r, domain, perms)
	case len(segs) >= 2 && segs[0] == "api" && securityKind(segs[1]).valid():
		name := ""
		if len(segs) > 2 { //nolint:mnd // api/<kind>/<name>
			name = strings.Join(segs[2:], "/")
		}

		h.serveSecurityResource(w, r, domain, perms, securityKind(segs[1]), name)
	default:
		writeRawJSON(w, http.StatusBadRequest, map[string]any{
			jsonKeyError: "no handler found for uri [/" + securityAPIPrefix + sub + "] and method [" + r.Method + "]",
		})
	}
}

func (h *Handler) writeAuthInfo(w http.ResponseWriter, domain string, perms EffectivePerms) {
	p := perms.Principal
	tenants := h.Backend.SecurityTenantAccess(domain, perms)

	roles := perms.Roles
	if roles == nil {
		roles = []string{}
	}

	attrNames := make([]string, 0, len(p.Attributes))
	for k := range p.Attributes {
		attrNames = append(attrNames, "attr.internal."+k)
	}

	sort.Strings(attrNames)

	writeRawJSON(w, http.StatusOK, map[string]any{
		"user":                   p.String(),
		"user_name":              p.Name,
		"user_requested_tenant":  nil,
		"remote_address":         "",
		jsonKeyBackendRoles:      nonNilStrings(p.BackendRoles),
		"custom_attribute_names": attrNames,
		"roles":                  roles,
		"tenants":                tenants,
		"principal":              nil,
		"peer_certificates":      "0",
		"sso_logout_url":         nil,
	})
}

func (h *Handler) serveAccount(w http.ResponseWriter, r *http.Request, domain string, perms EffectivePerms) {
	p := perms.Principal

	switch r.Method {
	case http.MethodGet:
		h.writeAccount(w, domain, perms)
	case http.MethodPut:
		var req struct {
			CurrentPassword string `json:"current_password"`
			Password        string `json:"password"`
		}

		body, _ := httputils.ReadBody(r)
		if err := json.Unmarshal(body, &req, jsonx.V1()); err != nil {
			writeSecError(w, secBadRequest("Could not parse content of request."))

			return
		}

		if p.IAM {
			writeSecError(w, secForbidden("Resource '"+p.Name+"' is read-only."))

			return
		}

		if err := h.Backend.ChangeOwnPassword(domain, p.Name, req.CurrentPassword, req.Password); err != nil {
			writeSecError(w, err)

			return
		}

		writeSecStatus(w, http.StatusOK, secStatusOK, "'"+p.Name+"' updated.")
	default:
		writeSecError(w, &SecError{Status: http.StatusMethodNotAllowed, Code: "METHOD_NOT_ALLOWED",
			Message: "Method " + r.Method + " not allowed for this action."})
	}
}

func (h *Handler) writeAccount(w http.ResponseWriter, domain string, perms EffectivePerms) {
	p := perms.Principal
	tenants := h.Backend.SecurityTenantAccess(domain, perms)

	roles := perms.Roles
	if roles == nil {
		roles = []string{}
	}

	writeRawJSON(w, http.StatusOK, map[string]any{
		"user_name":              p.Name,
		"is_reserved":            false,
		"is_hidden":              false,
		"is_internal_user":       !p.IAM && p.Name != anonymousUserName,
		"user_requested_tenant":  nil,
		jsonKeyBackendRoles:      nonNilStrings(p.BackendRoles),
		"custom_attribute_names": []string{},
		"tenants":                tenants,
		"roles":                  roles,
	})
}

func (h *Handler) serveSecurityResource(
	w http.ResponseWriter, r *http.Request, domain string, perms EffectivePerms, kind securityKind, name string,
) {
	if !perms.hasRole(roleAllAccess, roleSecurityManager, roleSecurityRESTAPI) {
		roles := slices.Sorted(slices.Values(perms.Roles))
		writeSecStatus(w, http.StatusForbidden, "FORBIDDEN", "No permission to access REST API: User "+
			perms.Principal.Name+" with Security roles ["+strings.Join(roles, ", ")+
			"] does not have any role privileged for admin access")

		return
	}

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		h.getSecurityResource(w, domain, kind, name)
	case http.MethodPut:
		h.putSecurityResource(w, r, domain, kind, name)
	case http.MethodDelete:
		h.deleteSecurityResource(w, domain, kind, name)
	case http.MethodPatch:
		h.patchSecurityResource(w, r, domain, kind, name)
	default:
		writeSecError(w, &SecError{Status: http.StatusMethodNotAllowed, Code: "METHOD_NOT_ALLOWED",
			Message: "Method " + r.Method + " not allowed for this action."})
	}
}

func (h *Handler) getSecurityResource(w http.ResponseWriter, domain string, kind securityKind, name string) {
	if name == "" {
		list, err := h.Backend.SecurityList(domain, kind)
		if err != nil {
			writeSecError(w, err)

			return
		}

		writeRawJSON(w, http.StatusOK, list)

		return
	}

	v, err := h.Backend.SecurityGet(domain, kind, name)
	if err != nil {
		writeSecError(w, err)

		return
	}

	writeRawJSON(w, http.StatusOK, map[string]any{name: v})
}

func (h *Handler) putSecurityResource(
	w http.ResponseWriter,
	r *http.Request,
	domain string,
	kind securityKind,
	name string,
) {
	if name == "" {
		writeSecError(w, secBadRequest("Method PUT is not allowed without a resource name."))

		return
	}

	body, _ := httputils.ReadBody(r)

	created, err := h.Backend.SecurityPut(domain, kind, name, body)
	if err != nil {
		writeSecError(w, err)

		return
	}

	if created {
		writeSecStatus(w, http.StatusCreated, secStatusCreated, "'"+name+"' created.")

		return
	}

	writeSecStatus(w, http.StatusOK, secStatusOK, "'"+name+"' updated.")
}

func (h *Handler) deleteSecurityResource(w http.ResponseWriter, domain string, kind securityKind, name string) {
	if name == "" {
		writeSecError(w, secBadRequest("Method DELETE is not allowed without a resource name."))

		return
	}

	if err := h.Backend.SecurityDelete(domain, kind, name); err != nil {
		writeSecError(w, err)

		return
	}

	writeSecStatus(w, http.StatusOK, secStatusOK, "'"+name+"' deleted.")
}

func (h *Handler) patchSecurityResource(
	w http.ResponseWriter,
	r *http.Request,
	domain string,
	kind securityKind,
	name string,
) {
	body, _ := httputils.ReadBody(r)

	var ops []patchOp
	if err := json.Unmarshal(body, &ops, jsonx.V1()); err != nil {
		writeSecError(w, secBadRequest("Could not parse content of request."))

		return
	}

	if err := h.Backend.SecurityPatch(domain, kind, name, ops); err != nil {
		writeSecError(w, err)

		return
	}

	msg := "Resource updated."
	if name != "" {
		msg = "'" + name + "' updated."
	}

	writeSecStatus(w, http.StatusOK, secStatusOK, msg)
}
