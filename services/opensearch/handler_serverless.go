package opensearch

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// Serverless path segments.
const (
	slPathCollection      = "/collection"
	slPathCollections     = "/collections"
	slPathAccessPolicies  = "/accesspolicies"
	slPathSecurityConfigs = "/securityconfigs"
	slPathEncryptPolicies = "/encryptionpolicies"
	slPathNetworkPolicies = "/networksecuritypolicies"
)

// handleServerlessRoutes dispatches OpenSearch Serverless API requests.
// Paths are under /2021-11-01/opensearch/serverless/...
func (h *Handler) handleServerlessRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchServerlessPath)

	switch {
	case strings.HasPrefix(rest, slPathCollections):
		h.handleServerlessCollectionBatchRoutes(w, r)
	case strings.HasPrefix(rest, slPathCollection):
		h.handleServerlessCollectionRoutes(w, r, strings.TrimPrefix(rest, slPathCollection))
	case strings.HasPrefix(rest, slPathAccessPolicies):
		h.handleServerlessAccessPolicyRoutes(w, r, strings.TrimPrefix(rest, slPathAccessPolicies))
	case strings.HasPrefix(rest, slPathSecurityConfigs):
		h.handleServerlessSecurityConfigRoutes(w, r, strings.TrimPrefix(rest, slPathSecurityConfigs))
	case strings.HasPrefix(rest, slPathEncryptPolicies):
		h.handleServerlessEncryptionPolicyRoutes(w, r, strings.TrimPrefix(rest, slPathEncryptPolicies))
	case strings.HasPrefix(rest, slPathNetworkPolicies):
		h.handleServerlessNetworkPolicyRoutes(w, r, strings.TrimPrefix(rest, slPathNetworkPolicies))
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "serverless route not found")
	}
}

// handleServerlessCollectionRoutes handles /collection and /collection/{id}.
func (h *Handler) handleServerlessCollectionRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	// /collection (root) — POST creates, GET with no ID lists single by name.
	if rest == "" || rest == "/" {
		if r.Method != http.MethodPost {
			h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")

			return
		}

		body, _ := httputils.ReadBody(r)
		var req struct {
			Tags        map[string]string `json:"tags,omitempty"`
			Name        string            `json:"name"`
			Type        string            `json:"type"`
			Description string            `json:"description"`
			KmsKeyArn   string            `json:"kmsKeyArn"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}

		coll, err := h.Backend.CreateServerlessCollection(
			req.Name, req.Type, req.Description, req.KmsKeyArn, req.Tags,
		)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

			return
		}

		h.writeJSON(r, w, map[string]any{"createCollectionDetail": coll})

		return
	}

	// /collection/{id} — DELETE removes a collection.
	id := strings.Trim(rest, "/")
	if strings.Contains(id, "/") {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	if r.Method != http.MethodDelete {
		h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")

		return
	}

	coll, err := h.Backend.DeleteServerlessCollection(id)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{"deleteCollectionDetail": coll})
}

// handleServerlessCollectionBatchRoutes handles POST /collections (batch-get) and GET /collections (list).
func (h *Handler) handleServerlessCollectionBatchRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		body, _ := httputils.ReadBody(r)
		var req struct {
			Ids   []string `json:"ids"`
			Names []string `json:"names"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		colls := h.Backend.BatchGetServerlessCollections(req.Ids, req.Names)
		if colls == nil {
			colls = []*ServerlessCollection{}
		}
		h.writeJSON(r, w, map[string]any{"collectionDetails": colls})
	case http.MethodGet:
		colls := h.Backend.BatchGetServerlessCollections(nil, nil)
		if colls == nil {
			colls = []*ServerlessCollection{}
		}
		h.writeJSON(r, w, map[string]any{"collectionSummaries": colls})
	default:
		h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleServerlessAccessPolicyRoutes handles /accesspolicies and /accesspolicies/{name}.
func (h *Handler) handleServerlessAccessPolicyRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" || rest == "/" {
		switch r.Method {
		case http.MethodPost:
			body, _ := httputils.ReadBody(r)
			var req struct {
				Name        string `json:"name"`
				Type        string `json:"type"`
				Policy      string `json:"policy"`
				Description string `json:"description"`
			}
			if len(body) > 0 {
				_ = json.Unmarshal(body, &req)
			}
			ap, err := h.Backend.CreateServerlessAccessPolicy(req.Type, req.Name, req.Description, req.Policy)
			if err != nil {
				h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

				return
			}
			h.writeJSON(r, w, map[string]any{"accessPolicyDetail": ap})
		case http.MethodGet:
			policyType := r.URL.Query().Get("type")
			aps := h.Backend.ListServerlessAccessPolicies(policyType)
			if aps == nil {
				aps = []*ServerlessAccessPolicy{}
			}
			h.writeJSON(r, w, map[string]any{"accessPolicySummaries": aps})
		default:
			h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}

		return
	}

	name := strings.Trim(rest, "/")
	policyType := r.URL.Query().Get("type")

	switch r.Method {
	case http.MethodGet:
		ap, err := h.Backend.GetServerlessAccessPolicy(policyType, name)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"accessPolicyDetail": ap})
	case http.MethodPut:
		body, _ := httputils.ReadBody(r)
		var req struct {
			Policy        string `json:"policy"`
			Description   string `json:"description"`
			PolicyVersion string `json:"policyVersion"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		ap, err := h.Backend.UpdateServerlessAccessPolicy(policyType, name, req.Description, req.Policy, req.PolicyVersion)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"accessPolicyDetail": ap})
	case http.MethodDelete:
		err := h.Backend.DeleteServerlessAccessPolicy(policyType, name)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"accessPolicyDetail": map[string]any{"name": name, "type": policyType}})
	default:
		h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleServerlessSecurityConfigRoutes handles /securityconfigs and /securityconfigs/{id}.
func (h *Handler) handleServerlessSecurityConfigRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" || rest == "/" {
		switch r.Method {
		case http.MethodPost:
			body, _ := httputils.ReadBody(r)
			var req struct {
				SamlOptions *ServerlessSAMLOptions `json:"samlOptions,omitempty"`
				Type        string                 `json:"type"`
				Description string                 `json:"description"`
			}
			if len(body) > 0 {
				_ = json.Unmarshal(body, &req)
			}
			sc, err := h.Backend.CreateServerlessSecurityConfig(req.Type, req.Description, req.SamlOptions)
			if err != nil {
				h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

				return
			}
			h.writeJSON(r, w, map[string]any{"securityConfigDetail": sc})
		case http.MethodGet:
			configType := r.URL.Query().Get("type")
			scs := h.Backend.ListServerlessSecurityConfigs(configType)
			if scs == nil {
				scs = []*ServerlessSecurityConfig{}
			}
			h.writeJSON(r, w, map[string]any{"securityConfigSummaries": scs})
		default:
			h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}

		return
	}

	id := strings.Trim(rest, "/")

	switch r.Method {
	case http.MethodGet:
		sc, err := h.Backend.GetServerlessSecurityConfig(id)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"securityConfigDetail": sc})
	case http.MethodPut:
		body, _ := httputils.ReadBody(r)
		var req struct {
			SamlOptions   *ServerlessSAMLOptions `json:"samlOptions,omitempty"`
			Description   string                 `json:"description"`
			ConfigVersion string                 `json:"configVersion"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		sc, err := h.Backend.UpdateServerlessSecurityConfig(id, req.Description, req.ConfigVersion, req.SamlOptions)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"securityConfigDetail": sc})
	case http.MethodDelete:
		err := h.Backend.DeleteServerlessSecurityConfig(id)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"securityConfigDetail": map[string]any{"id": id}})
	default:
		h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleServerlessEncryptionPolicyRoutes handles /encryptionpolicies and /encryptionpolicies/{name}.
func (h *Handler) handleServerlessEncryptionPolicyRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" || rest == "/" {
		switch r.Method {
		case http.MethodPost:
			body, _ := httputils.ReadBody(r)
			var req struct {
				Name        string `json:"name"`
				Type        string `json:"type"`
				Policy      string `json:"policy"`
				Description string `json:"description"`
			}
			if len(body) > 0 {
				_ = json.Unmarshal(body, &req)
			}
			ep, err := h.Backend.CreateServerlessEncryptionPolicy(req.Type, req.Name, req.Description, req.Policy)
			if err != nil {
				h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

				return
			}
			h.writeJSON(r, w, map[string]any{"encryptionPolicyDetail": ep})
		case http.MethodGet:
			policyType := r.URL.Query().Get("type")
			eps := h.Backend.ListServerlessEncryptionPolicies(policyType)
			if eps == nil {
				eps = []*ServerlessEncryptionPolicy{}
			}
			h.writeJSON(r, w, map[string]any{"encryptionPolicySummaries": eps})
		default:
			h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}

		return
	}

	name := strings.Trim(rest, "/")
	policyType := r.URL.Query().Get("type")

	switch r.Method {
	case http.MethodGet:
		ep, err := h.Backend.GetServerlessEncryptionPolicy(policyType, name)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"encryptionPolicyDetail": ep})
	case http.MethodPut:
		body, _ := httputils.ReadBody(r)
		var req struct {
			Policy        string `json:"policy"`
			Description   string `json:"description"`
			PolicyVersion string `json:"policyVersion"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		ep, err := h.Backend.UpdateServerlessEncryptionPolicy(policyType, name, req.Description, req.Policy, req.PolicyVersion)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"encryptionPolicyDetail": ep})
	case http.MethodDelete:
		err := h.Backend.DeleteServerlessEncryptionPolicy(policyType, name)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"encryptionPolicyDetail": map[string]any{"name": name, "type": policyType}})
	default:
		h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleServerlessNetworkPolicyRoutes handles /networksecuritypolicies and /networksecuritypolicies/{name}.
func (h *Handler) handleServerlessNetworkPolicyRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" || rest == "/" {
		switch r.Method {
		case http.MethodPost:
			body, _ := httputils.ReadBody(r)
			var req struct {
				Name        string `json:"name"`
				Type        string `json:"type"`
				Policy      string `json:"policy"`
				Description string `json:"description"`
			}
			if len(body) > 0 {
				_ = json.Unmarshal(body, &req)
			}
			np, err := h.Backend.CreateServerlessNetworkPolicy(req.Type, req.Name, req.Description, req.Policy)
			if err != nil {
				h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

				return
			}
			h.writeJSON(r, w, map[string]any{"networkPolicyDetail": np})
		case http.MethodGet:
			policyType := r.URL.Query().Get("type")
			nps := h.Backend.ListServerlessNetworkPolicies(policyType)
			if nps == nil {
				nps = []*ServerlessNetworkPolicy{}
			}
			h.writeJSON(r, w, map[string]any{"networkPolicySummaries": nps})
		default:
			h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}

		return
	}

	name := strings.Trim(rest, "/")
	policyType := r.URL.Query().Get("type")

	if r.Method == http.MethodDelete {
		err := h.Backend.DeleteServerlessNetworkPolicy(policyType, name)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"networkPolicyDetail": map[string]any{"name": name, "type": policyType}})

		return
	}

	h.writeError(r, w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}
