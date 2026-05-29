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
		h.handleServerlessSecurityConfigRoutes(
			w,
			r,
			strings.TrimPrefix(rest, slPathSecurityConfigs),
		)
	case strings.HasPrefix(rest, slPathEncryptPolicies):
		h.handleServerlessEncryptionPolicyRoutes(
			w,
			r,
			strings.TrimPrefix(rest, slPathEncryptPolicies),
		)
	case strings.HasPrefix(rest, slPathNetworkPolicies):
		h.handleServerlessNetworkPolicyRoutes(w, r, strings.TrimPrefix(rest, slPathNetworkPolicies))
	default:
		h.writeError(
			r,
			w,
			http.StatusNotFound,
			"ResourceNotFoundException",
			"serverless route not found",
		)
	}
}

// handleServerlessCollectionRoutes handles /collection and /collection/{id}.
func (h *Handler) handleServerlessCollectionRoutes(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
) {
	if rest == "" || rest == "/" {
		h.handleServerlessCollectionCreate(w, r)

		return
	}

	h.handleServerlessCollectionDelete(w, r, rest)
}

func (h *Handler) handleServerlessCollectionCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(
			r,
			w,
			http.StatusMethodNotAllowed,
			"MethodNotAllowedException",
			"method not allowed",
		)

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
		req.Name,
		req.Type,
		req.Description,
		req.KmsKeyArn,
		req.Tags,
	)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{"createCollectionDetail": coll})
}

func (h *Handler) handleServerlessCollectionDelete(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
) {
	id := strings.Trim(rest, "/")
	if strings.Contains(id, "/") {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	if r.Method != http.MethodDelete {
		h.writeError(
			r,
			w,
			http.StatusMethodNotAllowed,
			"MethodNotAllowedException",
			"method not allowed",
		)

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
		h.writeError(
			r,
			w,
			http.StatusMethodNotAllowed,
			"MethodNotAllowedException",
			"method not allowed",
		)
	}
}

// serverlessPolicyCRUD holds the backend operations and response keys for a typed named policy resource.
type serverlessPolicyCRUD struct {
	create       func(policyType, name, description, policy string) (any, error)
	list         func(policyType string) any
	get          func(policyType, name string) (any, error)
	update       func(policyType, name, description, policy, version string) (any, error)
	deleteByName func(policyType, name string) error
	singleKey    string
	listKey      string
}

// handleServerlessPolicyCRUDRoot handles POST (create) and GET (list) at the root path.
func (h *Handler) handleServerlessPolicyCRUDRoot(
	w http.ResponseWriter,
	r *http.Request,
	ops serverlessPolicyCRUD,
) {
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
		result, err := ops.create(req.Type, req.Name, req.Description, req.Policy)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{ops.singleKey: result})
	case http.MethodGet:
		policyType := r.URL.Query().Get("type")
		results := ops.list(policyType)
		h.writeJSON(r, w, map[string]any{ops.listKey: results})
	default:
		h.writeError(
			r,
			w,
			http.StatusMethodNotAllowed,
			"MethodNotAllowedException",
			"method not allowed",
		)
	}
}

// handleServerlessPolicyCRUDByName handles GET/PUT/DELETE for a named policy resource.
func (h *Handler) handleServerlessPolicyCRUDByName(
	w http.ResponseWriter,
	r *http.Request,
	name, policyType string,
	ops serverlessPolicyCRUD,
) {
	switch r.Method {
	case http.MethodGet:
		result, err := ops.get(policyType, name)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{ops.singleKey: result})
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
		result, err := ops.update(policyType, name, req.Description, req.Policy, req.PolicyVersion)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{ops.singleKey: result})
	case http.MethodDelete:
		err := ops.deleteByName(policyType, name)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(
			r,
			w,
			map[string]any{ops.singleKey: map[string]any{"name": name, "type": policyType}},
		)
	default:
		h.writeError(
			r,
			w,
			http.StatusMethodNotAllowed,
			"MethodNotAllowedException",
			"method not allowed",
		)
	}
}

// handleServerlessPolicyCRUD dispatches named policy resources (access, encryption).
// root: POST=create, GET=list; by-name: GET=get, PUT=update, DELETE=delete.
func (h *Handler) handleServerlessPolicyCRUD(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
	ops serverlessPolicyCRUD,
) {
	if rest == "" || rest == "/" {
		h.handleServerlessPolicyCRUDRoot(w, r, ops)

		return
	}

	h.handleServerlessPolicyCRUDByName(
		w,
		r,
		strings.Trim(rest, "/"),
		r.URL.Query().Get("type"),
		ops,
	)
}

func (h *Handler) accessPolicyCRUD() serverlessPolicyCRUD {
	return serverlessPolicyCRUD{
		create: func(pt, name, desc, policy string) (any, error) {
			return h.Backend.CreateServerlessAccessPolicy(pt, name, desc, policy)
		},
		list: func(pt string) any {
			aps := h.Backend.ListServerlessAccessPolicies(pt)
			if aps == nil {
				aps = []*ServerlessAccessPolicy{}
			}

			return aps
		},
		get: func(pt, name string) (any, error) {
			return h.Backend.GetServerlessAccessPolicy(pt, name)
		},
		update: func(pt, name, desc, policy, ver string) (any, error) {
			return h.Backend.UpdateServerlessAccessPolicy(pt, name, desc, policy, ver)
		},
		deleteByName: h.Backend.DeleteServerlessAccessPolicy,
		singleKey:    "accessPolicyDetail",
		listKey:      "accessPolicySummaries",
	}
}

// handleServerlessAccessPolicyRoutes handles /accesspolicies and /accesspolicies/{name}.
func (h *Handler) handleServerlessAccessPolicyRoutes(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
) {
	h.handleServerlessPolicyCRUD(w, r, rest, h.accessPolicyCRUD())
}

// handleServerlessSecurityConfigRoutes handles /securityconfigs and /securityconfigs/{id}.
func (h *Handler) handleServerlessSecurityConfigRoutes(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
) {
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
			sc, err := h.Backend.CreateServerlessSecurityConfig(
				req.Type,
				req.Description,
				req.SamlOptions,
			)
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
			h.writeError(
				r,
				w,
				http.StatusMethodNotAllowed,
				"MethodNotAllowedException",
				"method not allowed",
			)
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
		sc, err := h.Backend.UpdateServerlessSecurityConfig(
			id,
			req.Description,
			req.ConfigVersion,
			req.SamlOptions,
		)
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
		h.writeError(
			r,
			w,
			http.StatusMethodNotAllowed,
			"MethodNotAllowedException",
			"method not allowed",
		)
	}
}

func (h *Handler) encryptionPolicyCRUD() serverlessPolicyCRUD {
	return serverlessPolicyCRUD{
		create: func(pt, name, desc, policy string) (any, error) {
			return h.Backend.CreateServerlessEncryptionPolicy(pt, name, desc, policy)
		},
		list: func(pt string) any {
			eps := h.Backend.ListServerlessEncryptionPolicies(pt)
			if eps == nil {
				eps = []*ServerlessEncryptionPolicy{}
			}

			return eps
		},
		get: func(pt, name string) (any, error) {
			return h.Backend.GetServerlessEncryptionPolicy(pt, name)
		},
		update: func(pt, name, desc, policy, ver string) (any, error) {
			return h.Backend.UpdateServerlessEncryptionPolicy(pt, name, desc, policy, ver)
		},
		deleteByName: h.Backend.DeleteServerlessEncryptionPolicy,
		singleKey:    "encryptionPolicyDetail",
		listKey:      "encryptionPolicySummaries",
	}
}

// handleServerlessEncryptionPolicyRoutes handles /encryptionpolicies and /encryptionpolicies/{name}.
func (h *Handler) handleServerlessEncryptionPolicyRoutes(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
) {
	h.handleServerlessPolicyCRUD(w, r, rest, h.encryptionPolicyCRUD())
}

// handleServerlessNetworkPolicyRoutes handles /networksecuritypolicies and /networksecuritypolicies/{name}.
func (h *Handler) handleServerlessNetworkPolicyRoutes(
	w http.ResponseWriter,
	r *http.Request,
	rest string,
) {
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
			np, err := h.Backend.CreateServerlessNetworkPolicy(
				req.Type,
				req.Name,
				req.Description,
				req.Policy,
			)
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
			h.writeError(
				r,
				w,
				http.StatusMethodNotAllowed,
				"MethodNotAllowedException",
				"method not allowed",
			)
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
		h.writeJSON(
			r,
			w,
			map[string]any{"networkPolicyDetail": map[string]any{"name": name, "type": policyType}},
		)

		return
	}

	h.writeError(
		r,
		w,
		http.StatusMethodNotAllowed,
		"MethodNotAllowedException",
		"method not allowed",
	)
}
