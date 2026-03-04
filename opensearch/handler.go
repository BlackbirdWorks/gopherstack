package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	openSearchPathPrefix  = "/2021-01-01/opensearch/domain"
	openSearchTagsPath    = "/2021-01-01/tags"
	openSearchTagsRemoval = "/2021-01-01/tags-removal"
)

// Handler is the HTTP handler for OpenSearch operations.
type Handler struct {
	Backend   *InMemoryBackend
	Logger    *slog.Logger
	AccountID string
	Region    string
}

// NewHandler creates a new OpenSearch Handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "OpenSearch" }

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathSubdomain }

// RouteMatcher returns a matcher that selects OpenSearch requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, openSearchPathPrefix) ||
			path == openSearchTagsPath ||
			path == openSearchTagsRemoval
	}
}

// GetSupportedOperations returns supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDomain",
		"DescribeDomain",
		"DeleteDomain",
		"ListDomainNames",
	}
}

// ExtractOperation returns the operation name from a request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	rest := strings.TrimPrefix(path, openSearchPathPrefix)

	switch {
	case rest == "" || rest == "/":
		if method == http.MethodPost {
			return "CreateDomain"
		}

		if method == http.MethodGet {
			return "ListDomainNames"
		}

		return "Unknown"
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return "DescribeDomain"
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return "DeleteDomain"
	}

	return "Unknown"
}

// ExtractResource returns the domain name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	rest := strings.TrimPrefix(path, openSearchPathPrefix+"/")

	if rest == path {
		return ""
	}

	return strings.TrimSuffix(rest, "/")
}

// domainClusterConfig holds the cluster configuration request parameters for a domain.
type domainClusterConfig struct {
	InstanceType  string `json:"InstanceType"`
	InstanceCount int    `json:"InstanceCount"`
}

// domainJSON is the JSON request body for CreateDomain.
type domainJSON struct {
	ClusterConfig *domainClusterConfig `json:"ClusterConfig"`
	DomainName    string               `json:"DomainName"`
	EngineVersion string               `json:"EngineVersion"`
}

// domainStatusJSON is the JSON response for domain operations.
type domainStatusJSON struct {
	DomainName                  string                      `json:"DomainName"`
	ARN                         string                      `json:"ARN"`
	EngineVersion               string                      `json:"EngineVersion"`
	Endpoint                    string                      `json:"Endpoint"`
	DomainProcessingStatus      string                      `json:"DomainProcessingStatus"`
	ClusterConfig               clusterConfigJSON           `json:"ClusterConfig"`
	EBSOptions                  ebsOptionsJSON              `json:"EBSOptions"`
	CognitoOptions              cognitoOptionsJSON          `json:"CognitoOptions"`
	EncryptionAtRestOptions     encryptAtRestOptionsJSON    `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions nodeToNodeEncryptJSON       `json:"NodeToNodeEncryptionOptions"`
	AdvancedSecurityOptions     advancedSecurityOptionsJSON `json:"AdvancedSecurityOptions"`
	Processing                  bool                        `json:"Processing"`
}

// ebsOptionsJSON is the JSON representation of EBS options.
type ebsOptionsJSON struct {
	EBSEnabled bool `json:"EBSEnabled"`
}

// cognitoOptionsJSON is the JSON representation of Cognito options.
type cognitoOptionsJSON struct {
	Enabled bool `json:"Enabled"`
}

// encryptAtRestOptionsJSON is the JSON representation of encryption at rest options.
type encryptAtRestOptionsJSON struct {
	Enabled bool `json:"Enabled"`
}

// nodeToNodeEncryptJSON is the JSON representation of node-to-node encryption options.
type nodeToNodeEncryptJSON struct {
	Enabled bool `json:"Enabled"`
}

// advancedSecurityOptionsJSON is the JSON representation of advanced security options.
type advancedSecurityOptionsJSON struct {
	Enabled                     bool `json:"Enabled"`
	InternalUserDatabaseEnabled bool `json:"InternalUserDatabaseEnabled"`
}

// clusterConfigJSON is the JSON representation of cluster config.
type clusterConfigJSON struct {
	InstanceType  string `json:"InstanceType"`
	InstanceCount int    `json:"InstanceCount"`
}

// domainStatusWrapJSON wraps the domain status in a DomainStatus key.
type domainStatusWrapJSON struct {
	DomainStatus domainStatusJSON `json:"DomainStatus"`
}

// domainListJSON is the response for ListDomainNames.
type domainListJSON struct {
	DomainNames []domainNameEntry `json:"DomainNames"`
}

// domainNameEntry is an element of the ListDomainNames response.
type domainNameEntry struct {
	DomainName    string `json:"DomainName"`
	EngineVersion string `json:"EngineVersion"`
}

// ServeHTTP implements [http.Handler] for the OpenSearch service.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.handleTagRoutes(w, r) {
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, openSearchPathPrefix)

	switch {
	case (rest == "" || rest == "/") && r.Method == http.MethodPost:
		h.handleCreateDomain(w, r)
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		h.handleListDomainNames(w, r)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodGet:
		trimmed := domainNameFromRest(rest)
		if before, ok := strings.CutSuffix(trimmed, "/config"); ok {
			h.handleDescribeDomainConfig(w, r, before)
		} else {
			h.handleDescribeDomain(w, r, trimmed)
		}
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodDelete:
		h.handleDeleteDomain(w, r, domainNameFromRest(rest))
	default:
		h.writeError(w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func domainNameFromRest(rest string) string {
	name := strings.TrimPrefix(rest, "/")

	return strings.TrimSuffix(name, "/")
}

// handleTagRoutes processes /2021-01-01/tags and /2021-01-01/tags-removal requests.
// Returns true if the request was handled.
func (h *Handler) handleTagRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case path == openSearchTagsPath && r.Method == http.MethodGet:
		h.handleListTags(w, r)

		return true
	case path == openSearchTagsPath && r.Method == http.MethodPost:
		h.handleAddTags(w, r)

		return true
	case path == openSearchTagsRemoval && r.Method == http.MethodPost:
		h.handleRemoveTags(w, r)

		return true
	}

	return false
}

// Handle satisfies the Echo handler interface.
func (h *Handler) Handle(c *echo.Context) error {
	h.ServeHTTP(c.Response(), c.Request())

	return nil
}

// Handler returns the Echo HandlerFunc for this service.
func (h *Handler) Handler() echo.HandlerFunc {
	return h.Handle
}

func (h *Handler) handleCreateDomain(w http.ResponseWriter, r *http.Request) {
	body, err := httputil.ReadBody(r)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req domainJSON
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if req.DomainName == "" {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "DomainName is required")

		return
	}

	var cfg ClusterConfig
	if req.ClusterConfig != nil {
		cfg.InstanceType = req.ClusterConfig.InstanceType
		cfg.InstanceCount = req.ClusterConfig.InstanceCount
	}

	domain, err := h.Backend.CreateDomain(req.DomainName, req.EngineVersion, cfg)
	if err != nil {
		if errors.Is(err, ErrDomainAlreadyExists) {
			h.writeError(w, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
		} else {
			h.writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDescribeDomain(w http.ResponseWriter, _ *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDeleteDomain(w http.ResponseWriter, _ *http.Request, name string) {
	domain, err := h.Backend.DeleteDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleListDomainNames(w http.ResponseWriter, _ *http.Request) {
	names := h.Backend.ListDomainNames()
	entries := make([]domainNameEntry, 0, len(names))

	for _, name := range names {
		d, err := h.Backend.DescribeDomain(name)
		if err != nil {
			continue
		}

		entries = append(entries, domainNameEntry{
			DomainName:    name,
			EngineVersion: d.EngineVersion,
		})
	}

	h.writeJSON(w, domainListJSON{DomainNames: entries})
}

func toDomainStatusJSON(d *Domain) domainStatusJSON {
	return domainStatusJSON{
		DomainName:                  d.Name,
		ARN:                         d.ARN,
		EngineVersion:               d.EngineVersion,
		Endpoint:                    d.Endpoint,
		Processing:                  false,
		DomainProcessingStatus:      "Active",
		EBSOptions:                  ebsOptionsJSON{EBSEnabled: false},
		CognitoOptions:              cognitoOptionsJSON{Enabled: false},
		EncryptionAtRestOptions:     encryptAtRestOptionsJSON{Enabled: false},
		NodeToNodeEncryptionOptions: nodeToNodeEncryptJSON{Enabled: false},
		AdvancedSecurityOptions:     advancedSecurityOptionsJSON{Enabled: false},
		ClusterConfig: clusterConfigJSON{
			InstanceType:  d.ClusterConfig.InstanceType,
			InstanceCount: d.ClusterConfig.InstanceCount,
		},
	}
}

type errorResponseJSON struct {
	Message string `json:"message"`
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, message string) {
	h.Logger.Error("opensearch error", "code", code, "message", message)
	w.Header().Set("x-amzn-ErrorType", code)
	httputil.WriteJSON(h.Logger, w, status, errorResponseJSON{Message: message})
}

func (h *Handler) writeJSON(w http.ResponseWriter, v any) {
	httputil.WriteJSON(h.Logger, w, http.StatusOK, v)
}

type opensearchTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type listTagsOutput struct {
	TagList []opensearchTag `json:"TagList"`
}

type opensearchConfigStatus struct {
	State string `json:"State"`
}

type opensearchConfigValue struct {
	Options any                    `json:"Options"`
	Status  opensearchConfigStatus `json:"Status"`
}

// domainConfigFields holds the per-feature configuration values for a domain.
type domainConfigFields struct {
	EngineVersion   opensearchConfigValue `json:"EngineVersion"`
	ClusterConfig   opensearchConfigValue `json:"ClusterConfig"`
	EBSOptions      opensearchConfigValue `json:"EBSOptions"`
	AccessPolicies  opensearchConfigValue `json:"AccessPolicies"`
	AdvancedOptions opensearchConfigValue `json:"AdvancedOptions"`
}

type describeDomainConfigOutput struct {
	DomainConfig domainConfigFields `json:"DomainConfig"`
}

func (h *Handler) handleListTags(w http.ResponseWriter, r *http.Request) {
	domainARN := r.URL.Query().Get("arn")

	tags, err := h.Backend.ListTags(domainARN)
	if err != nil {
		h.writeJSON(w, &listTagsOutput{TagList: []opensearchTag{}})

		return
	}

	tagList := make([]opensearchTag, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, opensearchTag{Key: k, Value: v})
	}

	h.writeJSON(w, &listTagsOutput{TagList: tagList})
}

type addTagsInput struct {
	ARN     string          `json:"ARN"`
	TagList []opensearchTag `json:"TagList"`
}

func (h *Handler) handleAddTags(w http.ResponseWriter, r *http.Request) {
	body, err := httputil.ReadBody(r)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addTagsInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	tagMap := make(map[string]string, len(req.TagList))
	for _, t := range req.TagList {
		tagMap[t.Key] = t.Value
	}

	_ = h.Backend.AddTags(req.ARN, tagMap)
	w.WriteHeader(http.StatusOK)
}

type removeTagsInput struct {
	ARN     string   `json:"ARN"`
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleRemoveTags(w http.ResponseWriter, r *http.Request) {
	body, err := httputil.ReadBody(r)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req removeTagsInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	_ = h.Backend.RemoveTags(req.ARN, req.TagKeys)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDescribeDomainConfig(w http.ResponseWriter, _ *http.Request, name string) {
	_, err := h.Backend.DescribeDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(w, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("domain %s/config not found", name))
		} else {
			h.writeError(w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	activeStatus := opensearchConfigStatus{State: "Active"}
	out := describeDomainConfigOutput{}
	out.DomainConfig.EngineVersion = opensearchConfigValue{Options: "", Status: activeStatus}
	out.DomainConfig.ClusterConfig = opensearchConfigValue{Options: map[string]any{}, Status: activeStatus}
	out.DomainConfig.EBSOptions = opensearchConfigValue{Options: map[string]any{}, Status: activeStatus}
	out.DomainConfig.AccessPolicies = opensearchConfigValue{Options: "", Status: activeStatus}
	out.DomainConfig.AdvancedOptions = opensearchConfigValue{Options: map[string]any{}, Status: activeStatus}
	h.writeJSON(w, &out)
}
