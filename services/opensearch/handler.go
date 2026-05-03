package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	openSearchPathPrefix      = "/2021-01-01/opensearch/domain"
	openSearchTagsPath        = "/2021-01-01/tags"
	openSearchTagsRemoval     = "/2021-01-01/tags-removal"
	openSearchCCPath          = "/2021-01-01/opensearch/cc"
	openSearchDirectQueryPath = "/2021-01-01/opensearch/directQueryDataSource"
	openSearchPackagesPath    = "/2021-01-01/packages"
	openSearchServiceSwPath   = "/2021-01-01/opensearch/serviceSoftwareUpdate"
	openSearchApplicationPath = "/2021-01-01/opensearch/application"
	// pkgPathParts is the number of path segments after the associate prefix (PackageID/DomainName).
	pkgPathParts = 2
	// opUnknown is the sentinel returned when no operation can be determined from a request.
	opUnknown = "Unknown"
)

// Handler is the HTTP handler for OpenSearch operations.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new OpenSearch Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
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
			strings.HasPrefix(path, openSearchCCPath) ||
			strings.HasPrefix(path, openSearchDirectQueryPath) ||
			strings.HasPrefix(path, openSearchPackagesPath) ||
			strings.HasPrefix(path, openSearchServiceSwPath) ||
			strings.HasPrefix(path, openSearchApplicationPath) ||
			path == openSearchTagsPath ||
			path == openSearchTagsRemoval
	}
}

// GetSupportedOperations returns supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcceptInboundConnection",
		"AddDataSource",
		"AddDirectQueryDataSource",
		"AddTags",
		"AssociatePackage",
		"AssociatePackages",
		"AuthorizeVpcEndpointAccess",
		"CancelDomainConfigChange",
		"CancelServiceSoftwareUpdate",
		"CreateApplication",
		"CreateDomain",
		"DeleteDomain",
		"DescribeDomain",
		"ListDomainNames",
	}
}

// Reset clears the handler's backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "es" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this OpenSearch instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// ExtractOperation returns the operation name from a request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	if op := extractNonDomainOperation(path, method); op != "" {
		return op
	}

	return extractDomainOperation(path, method)
}

// extractNonDomainOperation derives the operation name from non-domain paths.
// Returns an empty string when the path does not match any known non-domain route.
func extractNonDomainOperation(path, method string) string {
	if op := extractCCOrDirectQueryOp(path, method); op != "" {
		return op
	}

	if op := extractPackageOp(path, method); op != "" {
		return op
	}

	return extractTagOrSoftwareOp(path, method)
}

// extractCCOrDirectQueryOp handles cross-cluster and direct-query operation extraction.
func extractCCOrDirectQueryOp(path, method string) string {
	switch {
	case strings.HasPrefix(path, openSearchCCPath) &&
		strings.Contains(path, "/inboundConnection/") && strings.HasSuffix(path, "/accept") &&
		method == http.MethodPut:
		return "AcceptInboundConnection"
	case strings.HasPrefix(path, openSearchDirectQueryPath) && method == http.MethodPost:
		return "AddDirectQueryDataSource"
	case strings.HasPrefix(path, openSearchApplicationPath) && method == http.MethodPost:
		return "CreateApplication"
	case strings.HasPrefix(path, openSearchServiceSwPath) && method == http.MethodPost:
		return "CancelServiceSoftwareUpdate"
	}

	return ""
}

// extractPackageOp handles package route operation extraction.
func extractPackageOp(path, method string) string {
	after, ok := strings.CutPrefix(path, openSearchPackagesPath)
	if !ok {
		return ""
	}

	if strings.HasPrefix(after, "/associate/") && method == http.MethodPost {
		return "AssociatePackage"
	}

	if after == "/associateMultiple" && method == http.MethodPost {
		return "AssociatePackages"
	}

	return ""
}

// extractTagOrSoftwareOp handles tag and service-software route operation extraction.
func extractTagOrSoftwareOp(path, method string) string {
	switch {
	case path == openSearchTagsPath && method == http.MethodGet:
		return "ListTags"
	case path == openSearchTagsPath && method == http.MethodPost:
		return "AddTags"
	case path == openSearchTagsRemoval && method == http.MethodPost:
		return "RemoveTags"
	}

	return ""
}

// extractDomainOperation derives the operation name from domain-prefix paths.
func extractDomainOperation(path, method string) string {
	rest := strings.TrimPrefix(path, openSearchPathPrefix)

	switch {
	case rest == "" || rest == "/":
		if method == http.MethodPost {
			return "CreateDomain"
		}

		if method == http.MethodGet {
			return "ListDomainNames"
		}

		return opUnknown
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return "DescribeDomain"
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return "DeleteDomain"
	case strings.HasPrefix(rest, "/") && method == http.MethodPost:
		return extractDomainSubOperation(rest)
	}

	return opUnknown
}

// extractDomainSubOperation derives the operation from a domain POST sub-route.
func extractDomainSubOperation(rest string) string {
	trimmed := strings.TrimPrefix(rest, "/")

	switch {
	case strings.HasSuffix(trimmed, "/dataSource"):
		return "AddDataSource"
	case strings.HasSuffix(trimmed, "/authorizeVpcEndpointAccess"):
		return "AuthorizeVpcEndpointAccess"
	case strings.HasSuffix(trimmed, "/config/cancel"):
		return "CancelDomainConfigChange"
	}

	return opUnknown
}

// ExtractResource returns the primary resource identifier from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	// Cross-cluster: extract connection ID
	if strings.HasPrefix(path, openSearchCCPath) {
		rest := strings.TrimPrefix(path, openSearchCCPath+"/inboundConnection/")

		return strings.TrimSuffix(rest, "/accept")
	}

	// Direct query data source: no ID in path
	if strings.HasPrefix(path, openSearchDirectQueryPath) {
		return ""
	}

	// Packages: extract package ID
	if strings.HasPrefix(path, openSearchPackagesPath) {
		rest := strings.TrimPrefix(path, openSearchPackagesPath+"/associate/")
		parts := strings.SplitN(rest, "/", pkgPathParts)
		if len(parts) > 0 {
			return parts[0]
		}

		return ""
	}

	// Tag routes: no domain name in path
	if path == openSearchTagsPath || path == openSearchTagsRemoval {
		return ""
	}

	// Domain path prefix
	rest := strings.TrimPrefix(path, openSearchPathPrefix+"/")
	if rest == path {
		return ""
	}

	// Extract domain name (first segment)
	if before, _, ok := strings.Cut(rest, "/"); ok {
		return before
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

	if h.dispatchNonDomainRoutes(w, r) {
		return
	}

	h.dispatchDomainRoutes(w, r)
}

// dispatchNonDomainRoutes handles all non-domain prefix paths.
// Returns true if the request was handled.
func (h *Handler) dispatchNonDomainRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case strings.HasPrefix(path, openSearchCCPath):
		h.handleCCRoutes(w, r)
	case strings.HasPrefix(path, openSearchDirectQueryPath):
		h.handleDirectQueryRoutes(w, r)
	case strings.HasPrefix(path, openSearchPackagesPath):
		h.handlePackageRoutes(w, r)
	case strings.HasPrefix(path, openSearchServiceSwPath):
		h.handleServiceSoftwareRoutes(w, r)
	case strings.HasPrefix(path, openSearchApplicationPath):
		h.handleApplicationRoutes(w, r)
	default:
		return false
	}

	return true
}

// dispatchDomainRoutes handles requests under /2021-01-01/opensearch/domain/...
func (h *Handler) dispatchDomainRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchPathPrefix)

	switch {
	case (rest == "" || rest == "/") && r.Method == http.MethodPost:
		h.handleCreateDomain(w, r)
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		h.handleListDomainNames(w, r)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodGet:
		h.dispatchDomainGetRoutes(w, r, rest)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodDelete:
		h.handleDeleteDomain(w, r, domainNameFromRest(rest))
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodPost:
		h.handleDomainSubRoutes(w, r, rest)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// dispatchDomainGetRoutes handles GET requests under a domain path.
func (h *Handler) dispatchDomainGetRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := domainNameFromRest(rest)
	if before, ok := strings.CutSuffix(trimmed, "/config"); ok {
		h.handleDescribeDomainConfig(w, r, before)
	} else {
		h.handleDescribeDomain(w, r, trimmed)
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
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req domainJSON
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if req.DomainName == "" {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "DomainName is required")

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
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDescribeDomain(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDeleteDomain(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DeleteDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleListDomainNames(w http.ResponseWriter, r *http.Request) {
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

	h.writeJSON(r, w, domainListJSON{DomainNames: entries})
}

func toDomainStatusJSON(d *Domain) domainStatusJSON {
	return domainStatusJSON{
		DomainName:                  d.Name,
		ARN:                         d.ARN,
		EngineVersion:               d.EngineVersion,
		Endpoint:                    d.Endpoint,
		Processing:                  false,
		DomainProcessingStatus:      domainStatusActive,
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

func (h *Handler) writeError(r *http.Request, w http.ResponseWriter, status int, code, message string) {
	ctx := r.Context()
	logger.Load(ctx).ErrorContext(r.Context(), "opensearch error", "code", code, "message", message)
	w.Header().Set("x-amzn-ErrorType", code)
	httputils.WriteJSON(ctx, w, status, errorResponseJSON{Message: message})
}

func (h *Handler) writeJSON(r *http.Request, w http.ResponseWriter, v any) {
	httputils.WriteJSON(r.Context(), w, http.StatusOK, v)
}

type listTagsOutput struct {
	TagList []svcTags.KV `json:"TagList"`
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
		h.writeJSON(r, w, &listTagsOutput{TagList: []svcTags.KV{}})

		return
	}

	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	h.writeJSON(r, w, &listTagsOutput{TagList: tagList})
}

type addTagsInput struct {
	ARN     string       `json:"ARN"`
	TagList []svcTags.KV `json:"TagList"`
}

func (h *Handler) handleAddTags(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addTagsInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	tagMap := make(map[string]string, len(req.TagList))
	for _, t := range req.TagList {
		tagMap[t.Key] = t.Value
	}

	if addErr := h.Backend.AddTags(req.ARN, tagMap); addErr != nil {
		if errors.Is(addErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", addErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	w.WriteHeader(http.StatusOK)
}

type removeTagsInput struct {
	ARN     string   `json:"ARN"`
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleRemoveTags(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req removeTagsInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if removeErr := h.Backend.RemoveTags(req.ARN, req.TagKeys); removeErr != nil {
		if errors.Is(removeErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", removeErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", removeErr.Error())
		}

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDescribeDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("domain %s/config not found", name))
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	activeStatus := opensearchConfigStatus{State: domainStatusActive}
	out := describeDomainConfigOutput{}
	out.DomainConfig.EngineVersion = opensearchConfigValue{Options: domain.EngineVersion, Status: activeStatus}
	out.DomainConfig.ClusterConfig = opensearchConfigValue{
		Options: map[string]any{
			"InstanceType":  domain.ClusterConfig.InstanceType,
			"InstanceCount": domain.ClusterConfig.InstanceCount,
		},
		Status: activeStatus,
	}
	out.DomainConfig.EBSOptions = opensearchConfigValue{Options: map[string]any{}, Status: activeStatus}
	out.DomainConfig.AccessPolicies = opensearchConfigValue{Options: "", Status: activeStatus}
	out.DomainConfig.AdvancedOptions = opensearchConfigValue{Options: map[string]any{}, Status: activeStatus}
	h.writeJSON(r, w, &out)
}

// handleDomainSubRoutes handles POST requests under /2021-01-01/opensearch/domain/{name}/...
func (h *Handler) handleDomainSubRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	// rest looks like "/{domainName}/dataSource", "/{domainName}/authorizeVpcEndpointAccess", etc.
	trimmed := strings.TrimPrefix(rest, "/")

	switch {
	case strings.HasSuffix(trimmed, "/dataSource"):
		domainName := strings.TrimSuffix(trimmed, "/dataSource")
		h.handleAddDataSource(w, r, domainName)
	case strings.HasSuffix(trimmed, "/authorizeVpcEndpointAccess"):
		domainName := strings.TrimSuffix(trimmed, "/authorizeVpcEndpointAccess")
		h.handleAuthorizeVpcEndpointAccess(w, r, domainName)
	case strings.HasSuffix(trimmed, "/config/cancel"):
		domainName := strings.TrimSuffix(trimmed, "/config/cancel")
		h.handleCancelDomainConfigChange(w, r, domainName)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleCCRoutes handles cross-cluster connection routes.
func (h *Handler) handleCCRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchCCPath)

	// PUT /2021-01-01/opensearch/cc/inboundConnection/{ConnectionId}/accept
	if strings.HasPrefix(rest, "/inboundConnection/") && strings.HasSuffix(rest, "/accept") &&
		r.Method == http.MethodPut {
		connID := strings.TrimSuffix(strings.TrimPrefix(rest, "/inboundConnection/"), "/accept")
		h.handleAcceptInboundConnection(w, r, connID)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleDirectQueryRoutes handles direct query data source routes.
func (h *Handler) handleDirectQueryRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchDirectQueryPath)

	// POST /2021-01-01/opensearch/directQueryDataSource
	if (rest == "" || rest == "/") && r.Method == http.MethodPost {
		h.handleAddDirectQueryDataSource(w, r)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handlePackageRoutes handles package routes.
func (h *Handler) handlePackageRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchPackagesPath)

	switch {
	// POST /2021-01-01/packages/associate/{PackageID}/{DomainName}
	case strings.HasPrefix(rest, "/associate/") && r.Method == http.MethodPost:
		parts := strings.SplitN(strings.TrimPrefix(rest, "/associate/"), "/", pkgPathParts)
		if len(parts) != pkgPathParts {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid associate package path")

			return
		}

		h.handleAssociatePackage(w, r, parts[0], parts[1])
	// POST /2021-01-01/packages/associateMultiple
	case rest == "/associateMultiple" && r.Method == http.MethodPost:
		h.handleAssociatePackages(w, r)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleServiceSoftwareRoutes handles service software update routes.
func (h *Handler) handleServiceSoftwareRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchServiceSwPath)

	// POST /2021-01-01/opensearch/serviceSoftwareUpdate/cancel
	if rest == "/cancel" && r.Method == http.MethodPost {
		h.handleCancelServiceSoftwareUpdate(w, r)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleApplicationRoutes handles application routes.
func (h *Handler) handleApplicationRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchApplicationPath)

	// POST /2021-01-01/opensearch/application
	if (rest == "" || rest == "/") && r.Method == http.MethodPost {
		h.handleCreateApplication(w, r)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// acceptInboundConnectionOutput is the JSON response for AcceptInboundConnection.
type acceptInboundConnectionOutput struct {
	Connection inboundConnectionJSON `json:"Connection"`
}

// inboundConnectionJSON is the JSON representation of an inbound connection.
type inboundConnectionJSON struct {
	ConnectionID     string                `json:"ConnectionId"`
	ConnectionStatus inboundConnStatusJSON `json:"ConnectionStatus"`
}

// inboundConnStatusJSON is the JSON representation of a connection status.
type inboundConnStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

func (h *Handler) handleAcceptInboundConnection(w http.ResponseWriter, r *http.Request, connectionID string) {
	conn, err := h.Backend.AcceptInboundConnection(connectionID)
	if err != nil {
		if errors.Is(err, ErrInvalidParameter) {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		} else {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, acceptInboundConnectionOutput{
		Connection: inboundConnectionJSON{
			ConnectionID: conn.ConnectionID,
			ConnectionStatus: inboundConnStatusJSON{
				StatusCode: conn.Status,
			},
		},
	})
}

// addDataSourceRequest is the JSON request body for AddDataSource.
type addDataSourceRequest struct {
	DataSourceType any    `json:"DataSourceType"`
	Name           string `json:"Name"`
	Description    string `json:"Description"`
}

// addDataSourceOutput is the JSON response for AddDataSource.
type addDataSourceOutput struct {
	Message string `json:"Message"`
}

func (h *Handler) handleAddDataSource(w http.ResponseWriter, r *http.Request, domainName string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addDataSourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	msg, addErr := h.Backend.AddDataSource(domainName, req.Name, req.Description, fmt.Sprintf("%v", req.DataSourceType))
	if addErr != nil {
		switch {
		case errors.Is(addErr, ErrDomainNotFound):
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", addErr.Error())
		case errors.Is(addErr, ErrDataSourceAlreadyExists):
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", addErr.Error())
		default:
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	h.writeJSON(r, w, addDataSourceOutput{Message: msg})
}

// addDirectQueryDataSourceRequest is the JSON request body for AddDirectQueryDataSource.
type addDirectQueryDataSourceRequest struct {
	DataSourceName string   `json:"DataSourceName"`
	Description    string   `json:"Description"`
	DataSourceType any      `json:"DataSourceType"`
	OpenSearchArns []string `json:"OpenSearchArns"`
}

// addDirectQueryDataSourceOutput is the JSON response for AddDirectQueryDataSource.
type addDirectQueryDataSourceOutput struct {
	DataSourceArn string `json:"DataSourceArn"`
}

func (h *Handler) handleAddDirectQueryDataSource(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addDirectQueryDataSourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	dsARN, addErr := h.Backend.AddDirectQueryDataSource(
		req.DataSourceName,
		req.Description,
		fmt.Sprintf("%v", req.DataSourceType),
		req.OpenSearchArns,
	)
	if addErr != nil {
		if errors.Is(addErr, ErrDataSourceAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", addErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	h.writeJSON(r, w, addDirectQueryDataSourceOutput{DataSourceArn: dsARN})
}

// associatePackageOutput is the JSON response for AssociatePackage.
type associatePackageOutput struct {
	DomainPackageDetails domainPackageDetailsJSON `json:"DomainPackageDetails"`
}

// domainPackageDetailsJSON is the JSON representation of package domain details.
type domainPackageDetailsJSON struct {
	PackageID           string `json:"PackageID"`
	DomainName          string `json:"DomainName"`
	DomainPackageStatus string `json:"DomainPackageStatus"`
}

func (h *Handler) handleAssociatePackage(w http.ResponseWriter, r *http.Request, packageID, domainName string) {
	details, err := h.Backend.AssociatePackage(packageID, domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, associatePackageOutput{
		DomainPackageDetails: domainPackageDetailsJSON{
			PackageID:           details.PackageID,
			DomainName:          details.DomainName,
			DomainPackageStatus: details.State,
		},
	})
}

// associatePackagesRequest is the JSON request body for AssociatePackages.
type associatePackagesRequest struct {
	DomainName  string            `json:"DomainName"`
	PackageList []packageForAssoc `json:"PackageList"`
}

// packageForAssoc is a package entry in AssociatePackages request.
type packageForAssoc struct {
	PackageID string `json:"PackageID"`
}

// associatePackagesOutput is the JSON response for AssociatePackages.
type associatePackagesOutput struct {
	DomainPackageDetailsList []domainPackageDetailsJSON `json:"DomainPackageDetailsList"`
}

func (h *Handler) handleAssociatePackages(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req associatePackagesRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	packageIDs := make([]string, 0, len(req.PackageList))
	for _, p := range req.PackageList {
		packageIDs = append(packageIDs, p.PackageID)
	}

	details, assocErr := h.Backend.AssociatePackages(req.DomainName, packageIDs)
	if assocErr != nil {
		if errors.Is(assocErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", assocErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", assocErr.Error())
		}

		return
	}

	outList := make([]domainPackageDetailsJSON, 0, len(details))
	for _, d := range details {
		outList = append(outList, domainPackageDetailsJSON{
			PackageID:           d.PackageID,
			DomainName:          d.DomainName,
			DomainPackageStatus: d.State,
		})
	}

	h.writeJSON(r, w, associatePackagesOutput{DomainPackageDetailsList: outList})
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

// cancelDomainConfigChangeRequest is the JSON request body for CancelDomainConfigChange.
type cancelDomainConfigChangeRequest struct {
	DryRun bool `json:"DryRun"`
}

// cancelDomainConfigChangeOutput is the JSON response for CancelDomainConfigChange.
type cancelDomainConfigChangeOutput struct {
	CancelledChangeIDs        []string `json:"CancelledChangeIds"`
	CancelledChangeProperties []any    `json:"CancelledChangeProperties"`
	DryRun                    bool     `json:"DryRun"`
}

func (h *Handler) handleCancelDomainConfigChange(w http.ResponseWriter, r *http.Request, domainName string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelDomainConfigChangeRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	ids, dryRun, cancelErr := h.Backend.CancelDomainConfigChange(domainName, req.DryRun)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelDomainConfigChangeOutput{
		CancelledChangeIDs:        ids,
		CancelledChangeProperties: []any{},
		DryRun:                    dryRun,
	})
}

// cancelServiceSoftwareUpdateRequest is the JSON request body for CancelServiceSoftwareUpdate.
type cancelServiceSoftwareUpdateRequest struct {
	DomainName string `json:"DomainName"`
}

// serviceSoftwareOptionsJSON is the JSON representation of service software options.
type serviceSoftwareOptionsJSON struct {
	CurrentVersion      string `json:"CurrentVersion"`
	NewVersion          string `json:"NewVersion"`
	UpdateStatus        string `json:"UpdateStatus"`
	Description         string `json:"Description"`
	AutomatedUpdateDate string `json:"AutomatedUpdateDate"`
	UpdateAvailable     bool   `json:"UpdateAvailable"`
	Cancellable         bool   `json:"Cancellable"`
	OptionalDeployment  bool   `json:"OptionalDeployment"`
}

// cancelServiceSoftwareUpdateOutput is the JSON response for CancelServiceSoftwareUpdate.
type cancelServiceSoftwareUpdateOutput struct {
	ServiceSoftwareOptions serviceSoftwareOptionsJSON `json:"ServiceSoftwareOptions"`
}

func (h *Handler) handleCancelServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelServiceSoftwareUpdateRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	opts, cancelErr := h.Backend.CancelServiceSoftwareUpdate(req.DomainName)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelServiceSoftwareUpdateOutput{
		ServiceSoftwareOptions: serviceSoftwareOptionsJSON{
			CurrentVersion:      opts.CurrentVersion,
			NewVersion:          opts.NewVersion,
			UpdateAvailable:     opts.UpdateAvailable,
			Cancellable:         opts.Cancellable,
			UpdateStatus:        opts.UpdateStatus,
			Description:         opts.Description,
			AutomatedUpdateDate: opts.AutomatedUpdateDate,
			OptionalDeployment:  opts.OptionalDeployment,
		},
	})
}

// createApplicationRequest is the JSON request body for CreateApplication.
type createApplicationRequest struct {
	Name        string          `json:"Name"`
	AppConfigs  []appConfigJSON `json:"AppConfigs"`
	DataSources []appDSJSON     `json:"DataSources"`
}

// appConfigJSON is the JSON representation of an AppConfig.
type appConfigJSON struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// appDSJSON is the JSON representation of an application data source.
type appDSJSON struct {
	DataSourceArn string `json:"DataSourceArn"`
}

// createApplicationOutput is the JSON response for CreateApplication.
type createApplicationOutput struct {
	ID          string          `json:"Id"`
	Name        string          `json:"Name"`
	ARN         string          `json:"Arn"`
	AppConfigs  []appConfigJSON `json:"AppConfigs"`
	DataSources []appDSJSON     `json:"DataSources"`
}

func (h *Handler) handleCreateApplication(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createApplicationRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	appConfigs := make([]AppConfig, 0, len(req.AppConfigs))
	for _, ac := range req.AppConfigs {
		appConfigs = append(appConfigs, AppConfig(ac))
	}

	dataSources := make([]AppDataSource, 0, len(req.DataSources))
	for _, ds := range req.DataSources {
		dataSources = append(dataSources, AppDataSource(ds))
	}

	app, createErr := h.Backend.CreateApplication(req.Name, appConfigs, dataSources)
	if createErr != nil {
		if errors.Is(createErr, ErrApplicationAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", createErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())
		}

		return
	}

	outConfigs := make([]appConfigJSON, 0, len(app.AppConfigs))
	for _, ac := range app.AppConfigs {
		outConfigs = append(outConfigs, appConfigJSON(ac))
	}

	outDS := make([]appDSJSON, 0, len(app.DataSources))
	for _, ds := range app.DataSources {
		outDS = append(outDS, appDSJSON(ds))
	}

	h.writeJSON(r, w, createApplicationOutput{
		ID:          app.ID,
		Name:        app.Name,
		ARN:         app.ARN,
		AppConfigs:  outConfigs,
		DataSources: outDS,
	})
}
