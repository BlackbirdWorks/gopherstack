package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	elasticsearchPathPrefix     = "/2015-01-01/es/domain"
	elasticsearchTagsPath       = "/2015-01-01/tags"
	elasticsearchTagsRemove     = "/2015-01-01/tags-removal"
	elasticsearchDomainInfo     = "/2015-01-01/es/domain-info"
	elasticsearchServiceRole    = "/2015-01-01/es/role"
	elasticsearchSoftwareUpdate = "/2015-01-01/es/serviceSoftwareUpdate"
	elasticsearchCCSInbound     = "/2015-01-01/es/ccs/inboundConnection"
	elasticsearchCCSOutbound    = "/2015-01-01/es/ccs/outboundConnection"
	elasticsearchVpcEndpoints   = "/2015-01-01/es/vpcEndpoints"
	elasticsearchPackages       = "/2015-01-01/packages"

	opUnknown = "Unknown"
)

// Handler is the HTTP handler for Elasticsearch operations.
type Handler struct {
	Backend   *InMemoryBackend
	ops       map[string]http.HandlerFunc
	AccountID string
	Region    string
}

// NewHandler creates a new Elasticsearch Handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// buildOps builds the cached dispatch table for fixed-path Elasticsearch routes.
// Routes with dynamic path segments (e.g., domain name, connection ID) are
// handled separately via the domain and prefix routers.
func (h *Handler) buildOps() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		http.MethodPost + " " + elasticsearchDomainInfo:                 h.handleDescribeElasticsearchDomains,
		http.MethodGet + " " + elasticsearchTagsPath:                    h.handleListTags,
		http.MethodPost + " " + elasticsearchTagsPath:                   h.handleAddTags,
		http.MethodPost + " " + elasticsearchTagsRemove:                 h.handleRemoveTags,
		http.MethodDelete + " " + elasticsearchServiceRole:              h.handleDeleteElasticsearchServiceRole,
		http.MethodPost + " " + elasticsearchSoftwareUpdate + "/cancel": h.handleCancelElasticsearchServiceSoftwareUpdate,
		http.MethodPost + " " + elasticsearchCCSOutbound:                h.handleCreateOutboundCrossClusterSearchConnection,
		http.MethodPost + " " + elasticsearchVpcEndpoints:               h.handleCreateVpcEndpoint,
		http.MethodPost + " " + elasticsearchPackages:                   h.handleCreatePackage,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Elasticsearch" }

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathSubdomain }

// RouteMatcher returns a matcher that selects Elasticsearch requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, elasticsearchPathPrefix) ||
			path == elasticsearchDomainInfo ||
			path == elasticsearchTagsPath ||
			path == elasticsearchTagsRemove ||
			path == elasticsearchServiceRole ||
			strings.HasPrefix(path, elasticsearchSoftwareUpdate) ||
			strings.HasPrefix(path, elasticsearchCCSInbound) ||
			path == elasticsearchCCSOutbound ||
			path == elasticsearchVpcEndpoints ||
			strings.HasPrefix(path, elasticsearchPackages)
	}
}

// GetSupportedOperations returns supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcceptInboundCrossClusterSearchConnection",
		"AddTags",
		"AssociatePackage",
		"AuthorizeVpcEndpointAccess",
		"CancelDomainConfigChange",
		"CancelElasticsearchServiceSoftwareUpdate",
		"CreateElasticsearchDomain",
		"CreateOutboundCrossClusterSearchConnection",
		"CreatePackage",
		"CreateVpcEndpoint",
		"DeleteElasticsearchDomain",
		"DeleteElasticsearchServiceRole",
		"DescribeElasticsearchDomain",
		"DescribeElasticsearchDomainConfig",
		"DescribeElasticsearchDomains",
		"ListDomainNames",
		"ListTags",
		"RemoveTags",
		"UpdateElasticsearchDomainConfig",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "es" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Elasticsearch instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// ExtractOperation returns the operation name from a request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	if op := extractNonDomainOperation(path, method); op != "" {
		return op
	}

	return extractDomainOperation(strings.TrimPrefix(path, elasticsearchPathPrefix), method)
}

// extractNonDomainOperation returns the operation name for paths not under the domain prefix.
func extractNonDomainOperation(path, method string) string {
	if op := extractTagAndServiceOperation(path, method); op != "" {
		return op
	}

	return extractCCSVpcPackageOperation(path, method)
}

// extractTagAndServiceOperation handles tag, domain-info, role, and software-update paths.
func extractTagAndServiceOperation(path, method string) string {
	switch {
	case path == elasticsearchDomainInfo && method == http.MethodPost:
		return "DescribeElasticsearchDomains"
	case path == elasticsearchTagsPath && method == http.MethodGet:
		return "ListTags"
	case path == elasticsearchTagsPath && method == http.MethodPost:
		return "AddTags"
	case path == elasticsearchTagsRemove && method == http.MethodPost:
		return "RemoveTags"
	case path == elasticsearchServiceRole && method == http.MethodDelete:
		return "DeleteElasticsearchServiceRole"
	case path == elasticsearchSoftwareUpdate+"/cancel" && method == http.MethodPost:
		return "CancelElasticsearchServiceSoftwareUpdate"
	}

	return ""
}

// extractCCSVpcPackageOperation handles CCS, VPC endpoint, and package paths.
func extractCCSVpcPackageOperation(path, method string) string {
	switch {
	case strings.HasPrefix(path, elasticsearchCCSInbound+"/") &&
		strings.HasSuffix(path, "/accept") &&
		method == http.MethodPut:
		return "AcceptInboundCrossClusterSearchConnection"
	case path == elasticsearchCCSOutbound && method == http.MethodPost:
		return "CreateOutboundCrossClusterSearchConnection"
	case path == elasticsearchVpcEndpoints && method == http.MethodPost:
		return "CreateVpcEndpoint"
	case strings.HasPrefix(path, elasticsearchPackages+"/associate/") && method == http.MethodPost:
		return "AssociatePackage"
	case path == elasticsearchPackages && method == http.MethodPost:
		return "CreatePackage"
	}

	return ""
}

// extractDomainOperation returns the operation name for paths under the domain prefix.
// rest is the path with the domain prefix stripped.
func extractDomainOperation(rest, method string) string {
	if rest == "" || rest == "/" {
		return extractRootDomainOperation(method)
	}

	if !strings.HasPrefix(rest, "/") {
		return opUnknown
	}

	return extractSubDomainOperation(rest, method)
}

// extractSubDomainOperation resolves operations on specific domain paths (rest starts with "/").
func extractSubDomainOperation(rest, method string) string {
	switch {
	case strings.HasSuffix(rest, "/config/cancel") && method == http.MethodPost:
		return "CancelDomainConfigChange"
	case strings.HasSuffix(rest, "/authorizeVpcEndpointAccess") && method == http.MethodPost:
		return "AuthorizeVpcEndpointAccess"
	case strings.HasSuffix(rest, "/config") && method == http.MethodPost:
		return "UpdateElasticsearchDomainConfig"
	case strings.HasSuffix(rest, "/config"):
		return "DescribeElasticsearchDomainConfig"
	case method == http.MethodGet:
		return "DescribeElasticsearchDomain"
	case method == http.MethodDelete:
		return "DeleteElasticsearchDomain"
	}

	return opUnknown
}

func extractRootDomainOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateElasticsearchDomain"
	case http.MethodGet:
		return "ListDomainNames"
	}

	return opUnknown
}

// ExtractResource returns the domain name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	rest := strings.TrimPrefix(path, elasticsearchPathPrefix+"/")

	if rest == path {
		return ""
	}

	return strings.TrimSuffix(rest, "/")
}

// domainClusterConfig holds the cluster configuration request parameters.
type domainClusterConfig struct {
	InstanceType  string `json:"InstanceType"`
	InstanceCount int    `json:"InstanceCount"`
}

// domainEBSOptions holds the EBS options request parameters.
type domainEBSOptions struct {
	VolumeType string `json:"VolumeType"`
	VolumeSize int    `json:"VolumeSize"`
	EBSEnabled bool   `json:"EBSEnabled"`
}

// domainJSON is the JSON request body for CreateElasticsearchDomain.
type domainJSON struct {
	ClusterConfig        *domainClusterConfig `json:"ElasticsearchClusterConfig"`
	EBSOptions           *domainEBSOptions    `json:"EBSOptions"`
	DomainName           string               `json:"DomainName"`
	ElasticsearchVersion string               `json:"ElasticsearchVersion"`
}

// domainStatusJSON is the JSON response for domain operations.
type domainStatusJSON struct {
	DomainName                 string             `json:"DomainName"`
	DomainID                   string             `json:"DomainId"`
	ARN                        string             `json:"ARN"`
	ElasticsearchVersion       string             `json:"ElasticsearchVersion"`
	Endpoint                   string             `json:"Endpoint"`
	DomainProcessingStatus     string             `json:"DomainProcessingStatus"`
	ElasticsearchClusterConfig clusterConfigJSON  `json:"ElasticsearchClusterConfig"`
	EBSOptions                 ebsOptionsJSON     `json:"EBSOptions"`
	CognitoOptions             cognitoOptionsJSON `json:"CognitoOptions"`
	Processing                 bool               `json:"Processing"`
}

// cognitoOptionsJSON is the JSON representation of Cognito options.
// The Terraform provider's flattenCognitoOptions does not guard against nil,
// so we always return this field with Enabled=false when Cognito is not configured.
type cognitoOptionsJSON struct {
	Enabled bool `json:"Enabled"`
}

// ebsOptionsJSON is the JSON representation of EBS options.
type ebsOptionsJSON struct {
	VolumeType string `json:"VolumeType"`
	VolumeSize int    `json:"VolumeSize"`
	EBSEnabled bool   `json:"EBSEnabled"`
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
	DomainName           string `json:"DomainName"`
	ElasticsearchVersion string `json:"ElasticsearchVersion"`
}

// describeDomainsRequest is the request body for DescribeElasticsearchDomains.
type describeDomainsRequest struct {
	DomainNames []string `json:"DomainNames"`
}

// describeDomainsResponse is the response for DescribeElasticsearchDomains.
type describeDomainsResponse struct {
	DomainStatusList []domainStatusJSON `json:"DomainStatusList"`
}

// updateDomainConfigRequest is the request body for UpdateElasticsearchDomainConfig.
type updateDomainConfigRequest struct {
	ClusterConfig *domainClusterConfig `json:"ElasticsearchClusterConfig"`
	EBSOptions    *domainEBSOptions    `json:"EBSOptions"`
}

// ServeHTTP implements [http.Handler] for the Elasticsearch service.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Fast O(1) lookup for fixed-path routes.
	key := r.Method + " " + r.URL.Path
	if fn, ok := h.ops[key]; ok {
		fn(w, r)

		return
	}

	if h.handlePrefixRoutes(w, r) {
		return
	}

	h.handleDomainRoutes(w, r)
}

// handlePrefixRoutes handles routes that require prefix matching with path params.
// Returns true if the request was handled.
func (h *Handler) handlePrefixRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case strings.HasPrefix(path, elasticsearchCCSInbound+"/") &&
		strings.HasSuffix(path, "/accept") &&
		r.Method == http.MethodPut:
		h.handleAcceptInboundCrossClusterSearchConnection(w, r)

		return true
	case strings.HasPrefix(path, elasticsearchPackages+"/associate/") && r.Method == http.MethodPost:
		h.handleAssociatePackage(w, r)

		return true
	}

	return false
}

func (h *Handler) handleDomainRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchPathPrefix)

	switch {
	case (rest == "" || rest == "/") && r.Method == http.MethodPost:
		h.handleCreateDomain(w, r)
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		h.handleListDomainNames(w, r)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodGet:
		h.handleGetDomainRoute(w, r, rest)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodDelete:
		h.handleDeleteDomain(w, r, domainNameFromRest(rest))
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodPost:
		h.handlePostDomainRoute(w, r, rest)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func (h *Handler) handleGetDomainRoute(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := domainNameFromRest(rest)
	if before, ok := strings.CutSuffix(trimmed, "/config"); ok {
		h.handleDescribeDomainConfig(w, r, before)
	} else {
		h.handleDescribeDomain(w, r, trimmed)
	}
}

func (h *Handler) handlePostDomainRoute(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := domainNameFromRest(rest)

	switch {
	case strings.HasSuffix(trimmed, "/config/cancel"):
		domainName, _ := strings.CutSuffix(trimmed, "/config/cancel")
		h.handleCancelDomainConfigChange(w, r, domainName)
	case strings.HasSuffix(trimmed, "/config"):
		domainName, _ := strings.CutSuffix(trimmed, "/config")
		h.handleUpdateDomainConfig(w, r, domainName)
	case strings.HasSuffix(trimmed, "/authorizeVpcEndpointAccess"):
		domainName, _ := strings.CutSuffix(trimmed, "/authorizeVpcEndpointAccess")
		h.handleAuthorizeVpcEndpointAccess(w, r, domainName)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func domainNameFromRest(rest string) string {
	name := strings.TrimPrefix(rest, "/")

	return strings.TrimSuffix(name, "/")
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

	var cfg ClusterConfig
	if req.ClusterConfig != nil {
		cfg.InstanceType = req.ClusterConfig.InstanceType
		cfg.InstanceCount = req.ClusterConfig.InstanceCount
	}

	var ebsOpts EBSOptions
	if req.EBSOptions != nil {
		ebsOpts.EBSEnabled = req.EBSOptions.EBSEnabled
		ebsOpts.VolumeSize = req.EBSOptions.VolumeSize
		ebsOpts.VolumeType = req.EBSOptions.VolumeType
	}

	domain, err := h.Backend.CreateDomain(req.DomainName, req.ElasticsearchVersion, cfg, ebsOpts)
	if err != nil {
		h.handleDomainError(r, w, err)

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

// handleDomainError maps backend domain errors to HTTP responses.
func (h *Handler) handleDomainError(r *http.Request, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrDomainAlreadyExists):
		h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
	case errors.Is(err, ErrValidation):
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
	case errors.Is(err, ErrDomainNotFound):
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	default:
		h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
	}
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
			DomainName:           name,
			ElasticsearchVersion: d.ElasticsearchVersion,
		})
	}

	// Ensure the slice is non-nil so JSON marshals as [] not null.
	if entries == nil {
		entries = []domainNameEntry{}
	}

	h.writeJSON(r, w, domainListJSON{DomainNames: entries})
}

func (h *Handler) handleDescribeElasticsearchDomains(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req describeDomainsRequest
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	list := make([]domainStatusJSON, 0, len(req.DomainNames))

	for _, name := range req.DomainNames {
		d, descErr := h.Backend.DescribeDomain(name)
		if descErr != nil {
			continue
		}

		list = append(list, toDomainStatusJSON(d))
	}

	h.writeJSON(r, w, describeDomainsResponse{DomainStatusList: list})
}

func (h *Handler) handleUpdateDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req updateDomainConfigRequest
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	upd := UpdateConfig{}

	if req.ClusterConfig != nil {
		upd.ClusterConfig = &ClusterConfig{
			InstanceType:  req.ClusterConfig.InstanceType,
			InstanceCount: req.ClusterConfig.InstanceCount,
		}
	}

	if req.EBSOptions != nil {
		upd.EBSOptions = &EBSOptions{
			EBSEnabled: req.EBSOptions.EBSEnabled,
			VolumeSize: req.EBSOptions.VolumeSize,
			VolumeType: req.EBSOptions.VolumeType,
		}
	}

	domain, err := h.Backend.UpdateDomainConfig(name, upd)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	activeStatus := elasticsearchConfigStatus{State: "Active"}
	out := describeDomainConfigOutput{}
	out.DomainConfig.ElasticsearchVersion = elasticsearchConfigValue{
		Options: domain.ElasticsearchVersion,
		Status:  activeStatus,
	}
	out.DomainConfig.ElasticsearchClusterConfig = elasticsearchConfigValue{Options: map[string]any{
		"InstanceType":  domain.ClusterConfig.InstanceType,
		"InstanceCount": domain.ClusterConfig.InstanceCount,
	}, Status: activeStatus}
	out.DomainConfig.EBSOptions = elasticsearchConfigValue{Options: map[string]any{
		"EBSEnabled": domain.EBSOptions.EBSEnabled,
		"VolumeSize": domain.EBSOptions.VolumeSize,
		"VolumeType": domain.EBSOptions.VolumeType,
	}, Status: activeStatus}
	out.DomainConfig.AccessPolicies = elasticsearchConfigValue{Options: "", Status: activeStatus}
	out.DomainConfig.AdvancedOptions = elasticsearchConfigValue{Options: map[string]any{}, Status: activeStatus}

	h.writeJSON(r, w, &out)
}

func toDomainStatusJSON(d *Domain) domainStatusJSON {
	return domainStatusJSON{
		DomainName:             d.Name,
		DomainID:               d.DomainID,
		ARN:                    d.ARN,
		ElasticsearchVersion:   d.ElasticsearchVersion,
		Endpoint:               d.Endpoint,
		Processing:             false,
		DomainProcessingStatus: "Active",
		EBSOptions: ebsOptionsJSON{
			EBSEnabled: d.EBSOptions.EBSEnabled,
			VolumeSize: d.EBSOptions.VolumeSize,
			VolumeType: d.EBSOptions.VolumeType,
		},
		ElasticsearchClusterConfig: clusterConfigJSON{
			InstanceType:  d.ClusterConfig.InstanceType,
			InstanceCount: d.ClusterConfig.InstanceCount,
		},
		CognitoOptions: cognitoOptionsJSON{
			Enabled: false,
		},
	}
}

type errorResponseJSON struct {
	Message string `json:"message"`
}

func (h *Handler) writeError(r *http.Request, w http.ResponseWriter, status int, code, message string) {
	ctx := r.Context()
	logger.Load(ctx).Error("elasticsearch error", "code", code, "message", message)
	w.Header().Set("x-amzn-ErrorType", code)
	httputils.WriteJSON(ctx, w, status, errorResponseJSON{Message: message})
}

func (h *Handler) writeJSON(r *http.Request, w http.ResponseWriter, v any) {
	httputils.WriteJSON(r.Context(), w, http.StatusOK, v)
}

type listTagsOutput struct {
	TagList []svcTags.KV `json:"TagList"`
}

type elasticsearchConfigStatus struct {
	State string `json:"State"`
}

type elasticsearchConfigValue struct {
	Options any                       `json:"Options"`
	Status  elasticsearchConfigStatus `json:"Status"`
}

// domainConfigFields holds the per-feature configuration values for a domain.
type domainConfigFields struct {
	ElasticsearchVersion       elasticsearchConfigValue `json:"ElasticsearchVersion"`
	ElasticsearchClusterConfig elasticsearchConfigValue `json:"ElasticsearchClusterConfig"`
	EBSOptions                 elasticsearchConfigValue `json:"EBSOptions"`
	AccessPolicies             elasticsearchConfigValue `json:"AccessPolicies"`
	AdvancedOptions            elasticsearchConfigValue `json:"AdvancedOptions"`
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

	slices.SortFunc(tagList, func(a, b svcTags.KV) int {
		return strings.Compare(a.Key, b.Key)
	})

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

	_ = h.Backend.AddTags(req.ARN, tagMap)
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

	_ = h.Backend.RemoveTags(req.ARN, req.TagKeys)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDescribeDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	d, err := h.Backend.DescribeDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("domain %s/config not found", name))
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	activeStatus := elasticsearchConfigStatus{State: "Active"}
	out := describeDomainConfigOutput{}
	out.DomainConfig.ElasticsearchVersion = elasticsearchConfigValue{
		Options: d.ElasticsearchVersion,
		Status:  activeStatus,
	}
	out.DomainConfig.ElasticsearchClusterConfig = elasticsearchConfigValue{Options: map[string]any{
		"InstanceType":  d.ClusterConfig.InstanceType,
		"InstanceCount": d.ClusterConfig.InstanceCount,
	}, Status: activeStatus}
	out.DomainConfig.EBSOptions = elasticsearchConfigValue{Options: map[string]any{
		"EBSEnabled": d.EBSOptions.EBSEnabled,
		"VolumeSize": d.EBSOptions.VolumeSize,
		"VolumeType": d.EBSOptions.VolumeType,
	}, Status: activeStatus}
	out.DomainConfig.AccessPolicies = elasticsearchConfigValue{Options: "", Status: activeStatus}
	out.DomainConfig.AdvancedOptions = elasticsearchConfigValue{Options: map[string]any{}, Status: activeStatus}
	h.writeJSON(r, w, &out)
}

// --- New operations ---

// createPackageRequest is the JSON body for CreatePackage.
type createPackageRequest struct {
	PackageName        string `json:"PackageName"`
	PackageType        string `json:"PackageType"`
	PackageDescription string `json:"PackageDescription"`
}

// packageJSON is the JSON representation of an Elasticsearch package.
type packageJSON struct {
	PackageID          string `json:"PackageID"`
	PackageName        string `json:"PackageName"`
	PackageType        string `json:"PackageType"`
	PackageDescription string `json:"PackageDescription"`
	PackageStatus      string `json:"PackageStatus"`
}

// createPackageOutput is the response for CreatePackage.
type createPackageOutput struct {
	PackageDetails packageJSON `json:"PackageDetails"`
}

func (h *Handler) handleCreatePackage(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createPackageRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	pkg, createErr := h.Backend.CreatePackage(req.PackageName, req.PackageType, req.PackageDescription)
	if createErr != nil {
		if errors.Is(createErr, ErrDomainAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", createErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())
		}

		return
	}

	h.writeJSON(r, w, createPackageOutput{PackageDetails: toPackageJSON(pkg)})
}

func toPackageJSON(p *Package) packageJSON {
	return packageJSON{
		PackageID:          p.ID,
		PackageName:        p.Name,
		PackageType:        p.PackageType,
		PackageDescription: p.Description,
		PackageStatus:      p.Status,
	}
}

// associatePackageOutput is the response for AssociatePackage.
type associatePackageOutput struct {
	DomainPackageDetails struct {
		PackageID   string `json:"PackageID"`
		DomainName  string `json:"DomainName"`
		PackageType string `json:"PackageType"`
		State       string `json:"DomainPackageStatus"`
	} `json:"DomainPackageDetails"`
}

// associatePackagePathParts is the expected number of path segments after /associate/.
const associatePackagePathParts = 2

func (h *Handler) handleAssociatePackage(w http.ResponseWriter, r *http.Request) {
	// Path: /2015-01-01/packages/associate/{packageID}/{domainName}
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchPackages+"/associate/")
	parts := strings.SplitN(rest, "/", associatePackagePathParts)

	if len(parts) != associatePackagePathParts {
		h.writeError(
			r,
			w,
			http.StatusBadRequest,
			"ValidationException",
			"invalid path: expected /associate/{packageID}/{domainName}",
		)

		return
	}

	packageID, domainName := parts[0], parts[1]

	if assocErr := h.Backend.AssociatePackage(packageID, domainName); assocErr != nil {
		if errors.Is(assocErr, ErrDomainNotFound) || errors.Is(assocErr, ErrPackageNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", assocErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", assocErr.Error())
		}

		return
	}

	var out associatePackageOutput
	out.DomainPackageDetails.PackageID = packageID
	out.DomainPackageDetails.DomainName = domainName
	out.DomainPackageDetails.State = "ACTIVE"

	h.writeJSON(r, w, &out)
}

// inboundConnectionJSON is the JSON representation of an inbound cross-cluster connection.
type inboundConnectionJSON struct {
	CrossClusterSearchConnectionID string                      `json:"CrossClusterSearchConnectionId"`
	ConnectionStatus               inboundConnectionStatusJSON `json:"ConnectionStatus"`
	SourceDomainInfo               crossClusterDomainInfoJSON  `json:"SourceDomainInfo"`
	DestinationDomainInfo          crossClusterDomainInfoJSON  `json:"DestinationDomainInfo"`
}

type inboundConnectionStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

type crossClusterDomainInfoJSON struct {
	OwnerID    string `json:"OwnerId"`
	DomainName string `json:"DomainName"`
	Region     string `json:"Region"`
}

// acceptInboundConnectionOutput wraps the accepted connection.
type acceptInboundConnectionOutput struct {
	CrossClusterSearchConnection inboundConnectionJSON `json:"CrossClusterSearchConnection"`
}

func (h *Handler) handleAcceptInboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	// Path: /2015-01-01/es/ccs/inboundConnection/{connectionId}/accept
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchCCSInbound+"/")
	connectionID, _ := strings.CutSuffix(rest, "/accept")

	conn, err := h.Backend.AcceptInboundCrossClusterSearchConnection(connectionID)
	if err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, acceptInboundConnectionOutput{
		CrossClusterSearchConnection: toInboundConnectionJSON(conn),
	})
}

func toInboundConnectionJSON(c *InboundConnection) inboundConnectionJSON {
	return inboundConnectionJSON{
		CrossClusterSearchConnectionID: c.ConnectionID,
		ConnectionStatus:               inboundConnectionStatusJSON{StatusCode: c.ConnectionStatus},
		SourceDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.SourceDomainInfo.OwnerID,
			DomainName: c.SourceDomainInfo.DomainName,
			Region:     c.SourceDomainInfo.Region,
		},
		DestinationDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.DestDomainInfo.OwnerID,
			DomainName: c.DestDomainInfo.DomainName,
			Region:     c.DestDomainInfo.Region,
		},
	}
}

// outboundConnectionJSON is the JSON representation of an outbound cross-cluster connection.
type outboundConnectionJSON struct {
	CrossClusterSearchConnectionID string                       `json:"CrossClusterSearchConnectionId"`
	ConnectionAlias                string                       `json:"ConnectionAlias"`
	ConnectionStatus               outboundConnectionStatusJSON `json:"ConnectionStatus"`
	LocalDomainInfo                crossClusterDomainInfoJSON   `json:"LocalDomainInfo"`
	RemoteDomainInfo               crossClusterDomainInfoJSON   `json:"RemoteDomainInfo"`
}

type outboundConnectionStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

// createOutboundConnectionRequest is the JSON body for CreateOutboundCrossClusterSearchConnection.
type createOutboundConnectionRequest struct {
	LocalDomainInfo  crossClusterDomainInfoJSON `json:"LocalDomainInfo"`
	RemoteDomainInfo crossClusterDomainInfoJSON `json:"RemoteDomainInfo"`
	ConnectionAlias  string                     `json:"ConnectionAlias"`
}

// createOutboundConnectionOutput wraps the new outbound connection.
type createOutboundConnectionOutput struct {
	CrossClusterSearchConnection outboundConnectionJSON `json:"CrossClusterSearchConnection"`
}

func (h *Handler) handleCreateOutboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createOutboundConnectionRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	localDomain := CrossClusterDomainInfo{
		OwnerID:    req.LocalDomainInfo.OwnerID,
		DomainName: req.LocalDomainInfo.DomainName,
		Region:     req.LocalDomainInfo.Region,
	}
	remoteDomain := CrossClusterDomainInfo{
		OwnerID:    req.RemoteDomainInfo.OwnerID,
		DomainName: req.RemoteDomainInfo.DomainName,
		Region:     req.RemoteDomainInfo.Region,
	}

	conn, createErr := h.Backend.CreateOutboundCrossClusterSearchConnection(
		localDomain,
		remoteDomain,
		req.ConnectionAlias,
	)
	if createErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

		return
	}

	h.writeJSON(r, w, createOutboundConnectionOutput{
		CrossClusterSearchConnection: toOutboundConnectionJSON(conn),
	})
}

func toOutboundConnectionJSON(c *OutboundConnection) outboundConnectionJSON {
	return outboundConnectionJSON{
		CrossClusterSearchConnectionID: c.ConnectionID,
		ConnectionAlias:                c.ConnectionAlias,
		ConnectionStatus:               outboundConnectionStatusJSON{StatusCode: c.ConnectionStatus},
		LocalDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.LocalDomainInfo.OwnerID,
			DomainName: c.LocalDomainInfo.DomainName,
			Region:     c.LocalDomainInfo.Region,
		},
		RemoteDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.RemoteDomainInfo.OwnerID,
			DomainName: c.RemoteDomainInfo.DomainName,
			Region:     c.RemoteDomainInfo.Region,
		},
	}
}

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

	endpoint, createErr := h.Backend.CreateVpcEndpoint(req.DomainArn, req.VpcOptions)
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

	if authErr := h.Backend.AuthorizeVpcEndpointAccess(domainName, req.Account); authErr != nil {
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

// cancelDomainConfigChangeOutput wraps the domain status after cancellation.
type cancelDomainConfigChangeOutput struct {
	DomainConfig domainConfigFields `json:"DomainConfig"`
}

func (h *Handler) handleCancelDomainConfigChange(w http.ResponseWriter, r *http.Request, domainName string) {
	d, err := h.Backend.CancelDomainConfigChange(domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	activeStatus := elasticsearchConfigStatus{State: "Active"}
	out := cancelDomainConfigChangeOutput{}
	out.DomainConfig.ElasticsearchVersion = elasticsearchConfigValue{
		Options: d.ElasticsearchVersion,
		Status:  activeStatus,
	}
	out.DomainConfig.ElasticsearchClusterConfig = elasticsearchConfigValue{
		Options: map[string]any{
			"InstanceType":  d.ClusterConfig.InstanceType,
			"InstanceCount": d.ClusterConfig.InstanceCount,
		},
		Status: activeStatus,
	}
	out.DomainConfig.EBSOptions = elasticsearchConfigValue{
		Options: map[string]any{
			"EBSEnabled": d.EBSOptions.EBSEnabled,
			"VolumeSize": d.EBSOptions.VolumeSize,
			"VolumeType": d.EBSOptions.VolumeType,
		},
		Status: activeStatus,
	}
	out.DomainConfig.AccessPolicies = elasticsearchConfigValue{Options: "", Status: activeStatus}
	out.DomainConfig.AdvancedOptions = elasticsearchConfigValue{Options: map[string]any{}, Status: activeStatus}

	h.writeJSON(r, w, &out)
}

// cancelSoftwareUpdateRequest is the JSON body for CancelElasticsearchServiceSoftwareUpdate.
type cancelSoftwareUpdateRequest struct {
	DomainName string `json:"DomainName"`
}

// serviceSoftwareOptionsJSON is the JSON representation of software update options.
type serviceSoftwareOptionsJSON struct {
	CurrentVersion      string `json:"CurrentVersion"`
	NewVersion          string `json:"NewVersion"`
	UpdateStatus        string `json:"UpdateStatus"`
	Description         string `json:"Description"`
	AutomatedUpdateDate string `json:"AutomatedUpdateDate"`
	UpdateAvailable     bool   `json:"UpdateAvailable"`
	Cancellable         bool   `json:"Cancellable"`
}

// cancelSoftwareUpdateOutput is the response for CancelElasticsearchServiceSoftwareUpdate.
type cancelSoftwareUpdateOutput struct {
	ServiceSoftwareOptions serviceSoftwareOptionsJSON `json:"ServiceSoftwareOptions"`
}

func (h *Handler) handleCancelElasticsearchServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelSoftwareUpdateRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	_, cancelErr := h.Backend.CancelElasticsearchServiceSoftwareUpdate(req.DomainName)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelSoftwareUpdateOutput{
		ServiceSoftwareOptions: serviceSoftwareOptionsJSON{
			UpdateAvailable: false,
			Cancellable:     false,
			UpdateStatus:    "NOT_ELIGIBLE",
			Description:     "No software update scheduled",
		},
	})
}

func (h *Handler) handleDeleteElasticsearchServiceRole(w http.ResponseWriter, _ *http.Request) {
	_ = h.Backend.DeleteElasticsearchServiceRole()
	w.WriteHeader(http.StatusOK)
}
