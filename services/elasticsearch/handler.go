package elasticsearch

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
	elasticsearchPathPrefix = "/2015-01-01/es/domain"
	elasticsearchTagsPath   = "/2015-01-01/tags"
	elasticsearchTagsRemove = "/2015-01-01/tags-removal"
	elasticsearchDomainInfo = "/2015-01-01/es/domain-info"
)

// Handler is the HTTP handler for Elasticsearch operations.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Elasticsearch Handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
			path == elasticsearchTagsRemove
	}
}

// GetSupportedOperations returns supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateElasticsearchDomain",
		"DescribeElasticsearchDomain",
		"DeleteElasticsearchDomain",
		"ListDomainNames",
		"DescribeElasticsearchDomains",
		"UpdateElasticsearchDomainConfig",
		"DescribeElasticsearchDomainConfig",
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

	if path == elasticsearchDomainInfo && method == http.MethodPost {
		return "DescribeElasticsearchDomains"
	}

	rest := strings.TrimPrefix(path, elasticsearchPathPrefix)

	switch {
	case rest == "" || rest == "/":
		if method == http.MethodPost {
			return "CreateElasticsearchDomain"
		}

		if method == http.MethodGet {
			return "ListDomainNames"
		}

		return "Unknown"
	case strings.HasPrefix(rest, "/") && strings.HasSuffix(rest, "/config"):
		if method == http.MethodPost {
			return "UpdateElasticsearchDomainConfig"
		}

		return "DescribeElasticsearchDomainConfig"
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return "DescribeElasticsearchDomain"
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return "DeleteElasticsearchDomain"
	}

	return "Unknown"
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
	DomainName                 string            `json:"DomainName"`
	DomainID                   string            `json:"DomainId"`
	ARN                        string            `json:"ARN"`
	ElasticsearchVersion       string            `json:"ElasticsearchVersion"`
	Endpoint                   string            `json:"Endpoint"`
	DomainProcessingStatus     string            `json:"DomainProcessingStatus"`
	ElasticsearchClusterConfig clusterConfigJSON `json:"ElasticsearchClusterConfig"`
	EBSOptions                 ebsOptionsJSON    `json:"EBSOptions"`
	Processing                 bool              `json:"Processing"`
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
	if h.handleTagRoutes(w, r) {
		return
	}

	// Handle domain-info (DescribeElasticsearchDomains) separately since it
	// shares the /2015-01-01/es/domain prefix but is not a single-domain path.
	if r.URL.Path == elasticsearchDomainInfo && r.Method == http.MethodPost {
		h.handleDescribeElasticsearchDomains(w, r)

		return
	}

	h.handleDomainRoutes(w, r)
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
	if before, ok := strings.CutSuffix(trimmed, "/config"); ok {
		h.handleUpdateDomainConfig(w, r, before)
	} else {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func domainNameFromRest(rest string) string {
	name := strings.TrimPrefix(rest, "/")

	return strings.TrimSuffix(name, "/")
}

// handleTagRoutes processes /2015-01-01/tags and /2015-01-01/tags-removal requests.
// Returns true if the request was handled.
func (h *Handler) handleTagRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case path == elasticsearchTagsPath && r.Method == http.MethodGet:
		h.handleListTags(w, r)

		return true
	case path == elasticsearchTagsPath && r.Method == http.MethodPost:
		h.handleAddTags(w, r)

		return true
	case path == elasticsearchTagsRemove && r.Method == http.MethodPost:
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

	var ebsOpts EBSOptions
	if req.EBSOptions != nil {
		ebsOpts.EBSEnabled = req.EBSOptions.EBSEnabled
		ebsOpts.VolumeSize = req.EBSOptions.VolumeSize
		ebsOpts.VolumeType = req.EBSOptions.VolumeType
	}

	domain, err := h.Backend.CreateDomain(req.DomainName, req.ElasticsearchVersion, cfg, ebsOpts)
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
			DomainName:           name,
			ElasticsearchVersion: d.ElasticsearchVersion,
		})
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
