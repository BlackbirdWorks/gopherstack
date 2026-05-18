package servicediscovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyTypeField    = "__type"
	keyMessageField = "message"
	errInvalidInput = "InvalidInput"
	keyOperationID  = "OperationId"
	keyService      = "Service"
	keyAttributes   = "Attributes"
	keyStatusField  = "Status"
	keyTags         = "Tags"
	keyArn          = "Arn"

	maxTagCount    = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

const (
	serviceDiscoveryService      = "servicediscovery"
	serviceDiscoveryTargetPrefix = "Route53AutoNaming_v20170314."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS Cloud Map service discovery API.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Cloud Map handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ServiceDiscovery" }

// GetSupportedOperations returns the list of supported Cloud Map operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateHttpNamespace",
		"CreatePrivateDnsNamespace",
		"CreatePublicDnsNamespace",
		"CreateService",
		"DeleteNamespace",
		"DeleteService",
		"DeleteServiceAttributes",
		"DeregisterInstance",
		"DiscoverInstances",
		"DiscoverInstancesRevision",
		"GetInstance",
		"GetInstancesHealthStatus",
		"GetNamespace",
		"GetOperation",
		"GetService",
		"GetServiceAttributes",
		"ListInstances",
		"ListNamespaces",
		"ListOperations",
		"ListServices",
		"ListTagsForResource",
		"RegisterInstance",
		"TagResource",
		"UntagResource",
		"UpdateHttpNamespace",
		"UpdateInstanceCustomHealthStatus",
		"UpdatePrivateDnsNamespace",
		"UpdatePublicDnsNamespace",
		"UpdateService",
		"UpdateServiceAttributes",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return serviceDiscoveryService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches Cloud Map API requests.
// Requests are identified by the X-Amz-Target header prefix "Route53AutoNaming_v20170314.".
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), serviceDiscoveryTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), serviceDiscoveryTargetPrefix)
}

// ExtractResource extracts the primary resource ID from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"Id", "ServiceId", "NamespaceId", "ResourceARN"} {
		if v, ok := data[key]; ok {
			if s, isStr := v.(string); isStr {
				return s
			}
		}
	}

	return ""
}

// Handler returns the Echo handler function for Cloud Map requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "servicediscovery: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)
		result, dispErr := h.dispatch(ctx, op, body)

		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	if result, ok, err := h.dispatchNamespace(ctx, op, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchService(ctx, op, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchInstance(ctx, op, body); ok {
		return result, err
	}

	return h.dispatchMeta(ctx, op, body)
}

func (h *Handler) dispatchNamespace(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "CreateHttpNamespace":
		r, err := h.handleCreateHTTPNamespace(ctx, body)

		return r, true, err
	case "CreatePrivateDnsNamespace":
		r, err := h.handleCreatePrivateDNSNamespace(ctx, body)

		return r, true, err
	case "CreatePublicDnsNamespace":
		r, err := h.handleCreatePublicDNSNamespace(ctx, body)

		return r, true, err
	case "DeleteNamespace":
		r, err := h.handleDeleteNamespace(ctx, body)

		return r, true, err
	case "GetNamespace":
		r, err := h.handleGetNamespace(ctx, body)

		return r, true, err
	case "ListNamespaces":
		r, err := h.handleListNamespaces(ctx, body)

		return r, true, err
	case "UpdateHttpNamespace":
		r, err := h.handleUpdateHTTPNamespace(ctx, body)

		return r, true, err
	case "UpdatePrivateDnsNamespace":
		r, err := h.handleUpdatePrivateDNSNamespace(ctx, body)

		return r, true, err
	case "UpdatePublicDnsNamespace":
		r, err := h.handleUpdatePublicDNSNamespace(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchService(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "CreateService":
		r, err := h.handleCreateService(ctx, body)

		return r, true, err
	case "DeleteService":
		err := h.handleDeleteService(ctx, body)

		return nil, true, err
	case "GetService":
		r, err := h.handleGetService(ctx, body)

		return r, true, err
	case "ListServices":
		r, err := h.handleListServices(ctx, body)

		return r, true, err
	case "UpdateService":
		r, err := h.handleUpdateService(ctx, body)

		return r, true, err
	case "GetServiceAttributes":
		r, err := h.handleGetServiceAttributes(ctx, body)

		return r, true, err
	case "UpdateServiceAttributes":
		err := h.handleUpdateServiceAttributes(ctx, body)

		return nil, true, err
	case "DeleteServiceAttributes":
		err := h.handleDeleteServiceAttributes(ctx, body)

		return nil, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchInstance(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "RegisterInstance":
		r, err := h.handleRegisterInstance(ctx, body)

		return r, true, err
	case "DeregisterInstance":
		r, err := h.handleDeregisterInstance(ctx, body)

		return r, true, err
	case "GetInstance":
		r, err := h.handleGetInstance(ctx, body)

		return r, true, err
	case "ListInstances":
		r, err := h.handleListInstances(ctx, body)

		return r, true, err
	case "DiscoverInstances":
		r, err := h.handleDiscoverInstances(ctx, body)

		return r, true, err
	case "DiscoverInstancesRevision":
		r, err := h.handleDiscoverInstancesRevision(ctx, body)

		return r, true, err
	case "GetInstancesHealthStatus":
		r, err := h.handleGetInstancesHealthStatus(ctx, body)

		return r, true, err
	case "UpdateInstanceCustomHealthStatus":
		err := h.handleUpdateInstanceCustomHealthStatus(ctx, body)

		return nil, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchMeta(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "GetOperation":
		return h.handleGetOperation(ctx, body)
	case "ListOperations":
		return h.handleListOperations(ctx, body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(ctx, body)
	case "TagResource":
		return h.handleTagResource(ctx, body)
	case "UntagResource":
		return h.handleUntagResource(ctx, body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNamespaceNotFound):
		return h.errorResponse(c, "NamespaceNotFound", err)
	case errors.Is(err, ErrServiceNotFound):
		return h.errorResponse(c, "ServiceNotFound", err)
	case errors.Is(err, ErrInstanceNotFound):
		return h.errorResponse(c, "InstanceNotFound", err)
	case errors.Is(err, ErrOperationNotFound):
		return h.errorResponse(c, "OperationNotFound", err)
	case errors.Is(err, ErrServiceAttributesNotFound):
		return h.errorResponse(c, "ServiceAttributesNotFound", err)
	case errors.Is(err, ErrResourceNotFound):
		return h.errorResponse(c, "ResourceNotFoundException", err)
	case errors.Is(err, ErrCustomHealthNotFound):
		return h.errorResponse(c, "CustomHealthNotFound", err)
	case errors.Is(err, ErrNamespaceAlreadyExists):
		return h.errorResponse(c, "NamespaceAlreadyExists", err)
	case errors.Is(err, ErrResourceInUse):
		return h.errorResponse(c, "ResourceInUse", err)
	case errors.Is(err, ErrInvalidInput):
		return h.errorResponse(c, errInvalidInput, err)
	case errors.Is(err, errUnknownAction):
		return h.errorResponse(c, errInvalidInput, err)
	case errors.Is(err, errInvalidRequest),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return h.errorResponse(c, errInvalidInput, err)
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalServiceError",
			keyMessageField: err.Error(),
		})
	}
}

func (h *Handler) errorResponse(c *echo.Context, errType string, err error) error {
	payload, _ := json.Marshal(map[string]string{
		keyTypeField:    errType,
		keyMessageField: err.Error(),
	})

	return c.JSONBlob(http.StatusBadRequest, payload)
}

// validateTags enforces AWS Cloud Map tag limits and reserved key rules.
func validateTags(tags []tagEntry) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot have more than %d tags", ErrInvalidInput, maxTagCount)
	}

	for _, t := range tags {
		if strings.HasPrefix(t.Key, "aws:") {
			return fmt.Errorf("%w: tag key %q uses reserved prefix \"aws:\"", ErrInvalidInput, t.Key)
		}

		if len(t.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds maximum length of %d", ErrInvalidInput, t.Key, maxTagKeyLen)
		}

		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q exceeds maximum length of %d", ErrInvalidInput, t.Key, maxTagValueLen)
		}
	}

	return nil
}

// --- Namespace handlers ---

type dnsPropertiesRequest struct {
	SOA *struct {
		TTL int64 `json:"TTL"`
	} `json:"SOA"`
}

type createHTTPNamespaceRequest struct {
	Name             string     `json:"Name"`
	Description      string     `json:"Description"`
	CreatorRequestID string     `json:"CreatorRequestId"`
	Tags             []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateHTTPNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req createHTTPNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	opID, err := h.Backend.CreateHTTPNamespace(req.Name, req.Description, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type createPrivateDNSNamespaceRequest struct {
	Name             string `json:"Name"`
	Description      string `json:"Description"`
	Vpc              string `json:"Vpc"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Properties       *struct {
		DnsProperties *dnsPropertiesRequest `json:"DnsProperties"`
	} `json:"Properties"`
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreatePrivateDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req createPrivateDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	var soaTTL int64
	if req.Properties != nil && req.Properties.DnsProperties != nil && req.Properties.DnsProperties.SOA != nil {
		soaTTL = req.Properties.DnsProperties.SOA.TTL
	}

	opID, err := h.Backend.CreatePrivateDNSNamespace(req.Name, req.Description, req.Vpc, soaTTL, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type createPublicDNSNamespaceRequest struct {
	Name             string `json:"Name"`
	Description      string `json:"Description"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Properties       *struct {
		DnsProperties *dnsPropertiesRequest `json:"DnsProperties"`
	} `json:"Properties"`
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreatePublicDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req createPublicDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	var soaTTL int64
	if req.Properties != nil && req.Properties.DnsProperties != nil && req.Properties.DnsProperties.SOA != nil {
		soaTTL = req.Properties.DnsProperties.SOA.TTL
	}

	opID, err := h.Backend.CreatePublicDNSNamespace(req.Name, req.Description, soaTTL, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type deleteNamespaceRequest struct {
	ID string `json:"Id"`
}

func (h *Handler) handleDeleteNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req deleteNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.DeleteNamespace(req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type getNamespaceRequest struct {
	ID string `json:"Id"`
}

func (h *Handler) handleGetNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req getNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	ns, err := h.Backend.GetNamespace(req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Namespace": namespaceToMap(ns),
	})
}

type namespaceFilter struct {
	Name      string   `json:"Name"`
	Condition string   `json:"Condition"`
	Values    []string `json:"Values"`
}

type listNamespacesRequest struct {
	MaxResults *int              `json:"MaxResults"`
	NextToken  string            `json:"NextToken"`
	Filters    []namespaceFilter `json:"Filters"`
}

func (h *Handler) handleListNamespaces(_ context.Context, body []byte) ([]byte, error) {
	var req listNamespacesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	filter := ListNamespacesFilter{}

	for _, f := range req.Filters {
		switch f.Name {
		case "TYPE":
			if len(f.Values) > 0 {
				filter.Type = f.Values[0]
			}
		case "NAME":
			if len(f.Values) > 0 {
				filter.Name = f.Values[0]
			}
		}
	}

	namespaces := h.Backend.ListNamespaces(filter)

	maxResults := maxResultsDefault
	if req.MaxResults != nil && *req.MaxResults > 0 {
		maxResults = *req.MaxResults
	}

	page, nextToken := applyPaginationNamespaces(namespaces, req.NextToken, maxResults)

	items := make([]map[string]any, 0, len(page))
	for i := range page {
		items = append(items, namespaceToMap(&page[i]))
	}

	resp := map[string]any{
		"Namespaces": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// --- Service handlers ---

type dnsRecordRequest struct {
	Type string `json:"Type"`
	TTL  int64  `json:"TTL"`
}

type dnsConfigRequest struct {
	NamespaceID   string             `json:"NamespaceId"`
	RoutingPolicy string             `json:"RoutingPolicy"`
	DnsRecords    []dnsRecordRequest `json:"DnsRecords"`
}

type healthCheckConfigRequest struct {
	Type             string `json:"Type"`
	ResourcePath     string `json:"ResourcePath"`
	FailureThreshold int    `json:"FailureThreshold"`
}

type healthCheckCustomConfigRequest struct {
	FailureThreshold int `json:"FailureThreshold"`
}

type createServiceRequest struct {
	Name                    string                          `json:"Name"`
	Description             string                          `json:"Description"`
	NamespaceID             string                          `json:"NamespaceId"`
	Type                    string                          `json:"Type"`
	CreatorRequestID        string                          `json:"CreatorRequestId"`
	DnsConfig               *dnsConfigRequest               `json:"DnsConfig"`
	HealthCheckConfig       *healthCheckConfigRequest        `json:"HealthCheckConfig"`
	HealthCheckCustomConfig *healthCheckCustomConfigRequest  `json:"HealthCheckCustomConfig"`
	Tags                    []tagEntry                      `json:"Tags"`
}

func parseDnsConfig(req *dnsConfigRequest) *DnsConfig {
	if req == nil {
		return nil
	}

	dc := &DnsConfig{
		NamespaceID:   req.NamespaceID,
		RoutingPolicy: req.RoutingPolicy,
	}

	for _, r := range req.DnsRecords {
		dc.DnsRecords = append(dc.DnsRecords, DnsRecord{Type: r.Type, TTL: r.TTL})
	}

	return dc
}

func parseHealthCheckConfig(req *healthCheckConfigRequest) *HealthCheckConfig {
	if req == nil {
		return nil
	}

	return &HealthCheckConfig{
		Type:             req.Type,
		ResourcePath:     req.ResourcePath,
		FailureThreshold: req.FailureThreshold,
	}
}

func parseHealthCheckCustomConfig(req *healthCheckCustomConfigRequest) *HealthCheckCustomConfig {
	if req == nil {
		return nil
	}

	return &HealthCheckCustomConfig{
		FailureThreshold: req.FailureThreshold,
	}
}

func (h *Handler) handleCreateService(_ context.Context, body []byte) ([]byte, error) {
	var req createServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.HealthCheckConfig != nil && req.HealthCheckCustomConfig != nil {
		return nil, fmt.Errorf("%w: HealthCheckConfig and HealthCheckCustomConfig are mutually exclusive", ErrInvalidInput)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	svc, err := h.Backend.CreateService(
		req.Name,
		req.NamespaceID,
		req.Description,
		req.Type,
		parseDnsConfig(req.DnsConfig),
		parseHealthCheckConfig(req.HealthCheckConfig),
		parseHealthCheckCustomConfig(req.HealthCheckCustomConfig),
		tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyService: serviceToMap(svc),
	})
}

type deleteServiceRequest struct {
	ID string `json:"Id"`
}

func (h *Handler) handleDeleteService(_ context.Context, body []byte) error {
	var req deleteServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	return h.Backend.DeleteService(req.ID)
}

type getServiceRequest struct {
	ID string `json:"Id"`
}

func (h *Handler) handleGetService(_ context.Context, body []byte) ([]byte, error) {
	var req getServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	svc, err := h.Backend.GetService(req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyService: serviceToMap(svc),
	})
}

type serviceFilter struct {
	Name      string   `json:"Name"`
	Condition string   `json:"Condition"`
	Values    []string `json:"Values"`
}

type listServicesRequest struct {
	MaxResults *int            `json:"MaxResults"`
	NextToken  string          `json:"NextToken"`
	Filters    []serviceFilter `json:"Filters"`
}

func (h *Handler) handleListServices(_ context.Context, body []byte) ([]byte, error) {
	var req listServicesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	filter := ListServicesFilter{}

	for _, f := range req.Filters {
		if f.Name == "NAMESPACE_ID" && len(f.Values) > 0 {
			filter.NamespaceID = f.Values[0]

			break
		}
	}

	services := h.Backend.ListServices(filter)

	maxResults := maxResultsDefault
	if req.MaxResults != nil && *req.MaxResults > 0 {
		maxResults = *req.MaxResults
	}

	page, nextToken := applyPaginationServices(services, req.NextToken, maxResults)

	items := make([]map[string]any, 0, len(page))
	for i := range page {
		items = append(items, serviceToMap(&page[i]))
	}

	resp := map[string]any{
		"Services": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// --- Instance handlers ---

type registerInstanceRequest struct {
	ServiceID        string            `json:"ServiceId"`
	InstanceID       string            `json:"InstanceId"`
	Attributes       map[string]string `json:"Attributes"`
	CreatorRequestID string            `json:"CreatorRequestId"`
}

func (h *Handler) handleRegisterInstance(_ context.Context, body []byte) ([]byte, error) {
	var req registerInstanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	if req.InstanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", errInvalidRequest)
	}

	opID, err := h.Backend.RegisterInstance(req.ServiceID, req.InstanceID, req.Attributes)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type deregisterInstanceRequest struct {
	ServiceID  string `json:"ServiceId"`
	InstanceID string `json:"InstanceId"`
}

func (h *Handler) handleDeregisterInstance(_ context.Context, body []byte) ([]byte, error) {
	var req deregisterInstanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	if req.InstanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", errInvalidRequest)
	}

	opID, err := h.Backend.DeregisterInstance(req.ServiceID, req.InstanceID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type getInstanceRequest struct {
	ServiceID  string `json:"ServiceId"`
	InstanceID string `json:"InstanceId"`
}

func (h *Handler) handleGetInstance(_ context.Context, body []byte) ([]byte, error) {
	var req getInstanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	if req.InstanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", errInvalidRequest)
	}

	inst, err := h.Backend.GetInstance(req.ServiceID, req.InstanceID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Instance": map[string]any{
			"Id":          inst.ID,
			keyAttributes: inst.Attributes,
		},
	})
}

type listInstancesRequest struct {
	ServiceID  string `json:"ServiceId"`
	MaxResults *int   `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
}

func (h *Handler) handleListInstances(_ context.Context, body []byte) ([]byte, error) {
	var req listInstancesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	instances, err := h.Backend.ListInstances(req.ServiceID)
	if err != nil {
		return nil, err
	}

	maxResults := maxResultsDefault
	if req.MaxResults != nil && *req.MaxResults > 0 {
		maxResults = *req.MaxResults
	}

	page, nextToken := applyPaginationInstances(instances, req.NextToken, maxResults)

	items := make([]map[string]any, 0, len(page))
	for _, inst := range page {
		items = append(items, map[string]any{
			"Id":          inst.ID,
			keyAttributes: inst.Attributes,
		})
	}

	resp := map[string]any{
		"Instances": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

type discoverInstancesRequest struct {
	QueryParameters    map[string]string `json:"QueryParameters"`
	OptionalParameters map[string]string `json:"OptionalParameters"`
	MaxResults         *int              `json:"MaxResults"`
	NamespaceName      string            `json:"NamespaceName"`
	ServiceName        string            `json:"ServiceName"`
	HealthStatus       string            `json:"HealthStatus"`
}

func (h *Handler) handleDiscoverInstances(_ context.Context, body []byte) ([]byte, error) {
	var req discoverInstancesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NamespaceName == "" {
		return nil, fmt.Errorf("%w: NamespaceName is required", errInvalidRequest)
	}

	if req.ServiceName == "" {
		return nil, fmt.Errorf("%w: ServiceName is required", errInvalidRequest)
	}

	discovered, revision, err := h.Backend.DiscoverInstances(
		req.NamespaceName,
		req.ServiceName,
		req.HealthStatus,
		req.QueryParameters,
	)
	if err != nil {
		return nil, err
	}

	maxResults := 0
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}

	if maxResults > 0 && maxResults < len(discovered) {
		discovered = discovered[:maxResults]
	}

	items := make([]map[string]any, 0, len(discovered))
	for _, inst := range discovered {
		attrs := inst.Attributes
		if attrs == nil {
			attrs = map[string]string{}
		}

		items = append(items, map[string]any{
			"InstanceId":    inst.InstanceID,
			"NamespaceName": inst.NamespaceName,
			"ServiceName":   inst.ServiceName,
			"HealthStatus":  inst.HealthStatus,
			keyAttributes:   attrs,
		})
	}

	return json.Marshal(map[string]any{
		"Instances":         items,
		"InstancesRevision": revision,
	})
}

// --- Operation handlers ---

type getOperationRequest struct {
	OperationID string `json:"OperationId"`
}

func (h *Handler) handleGetOperation(_ context.Context, body []byte) ([]byte, error) {
	var req getOperationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OperationID == "" {
		return nil, fmt.Errorf("%w: OperationId is required", errInvalidRequest)
	}

	op, err := h.Backend.GetOperation(req.OperationID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Operation": operationToMap(op),
	})
}

type operationFilter struct {
	Name      string   `json:"Name"`
	Condition string   `json:"Condition"`
	Values    []string `json:"Values"`
}

type listOperationsRequest struct {
	MaxResults *int              `json:"MaxResults"`
	NextToken  string            `json:"NextToken"`
	Filters    []operationFilter `json:"Filters"`
}

func (h *Handler) handleListOperations(_ context.Context, body []byte) ([]byte, error) {
	var req listOperationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	filter := ListOperationsFilter{}

	for _, f := range req.Filters {
		switch f.Name {
		case "STATUS":
			if len(f.Values) > 0 {
				filter.Status = f.Values[0]
			}
		case "TYPE":
			if len(f.Values) > 0 {
				filter.Type = f.Values[0]
			}
		}
	}

	ops := h.Backend.ListOperations(filter)

	maxResults := maxResultsDefault
	if req.MaxResults != nil && *req.MaxResults > 0 {
		maxResults = *req.MaxResults
	}

	page, nextToken := applyPaginationOperations(ops, req.NextToken, maxResults)

	items := make([]map[string]any, 0, len(page))
	for i := range page {
		items = append(items, operationToMap(&page[i]))
	}

	resp := map[string]any{
		"Operations": items,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// --- Tags handlers ---

type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	tagList := mapToTagEntries(tags)

	return json.Marshal(map[string]any{
		keyTags: tagList,
	})
}

type tagResourceRequest struct {
	ResourceARN string     `json:"ResourceARN"`
	Tags        []tagEntry `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(req.ResourceARN, tagsToMap(req.Tags)); err != nil {
		return nil, err
	}

	return nil, nil
}

type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) ([]byte, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return nil, nil
}

// --- Helpers ---

// tagEntry is a key-value tag as used in the Cloud Map API JSON protocol.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsToMap converts a slice of tag entries to a map.
func tagsToMap(tags []tagEntry) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))

	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// mapToTagEntries converts a tag map to a sorted slice of tag entries.
func mapToTagEntries(tags map[string]string) []tagEntry {
	keys := make([]string, 0, len(tags))

	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	entries := make([]tagEntry, 0, len(keys))

	for _, k := range keys {
		entries = append(entries, tagEntry{Key: k, Value: tags[k]})
	}

	return entries
}

// namespaceToMap converts a Namespace to a JSON-serialisable map including Properties.
func namespaceToMap(ns *Namespace) map[string]any {
	m := map[string]any{
		"Id":          ns.ID,
		keyArn:        ns.ARN,
		"Name":        ns.Name,
		"Type":        ns.Type,
		"Description": ns.Description,
		keyTags:       mapToTagEntries(ns.Tags),
		"CreateDate":  ns.CreatedAt.Unix(),
	}

	if ns.Properties != nil {
		props := map[string]any{}

		if ns.Properties.DnsProperties != nil {
			dp := map[string]any{}

			if ns.Properties.DnsProperties.HostedZoneID != "" {
				dp["HostedZoneId"] = ns.Properties.DnsProperties.HostedZoneID
			}

			if ns.Properties.DnsProperties.SOA != nil {
				dp["SOA"] = map[string]any{
					"TTL": ns.Properties.DnsProperties.SOA.TTL,
				}
			}

			props["DnsProperties"] = dp
		}

		if ns.Properties.HttpProperties != nil {
			props["HttpProperties"] = map[string]any{
				"HttpName": ns.Properties.HttpProperties.HttpName,
			}
		}

		m["Properties"] = props
	}

	return m
}

// serviceToMap converts a Service to a JSON-serialisable map including DNS and health check config.
func serviceToMap(svc *Service) map[string]any {
	m := map[string]any{
		"Id":          svc.ID,
		keyArn:        svc.ARN,
		"Name":        svc.Name,
		"NamespaceId": svc.NamespaceID,
		"Description": svc.Description,
		keyTags:       mapToTagEntries(svc.Tags),
		"CreateDate":  svc.CreatedAt.Unix(),
	}

	if svc.Type != "" {
		m["Type"] = svc.Type
	}

	if svc.DnsConfig != nil {
		dc := map[string]any{
			"NamespaceId":   svc.DnsConfig.NamespaceID,
			"RoutingPolicy": svc.DnsConfig.RoutingPolicy,
		}

		records := make([]map[string]any, 0, len(svc.DnsConfig.DnsRecords))
		for _, r := range svc.DnsConfig.DnsRecords {
			records = append(records, map[string]any{
				"Type": r.Type,
				"TTL":  r.TTL,
			})
		}

		dc["DnsRecords"] = records
		m["DnsConfig"] = dc
	}

	if svc.HealthCheckConfig != nil {
		m["HealthCheckConfig"] = map[string]any{
			"Type":             svc.HealthCheckConfig.Type,
			"ResourcePath":     svc.HealthCheckConfig.ResourcePath,
			"FailureThreshold": svc.HealthCheckConfig.FailureThreshold,
		}
	}

	if svc.HealthCheckCustomConfig != nil {
		m["HealthCheckCustomConfig"] = map[string]any{
			"FailureThreshold": svc.HealthCheckCustomConfig.FailureThreshold,
		}
	}

	return m
}

// operationToMap converts an Operation to a JSON-serialisable map with full fields.
func operationToMap(op *Operation) map[string]any {
	m := map[string]any{
		"Id":         op.ID,
		"Type":       op.Type,
		keyStatusField: op.Status,
		"CreateDate": op.CreateDate.Unix(),
		"UpdateDate": op.UpdateDate.Unix(),
	}

	if len(op.Targets) > 0 {
		m["Targets"] = op.Targets
	}

	if op.ErrorCode != "" {
		m["ErrorCode"] = op.ErrorCode
	}

	if op.ErrorMessage != "" {
		m["ErrorMessage"] = op.ErrorMessage
	}

	return m
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// --- GetInstancesHealthStatus ---

type getInstancesHealthStatusRequest struct {
	MaxResults *int     `json:"MaxResults,omitempty"`
	ServiceID  string   `json:"ServiceId"`
	NextToken  string   `json:"NextToken,omitempty"`
	Instances  []string `json:"Instances,omitempty"`
}

// handleGetInstancesHealthStatus returns the health status for instances in a service.
// Instances without a recorded custom status default to HEALTHY.
func (h *Handler) handleGetInstancesHealthStatus(_ context.Context, body []byte) ([]byte, error) {
	var req getInstancesHealthStatusRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	statuses, err := h.Backend.GetInstancesHealthStatus(req.ServiceID, req.Instances)
	if err != nil {
		return nil, err
	}

	// Build sorted list of instance IDs for stable pagination.
	ids := make([]string, 0, len(statuses))
	for id := range statuses {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	maxResults := maxResultsDefault
	if req.MaxResults != nil && *req.MaxResults > 0 {
		maxResults = *req.MaxResults
	}

	page, nextToken := applyPaginationHealthStatuses(ids, req.NextToken, maxResults)

	paged := make(map[string]string, len(page))
	for _, id := range page {
		paged[id] = statuses[id]
	}

	resp := map[string]any{
		keyStatusField: paged,
	}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// --- UpdateHttpNamespace / UpdatePrivateDnsNamespace / UpdatePublicDnsNamespace ---

type updateNamespaceChange struct {
	Description string `json:"Description"`
}

type updateHTTPNamespaceRequest struct {
	ID               string                `json:"Id"`
	UpdaterRequestID string                `json:"UpdaterRequestId"`
	Namespace        updateNamespaceChange `json:"Namespace"`
}

func (h *Handler) handleUpdateHTTPNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req updateHTTPNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.UpdateHTTPNamespace(req.ID, req.Namespace.Description)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type updatePrivateDNSNamespaceRequest struct {
	ID               string                `json:"Id"`
	UpdaterRequestID string                `json:"UpdaterRequestId"`
	Namespace        updateNamespaceChange `json:"Namespace"`
}

func (h *Handler) handleUpdatePrivateDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req updatePrivateDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.UpdatePrivateDNSNamespace(req.ID, req.Namespace.Description)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

type updatePublicDNSNamespaceRequest struct {
	ID               string                `json:"Id"`
	UpdaterRequestID string                `json:"UpdaterRequestId"`
	Namespace        updateNamespaceChange `json:"Namespace"`
}

func (h *Handler) handleUpdatePublicDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req updatePublicDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	opID, err := h.Backend.UpdatePublicDNSNamespace(req.ID, req.Namespace.Description)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyOperationID: opID})
}

// --- UpdateService ---

type updateServiceChange struct {
	Description       string                    `json:"Description"`
	DnsConfig         *dnsConfigRequest         `json:"DnsConfig"`
	HealthCheckConfig *healthCheckConfigRequest  `json:"HealthCheckConfig"`
}

type updateServiceRequest struct {
	ID      string              `json:"Id"`
	Service updateServiceChange `json:"Service"`
}

func (h *Handler) handleUpdateService(_ context.Context, body []byte) ([]byte, error) {
	var req updateServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	svc, err := h.Backend.UpdateService(
		req.ID,
		req.Service.Description,
		parseDnsConfig(req.Service.DnsConfig),
		parseHealthCheckConfig(req.Service.HealthCheckConfig),
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyService: serviceToMap(svc),
	})
}

// --- GetServiceAttributes / UpdateServiceAttributes / DeleteServiceAttributes ---

type getServiceAttributesRequest struct {
	ServiceID string `json:"ServiceId"`
}

func (h *Handler) handleGetServiceAttributes(_ context.Context, body []byte) ([]byte, error) {
	var req getServiceAttributesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	arn, attrs, err := h.Backend.GetServiceAttributes(req.ServiceID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ServiceAttributes": map[string]any{
			keyArn:        arn,
			keyAttributes: attrs,
		},
	})
}

type updateServiceAttributesRequest struct {
	Attributes map[string]string `json:"Attributes"`
	ServiceARN string            `json:"ServiceArn"`
}

func (h *Handler) handleUpdateServiceAttributes(_ context.Context, body []byte) error {
	var req updateServiceAttributesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceARN == "" {
		return fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	return h.Backend.UpdateServiceAttributes(req.ServiceARN, req.Attributes)
}

type deleteServiceAttributesRequest struct {
	ServiceID string `json:"ServiceId"`
}

func (h *Handler) handleDeleteServiceAttributes(_ context.Context, body []byte) error {
	var req deleteServiceAttributesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	return h.Backend.DeleteServiceAttributes(req.ServiceID)
}

// --- UpdateInstanceCustomHealthStatus ---

type updateInstanceCustomHealthStatusRequest struct {
	ServiceID  string `json:"ServiceId"`
	InstanceID string `json:"InstanceId"`
	Status     string `json:"Status"`
}

func (h *Handler) handleUpdateInstanceCustomHealthStatus(_ context.Context, body []byte) error {
	var req updateInstanceCustomHealthStatusRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ServiceID == "" {
		return fmt.Errorf("%w: ServiceId is required", errInvalidRequest)
	}

	if req.InstanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", errInvalidRequest)
	}

	if req.Status == "" {
		return fmt.Errorf("%w: Status is required", errInvalidRequest)
	}

	return h.Backend.UpdateInstanceCustomHealthStatus(req.ServiceID, req.InstanceID, req.Status)
}

// --- DiscoverInstancesRevision ---

type discoverInstancesRevisionRequest struct {
	NamespaceName string `json:"NamespaceName"`
	ServiceName   string `json:"ServiceName"`
}

func (h *Handler) handleDiscoverInstancesRevision(_ context.Context, body []byte) ([]byte, error) {
	var req discoverInstancesRevisionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NamespaceName == "" {
		return nil, fmt.Errorf("%w: NamespaceName is required", errInvalidRequest)
	}

	if req.ServiceName == "" {
		return nil, fmt.Errorf("%w: ServiceName is required", errInvalidRequest)
	}

	revision, err := h.Backend.DiscoverInstancesRevision(req.NamespaceName, req.ServiceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"InstancesRevision": revision,
	})
}
