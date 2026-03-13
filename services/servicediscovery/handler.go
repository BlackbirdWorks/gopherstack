package servicediscovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Cloud Map handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
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
		"DeleteNamespace",
		"GetNamespace",
		"ListNamespaces",
		"CreateService",
		"DeleteService",
		"GetService",
		"ListServices",
		"RegisterInstance",
		"DeregisterInstance",
		"GetInstance",
		"ListInstances",
		"DiscoverInstances",
		"GetOperation",
		"ListOperations",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
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
	case errors.Is(err, ErrNamespaceNotFound), errors.Is(err, ErrServiceNotFound),
		errors.Is(err, ErrInstanceNotFound), errors.Is(err, ErrOperationNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceNotFoundException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrNamespaceAlreadyExists):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "NamespaceAlreadyExists",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "InvalidInput",
			"message": err.Error(),
		})
	case errors.Is(err, errInvalidRequest),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "InvalidInput",
			"message": err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"__type":  "InternalServiceError",
			"message": err.Error(),
		})
	}
}

// --- Namespace handlers ---

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

	opID, err := h.Backend.CreateHTTPNamespace(req.Name, req.Description, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"OperationId": opID})
}

type createPrivateDNSNamespaceRequest struct {
	Name             string     `json:"Name"`
	Description      string     `json:"Description"`
	Vpc              string     `json:"Vpc"`
	CreatorRequestID string     `json:"CreatorRequestId"`
	Tags             []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreatePrivateDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req createPrivateDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	opID, err := h.Backend.CreatePrivateDNSNamespace(req.Name, req.Description, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"OperationId": opID})
}

type createPublicDNSNamespaceRequest struct {
	Name             string     `json:"Name"`
	Description      string     `json:"Description"`
	CreatorRequestID string     `json:"CreatorRequestId"`
	Tags             []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreatePublicDNSNamespace(_ context.Context, body []byte) ([]byte, error) {
	var req createPublicDNSNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	opID, err := h.Backend.CreatePublicDNSNamespace(req.Name, req.Description, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"OperationId": opID})
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

	return json.Marshal(map[string]string{"OperationId": opID})
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

type listNamespacesRequest struct {
	MaxResults *int   `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
	Filters    []any  `json:"Filters"`
}

func (h *Handler) handleListNamespaces(_ context.Context, body []byte) ([]byte, error) {
	var req listNamespacesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	namespaces := h.Backend.ListNamespaces()
	items := make([]map[string]any, 0, len(namespaces))

	for i := range namespaces {
		items = append(items, namespaceToMap(&namespaces[i]))
	}

	return json.Marshal(map[string]any{
		"Namespaces": items,
	})
}

// --- Service handlers ---

type createServiceRequest struct {
	DNSConfig        any        `json:"DnsConfig"`
	Name             string     `json:"Name"`
	Description      string     `json:"Description"`
	NamespaceID      string     `json:"NamespaceId"`
	CreatorRequestID string     `json:"CreatorRequestId"`
	Tags             []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateService(_ context.Context, body []byte) ([]byte, error) {
	var req createServiceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	svc, err := h.Backend.CreateService(req.Name, req.NamespaceID, req.Description, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Service": serviceToMap(svc),
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
		"Service": serviceToMap(svc),
	})
}

type listServicesRequest struct {
	MaxResults *int   `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
	Filters    []any  `json:"Filters"`
}

func (h *Handler) handleListServices(_ context.Context, body []byte) ([]byte, error) {
	var req listServicesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	services := h.Backend.ListServices("")
	items := make([]map[string]any, 0, len(services))

	for i := range services {
		items = append(items, serviceToMap(&services[i]))
	}

	return json.Marshal(map[string]any{
		"Services": items,
	})
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

	if err := h.Backend.RegisterInstance(req.ServiceID, req.InstanceID, req.Attributes); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"OperationId": "op-register"})
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

	if err := h.Backend.DeregisterInstance(req.ServiceID, req.InstanceID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"OperationId": "op-deregister"})
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
			"Id":         inst.ID,
			"Attributes": inst.Attributes,
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

	items := make([]map[string]any, 0, len(instances))

	for _, inst := range instances {
		items = append(items, map[string]any{
			"Id":         inst.ID,
			"Attributes": inst.Attributes,
		})
	}

	return json.Marshal(map[string]any{
		"Instances": items,
	})
}

type discoverInstancesRequest struct {
	NamespaceName string `json:"NamespaceName"`
	ServiceName   string `json:"ServiceName"`
	MaxResults    *int   `json:"MaxResults"`
	HealthStatus  string `json:"HealthStatus"`
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

	instances, err := h.Backend.DiscoverInstances(req.NamespaceName, req.ServiceName)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(instances))

	for _, inst := range instances {
		items = append(items, map[string]any{
			"InstanceId": inst.ID,
			"Attributes": inst.Attributes,
		})
	}

	return json.Marshal(map[string]any{
		"Instances": items,
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
		"Operation": map[string]any{
			"Id":     op.ID,
			"Type":   op.Type,
			"Status": op.Status,
			"Targets": map[string]string{
				op.TargetType: op.TargetID,
			},
		},
	})
}

type listOperationsRequest struct {
	MaxResults *int   `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
	Filters    []any  `json:"Filters"`
}

func (h *Handler) handleListOperations(_ context.Context, body []byte) ([]byte, error) {
	var req listOperationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ops := h.Backend.ListOperations()
	items := make([]map[string]any, 0, len(ops))

	for _, op := range ops {
		items = append(items, map[string]any{
			"Id":     op.ID,
			"Status": op.Status,
		})
	}

	return json.Marshal(map[string]any{
		"Operations": items,
	})
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
		"Tags": tagList,
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
		return map[string]string{}
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

	entries := make([]tagEntry, 0, len(keys))

	for _, k := range keys {
		entries = append(entries, tagEntry{Key: k, Value: tags[k]})
	}

	return entries
}

// namespaceToMap converts a Namespace to a JSON-serialisable map.
func namespaceToMap(ns *Namespace) map[string]any {
	return map[string]any{
		"Id":          ns.ID,
		"Arn":         ns.ARN,
		"Name":        ns.Name,
		"Type":        ns.Type,
		"Description": ns.Description,
	}
}

// serviceToMap converts a Service to a JSON-serialisable map.
func serviceToMap(svc *Service) map[string]any {
	return map[string]any{
		"Id":          svc.ID,
		"Arn":         svc.ARN,
		"Name":        svc.Name,
		"NamespaceId": svc.NamespaceID,
		"Description": svc.Description,
	}
}
