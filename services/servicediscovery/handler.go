package servicediscovery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

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

	// RegisterInstance custom-attribute quota, per the api_op_RegisterInstance.go
	// doc comment: "You can add up to 30 custom attributes. For each key-value
	// pair, the maximum length of the attribute name is 255 characters, and the
	// maximum length of the attribute value is 1,024 characters. The total size
	// of all provided attributes (sum of all keys and values) must not exceed
	// 5,000 characters".
	maxInstanceAttrCount    = 30
	maxInstanceAttrKeyLen   = 255
	maxInstanceAttrValueLen = 1024
	maxInstanceAttrTotalLen = 5000

	// UpdateServiceAttributes quota, per the botocore model
	// (servicediscovery/2017-03-14/service-2.json): shape ServiceAttributesMap
	// {max:30,min:1}, ServiceAttributeKey{max:255}, ServiceAttributeValue{max:1024}.
	maxServiceAttrCount    = 30
	maxServiceAttrKeyLen   = 255
	maxServiceAttrValueLen = 1024
)

const (
	serviceDiscoveryService      = "servicediscovery"
	serviceDiscoveryTargetPrefix = "Route53AutoNaming_v20170314."

	keyNamespaceID = "NamespaceId"
	keyType        = "Type"
	keyCreateDate  = "CreateDate"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

func resolveMaxResults(ptr *int) int {
	if ptr != nil && *ptr > 0 {
		return *ptr
	}

	return maxResultsDefault
}

func marshalPagedResponse(key string, items []map[string]any, nextToken string) ([]byte, error) {
	resp := map[string]any{key: items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

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

	for _, key := range []string{"Id", "ServiceId", keyNamespaceID, "ResourceARN"} {
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

// sentinelErrorCodes maps each backend sentinel error to its AWS error code,
// checked in order via errors.Is. A sync.OnceValue route table, same pattern
// as apigatewayv2's op dispatch: a fixed, read-only lookup built once, not
// mutated per-request.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var sentinelErrorCodes = sync.OnceValue(func() []struct {
	err  error
	code string
} {
	return []struct {
		err  error
		code string
	}{
		{ErrNamespaceNotFound, "NamespaceNotFound"},
		{ErrServiceNotFound, "ServiceNotFound"},
		{ErrInstanceNotFound, "InstanceNotFound"},
		{ErrOperationNotFound, "OperationNotFound"},
		{ErrServiceAttributesNotFound, "ServiceAttributesNotFound"},
		{ErrResourceNotFound, "ResourceNotFoundException"},
		{ErrCustomHealthNotFound, "CustomHealthNotFound"},
		{ErrNamespaceAlreadyExists, "NamespaceAlreadyExists"},
		{ErrServiceAlreadyExists, "ServiceAlreadyExists"},
		{ErrResourceInUse, "ResourceInUse"},
		{ErrTooManyTags, "TooManyTagsException"},
		{ErrServiceAttributesLimitExceeded, "ServiceAttributesLimitExceededException"},
		{ErrInvalidInput, errInvalidInput},
		{errUnknownAction, errInvalidInput},
	}
})

// isMalformedRequest reports whether err represents a request the handler
// couldn't even parse (bad JSON syntax/shape), which maps to InvalidInput
// just like errInvalidRequest but isn't itself a sentinel in
// sentinelErrorCodes.
func isMalformedRequest(err error) bool {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	return errors.Is(err, errInvalidRequest) || errors.As(err, &syntaxErr) || errors.As(err, &typeErr)
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	for _, entry := range sentinelErrorCodes() {
		if errors.Is(err, entry.err) {
			return h.errorResponse(c, entry.code, err)
		}
	}

	if isMalformedRequest(err) {
		return h.errorResponse(c, errInvalidInput, err)
	}

	return c.JSON(http.StatusInternalServerError, map[string]string{
		keyTypeField:    "InternalServiceError",
		keyMessageField: err.Error(),
	})
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
		return fmt.Errorf("%w: cannot have more than %d tags", ErrTooManyTags, maxTagCount)
	}

	for _, t := range tags {
		if strings.HasPrefix(t.Key, "aws:") {
			return fmt.Errorf("%w: tag key %q uses reserved prefix \"aws:\"", ErrInvalidInput, t.Key)
		}

		if len(t.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds maximum length of %d", ErrInvalidInput, t.Key, maxTagKeyLen)
		}

		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value for key %q exceeds maximum length of %d",
				ErrInvalidInput,
				t.Key,
				maxTagValueLen,
			)
		}
	}

	return nil
}

// validateInstanceAttributes enforces the RegisterInstance custom-attribute
// quota: at most maxInstanceAttrCount entries, key/value length caps, and a
// combined key+value size cap. See the maxInstanceAttr* constants for the
// exact documented numbers.
func validateInstanceAttributes(attrs map[string]string) error {
	if len(attrs) > maxInstanceAttrCount {
		return fmt.Errorf("%w: cannot have more than %d attributes", ErrInvalidInput, maxInstanceAttrCount)
	}

	total := 0

	for k, v := range attrs {
		if len(k) > maxInstanceAttrKeyLen {
			return fmt.Errorf(
				"%w: attribute key %q exceeds maximum length of %d",
				ErrInvalidInput,
				k,
				maxInstanceAttrKeyLen,
			)
		}

		if len(v) > maxInstanceAttrValueLen {
			return fmt.Errorf(
				"%w: attribute value for key %q exceeds maximum length of %d",
				ErrInvalidInput,
				k,
				maxInstanceAttrValueLen,
			)
		}

		total += len(k) + len(v)
	}

	if total > maxInstanceAttrTotalLen {
		return fmt.Errorf(
			"%w: total attribute size exceeds maximum of %d characters",
			ErrInvalidInput,
			maxInstanceAttrTotalLen,
		)
	}

	return nil
}

// validateServiceAttributeShape enforces UpdateServiceAttributes' per-request
// shape constraints (non-empty map, key/value length caps). The total-count
// quota (maxServiceAttrCount, post-merge with existing attributes) is
// enforced in the backend, which alone knows the service's current attributes.
func validateServiceAttributeShape(attrs map[string]string) error {
	if len(attrs) == 0 {
		return fmt.Errorf("%w: Attributes must contain at least one entry", ErrInvalidInput)
	}

	for k, v := range attrs {
		if len(k) > maxServiceAttrKeyLen {
			return fmt.Errorf(
				"%w: attribute key %q exceeds maximum length of %d",
				ErrInvalidInput,
				k,
				maxServiceAttrKeyLen,
			)
		}

		if len(v) > maxServiceAttrValueLen {
			return fmt.Errorf(
				"%w: attribute value for key %q exceeds maximum length of %d",
				ErrInvalidInput,
				k,
				maxServiceAttrValueLen,
			)
		}
	}

	return nil
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// encodeCursor encodes an integer offset as an opaque NextToken.
func encodeCursor(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodeCursor decodes an opaque NextToken to an integer offset.
func decodeCursor(token string) int {
	if token == "" {
		return 0
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0
	}

	var offset int

	_, _ = fmt.Sscanf(string(b), "%d", &offset)

	return offset
}
