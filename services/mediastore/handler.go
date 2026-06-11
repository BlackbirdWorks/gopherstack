package mediastore

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	mediastoreService       = "mediastore"
	mediastoreMatchPriority = 87
	mediastoreTargetPrefix  = "MediaStore_20170901."
)

// MediaStore operation name constants.
const (
	opMSCreateContainer       = "CreateContainer"
	opMSDeleteContainer       = "DeleteContainer"
	opMSDescribeContainer     = "DescribeContainer"
	opMSListContainers        = "ListContainers"
	opMSPutContainerPolicy    = "PutContainerPolicy"
	opMSGetContainerPolicy    = "GetContainerPolicy"
	opMSDeleteContainerPolicy = "DeleteContainerPolicy"
	opMSPutCorsPolicy         = "PutCorsPolicy"
	opMSGetCorsPolicy         = "GetCorsPolicy"
	opMSDeleteCorsPolicy      = "DeleteCorsPolicy"
	opMSPutLifecyclePolicy    = "PutLifecyclePolicy"
	opMSGetLifecyclePolicy    = "GetLifecyclePolicy"
	opMSDeleteLifecyclePolicy = "DeleteLifecyclePolicy"
	opMSPutMetricPolicy       = "PutMetricPolicy"
	opMSGetMetricPolicy       = "GetMetricPolicy"
	opMSDeleteMetricPolicy    = "DeleteMetricPolicy"
	opMSStartAccessLogging    = "StartAccessLogging"
	opMSStopAccessLogging     = "StopAccessLogging"
	opMSTagResource           = "TagResource"
	opMSUntagResource         = "UntagResource"
	opMSListTagsForResource   = "ListTagsForResource"
)

// regionContextKey is the context key under which the resolved AWS region for
// the current request is stored.
type regionContextKey struct{}

// Handler is the HTTP handler for the AWS Elemental MediaStore JSON 1.1 API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new MediaStore handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaStore" }

// GetSupportedOperations returns the list of supported MediaStore operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opMSCreateContainer,
		opMSDeleteContainer,
		opMSDescribeContainer,
		opMSListContainers,
		opMSPutContainerPolicy,
		opMSGetContainerPolicy,
		opMSDeleteContainerPolicy,
		opMSPutCorsPolicy,
		opMSGetCorsPolicy,
		opMSDeleteCorsPolicy,
		opMSPutLifecyclePolicy,
		opMSGetLifecyclePolicy,
		opMSDeleteLifecyclePolicy,
		opMSPutMetricPolicy,
		opMSGetMetricPolicy,
		opMSDeleteMetricPolicy,
		opMSStartAccessLogging,
		opMSStopAccessLogging,
		opMSTagResource,
		opMSUntagResource,
		opMSListTagsForResource,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return mediastoreService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches MediaStore JSON 1.1 API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		if strings.HasPrefix(target, mediastoreTargetPrefix) {
			return true
		}

		return httputils.ExtractServiceFromRequest(c.Request()) == mediastoreService
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return mediastoreMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	if !strings.HasPrefix(target, mediastoreTargetPrefix) {
		return "Unknown"
	}

	return strings.TrimPrefix(target, mediastoreTargetPrefix)
}

// ExtractResource extracts the container name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if name, ok := data["ContainerName"]; ok {
		nameStr, isStr := name.(string)
		if isStr {
			return nameStr
		}
	}

	if res, ok := data["Resource"]; ok {
		resStr, isStr := res.(string)
		if isStr {
			return resStr
		}
	}

	return ""
}

// Handler returns the Echo handler function for MediaStore requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		c.SetRequest(c.Request().WithContext(ctx))

		log := logger.Load(ctx)

		target := c.Request().Header.Get("X-Amz-Target")

		if !strings.HasPrefix(target, mediastoreTargetPrefix) {
			return writeError(c, http.StatusBadRequest, "BadRequestException", "missing or invalid X-Amz-Target header")
		}

		op := strings.TrimPrefix(target, mediastoreTargetPrefix)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "mediastore: failed to read request body", "error", err)

			return writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "mediastore request", "op", op)

		return h.dispatch(c, op, body)
	}
}

// mediastoreDispatch maps operation names to their handler functions.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var mediastoreDispatch = map[string]func(*Handler, *echo.Context, []byte) error{
	opMSCreateContainer:       (*Handler).handleCreateContainer,
	opMSDeleteContainer:       (*Handler).handleDeleteContainer,
	opMSDescribeContainer:     (*Handler).handleDescribeContainer,
	opMSListContainers:        (*Handler).handleListContainers,
	opMSPutContainerPolicy:    (*Handler).handlePutContainerPolicy,
	opMSGetContainerPolicy:    (*Handler).handleGetContainerPolicy,
	opMSDeleteContainerPolicy: (*Handler).handleDeleteContainerPolicy,
	opMSPutCorsPolicy:         (*Handler).handlePutCorsPolicy,
	opMSGetCorsPolicy:         (*Handler).handleGetCorsPolicy,
	opMSDeleteCorsPolicy:      (*Handler).handleDeleteCorsPolicy,
	opMSPutLifecyclePolicy:    (*Handler).handlePutLifecyclePolicy,
	opMSGetLifecyclePolicy:    (*Handler).handleGetLifecyclePolicy,
	opMSDeleteLifecyclePolicy: (*Handler).handleDeleteLifecyclePolicy,
	opMSPutMetricPolicy:       (*Handler).handlePutMetricPolicy,
	opMSGetMetricPolicy:       (*Handler).handleGetMetricPolicy,
	opMSDeleteMetricPolicy:    (*Handler).handleDeleteMetricPolicy,
	opMSStartAccessLogging:    (*Handler).handleStartAccessLogging,
	opMSStopAccessLogging:     (*Handler).handleStopAccessLogging,
	opMSTagResource:           (*Handler).handleTagResource,
	opMSUntagResource:         (*Handler).handleUntagResource,
	opMSListTagsForResource:   (*Handler).handleListTagsForResource,
}

// dispatch routes to the appropriate handler based on the operation name.
func (h *Handler) dispatch(c *echo.Context, op string, body []byte) error {
	if fn, ok := mediastoreDispatch[op]; ok {
		return fn(h, c, body)
	}

	return writeError(c, http.StatusBadRequest, "UnknownOperationException", "unknown operation: "+op)
}

func (h *Handler) handleCreateContainer(c *echo.Context, body []byte) error {
	var req createContainerRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	tags := tagsFromSlice(req.Tags)

	container, err := h.Backend.CreateContainer(c.Request().Context(), h.AccountID, req.ContainerName, tags)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createContainerResponse{
		Container: toContainerObject(container),
	})
}

func (h *Handler) handleDeleteContainer(c *echo.Context, body []byte) error {
	var req deleteContainerRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteContainer(c.Request().Context(), req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDescribeContainer(c *echo.Context, body []byte) error {
	var req describeContainerRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	container, err := h.Backend.DescribeContainer(c.Request().Context(), req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeContainerResponse{
		Container: toContainerObject(container),
	})
}

func (h *Handler) handleListContainers(c *echo.Context, body []byte) error {
	var req listContainersRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	containers, nextToken, err := h.Backend.ListContainers(c.Request().Context(), req.NextToken, req.MaxResults)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]containerObject, 0, len(containers))

	for _, ct := range containers {
		objs = append(objs, toContainerObject(ct))
	}

	resp := listContainersResponse{Containers: objs}
	if nextToken != "" {
		resp.NextToken = &nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handlePutContainerPolicy(c *echo.Context, body []byte) error {
	var req putContainerPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.PutContainerPolicy(c.Request().Context(), req.ContainerName, req.Policy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleGetContainerPolicy(c *echo.Context, body []byte) error {
	var req getContainerPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	policy, err := h.Backend.GetContainerPolicy(c.Request().Context(), req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getContainerPolicyResponse{Policy: policy})
}

func (h *Handler) handleDeleteContainerPolicy(c *echo.Context, body []byte) error {
	var req deleteContainerPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteContainerPolicy(c.Request().Context(), req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handlePutCorsPolicy(c *echo.Context, body []byte) error {
	var req putCorsPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.PutCorsPolicy(c.Request().Context(), req.ContainerName, req.CorsPolicy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleGetCorsPolicy(c *echo.Context, body []byte) error {
	var req getCorsPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	rules, err := h.Backend.GetCorsPolicy(c.Request().Context(), req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getCorsPolicyResponse{CorsPolicy: rules})
}

func (h *Handler) handleDeleteCorsPolicy(c *echo.Context, body []byte) error {
	var req deleteCorsPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteCorsPolicy(c.Request().Context(), req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handlePutLifecyclePolicy(c *echo.Context, body []byte) error {
	var req putLifecyclePolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.PutLifecyclePolicy(c.Request().Context(), req.ContainerName, req.LifecyclePolicy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleGetLifecyclePolicy(c *echo.Context, body []byte) error {
	var req getLifecyclePolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	policy, err := h.Backend.GetLifecyclePolicy(c.Request().Context(), req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getLifecyclePolicyResponse{LifecyclePolicy: policy})
}

func (h *Handler) handleDeleteLifecyclePolicy(c *echo.Context, body []byte) error {
	var req deleteLifecyclePolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteLifecyclePolicy(c.Request().Context(), req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handlePutMetricPolicy(c *echo.Context, body []byte) error {
	var req putMetricPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.PutMetricPolicy(c.Request().Context(), req.ContainerName, req.MetricPolicy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleGetMetricPolicy(c *echo.Context, body []byte) error {
	var req getMetricPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	policy, err := h.Backend.GetMetricPolicy(c.Request().Context(), req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getMetricPolicyResponse{MetricPolicy: policy})
}

func (h *Handler) handleDeleteMetricPolicy(c *echo.Context, body []byte) error {
	var req deleteMetricPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteMetricPolicy(c.Request().Context(), req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStartAccessLogging(c *echo.Context, body []byte) error {
	var req startAccessLoggingRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.StartAccessLogging(c.Request().Context(), req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStopAccessLogging(c *echo.Context, body []byte) error {
	var req stopAccessLoggingRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", ErrMissingContainerName.Error())
	}

	if err := h.Backend.StopAccessLogging(c.Request().Context(), req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req tagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Resource == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "Resource is required")
	}

	tags := tagsFromSlice(req.Tags)

	if err := h.Backend.TagResource(c.Request().Context(), req.Resource, tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var req untagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Resource == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "Resource is required")
	}

	if err := h.Backend.UntagResource(c.Request().Context(), req.Resource, req.TagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req listTagsForResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.Resource == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "Resource is required")
	}

	tags, err := h.Backend.ListTagsForResource(c.Request().Context(), req.Resource)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsForResourceResponse{Tags: tagsToSlice(tags)})
}

// writeBackendError translates a backend error to an HTTP response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrContainerNotFound):
		// AWS MediaStore returns ContainerNotFoundException (not the generic
		// ResourceNotFoundException) when a container does not exist. The
		// terraform-provider-aws delete waiter matches this exact type to
		// detect that a container has finished deleting, so it must be exact.

		return writeError(c, http.StatusNotFound, "ContainerNotFoundException", err.Error())
	case errors.Is(err, ErrPolicyNotFound),
		errors.Is(err, ErrLifecyclePolicyNotFound),
		errors.Is(err, ErrMetricPolicyNotFound):

		return writeError(c, http.StatusNotFound, "PolicyNotFoundException", err.Error())
	case errors.Is(err, ErrCorsPolicyNotFound):

		return writeError(c, http.StatusNotFound, "CorsPolicyNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrNotFound):

		return writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):

		return writeError(c, http.StatusConflict, "ContainerInUseException", err.Error())
	case errors.Is(err, ErrInvalidContainerName),
		errors.Is(err, ErrInvalidPolicy),
		errors.Is(err, ErrCorsRuleInvalid),
		errors.Is(err, ErrInvalidMetricPolicy),
		errors.Is(err, ErrTooManyMetricRules),
		errors.Is(err, ErrEmptyTagKey):

		return writeError(c, http.StatusBadRequest, "ValidationException", err.Error())
	default:

		return writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}
}

// writeError writes a JSON error response using the standard AWS JSON 1.1 envelope.
func writeError(c *echo.Context, status int, errType, message string) error {
	return c.JSON(status, errorResponse{Type: errType, Message: message})
}

// toContainerObject converts a Container to its JSON representation.
func toContainerObject(c *Container) containerObject {
	var creationTime float64
	if c.CreationTime != nil {
		creationTime = float64(c.CreationTime.Unix())
	}

	return containerObject{
		Name:                 c.Name,
		ARN:                  c.ARN,
		Endpoint:             c.Endpoint,
		Status:               c.Status,
		CreationTime:         creationTime,
		AccessLoggingEnabled: c.AccessLoggingEnabled,
	}
}

// tagsFromSlice converts a []tagEntry to a map[string]string.
func tagsFromSlice(tags []tagEntry) map[string]string {
	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

// tagsToSlice converts a map[string]string to []tagEntry, sorted by key.
func tagsToSlice(tags map[string]string) []tagEntry {
	result := make([]tagEntry, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagEntry{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}
