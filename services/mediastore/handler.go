package mediastore

import (
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
		"CreateContainer",
		"DeleteContainer",
		"DescribeContainer",
		"ListContainers",
		"PutContainerPolicy",
		"GetContainerPolicy",
		"DeleteContainerPolicy",
		"PutCorsPolicy",
		"GetCorsPolicy",
		"DeleteCorsPolicy",
		"PutLifecyclePolicy",
		"GetLifecyclePolicy",
		"DeleteLifecyclePolicy",
		"PutMetricPolicy",
		"GetMetricPolicy",
		"DeleteMetricPolicy",
		"StartAccessLogging",
		"StopAccessLogging",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
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
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		target := c.Request().Header.Get("X-Amz-Target")

		if !strings.HasPrefix(target, mediastoreTargetPrefix) {
			return writeError(c, http.StatusBadRequest, "missing or invalid X-Amz-Target header")
		}

		op := strings.TrimPrefix(target, mediastoreTargetPrefix)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "mediastore: failed to read request body", "error", err)

			return writeError(c, http.StatusInternalServerError, "failed to read request body")
		}

		log.DebugContext(ctx, "mediastore request", "op", op)

		return h.dispatch(c, op, body)
	}
}

// dispatch routes to the appropriate handler based on the operation name.
//
//nolint:cyclop // switch-based operation dispatch; each case is a single delegation.
func (h *Handler) dispatch(c *echo.Context, op string, body []byte) error {
	switch op {
	case "CreateContainer":
		return h.handleCreateContainer(c, body)
	case "DeleteContainer":
		return h.handleDeleteContainer(c, body)
	case "DescribeContainer":
		return h.handleDescribeContainer(c, body)
	case "ListContainers":
		return h.handleListContainers(c)
	case "PutContainerPolicy":
		return h.handlePutContainerPolicy(c, body)
	case "GetContainerPolicy":
		return h.handleGetContainerPolicy(c, body)
	case "DeleteContainerPolicy":
		return h.handleDeleteContainerPolicy(c, body)
	case "PutCorsPolicy":
		return h.handlePutCorsPolicy(c, body)
	case "GetCorsPolicy":
		return h.handleGetCorsPolicy(c, body)
	case "DeleteCorsPolicy":
		return h.handleDeleteCorsPolicy(c, body)
	case "PutLifecyclePolicy":
		return h.handlePutLifecyclePolicy(c, body)
	case "GetLifecyclePolicy":
		return h.handleGetLifecyclePolicy(c, body)
	case "DeleteLifecyclePolicy":
		return h.handleDeleteLifecyclePolicy(c, body)
	case "PutMetricPolicy":
		return h.handlePutMetricPolicy(c, body)
	case "GetMetricPolicy":
		return h.handleGetMetricPolicy(c, body)
	case "DeleteMetricPolicy":
		return h.handleDeleteMetricPolicy(c, body)
	case "StartAccessLogging":
		return h.handleStartAccessLogging(c, body)
	case "StopAccessLogging":
		return h.handleStopAccessLogging(c, body)
	case "TagResource":
		return h.handleTagResource(c, body)
	case "UntagResource":
		return h.handleUntagResource(c, body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, body)
	}

	return writeError(c, http.StatusBadRequest, "unknown operation: "+op)
}

func (h *Handler) handleCreateContainer(c *echo.Context, body []byte) error {
	var req createContainerRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	tags := tagsFromSlice(req.Tags)

	container, err := h.Backend.CreateContainer(h.DefaultRegion, h.AccountID, req.ContainerName, tags)
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
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteContainer(req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDescribeContainer(c *echo.Context, body []byte) error {
	var req describeContainerRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	container, err := h.Backend.DescribeContainer(req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeContainerResponse{
		Container: toContainerObject(container),
	})
}

func (h *Handler) handleListContainers(c *echo.Context) error {
	containers, err := h.Backend.ListContainers()
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]containerObject, 0, len(containers))

	for _, ct := range containers {
		objs = append(objs, toContainerObject(ct))
	}

	return c.JSON(http.StatusOK, listContainersResponse{Containers: objs})
}

func (h *Handler) handlePutContainerPolicy(c *echo.Context, body []byte) error {
	var req putContainerPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.PutContainerPolicy(req.ContainerName, req.Policy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleGetContainerPolicy(c *echo.Context, body []byte) error {
	var req getContainerPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	policy, err := h.Backend.GetContainerPolicy(req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getContainerPolicyResponse{Policy: policy})
}

func (h *Handler) handleDeleteContainerPolicy(c *echo.Context, body []byte) error {
	var req deleteContainerPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteContainerPolicy(req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handlePutCorsPolicy(c *echo.Context, body []byte) error {
	var req putCorsPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.PutCorsPolicy(req.ContainerName, req.CorsPolicy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleGetCorsPolicy(c *echo.Context, body []byte) error {
	var req getCorsPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	rules, err := h.Backend.GetCorsPolicy(req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getCorsPolicyResponse{CorsPolicy: rules})
}

func (h *Handler) handleDeleteCorsPolicy(c *echo.Context, body []byte) error {
	var req deleteCorsPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteCorsPolicy(req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handlePutLifecyclePolicy(c *echo.Context, body []byte) error {
	var req putLifecyclePolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.PutLifecyclePolicy(req.ContainerName, req.LifecyclePolicy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleGetLifecyclePolicy(c *echo.Context, body []byte) error {
	var req getLifecyclePolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	policy, err := h.Backend.GetLifecyclePolicy(req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getLifecyclePolicyResponse{LifecyclePolicy: policy})
}

func (h *Handler) handleDeleteLifecyclePolicy(c *echo.Context, body []byte) error {
	var req deleteLifecyclePolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteLifecyclePolicy(req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handlePutMetricPolicy(c *echo.Context, body []byte) error {
	var req putMetricPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.PutMetricPolicy(req.ContainerName, req.MetricPolicy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleGetMetricPolicy(c *echo.Context, body []byte) error {
	var req getMetricPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	policy, err := h.Backend.GetMetricPolicy(req.ContainerName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getMetricPolicyResponse{MetricPolicy: policy})
}

func (h *Handler) handleDeleteMetricPolicy(c *echo.Context, body []byte) error {
	var req deleteMetricPolicyRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.DeleteMetricPolicy(req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStartAccessLogging(c *echo.Context, body []byte) error {
	var req startAccessLoggingRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.StartAccessLogging(req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStopAccessLogging(c *echo.Context, body []byte) error {
	var req stopAccessLoggingRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.ContainerName == "" {
		return writeError(c, http.StatusBadRequest, ErrMissingContainerName.Error())
	}

	if err := h.Backend.StopAccessLogging(req.ContainerName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req tagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Resource == "" {
		return writeError(c, http.StatusBadRequest, "Resource is required")
	}

	tags := tagsFromSlice(req.Tags)

	if err := h.Backend.TagResource(req.Resource, tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var req untagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Resource == "" {
		return writeError(c, http.StatusBadRequest, "Resource is required")
	}

	if err := h.Backend.UntagResource(req.Resource, req.TagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req listTagsForResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Resource == "" {
		return writeError(c, http.StatusBadRequest, "Resource is required")
	}

	tags, err := h.Backend.ListTagsForResource(req.Resource)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsForResourceResponse{Tags: tagsToSlice(tags)})
}

// writeBackendError translates a backend error to an HTTP response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return writeError(c, http.StatusNotFound, err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return writeError(c, http.StatusConflict, err.Error())
	default:
		return writeError(c, http.StatusInternalServerError, err.Error())
	}
}

// writeError writes a JSON error response.
func writeError(c *echo.Context, status int, message string) error {
	return c.JSON(status, errorResponse{Message: message})
}

// toContainerObject converts a Container to its JSON representation.
func toContainerObject(c *Container) containerObject {
	return containerObject{
		Name:                 c.Name,
		ARN:                  c.ARN,
		Endpoint:             c.Endpoint,
		Status:               c.Status,
		CreationTime:         c.CreationTime,
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
