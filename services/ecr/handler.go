package ecr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyMessageField = "message"
)

const (
	keyTypeField = "__type"
)

const (
	ecrTargetPrefix   = "AmazonEC2ContainerRegistry_V20150921."
	v2Root            = "/v2"
	v2Prefix          = "/v2/"
	unknownActionName = "Unknown"
)

// regionContextKey is used to store the AWS region in request context.
type regionContextKey struct{}

var (
	errUnknownAction  = errors.New("UnknownOperationException")
	errInvalidRequest = errors.New("InvalidParameterException")
)

// Handler is the Echo HTTP handler for ECR operations.
type Handler struct {
	Backend         Backend
	ops             map[string]service.JSONOpFunc
	registryHandler http.Handler
	janitor         *Janitor
	setEndpointOnce sync.Once
	registryEnabled bool
}

// NewHandler creates a new ECR handler.
// registryHandler may be nil when the local registry is disabled.
func NewHandler(backend Backend, registryHandler http.Handler) *Handler {
	h := &Handler{
		Backend:         backend,
		registryHandler: registryHandler,
		registryEnabled: registryHandler != nil,
	}
	h.ops = h.buildOps()

	return h
}

// WithJanitor attaches a background lifecycle-expiry janitor to the handler.
// interval=0 uses the default of one minute. The optional taskTimeout bounds
// each sweep; 0 means no per-task timeout. It is a no-op unless the backend is
// an *InMemoryBackend.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(memBackend, interval)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}

		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if one is configured. It satisfies
// the service.BackgroundWorker interface.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// RegistryEnabled returns true if the embedded Docker registry is enabled.
func (h *Handler) RegistryEnabled() bool { return h.registryEnabled }

// Name returns the service name.
func (h *Handler) Name() string { return "ECR" }

// GetSupportedOperations returns the list of supported ECR operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchCheckLayerAvailability",
		"BatchDeleteImage",
		"BatchGetImage",
		"BatchGetRepositoryScanningConfiguration",
		"CompleteLayerUpload",
		"CreatePullThroughCacheRule",
		"CreateRepository",
		"CreateRepositoryCreationTemplate",
		"DeleteLifecyclePolicy",
		"DeletePullThroughCacheRule",
		"DeleteRegistryPolicy",
		"DeleteRepository",
		"DeleteRepositoryCreationTemplate",
		"DeleteRepositoryPolicy",
		"DeleteSigningConfiguration",
		"DeregisterPullTimeUpdateExclusion",
		"DescribeImageReplicationStatus",
		"DescribeImageScanFindings",
		"DescribeImageSigningStatus",
		"DescribeImages",
		"DescribePullThroughCacheRules",
		"DescribeRegistry",
		"DescribeRepositories",
		"DescribeRepositoryCreationTemplates",
		"GetAccountSetting",
		"GetAuthorizationToken",
		"GetDownloadUrlForLayer",
		"GetLifecyclePolicy",
		"GetLifecyclePolicyPreview",
		"GetRegistryPolicy",
		"GetRegistryScanningConfiguration",
		"GetRepositoryPolicy",
		"GetSigningConfiguration",
		"InitiateLayerUpload",
		"ListImageReferrers",
		"ListImages",
		"ListPullTimeUpdateExclusions",
		"ListTagsForResource",
		"PutAccountSetting",
		"PutImage",
		"PutImageScanningConfiguration",
		"PutImageTagMutability",
		"PutLifecyclePolicy",
		"PutRegistryPolicy",
		"PutRegistryScanningConfiguration",
		"PutReplicationConfiguration",
		"PutSigningConfiguration",
		"RegisterPullTimeUpdateExclusion",
		"SetRepositoryPolicy",
		"StartImageScan",
		"StartLifecyclePolicyPreview",
		"TagResource",
		"UntagResource",
		"UpdateImageStorageClass",
		"UpdatePullThroughCacheRule",
		"UpdateRepositoryCreationTemplate",
		"UploadLayerPart",
		"ValidatePullThroughCacheRule",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ecr" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ECR instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// isRegistryPath returns true when path is exactly "/v2" or starts with "/v2/".
// This prevents false matches against unrelated paths like "/v20180820/..." (S3Control).
func isRegistryPath(path string) bool {
	return path == v2Root || strings.HasPrefix(path, v2Prefix)
}

// RouteMatcher returns a function that matches ECR requests.
// It matches on:
//   - X-Amz-Target header with AmazonEC2ContainerRegistry_V20150921. prefix (control plane)
//   - /v2 or /v2/ path prefix (Docker registry v2 API, when local registry is enabled)
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if strings.HasPrefix(target, ecrTargetPrefix) {
			return true
		}

		if h.registryEnabled && isRegistryPath(c.Request().URL.Path) {
			return true
		}

		return false
	}
}

// MatchPriority returns the routing priority for ECR.
// Control plane uses header-exact priority; registry uses path-based priority
// elevated above catch-alls.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the ECR action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, ecrTargetPrefix)

	if action == "" || action == target {
		if h.registryEnabled && isRegistryPath(c.Request().URL.Path) {
			return "RegistryV2"
		}

		return unknownActionName
	}

	return action
}

// ExtractResource extracts the repository name from the request.
// For registry v2 paths (/v2/<name>/...) the name is taken from the URL to
// avoid buffering potentially large binary upload bodies.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	if h.registryEnabled && isRegistryPath(path) {
		// /v2 alone (root ping) has no repository component.
		if path == v2Root {
			return ""
		}

		// Extract name from /v2/<name>/...
		trimmed := strings.TrimPrefix(path, v2Prefix)
		name, _, _ := strings.Cut(trimmed, "/")

		return name
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		RepositoryName  string   `json:"repositoryName"`
		RepositoryNames []string `json:"repositoryNames"`
	}

	_ = json.Unmarshal(body, &req)

	if req.RepositoryName != "" {
		return req.RepositoryName
	}

	if len(req.RepositoryNames) > 0 {
		return req.RepositoryNames[0]
	}

	return ""
}

// Handler returns the Echo handler function for ECR requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Lazily set the proxy endpoint from the first request's Host header so
		// that repository URIs and authorization tokens reflect the local server
		// address rather than a default AWS-style endpoint.
		h.setEndpointOnce.Do(func() {
			if h.Backend.ProxyEndpoint() == "" {
				if host := c.Request().Host; host != "" {
					h.Backend.SetEndpoint(host)
				}
			}
		})

		// Docker registry v2 requests are proxied to the embedded registry.
		if h.registryEnabled && isRegistryPath(c.Request().URL.Path) {
			h.registryHandler.ServeHTTP(c.Response(), c.Request())

			return nil
		}

		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		c.SetRequest(c.Request().WithContext(ctx))

		return service.HandleTarget(
			c, logger.Load(ctx),
			"ECR", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// Reset clears the backend state and resets the endpoint lazy-init.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}

	h.setEndpointOnce = sync.Once{}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	ops := h.buildCoreOps()
	maps.Copy(ops, h.buildExtOps())

	return ops
}

func (h *Handler) buildCoreOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"BatchCheckLayerAvailability": service.WrapOp(
			h.handleBatchCheckLayerAvailability,
		),
		"BatchDeleteImage": service.WrapOp(h.handleBatchDeleteImage),
		"BatchGetImage":    service.WrapOp(h.handleBatchGetImage),
		"BatchGetRepositoryScanningConfiguration": service.WrapOp(
			h.handleBatchGetRepositoryScanningConfiguration,
		),
		"CompleteLayerUpload": service.WrapOp(h.handleCompleteLayerUpload),
		"CreatePullThroughCacheRule": service.WrapOp(
			h.handleCreatePullThroughCacheRule,
		),
		"CreateRepository": service.WrapOp(h.handleCreateRepository),
		"CreateRepositoryCreationTemplate": service.WrapOp(
			h.handleCreateRepositoryCreationTemplate,
		),
		"DeleteLifecyclePolicy": service.WrapOp(h.handleDeleteLifecyclePolicy),
		"DeletePullThroughCacheRule": service.WrapOp(
			h.handleDeletePullThroughCacheRule,
		),
		"DeleteRegistryPolicy": service.WrapOp(h.handleDeleteRegistryPolicy),
		"DeleteRepository":     service.WrapOp(h.handleDeleteRepository),
		"DeleteRepositoryCreationTemplate": service.WrapOp(
			h.handleDeleteRepositoryCreationTemplate,
		),
		"DeleteRepositoryPolicy": service.WrapOp(h.handleDeleteRepositoryPolicy),
		"DeleteSigningConfiguration": service.WrapOp(
			h.handleDeleteSigningConfiguration,
		),
		"DeregisterPullTimeUpdateExclusion": service.WrapOp(
			h.handleDeregisterPullTimeUpdateExclusion,
		),
		"DescribeImageReplicationStatus": service.WrapOp(
			h.handleDescribeImageReplicationStatus,
		),
		"DescribeImageScanFindings": service.WrapOp(
			h.handleDescribeImageScanFindings,
		),
		"DescribeImageSigningStatus": service.WrapOp(
			h.handleDescribeImageSigningStatus,
		),
		"DescribeImages": service.WrapOp(h.handleDescribeImages),
		"DescribePullThroughCacheRules": service.WrapOp(
			h.handleDescribePullThroughCacheRules,
		),
		"DescribeRegistry":     service.WrapOp(h.handleDescribeRegistry),
		"DescribeRepositories": service.WrapOp(h.handleDescribeRepositories),
		"DescribeRepositoryCreationTemplates": service.WrapOp(
			h.handleDescribeRepositoryCreationTemplates,
		),
	}
}

func (h *Handler) buildExtOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetAccountSetting":      service.WrapOp(h.handleGetAccountSetting),
		"GetAuthorizationToken":  service.WrapOp(h.handleGetAuthorizationToken),
		"GetDownloadUrlForLayer": service.WrapOp(h.handleGetDownloadURLForLayer),
		"GetLifecyclePolicy":     service.WrapOp(h.handleGetLifecyclePolicy),
		"GetLifecyclePolicyPreview": service.WrapOp(
			h.handleGetLifecyclePolicyPreview,
		),
		"GetRegistryPolicy": service.WrapOp(h.handleGetRegistryPolicy),
		"GetRegistryScanningConfiguration": service.WrapOp(
			h.handleGetRegistryScanningConfiguration,
		),
		"GetRepositoryPolicy":     service.WrapOp(h.handleGetRepositoryPolicy),
		"GetSigningConfiguration": service.WrapOp(h.handleGetSigningConfiguration),
		"InitiateLayerUpload":     service.WrapOp(h.handleInitiateLayerUpload),
		"ListImageReferrers":      service.WrapOp(h.handleListImageReferrers),
		"ListImages":              service.WrapOp(h.handleListImages),
		"ListPullTimeUpdateExclusions": service.WrapOp(
			h.handleListPullTimeUpdateExclusions,
		),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"PutAccountSetting":   service.WrapOp(h.handlePutAccountSetting),
		"PutImage":            service.WrapOp(h.handlePutImage),
		"PutImageScanningConfiguration": service.WrapOp(
			h.handlePutImageScanningConfiguration,
		),
		"PutImageTagMutability": service.WrapOp(h.handlePutImageTagMutability),
		"PutLifecyclePolicy":    service.WrapOp(h.handlePutLifecyclePolicy),
		"PutRegistryPolicy":     service.WrapOp(h.handlePutRegistryPolicy),
		"PutRegistryScanningConfiguration": service.WrapOp(
			h.handlePutRegistryScanningConfiguration,
		),
		"PutReplicationConfiguration": service.WrapOp(
			h.handlePutReplicationConfiguration,
		),
		"PutSigningConfiguration": service.WrapOp(h.handlePutSigningConfiguration),
		"RegisterPullTimeUpdateExclusion": service.WrapOp(
			h.handleRegisterPullTimeUpdateExclusion,
		),
		"SetRepositoryPolicy": service.WrapOp(h.handleSetRepositoryPolicy),
		"StartImageScan":      service.WrapOp(h.handleStartImageScan),
		"StartLifecyclePolicyPreview": service.WrapOp(
			h.handleStartLifecyclePolicyPreview,
		),
		"TagResource":             service.WrapOp(h.handleTagResource),
		"UntagResource":           service.WrapOp(h.handleUntagResource),
		"UpdateImageStorageClass": service.WrapOp(h.handleUpdateImageStorageClass),
		"UpdatePullThroughCacheRule": service.WrapOp(
			h.handleUpdatePullThroughCacheRule,
		),
		"UpdateRepositoryCreationTemplate": service.WrapOp(
			h.handleUpdateRepositoryCreationTemplate,
		),
		"UploadLayerPart": service.WrapOp(h.handleUploadLayerPart),
		"ValidatePullThroughCacheRule": service.WrapOp(
			h.handleValidatePullThroughCacheRule,
		),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// ecrErr builds the error body for an ECR error response.
func ecrErr(errType, msg string) map[string]string {
	return map[string]string{keyTypeField: errType, keyMessageField: msg}
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	status, errType := h.classifyError(err)

	return c.JSON(status, ecrErr(errType, err.Error()))
}

// classifyError returns the HTTP status code and AWS error type string for err.
//
// singleErrStatus maps a single sentinel error to its HTTP status and AWS
// error type string; classifyError ranges over it with errors.Is so each
// entry adds a table row instead of a branch, keeping cyclomatic complexity
// low regardless of how many sentinel errors ECR grows. notFoundGroupErrs are
// distinct sentinel errors that all map to the same generic 404
// NotFoundException response.
func (h *Handler) classifyError(err error) (int, string) {
	singleErrStatus := []struct {
		err     error
		errType string
		status  int
	}{
		{ErrRepositoryNotFound, "RepositoryNotFoundException", http.StatusNotFound},
		{ErrRepositoryPolicyNotFound, "RepositoryPolicyNotFoundException", http.StatusBadRequest},
		{ErrImageNotFound, "ImageNotFoundException", http.StatusBadRequest},
		{ErrScanNotFoundException, "ScanNotFoundException", http.StatusBadRequest},
		{ErrRepositoryAlreadyExists, "RepositoryAlreadyExistsException", http.StatusBadRequest},
		{ErrRepositoryNotEmpty, "RepositoryNotEmptyException", http.StatusBadRequest},
		{ErrImageTagAlreadyExists, "ImageTagAlreadyExistsException", http.StatusBadRequest},
		{ErrLayerInaccessible, "LayerInaccessibleException", http.StatusBadRequest},
		{ErrLayersNotFound, "LayersNotFoundException", http.StatusBadRequest},
		{ErrLayerAlreadyExists, "LayerAlreadyExistsException", http.StatusBadRequest},
		{ErrInvalidLayerPart, "InvalidLayerPartException", http.StatusBadRequest},
		{ErrImageDigestDoesNotMatch, "ImageDigestDoesNotMatchException", http.StatusBadRequest},
		{ErrPullThroughCacheRuleAlreadyExists, "PullThroughCacheRuleAlreadyExistsException", http.StatusBadRequest},
		{ErrRepositoryCreationTemplateAlreadyExists, "TemplateAlreadyExistsException", http.StatusBadRequest},
		{ErrUploadNotFound, "UploadNotFoundException", http.StatusBadRequest},
		{ErrEmptyUpload, "EmptyUploadException", http.StatusBadRequest},
		{ErrLayerPartTooSmall, "LayerPartTooSmallException", http.StatusBadRequest},
		{ErrImageAlreadyExists, "ImageAlreadyExistsException", http.StatusBadRequest},
	}

	for _, e := range singleErrStatus {
		if errors.Is(err, e.err) {
			return e.status, e.errType
		}
	}

	notFoundGroupErrs := []error{
		ErrPullThroughCacheRuleNotFound,
		ErrLifecyclePolicyNotFound,
		ErrRepositoryCreationTemplateNotFound,
		ErrRegistryPolicyNotFound,
	}

	for _, e := range notFoundGroupErrs {
		if errors.Is(err, e) {
			return http.StatusNotFound, "NotFoundException"
		}
	}

	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, errUnknownAction):
		return http.StatusBadRequest, "UnknownOperationException"
	case errors.Is(err, ErrInvalidRepositoryName), errors.Is(err, errInvalidRequest),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return http.StatusBadRequest, "InvalidParameterException"
	default:
		return http.StatusInternalServerError, "InternalServerError"
	}
}
