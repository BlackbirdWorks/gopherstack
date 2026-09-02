package mediaconvert

import (
	"context"
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
	opUnknown = "Unknown"

	// defaultListPageSize matches every List* op's own doc comment ("...up
	// to twenty, that will be returned at one time"): ListJobs, ListQueues,
	// ListJobTemplates, ListPresets.
	defaultListPageSize = 20
)

const (
	orderDescending = "DESCENDING"
)

const (
	listByCreationDate = "CREATION_DATE"
)

const (
	opAssociateCertificate    = "AssociateCertificate"
	opCancelJob               = "CancelJob"
	opCreateJob               = "CreateJob"
	opCreateJobTemplate       = "CreateJobTemplate"
	opCreatePreset            = "CreatePreset"
	opCreateQueue             = "CreateQueue"
	opCreateResourceShare     = "CreateResourceShare"
	opDeleteJobTemplate       = "DeleteJobTemplate"
	opDeletePolicy            = "DeletePolicy"
	opDeletePreset            = "DeletePreset"
	opDeleteQueue             = "DeleteQueue"
	opDescribeEndpoints       = "DescribeEndpoints"
	opDisassociateCertificate = "DisassociateCertificate"
	opGetJob                  = "GetJob"
	opGetJobTemplate          = "GetJobTemplate"
	opGetJobsQueryResults     = "GetJobsQueryResults"
	opGetPolicy               = "GetPolicy"
	opGetPreset               = "GetPreset"
	opGetQueue                = "GetQueue"
	opListJobTemplates        = "ListJobTemplates"
	opListJobs                = "ListJobs"
	opListPresets             = "ListPresets"
	opListQueues              = "ListQueues"
	opListTagsForResource     = "ListTagsForResource"
	opPutPolicy               = "PutPolicy"
	opTagResource             = "TagResource"
	opUntagResource           = "UntagResource"
	// opUpdateJob is an internal route label for PUT /2017-08-29/jobs/{id}. It is
	// NOT a real AWS MediaConvert SDK operation — the real API has no UpdateJob
	// action at all (verified against aws-sdk-go-v2/service/mediaconvert: no
	// UpdateJobInput/UpdateJobOutput/Client.UpdateJob exist, and botocore's
	// mediaconvert service-2.json has no PUT route under /jobs/{id}; only
	// UpdateJobTemplate exists in the Update family). Real MediaConvert jobs are
	// immutable once created — only CancelJob and priority/queue changes via
	// CreateJob's siblings exist, not an in-place update. No real SDK client can
	// ever construct a request that reaches this route, so it stays wired below
	// as internal test scaffolding only, unadvertised — see gopherstack-vhw2
	// category A, same resolution as EMR's ListTagsForResource and CloudFront's
	// GetFunctionAssociations/SetFunctionAssociations.
	opUpdateJob         = "UpdateJob"
	opUpdateJobTemplate = "UpdateJobTemplate"
	opUpdatePreset      = "UpdatePreset"
	opUpdateQueue       = "UpdateQueue"
	opListVersions      = "ListVersions"
	opProbe             = "Probe"
	opSearchJobs        = "SearchJobs"
	opStartJobsQuery    = "StartJobsQuery"
)

const (
	mcMatchPriority    = service.PriorityPathVersioned
	pathPrefix         = "/2017-08-29/"
	queuesPath         = "/2017-08-29/queues"
	jobTemplatesPath   = "/2017-08-29/jobTemplates"
	jobsPath           = "/2017-08-29/jobs"
	endpointsPath      = "/2017-08-29/endpoints"
	tagsPath           = "/2017-08-29/tags"
	presetsPath        = "/2017-08-29/presets"
	policyPath         = "/2017-08-29/policy"
	certificatesPath   = "/2017-08-29/certificates"
	jobsQueriesPath    = "/2017-08-29/jobsQueries"
	resourceSharesPath = "/2017-08-29/resourceShares"
	versionsPath       = "/2017-08-29/versions"
	probePath          = "/2017-08-29/probe"
	searchPath         = "/2017-08-29/search"
)

// Handler is the Echo HTTP handler for Amazon MediaConvert operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new MediaConvert handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state. Implements service.Resettable.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Without this delegation, cli.go's setupPersistence type-asserts the
// service.Registerable value returned by Provider.Init (this Handler, not
// InMemoryBackend) against a Snapshot/Restore interface -- since Handler
// itself never exposed either method, InMemoryBackend.Snapshot/Restore
// (persistence.go) were dead code and this service was never actually
// persisted, despite StorageBackend already declaring the Persistable
// contract.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaConvert" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opAssociateCertificate,
		opCancelJob,
		opCreateJob,
		opCreateJobTemplate,
		opCreatePreset,
		opCreateQueue,
		opCreateResourceShare,
		opDeleteJobTemplate,
		opDeletePolicy,
		opDeletePreset,
		opDeleteQueue,
		opDescribeEndpoints,
		opDisassociateCertificate,
		opGetJob,
		opGetJobTemplate,
		opGetJobsQueryResults,
		opGetPolicy,
		opGetPreset,
		opGetQueue,
		opListJobTemplates,
		opListJobs,
		opListPresets,
		opListQueues,
		opListTagsForResource,
		opPutPolicy,
		opTagResource,
		opUntagResource,
		// opUpdateJob is deliberately NOT advertised — see its doc comment above;
		// it is not a real MediaConvert SDK operation.
		opUpdateJobTemplate,
		opUpdatePreset,
		opUpdateQueue,
		opListVersions,
		opProbe,
		opSearchJobs,
		opStartJobsQuery,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "mediaconvert" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches MediaConvert requests.
// MediaConvert uses REST paths prefixed with /2017-08-29/.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, pathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return mcMatchPriority }

// ExtractOperation returns the operation name from the request path and method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.Path).operation
}

// ExtractResource extracts a resource ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.Path).resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		route := parseRoute(r.Method, r.URL.Path)

		return h.dispatch(c, route)
	}
}

// dispatch routes the request to the appropriate handler based on the parsed route.
func (h *Handler) dispatch(c *echo.Context, route mcRoute) error {
	r := c.Request()
	log := logger.Load(r.Context())

	readBody := func() ([]byte, bool) {
		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "mediaconvert: failed to read request body", "error", err)

			return nil, false
		}

		return body, true
	}

	if handled, result := h.dispatchReadOnly(c, route); handled {
		return result
	}

	return h.dispatchMutating(c, route, readBody)
}

// dispatchReadOnly handles operations that do not require a request body.
// This includes both read operations and DELETE operations where no body is needed.
func (h *Handler) dispatchReadOnly(c *echo.Context, route mcRoute) (bool, error) {
	switch route.operation {
	case opListQueues:
		return true, h.handleListQueues(c)
	case opGetQueue:
		return true, h.handleGetQueue(c, route.resource)
	case opDeleteQueue:
		return true, h.handleDeleteQueue(c, route.resource)
	case opListJobTemplates:
		return true, h.handleListJobTemplates(c)
	case opGetJobTemplate:
		return true, h.handleGetJobTemplate(c, route.resource)
	case opDeleteJobTemplate:
		return true, h.handleDeleteJobTemplate(c, route.resource)
	case opListJobs:
		return true, h.handleListJobs(c)
	case opGetJob:
		return true, h.handleGetJob(c, route.resource)
	case opCancelJob:
		return true, h.handleCancelJob(c, route.resource)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c, route.resource)
	}

	return h.dispatchReadOnlyNewOps(c, route)
}

// dispatchReadOnlyNewOps handles newer operations that do not require a request body.
// These are the operations added after the initial implementation.
func (h *Handler) dispatchReadOnlyNewOps(c *echo.Context, route mcRoute) (bool, error) {
	switch route.operation {
	case opListPresets:
		return true, h.handleListPresets(c)
	case opGetPreset:
		return true, h.handleGetPreset(c, route.resource)
	case opDeletePreset:
		return true, h.handleDeletePreset(c, route.resource)
	case opGetPolicy:
		return true, h.handleGetPolicy(c)
	case opDeletePolicy:
		return true, h.handleDeletePolicy(c)
	case opDisassociateCertificate:
		return true, h.handleDisassociateCertificate(c, route.resource)
	case opGetJobsQueryResults:
		return true, h.handleGetJobsQueryResults(c, route.resource)
	case opListVersions:
		return true, h.handleListVersions(c)
	case opSearchJobs:
		return true, h.handleSearchJobs(c)
	}

	return false, nil
}

// dispatchMutating handles write operations that require reading a request body.
func (h *Handler) dispatchMutating(c *echo.Context, route mcRoute, readBody func() ([]byte, bool)) error {
	body, ok := readBody()
	if !ok {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalError", "internal server error"))
	}

	switch route.operation {
	case opCreateQueue:
		return h.handleCreateQueue(c, body)
	case opUpdateQueue:
		return h.handleUpdateQueue(c, route.resource, body)
	case opCreateJobTemplate:
		return h.handleCreateJobTemplate(c, body)
	case opUpdateJobTemplate:
		return h.handleUpdateJobTemplate(c, route.resource, body)
	case opCreateJob:
		return h.handleCreateJob(c, body)
	case opUpdateJob:
		return h.handleUpdateJob(c, route.resource, body)
	case opTagResource:
		return h.handleTagResource(c, body)
	case opUntagResource:
		return h.handleUntagResource(c, route.resource, body)
	}

	return h.dispatchMutatingNewOps(c, route, body)
}

// dispatchMutatingNewOps handles write operations added after the initial implementation.
func (h *Handler) dispatchMutatingNewOps(c *echo.Context, route mcRoute, body []byte) error {
	switch route.operation {
	case opCreatePreset:
		return h.handleCreatePreset(c, body)
	case opUpdatePreset:
		return h.handleUpdatePreset(c, route.resource, body)
	case opPutPolicy:
		return h.handlePutPolicy(c, body)
	case opAssociateCertificate:
		return h.handleAssociateCertificate(c, body)
	case opCreateResourceShare:
		return h.handleCreateResourceShare(c, body)
	case opProbe:
		return h.handleProbe(c, body)
	case opStartJobsQuery:
		return h.handleStartJobsQuery(c, body)
	case opDescribeEndpoints:
		return h.handleDescribeEndpoints(c, body)
	}

	return c.JSON(
		http.StatusNotFound,
		errorResponse("NotFoundException", "unknown operation: "+c.Request().URL.Path),
	)
}

// mcRoute holds the parsed route information.
type mcRoute struct {
	resource  string
	operation string
}

// parseRoute maps HTTP method + path to an operation name and resource ID.
func parseRoute(method, path string) mcRoute {
	switch {
	case strings.HasPrefix(path, queuesPath):
		return parseQueueRoute(method, strings.TrimPrefix(path, queuesPath))
	case strings.HasPrefix(path, jobTemplatesPath):
		return parseJobTemplateRoute(method, strings.TrimPrefix(path, jobTemplatesPath))
	case strings.HasPrefix(path, jobsQueriesPath):
		return parseJobsQueriesRoute(method, strings.TrimPrefix(path, jobsQueriesPath))
	case strings.HasPrefix(path, jobsPath):
		return parseJobRoute(method, strings.TrimPrefix(path, jobsPath))
	case strings.HasPrefix(path, presetsPath):
		return parsePresetRoute(method, strings.TrimPrefix(path, presetsPath))
	case strings.HasPrefix(path, tagsPath):
		return parseTagRoute(method, strings.TrimPrefix(path, tagsPath))
	case strings.HasPrefix(path, certificatesPath):
		return parseCertificateRoute(method, strings.TrimPrefix(path, certificatesPath))
	}

	return parseStaticPathRoute(method, path)
}

// parseStaticPathRoute handles non-prefix (exact-match) path routes.
func parseStaticPathRoute(method, path string) mcRoute {
	switch path {
	case policyPath:
		return parsePolicyRoute(method)
	case endpointsPath:
		if method == http.MethodPost {
			return mcRoute{operation: opDescribeEndpoints}
		}
	case resourceSharesPath:
		if method == http.MethodPost {
			return mcRoute{operation: opCreateResourceShare}
		}
	case versionsPath:
		if method == http.MethodGet {
			return mcRoute{operation: opListVersions}
		}
	case probePath:
		if method == http.MethodPost {
			return mcRoute{operation: opProbe}
		}
	case searchPath:
		if method == http.MethodGet {
			return mcRoute{operation: opSearchJobs}
		}
	}

	return mcRoute{operation: opUnknown}
}

// reverseSlice reverses items in-place.
func reverseSlice[T any](items []T) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

// applyListOrdering is the shared category/listBy/order logic for
// ListJobTemplates, ListPresets, and ListQueues (getCategory returns "" for
// ListQueues, which has no category field on the real wire). listBy ==
// listByCreationDate re-sorts by createdAt; the caller's backend List* call
// already returns NAME order, the documented default.
func applyListOrdering[T any](
	items []T, category string, getCategory func(T) string, createdAt func(T) float64, listBy, order string,
) []T {
	if category != "" {
		filtered := items[:0:0]

		for _, it := range items {
			if getCategory(it) == category {
				filtered = append(filtered, it)
			}
		}

		items = filtered
	}

	if listBy == listByCreationDate {
		sort.Slice(items, func(i, j int) bool { return createdAt(items[i]) < createdAt(items[j]) })
	}

	if order == orderDescending {
		reverseSlice(items)
	}

	return items
}

// limitSlice returns at most maxResults items; 0 means no limit.
// parseMaxResults converts a query-parameter string to a non-negative int,
// returning 0 (no limit) when the string is empty or unparseable.
func parseMaxResults(s string) int {
	if s == "" {
		return 0
	}

	var n int

	if _, err := fmt.Sscanf(s, "%d", &n); err != nil || n < 0 {
		return 0
	}

	return n
}

// --- Error handling ---

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ConflictException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalError", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}
