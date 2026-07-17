package codebuild

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codebuildTargetPrefix = "CodeBuild_20161006."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for CodeBuild operations.
type Handler struct {
	Backend *InMemoryBackend
	janitor *Janitor
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new CodeBuild handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.dispatchTable()

	return h
}

// Reset clears the handler state by delegating to the backend Reset.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// WithJanitor attaches a background janitor to the handler.
func (h *Handler) WithJanitor(interval, buildTTL time.Duration, taskTimeout ...time.Duration) *Handler {
	j := NewJanitor(h.Backend, interval, buildTTL)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}
	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeBuild" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchDeleteBuilds",
		"BatchGetBuildBatches",
		"BatchGetBuilds",
		"BatchGetCommandExecutions",
		"BatchGetFleets",
		"BatchGetProjects",
		"BatchGetReportGroups",
		"BatchGetReports",
		"BatchGetSandboxes",
		"CreateFleet",
		"CreateProject",
		"CreateReportGroup",
		"CreateWebhook",
		"DeleteBuildBatch",
		"DeleteFleet",
		"DeleteProject",
		"DeleteReport",
		"DeleteReportGroup",
		"DeleteResourcePolicy",
		"DeleteSourceCredentials",
		"DeleteWebhook",
		"DescribeCodeCoverages",
		"DescribeTestCases",
		"GetReportGroupTrend",
		"GetResourcePolicy",
		"ImportSourceCredentials",
		"InvalidateProjectCache",
		"ListBuildBatches",
		"ListBuildBatchesForProject",
		"ListBuilds",
		"ListBuildsForProject",
		"ListCommandExecutionsForSandbox",
		"ListCuratedEnvironmentImages",
		"ListFleets",
		"ListProjects",
		"ListReportGroups",
		"ListReports",
		"ListReportsForReportGroup",
		"ListSandboxes",
		"ListSandboxesForProject",
		"ListSharedProjects",
		"ListSharedReportGroups",
		"ListSourceCredentials",
		"ListTagsForResource",
		"PutResourcePolicy",
		"RetryBuild",
		"RetryBuildBatch",
		"StartBuild",
		"StartBuildBatch",
		"StartCommandExecution",
		"StartSandbox",
		"StartSandboxConnection",
		"StopBuild",
		"StopBuildBatch",
		"StopSandbox",
		"TagResource",
		"UntagResource",
		"UpdateFleet",
		"UpdateProject",
		"UpdateProjectVisibility",
		"UpdateReportGroup",
		"UpdateWebhook",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codebuild" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches CodeBuild requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codebuildTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeBuild action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, codebuildTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request (not used for CodeBuild).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for CodeBuild requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeBuild", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"BatchDeleteBuilds":               service.WrapOp(h.handleBatchDeleteBuilds),
		"BatchGetBuildBatches":            service.WrapOp(h.handleBatchGetBuildBatches),
		"BatchGetBuilds":                  service.WrapOp(h.handleBatchGetBuilds),
		"BatchGetCommandExecutions":       service.WrapOp(h.handleBatchGetCommandExecutions),
		"BatchGetFleets":                  service.WrapOp(h.handleBatchGetFleets),
		"BatchGetProjects":                service.WrapOp(h.handleBatchGetProjects),
		"BatchGetReportGroups":            service.WrapOp(h.handleBatchGetReportGroups),
		"BatchGetReports":                 service.WrapOp(h.handleBatchGetReports),
		"BatchGetSandboxes":               service.WrapOp(h.handleBatchGetSandboxes),
		"CreateFleet":                     service.WrapOp(h.handleCreateFleet),
		"CreateProject":                   service.WrapOp(h.handleCreateProject),
		"CreateReportGroup":               service.WrapOp(h.handleCreateReportGroup),
		"CreateWebhook":                   service.WrapOp(h.handleCreateWebhook),
		"DeleteBuildBatch":                service.WrapOp(h.handleDeleteBuildBatch),
		"DeleteFleet":                     service.WrapOp(h.handleDeleteFleet),
		"DeleteProject":                   service.WrapOp(h.handleDeleteProject),
		"DeleteReport":                    service.WrapOp(h.handleDeleteReport),
		"DeleteReportGroup":               service.WrapOp(h.handleDeleteReportGroup),
		"DeleteResourcePolicy":            service.WrapOp(h.handleDeleteResourcePolicy),
		"DeleteSourceCredentials":         service.WrapOp(h.handleDeleteSourceCredentials),
		"DeleteWebhook":                   service.WrapOp(h.handleDeleteWebhook),
		"DescribeCodeCoverages":           service.WrapOp(h.handleDescribeCodeCoverages),
		"DescribeTestCases":               service.WrapOp(h.handleDescribeTestCases),
		"GetReportGroupTrend":             service.WrapOp(h.handleGetReportGroupTrend),
		"GetResourcePolicy":               service.WrapOp(h.handleGetResourcePolicy),
		"ImportSourceCredentials":         service.WrapOp(h.handleImportSourceCredentials),
		"InvalidateProjectCache":          service.WrapOp(h.handleInvalidateProjectCache),
		"ListBuildBatches":                service.WrapOp(h.handleListBuildBatches),
		"ListBuildBatchesForProject":      service.WrapOp(h.handleListBuildBatchesForProject),
		"ListBuilds":                      service.WrapOp(h.handleListBuilds),
		"ListBuildsForProject":            service.WrapOp(h.handleListBuildsForProject),
		"ListCommandExecutionsForSandbox": service.WrapOp(h.handleListCommandExecutionsForSandbox),
		"ListCuratedEnvironmentImages":    service.WrapOp(h.handleListCuratedEnvironmentImages),
		"ListFleets":                      service.WrapOp(h.handleListFleets),
		"ListProjects":                    service.WrapOp(h.handleListProjects),
		"ListReportGroups":                service.WrapOp(h.handleListReportGroups),
		"ListReports":                     service.WrapOp(h.handleListReports),
		"ListReportsForReportGroup":       service.WrapOp(h.handleListReportsForReportGroup),
		"ListSandboxes":                   service.WrapOp(h.handleListSandboxes),
		"ListSandboxesForProject":         service.WrapOp(h.handleListSandboxesForProject),
		"ListSharedProjects":              service.WrapOp(h.handleListSharedProjects),
		"ListSharedReportGroups":          service.WrapOp(h.handleListSharedReportGroups),
		"ListSourceCredentials":           service.WrapOp(h.handleListSourceCredentials),
		"ListTagsForResource":             service.WrapOp(h.handleListTagsForResource),
		"PutResourcePolicy":               service.WrapOp(h.handlePutResourcePolicy),
		"RetryBuild":                      service.WrapOp(h.handleRetryBuild),
		"RetryBuildBatch":                 service.WrapOp(h.handleRetryBuildBatch),
		"StartBuild":                      service.WrapOp(h.handleStartBuild),
		"StartBuildBatch":                 service.WrapOp(h.handleStartBuildBatch),
		"StartCommandExecution":           service.WrapOp(h.handleStartCommandExecution),
		"StartSandbox":                    service.WrapOp(h.handleStartSandbox),
		"StartSandboxConnection":          service.WrapOp(h.handleStartSandboxConnection),
		"StopBuild":                       service.WrapOp(h.handleStopBuild),
		"StopBuildBatch":                  service.WrapOp(h.handleStopBuildBatch),
		"StopSandbox":                     service.WrapOp(h.handleStopSandbox),
		"TagResource":                     service.WrapOp(h.handleTagResource),
		"UntagResource":                   service.WrapOp(h.handleUntagResource),
		"UpdateFleet":                     service.WrapOp(h.handleUpdateFleet),
		"UpdateProject":                   service.WrapOp(h.handleUpdateProject),
		"UpdateProjectVisibility":         service.WrapOp(h.handleUpdateProjectVisibility),
		"UpdateReportGroup":               service.WrapOp(h.handleUpdateReportGroup),
		"UpdateWebhook":                   service.WrapOp(h.handleUpdateWebhook),
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

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceAlreadyExistsException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrValidation):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidInputException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}
