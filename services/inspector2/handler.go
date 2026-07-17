package inspector2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	inspector2ServiceName = "inspector2"
	matchPriority         = service.PriorityPathVersioned

	opEnable                = "Enable"
	opDisable               = "Disable"
	opBatchGetAccountStatus = "BatchGetAccountStatus"
	opCreateFilter          = "CreateFilter"
	opUpdateFilter          = "UpdateFilter"
	opDeleteFilter          = "DeleteFilter"
	opListFilters           = "ListFilters"
	opListFindings          = "ListFindings"
	opGetConfiguration      = "GetConfiguration"
	opUpdateConfiguration   = "UpdateConfiguration"
	opTagResource           = "TagResource"
	opUntagResource         = "UntagResource"
	opListTagsForResource   = "ListTagsForResource"
	opUnknown               = "Unknown"

	pathEnable              = "/enable"
	pathDisable             = "/disable"
	pathStatusBatchGet      = "/status/batch/get"
	pathFiltersCreate       = "/filters/create"
	pathFiltersUpdate       = "/filters/update"
	pathFiltersDelete       = "/filters/delete"
	pathFiltersList         = "/filters/list"
	pathFindingsList        = "/findings/list"
	pathConfigurationGet    = "/configuration/get"
	pathConfigurationUpdate = "/configuration/update"
	pathTagsPrefix          = "/tags/"

	keyAccounts       = "accounts"
	keyAccountID      = "accountId"
	keyResourceStatus = "resourceStatus"
	keyResourceState  = "resourceState"
	keyStatus         = "status"
	keyFailedAccounts = "failedAccounts"
	keyArn            = "arn"
	keyErrorCode      = "errorCode"
	keyErrorMessage   = "errorMessage"
	keyName           = "name"
	keyUpdatedAt      = "updatedAt"
	keyType           = "type"
)

// Handler handles Inspector2 HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Inspector2" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	base := []string{ //nolint:prealloc // existing issue.
		opEnable,
		opDisable,
		opBatchGetAccountStatus,
		opCreateFilter,
		opUpdateFilter,
		opDeleteFilter,
		opListFilters,
		opListFindings,
		opGetConfiguration,
		opUpdateConfiguration,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}

	return append(base, extendedOps()...)
}

// RouteMatcher returns a matcher that accepts Inspector2 REST paths.
func (h *Handler) RouteMatcher() service.Matcher { //nolint:cyclop // existing issue.
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, pathEnable) ||
			strings.HasPrefix(path, pathDisable) ||
			strings.HasPrefix(path, "/status/") ||
			strings.HasPrefix(path, "/filters/") ||
			strings.HasPrefix(path, "/findings/") ||
			strings.HasPrefix(path, "/configuration/") ||
			strings.HasPrefix(path, pathTagsPrefix+"arn:aws:inspector2:") ||
			strings.HasPrefix(path, "/members/") ||
			strings.HasPrefix(path, "/delegatedadminaccounts/") ||
			strings.HasPrefix(path, "/organizationconfiguration/") ||
			strings.HasPrefix(path, "/ec2deepinspection") ||
			strings.HasPrefix(path, "/encryptionkey/") ||
			strings.HasPrefix(path, "/cis/") ||
			strings.HasPrefix(path, "/cissession/") ||
			strings.HasPrefix(path, "/codesecurity/") ||
			strings.HasPrefix(path, "/reporting/") ||
			strings.HasPrefix(path, "/sbomexport/") ||
			strings.HasPrefix(path, "/coverage/") ||
			strings.HasPrefix(path, "/findings/aggregation/") ||
			strings.HasPrefix(path, "/usage/") ||
			strings.HasPrefix(path, "/accountpermissions/") ||
			strings.HasPrefix(path, "/vulnerabilities/") ||
			strings.HasPrefix(path, "/codesnippet/") ||
			strings.HasPrefix(path, "/freetrialinfo/") ||
			strings.HasPrefix(path, "/cluster/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	if op := classifyPath(method, path); op != opUnknown {
		return op
	}

	if op := classifyExtendedPath(method, path); op != opUnknown {
		return op
	}

	return opUnknown
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	if resource, ok := strings.CutPrefix(path, pathTagsPrefix); ok {
		return resource
	}

	return path
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	path := c.Request().URL.Path
	method := c.Request().Method

	switch classifyPath(method, path) {
	case opEnable:
		return h.handleToggle(c, true)
	case opDisable:
		return h.handleToggle(c, false)
	case opBatchGetAccountStatus:
		return h.handleBatchGetAccountStatus(c)
	case opCreateFilter:
		return h.handleCreateFilter(c)
	case opUpdateFilter:
		return h.handleUpdateFilter(c)
	case opDeleteFilter:
		return h.handleDeleteFilter(c)
	case opListFilters:
		return h.handleListFilters(c)
	case opListFindings:
		return h.handleListFindings(c)
	case opGetConfiguration:
		return h.handleGetConfiguration(c)
	case opUpdateConfiguration:
		return h.handleUpdateConfiguration(c)
	case opListTagsForResource:
		return h.handleListTagsForResource(c)
	case opTagResource:
		return h.handleTagResource(c)
	case opUntagResource:
		return h.handleUntagResource(c)
	}

	if handled, err := h.handleExtendedOps(c); handled {
		return err
	}

	log := logger.Load(c.Request().Context())
	log.Debug("inspector2: unhandled request", "method", method, "path", path)

	return c.JSON(http.StatusNotImplemented, map[string]string{
		"__type":  "NotImplementedException",
		"message": "Operation not implemented: " + method + " " + path,
	})
}

//nolint:cyclop // exhaustive path-to-operation mapping is inherently complex
func classifyPath(method, path string) string {
	switch {
	case method == http.MethodPost && path == pathEnable:
		return opEnable
	case method == http.MethodPost && path == pathDisable:
		return opDisable
	case method == http.MethodPost && path == pathStatusBatchGet:
		return opBatchGetAccountStatus
	case method == http.MethodPost && path == pathFiltersCreate:
		return opCreateFilter
	case method == http.MethodPost && path == pathFiltersUpdate:
		return opUpdateFilter
	case method == http.MethodPost && path == pathFiltersDelete:
		return opDeleteFilter
	case method == http.MethodPost && path == pathFiltersList:
		return opListFilters
	case method == http.MethodPost && path == pathFindingsList:
		return opListFindings
	case method == http.MethodPost && path == pathConfigurationGet:
		return opGetConfiguration
	case method == http.MethodPost && path == pathConfigurationUpdate:
		return opUpdateConfiguration
	case method == http.MethodGet && strings.HasPrefix(path, pathTagsPrefix):
		return opListTagsForResource
	case method == http.MethodPost && strings.HasPrefix(path, pathTagsPrefix):
		return opTagResource
	case method == http.MethodDelete && strings.HasPrefix(path, pathTagsPrefix):
		return opUntagResource
	}

	return opUnknown
}

// filterListRequest is the shared shape of the filterCriteria/maxResults/
// nextToken list requests used by ListFindings and ListCoverage.
type filterListRequest struct {
	FilterCriteria map[string]any `json:"filterCriteria"`
	NextToken      string         `json:"nextToken"`
	MaxResults     int32          `json:"maxResults"`
}

// decodeFilterListRequest reads and decodes a filterListRequest. On a malformed
// body it returns ok=false after writing the appropriate error response.
func decodeFilterListRequest(c *echo.Context) (filterListRequest, bool) {
	var req filterListRequest

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		_ = c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))

		return req, false
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			_ = c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))

			return req, false
		}
	}

	return req, true
}

// extractResourceARN extracts the resource ARN from the URL path.
func extractResourceARN(path string) string {
	resource, _ := strings.CutPrefix(path, pathTagsPrefix)

	if resource == path {
		return ""
	}

	return resource
}

// errorResponse builds a standard Inspector2 error JSON body.
func errorResponse(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}

// mapError translates backend errors to HTTP responses.
func (h *Handler) mapError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse(errResourceNotFound, err.Error()))
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusConflict, errorResponse(errConflict, err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errorResponse(errValidation, err.Error()))
	default:
		log := logger.Load(c.Request().Context())
		log.Error("inspector2: unexpected error", "err", err)

		return c.JSON(
			http.StatusInternalServerError,
			errorResponse("InternalServerException", "internal error"),
		)
	}
}
