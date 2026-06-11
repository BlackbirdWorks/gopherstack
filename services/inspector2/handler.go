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
	keyStatus         = "status"
	keyFailedAccounts = "failedAccounts"
	keyArn            = "arn"
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

	return append(base, appendixAOps()...)
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

	if op := classifyAppendixAPath(method, path); op != opUnknown {
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

	if handled, err := h.handleAppendixA(c); handled {
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

// handleToggle handles POST /enable and POST /disable.
func (h *Handler) handleToggle(c *echo.Context, enable bool) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ResourceTypes []string `json:"resourceTypes"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	if enable {
		err = h.Backend.Enable(req.ResourceTypes)
	} else {
		err = h.Backend.Disable(req.ResourceTypes)
	}

	if err != nil {
		return h.mapError(c, err)
	}

	status := h.Backend.GetStatus()

	return c.JSON(http.StatusOK, map[string]any{
		keyAccounts: []map[string]any{
			{
				keyAccountID:      status.AccountID,
				keyResourceStatus: buildResourceStatus(status),
				keyStatus:         status.Status,
			},
		},
		keyFailedAccounts: []any{},
	})
}

// handleBatchGetAccountStatus handles POST /status/batch/get.
func (h *Handler) handleBatchGetAccountStatus(c *echo.Context) error {
	status := h.Backend.GetStatus()

	return c.JSON(http.StatusOK, map[string]any{
		keyAccounts: []map[string]any{
			{
				keyAccountID:      status.AccountID,
				keyResourceStatus: buildResourceStatus(status),
				"state": map[string]any{
					keyStatus: map[string]any{
						keyStatus: status.Status,
					},
				},
			},
		},
		keyFailedAccounts: []any{},
	})
}

// handleCreateFilter handles POST /filters/create.
func (h *Handler) handleCreateFilter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any    `json:"filterCriteria"`
		Tags           map[string]string `json:"tags"`
		Name           string            `json:"name"`
		Action         string            `json:"action"`
		Description    string            `json:"description"`
		Reason         string            `json:"reason"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "name is required"))
	}

	if req.Action == "" {
		req.Action = "NONE"
	}

	f, createErr := h.Backend.CreateFilter(
		req.Name,
		req.Action,
		req.Description,
		req.Reason,
		req.FilterCriteria,
		req.Tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyArn: f.Arn})
}

// handleUpdateFilter handles POST /filters/update.
func (h *Handler) handleUpdateFilter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
		FilterArn      string         `json:"filterArn"`
		Action         string         `json:"action"`
		Description    string         `json:"description"`
		Reason         string         `json:"reason"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.FilterArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "filterArn is required"))
	}

	f, updateErr := h.Backend.UpdateFilter(req.FilterArn, req.Action, req.Description, req.Reason, req.FilterCriteria)
	if updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyArn: f.Arn})
}

// handleDeleteFilter handles POST /filters/delete.
func (h *Handler) handleDeleteFilter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Arn string `json:"arn"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.Arn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "arn is required"))
	}

	if deleteErr := h.Backend.DeleteFilter(req.Arn); deleteErr != nil {
		return h.mapError(c, deleteErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyArn: req.Arn})
}

// handleListFilters handles POST /filters/list.
func (h *Handler) handleListFilters(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Action string   `json:"action"`
		Arns   []string `json:"arns"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	filters, listErr := h.Backend.ListFilters(req.Arns, req.Action)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	result := make([]map[string]any, 0, len(filters))
	for _, f := range filters {
		entry := map[string]any{
			keyArn:      f.Arn,
			"name":      f.Name,
			"action":    f.Action,
			"ownerId":   f.OwnerID,
			"createdAt": f.CreatedAt,
			"updatedAt": f.UpdatedAt,
		}

		if f.Description != "" {
			entry["description"] = f.Description
		}

		if f.Reason != "" {
			entry["reason"] = f.Reason
		}

		if f.Criteria != nil {
			entry["filterCriteria"] = f.Criteria
		}

		if len(f.Tags) > 0 {
			entry["tags"] = f.Tags
		}

		result = append(result, entry)
	}

	return c.JSON(http.StatusOK, map[string]any{"filters": result})
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

// handleListFindings handles POST /findings/list.
func (h *Handler) handleListFindings(c *echo.Context) error {
	req, ok := decodeFilterListRequest(c)
	if !ok {
		return nil
	}

	findings, nextToken, findErr := h.Backend.ListFindings(req.MaxResults, req.NextToken, req.FilterCriteria)
	if findErr != nil {
		return h.mapError(c, findErr)
	}

	resp := map[string]any{"findings": findings}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// handleGetConfiguration handles POST /configuration/get.
func (h *Handler) handleGetConfiguration(c *echo.Context) error {
	cfg := h.Backend.GetConfiguration()

	return c.JSON(http.StatusOK, map[string]any{
		"ec2Configuration": map[string]any{
			"scanModeState": map[string]any{
				"scanMode":       cfg.Ec2ScanMode,
				"scanModeStatus": statusEnabled,
			},
		},
		"ecrConfiguration": map[string]any{
			"rescanDurationState": map[string]any{
				"rescanDuration": cfg.EcrRescanDuration,
				keyStatus:        statusEnabled,
				"updatedAt":      nil,
			},
		},
	})
}

// handleUpdateConfiguration handles POST /configuration/update.
func (h *Handler) handleUpdateConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Ec2Configuration *struct {
			ScanMode string `json:"scanMode"`
		} `json:"ec2Configuration"`
		EcrConfiguration *struct {
			RescanDuration string `json:"rescanDuration"`
		} `json:"ecrConfiguration"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	var ec2ScanMode, ecrRescanDuration string

	if req.Ec2Configuration != nil {
		ec2ScanMode = req.Ec2Configuration.ScanMode
	}

	if req.EcrConfiguration != nil {
		ecrRescanDuration = req.EcrConfiguration.RescanDuration
	}

	if updateErr := h.Backend.UpdateConfiguration(ec2ScanMode, ecrRescanDuration); updateErr != nil {
		return h.mapError(c, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleListTagsForResource handles GET /tags/{resourceArn}.
func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.Path)
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "resourceArn is required"))
	}

	tags, listErr := h.Backend.ListTagsForResource(resourceARN)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}

// handleTagResource handles POST /tags/{resourceArn}.
func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.Path)
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "resourceArn is required"))
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if tagErr := h.Backend.TagResource(resourceARN, req.Tags); tagErr != nil {
		return h.mapError(c, tagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleUntagResource handles DELETE /tags/{resourceArn}.
func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.Path)
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "resourceArn is required"))
	}

	tagKeys := c.Request().URL.Query()["tagKeys"]

	if untagErr := h.Backend.UntagResource(resourceARN, tagKeys); untagErr != nil {
		return h.mapError(c, untagErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// extractResourceARN extracts the resource ARN from the URL path.
func extractResourceARN(path string) string {
	resource, _ := strings.CutPrefix(path, pathTagsPrefix)

	if resource == path {
		return ""
	}

	return resource
}

// buildResourceStatus constructs the resourceStatus map.
func buildResourceStatus(status *AccountStatusResponse) map[string]any {
	return map[string]any{
		"ec2":    status.Ec2Status,
		"ecr":    status.EcrStatus,
		"lambda": status.LambdaStatus,
	}
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

		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal error"))
	}
}
