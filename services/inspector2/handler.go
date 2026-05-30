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
	return []string{
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
}

// RouteMatcher returns a matcher that accepts Inspector2 REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		for _, prefix := range inspector2Prefixes {
			if strings.HasPrefix(path, prefix) {
				return true
			}
		}

		return false
	}
}

var inspector2Prefixes = []string{
	"/enable",
	"/disable",
	"/status/",
	"/filters/",
	"/findings/",
	"/configuration/",
	"/tags/arn:aws:inspector2:",
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	if strings.HasPrefix(path, "/tags/") {
		return path[len("/tags/"):]
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

	switch {
	case method == http.MethodPost && path == "/enable":
		return h.handleEnable(c)
	case method == http.MethodPost && path == "/disable":
		return h.handleDisable(c)
	case method == http.MethodPost && path == "/status/batch/get":
		return h.handleBatchGetAccountStatus(c)
	case method == http.MethodPost && path == "/filters/create":
		return h.handleCreateFilter(c)
	case method == http.MethodPost && path == "/filters/update":
		return h.handleUpdateFilter(c)
	case method == http.MethodPost && path == "/filters/delete":
		return h.handleDeleteFilter(c)
	case method == http.MethodPost && path == "/filters/list":
		return h.handleListFilters(c)
	case method == http.MethodPost && path == "/findings/list":
		return h.handleListFindings(c)
	case method == http.MethodPost && path == "/configuration/get":
		return h.handleGetConfiguration(c)
	case method == http.MethodPost && path == "/configuration/update":
		return h.handleUpdateConfiguration(c)
	case method == http.MethodGet && strings.HasPrefix(path, "/tags/"):
		return h.handleListTagsForResource(c)
	case method == http.MethodPost && strings.HasPrefix(path, "/tags/"):
		return h.handleTagResource(c)
	case method == http.MethodDelete && strings.HasPrefix(path, "/tags/"):
		return h.handleUntagResource(c)
	}

	log := logger.Load(c.Request().Context())
	log.Debug("inspector2: unhandled request", "method", method, "path", path)

	return c.JSON(http.StatusNotImplemented, map[string]string{
		"__type":  "NotImplementedException",
		"message": "Operation not implemented: " + method + " " + path,
	})
}

// classifyPath maps a method+path to an operation name.
func classifyPath(method, path string) string {
	switch {
	case method == http.MethodPost && path == "/enable":
		return opEnable
	case method == http.MethodPost && path == "/disable":
		return opDisable
	case method == http.MethodPost && path == "/status/batch/get":
		return opBatchGetAccountStatus
	case method == http.MethodPost && path == "/filters/create":
		return opCreateFilter
	case method == http.MethodPost && path == "/filters/update":
		return opUpdateFilter
	case method == http.MethodPost && path == "/filters/delete":
		return opDeleteFilter
	case method == http.MethodPost && path == "/filters/list":
		return opListFilters
	case method == http.MethodPost && path == "/findings/list":
		return opListFindings
	case method == http.MethodPost && path == "/configuration/get":
		return opGetConfiguration
	case method == http.MethodPost && path == "/configuration/update":
		return opUpdateConfiguration
	case method == http.MethodGet && strings.HasPrefix(path, "/tags/"):
		return opListTagsForResource
	case method == http.MethodPost && strings.HasPrefix(path, "/tags/"):
		return opTagResource
	case method == http.MethodDelete && strings.HasPrefix(path, "/tags/"):
		return opUntagResource
	}

	return opUnknown
}

// handleEnable handles POST /enable.
func (h *Handler) handleEnable(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ResourceTypes []string `json:"resourceTypes"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	if err := h.Backend.Enable(req.ResourceTypes); err != nil {
		return h.mapError(c, err)
	}

	status := h.Backend.GetStatus()

	return c.JSON(http.StatusOK, map[string]any{
		"accounts": []map[string]any{
			{
				"accountId":      status.AccountId,
				"resourceStatus": buildResourceStatus(status),
				"status":         status.Status,
			},
		},
		"failedAccounts": []any{},
	})
}

// handleDisable handles POST /disable.
func (h *Handler) handleDisable(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ResourceTypes []string `json:"resourceTypes"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	if err := h.Backend.Disable(req.ResourceTypes); err != nil {
		return h.mapError(c, err)
	}

	status := h.Backend.GetStatus()

	return c.JSON(http.StatusOK, map[string]any{
		"accounts": []map[string]any{
			{
				"accountId":      status.AccountId,
				"resourceStatus": buildResourceStatus(status),
				"status":         status.Status,
			},
		},
		"failedAccounts": []any{},
	})
}

// handleBatchGetAccountStatus handles POST /status/batch/get.
func (h *Handler) handleBatchGetAccountStatus(c *echo.Context) error {
	status := h.Backend.GetStatus()

	return c.JSON(http.StatusOK, map[string]any{
		"accounts": []map[string]any{
			{
				"accountId":      status.AccountId,
				"resourceStatus": buildResourceStatus(status),
				"state": map[string]any{
					"status": map[string]any{
						"status": status.Status,
					},
				},
			},
		},
		"failedAccounts": []any{},
	})
}

// handleCreateFilter handles POST /filters/create.
func (h *Handler) handleCreateFilter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Name           string            `json:"name"`
		Action         string            `json:"action"`
		Description    string            `json:"description"`
		Reason         string            `json:"reason"`
		FilterCriteria map[string]any    `json:"filterCriteria"`
		Tags           map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "name is required"))
	}

	if req.Action == "" {
		req.Action = "NONE"
	}

	f, err := h.Backend.CreateFilter(req.Name, req.Action, req.Description, req.Reason, req.FilterCriteria, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"arn": f.Arn,
	})
}

// handleUpdateFilter handles POST /filters/update.
func (h *Handler) handleUpdateFilter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterArn      string         `json:"filterArn"`
		Action         string         `json:"action"`
		Description    string         `json:"description"`
		Reason         string         `json:"reason"`
		FilterCriteria map[string]any `json:"filterCriteria"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.FilterArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "filterArn is required"))
	}

	f, err := h.Backend.UpdateFilter(req.FilterArn, req.Action, req.Description, req.Reason, req.FilterCriteria)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"arn": f.Arn,
	})
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

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if req.Arn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "arn is required"))
	}

	if err := h.Backend.DeleteFilter(req.Arn); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"arn": req.Arn,
	})
}

// handleListFilters handles POST /filters/list.
func (h *Handler) handleListFilters(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		Arns      []string `json:"arns"`
		Action    string   `json:"action"`
		NextToken string   `json:"nextToken"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	filters, err := h.Backend.ListFilters(req.Arns, req.Action)
	if err != nil {
		return h.mapError(c, err)
	}

	result := make([]map[string]any, 0, len(filters))
	for _, f := range filters {
		entry := map[string]any{
			"arn":       f.Arn,
			"name":      f.Name,
			"action":    f.Action,
			"ownerId":   f.OwnerId,
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

	return c.JSON(http.StatusOK, map[string]any{
		"filters": result,
	})
}

// handleListFindings handles POST /findings/list.
func (h *Handler) handleListFindings(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		MaxResults int32  `json:"maxResults"`
		NextToken  string `json:"nextToken"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
		}
	}

	findings, nextToken, err := h.Backend.ListFindings(req.MaxResults, req.NextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	resp := map[string]any{
		"findings": findings,
	}

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
				"status":         statusEnabled,
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
		if err := json.Unmarshal(body, &req); err != nil {
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

	if err := h.Backend.UpdateConfiguration(ec2ScanMode, ecrRescanDuration); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleListTagsForResource handles GET /tags/{resourceArn}.
func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := extractResourceARN(c.Request().URL.Path)
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "resourceArn is required"))
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.mapError(c, err)
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"tags": tags,
	})
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

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return h.mapError(c, err)
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

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// extractResourceARN extracts the resource ARN from the URL path.
func extractResourceARN(path string) string {
	const prefix = "/tags/"

	if idx := strings.Index(path, prefix); idx >= 0 {
		return path[idx+len(prefix):]
	}

	return ""
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
