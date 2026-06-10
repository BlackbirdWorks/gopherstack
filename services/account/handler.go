package account

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	accountService = "account"
	matchPriority  = service.PriorityPathVersioned

	pathAccount          = "/account"
	pathRegions          = "/regions"
	pathAlternateContact = "/account/alternateContact"
	pathContact          = "/account/contact"

	queryAlternateContactType    = "alternateContactType"
	queryRegionOptStatusContains = "regionOptStatusContains"
	queryMaxResults              = "maxResults"
	queryNextToken               = "nextToken"
)

// Handler implements service.Registerable for the AWS Account Management API.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a Handler backed by the given StorageBackend.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Account" }

// Reset delegates to the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations lists the operations the handler implements.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"DescribeAccount",
		"ListRegions",
		"GetAlternateContact",
		"PutAlternateContact",
		"DeleteAlternateContact",
		"GetContactInformation",
		"PutContactInformation",
	}
}

// RouteMatcher identifies account service requests by inspecting the SigV4 service name
// from the Authorization header and matching the expected URL paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		svc := httputils.ExtractServiceFromRequest(c.Request())
		if svc != accountService {
			return false
		}

		path := c.Request().URL.Path

		return path == pathAccount ||
			path == pathRegions ||
			path == pathAlternateContact ||
			path == pathContact
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation returns the operation name for logging/telemetry.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	switch {
	case path == pathAccount && method == http.MethodGet:
		return "DescribeAccount"
	case path == pathRegions && method == http.MethodGet:
		return "ListRegions"
	case path == pathAlternateContact && method == http.MethodGet:
		return "GetAlternateContact"
	case path == pathAlternateContact && method == http.MethodPut:
		return "PutAlternateContact"
	case path == pathAlternateContact && method == http.MethodDelete:
		return "DeleteAlternateContact"
	case path == pathContact && method == http.MethodGet:
		return "GetContactInformation"
	case path == pathContact && method == http.MethodPut:
		return "PutContactInformation"
	}

	return "Unknown"
}

// ExtractResource returns a resource identifier for logging.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().URL.Path, "/")
}

// Handler returns the echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.route(c)
	}
}

func (h *Handler) route(c *echo.Context) error {
	path := c.Request().URL.Path
	method := c.Request().Method
	q := c.Request().URL.Query()

	switch path {
	case pathAccount:
		return h.routeAccount(c, method)
	case pathRegions:
		return h.routeRegions(c, method, q)
	case pathAlternateContact:
		return h.routeAlternateContact(c, method, q)
	case pathContact:
		return h.routeContact(c, method)
	}

	return writeError(c, http.StatusNotFound, "InvalidAction", "unsupported operation")
}

func (h *Handler) routeAccount(c *echo.Context, method string) error {
	if method != http.MethodGet {
		return writeError(c, http.StatusMethodNotAllowed, "InvalidAction", "unsupported method")
	}

	return h.handleDescribeAccount(c)
}

func (h *Handler) routeRegions(c *echo.Context, method string, q interface{ Get(string) string }) error {
	if method != http.MethodGet {
		return writeError(c, http.StatusMethodNotAllowed, "InvalidAction", "unsupported method")
	}

	return h.handleListRegions(c, q)
}

func (h *Handler) routeAlternateContact(c *echo.Context, method string, q interface{ Get(string) string }) error {
	ct := q.Get(queryAlternateContactType)

	switch method {
	case http.MethodGet:
		return h.handleGetAlternateContact(c, ct)
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return writeError(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		}

		return h.handlePutAlternateContact(c, body)
	case http.MethodDelete:
		return h.handleDeleteAlternateContact(c, ct)
	}

	return writeError(c, http.StatusMethodNotAllowed, "InvalidAction", "unsupported method")
}

func (h *Handler) routeContact(c *echo.Context, method string) error {
	switch method {
	case http.MethodGet:
		return h.handleGetContactInformation(c)
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return writeError(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		}

		return h.handlePutContactInformation(c, body)
	}

	return writeError(c, http.StatusMethodNotAllowed, "InvalidAction", "unsupported method")
}

func (h *Handler) handleDescribeAccount(c *echo.Context) error {
	details, err := h.Backend.DescribeAccount()
	if err != nil {
		return writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Account": details})
}

func (h *Handler) handleListRegions(c *echo.Context, q interface{ Get(string) string }) error {
	statusRaw := c.Request().URL.Query()[queryRegionOptStatusContains]
	statusFilter := make([]RegionOptStatus, 0, len(statusRaw))

	for _, s := range statusRaw {
		statusFilter = append(statusFilter, RegionOptStatus(s))
	}

	maxResults := 0
	if raw := q.Get(queryMaxResults); raw != "" {
		if n, convErr := strconv.Atoi(raw); convErr == nil && n > 0 {
			maxResults = n
		}
	}
	nextToken := q.Get(queryNextToken)

	regions, next, err := h.Backend.ListRegions(statusFilter, maxResults, nextToken)
	if err != nil {
		return writeBackendError(c, err)
	}

	resp := map[string]any{"Regions": regions}
	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetAlternateContact(c *echo.Context, contactType string) error {
	contact, err := h.Backend.GetAlternateContact(ContactType(contactType))
	if err != nil {
		return writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"AlternateContact": contact})
}

func (h *Handler) handlePutAlternateContact(c *echo.Context, body []byte) error {
	var req struct {
		AlternateContactType ContactType `json:"AlternateContactType"`
		EmailAddress         string      `json:"EmailAddress"`
		Name                 string      `json:"Name"`
		PhoneNumber          string      `json:"PhoneNumber"`
		Title                string      `json:"Title"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequest", err.Error())
	}

	contact := &AlternateContact{
		AlternateContactType: req.AlternateContactType,
		EmailAddress:         req.EmailAddress,
		Name:                 req.Name,
		PhoneNumber:          req.PhoneNumber,
		Title:                req.Title,
	}

	if err := h.Backend.PutAlternateContact(contact); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteAlternateContact(c *echo.Context, contactType string) error {
	if err := h.Backend.DeleteAlternateContact(ContactType(contactType)); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetContactInformation(c *echo.Context) error {
	info, err := h.Backend.GetContactInformation()
	if err != nil {
		return writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"ContactInformation": info})
}

func (h *Handler) handlePutContactInformation(c *echo.Context, body []byte) error {
	var info ContactInformation

	if err := json.Unmarshal(body, &info); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequest", err.Error())
	}

	if err := h.Backend.PutContactInformation(&info); err != nil {
		return writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, map[string]any{
		"__type":  code,
		"message": message,
	})
}

func writeBackendError(c *echo.Context, err error) error {
	type awsErr interface {
		Code() string
		HTTPStatusCode() int
	}

	var ae awsErr
	if errors.As(err, &ae) {
		return writeError(c, ae.HTTPStatusCode(), ae.Code(), err.Error())
	}

	code := "InternalServerError"
	status := http.StatusInternalServerError

	switch {
	case strings.Contains(err.Error(), "ResourceNotFoundException"):
		code = "ResourceNotFoundException"
		status = http.StatusNotFound
	case strings.Contains(err.Error(), "ValidationException"):
		code = "ValidationException"
		status = http.StatusBadRequest
	}

	return writeError(c, status, code, err.Error())
}
