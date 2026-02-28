package support

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const supportTargetPrefix = "AmazonSupport."

// Handler is the Echo HTTP handler for AWS Support operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new Support handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Support" }

// GetSupportedOperations returns the list of supported Support operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCase",
		"DescribeCases",
		"ResolveCase",
	}
}

// RouteMatcher returns a function that matches Support requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), supportTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Support action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, supportTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type extractSupportResourceInput struct {
	CaseID  string `json:"caseId"`
	Subject string `json:"subject"`
}

// ExtractResource extracts the case ID from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractSupportResourceInput
	_ = json.Unmarshal(body, &req)

	if req.CaseID != "" {
		return req.CaseID
	}

	return req.Subject
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read body"})
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), supportTargetPrefix)
		switch action {
		case "CreateCase":
			return h.handleCreateCase(c, body)
		case "DescribeCases":
			return h.handleDescribeCases(c, body)
		case "ResolveCase":
			return h.handleResolveCase(c, body)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "unknown action: " + action})
		}
	}
}

type handleCreateCaseInput struct {
	Subject           string `json:"subject"`
	ServiceCode       string `json:"serviceCode"`
	CategoryCode      string `json:"categoryCode"`
	SeverityCode      string `json:"severityCode"`
	CommunicationBody string `json:"communicationBody"`
}

func (h *Handler) handleCreateCase(c *echo.Context, body []byte) error {
	var req handleCreateCaseInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if req.Subject == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "subject is required"})
	}

	c2, err := h.Backend.CreateCase(
		req.Subject,
		req.ServiceCode,
		req.CategoryCode,
		req.SeverityCode,
		req.CommunicationBody,
	)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"caseId": c2.CaseID,
	})
}

type handleDescribeCasesInput struct {
	CaseIDList []string `json:"caseIdList"`
}

func (h *Handler) handleDescribeCases(c *echo.Context, body []byte) error {
	var req handleDescribeCasesInput
	_ = json.Unmarshal(body, &req)

	cases := h.Backend.DescribeCases(req.CaseIDList)

	views := make([]map[string]any, 0, len(cases))
	for _, cs := range cases {
		v := map[string]any{
			"caseId":       cs.CaseID,
			"subject":      cs.Subject,
			"status":       cs.Status,
			"serviceCode":  cs.ServiceCode,
			"categoryCode": cs.CategoryCode,
			"severityCode": cs.SeverityCode,
		}
		views = append(views, v)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"cases": views,
	})
}

type handleResolveCaseInput struct {
	CaseID string `json:"caseId"`
}

func (h *Handler) handleResolveCase(c *echo.Context, body []byte) error {
	var req handleResolveCaseInput
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	cs, err := h.Backend.ResolveCase(req.CaseID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		if errors.Is(err, ErrAlreadyResolved) {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"initialCaseStatus": "opened",
		"finalCaseStatus":   cs.Status,
	})
}
