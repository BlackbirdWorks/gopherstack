package support

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const supportTargetPrefix = "AmazonSupport."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

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
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Support", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var result any
	var err error

	switch action {
	case "CreateCase":
		result, err = h.handleCreateCase(body)
	case "DescribeCases":
		result, err = h.handleDescribeCases(body)
	case "ResolveCase":
		result, err = h.handleResolveCase(body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	case errors.Is(err, ErrAlreadyResolved), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type handleCreateCaseInput struct {
	Subject           string `json:"subject"`
	ServiceCode       string `json:"serviceCode"`
	CategoryCode      string `json:"categoryCode"`
	SeverityCode      string `json:"severityCode"`
	CommunicationBody string `json:"communicationBody"`
}

func (h *Handler) handleCreateCase(body []byte) (any, error) {
	var req handleCreateCaseInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if req.Subject == "" {
		return nil, fmt.Errorf("%w: subject is required", errInvalidRequest)
	}

	c2, err := h.Backend.CreateCase(
		req.Subject,
		req.ServiceCode,
		req.CategoryCode,
		req.SeverityCode,
		req.CommunicationBody,
	)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"caseId": c2.CaseID,
	}, nil
}

type handleDescribeCasesInput struct {
	CaseIDList []string `json:"caseIdList"`
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleDescribeCases(body []byte) (any, error) {
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

	return map[string]any{
		"cases": views,
	}, nil
}

type handleResolveCaseInput struct {
	CaseID string `json:"caseId"`
}

func (h *Handler) handleResolveCase(body []byte) (any, error) {
	var req handleResolveCaseInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	cs, err := h.Backend.ResolveCase(req.CaseID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"initialCaseStatus": "opened",
		"finalCaseStatus":   cs.Status,
	}, nil
}
