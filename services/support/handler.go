package support

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const supportTargetPrefix = "AWSSupport_20130415."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS Support operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Support handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "support" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Support instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

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
	body, err := httputils.ReadBody(c.Request())
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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCase":    service.WrapOp(h.handleCreateCase),
		"DescribeCases": service.WrapOp(h.handleDescribeCases),
		"ResolveCase":   service.WrapOp(h.handleResolveCase),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
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
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	case errors.Is(err, ErrAlreadyResolved), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type caseView struct {
	CaseID       string `json:"caseId"`
	Subject      string `json:"subject"`
	Status       string `json:"status"`
	ServiceCode  string `json:"serviceCode"`
	CategoryCode string `json:"categoryCode"`
	SeverityCode string `json:"severityCode"`
}

type createCaseOutput struct {
	CaseID string `json:"caseId"`
}

type describeCasesOutput struct {
	Cases []caseView `json:"cases"`
}

type resolveCaseOutput struct {
	InitialCaseStatus string `json:"initialCaseStatus"`
	FinalCaseStatus   string `json:"finalCaseStatus"`
}

type handleCreateCaseInput struct {
	Subject           string `json:"subject"`
	ServiceCode       string `json:"serviceCode"`
	CategoryCode      string `json:"categoryCode"`
	SeverityCode      string `json:"severityCode"`
	CommunicationBody string `json:"communicationBody"`
}

func (h *Handler) handleCreateCase(_ context.Context, in *handleCreateCaseInput) (*createCaseOutput, error) {
	if in.Subject == "" {
		return nil, fmt.Errorf("%w: subject is required", errInvalidRequest)
	}

	c2, err := h.Backend.CreateCase(
		in.Subject,
		in.ServiceCode,
		in.CategoryCode,
		in.SeverityCode,
		in.CommunicationBody,
	)
	if err != nil {
		return nil, err
	}

	return &createCaseOutput{CaseID: c2.CaseID}, nil
}

type handleDescribeCasesInput struct {
	CaseIDList []string `json:"caseIdList"`
}

func (h *Handler) handleDescribeCases(_ context.Context, in *handleDescribeCasesInput) (*describeCasesOutput, error) {
	cases := h.Backend.DescribeCases(in.CaseIDList)

	views := make([]caseView, 0, len(cases))
	for _, cs := range cases {
		views = append(views, caseView{
			CaseID:       cs.CaseID,
			Subject:      cs.Subject,
			Status:       cs.Status,
			ServiceCode:  cs.ServiceCode,
			CategoryCode: cs.CategoryCode,
			SeverityCode: cs.SeverityCode,
		})
	}

	return &describeCasesOutput{Cases: views}, nil
}

type handleResolveCaseInput struct {
	CaseID string `json:"caseId"`
}

func (h *Handler) handleResolveCase(_ context.Context, in *handleResolveCaseInput) (*resolveCaseOutput, error) {
	cs, err := h.Backend.ResolveCase(in.CaseID)
	if err != nil {
		return nil, err
	}

	return &resolveCaseOutput{
		InitialCaseStatus: "opened",
		FinalCaseStatus:   cs.Status,
	}, nil
}
