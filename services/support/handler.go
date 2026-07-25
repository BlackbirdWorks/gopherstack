package support

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const supportTargetPrefix = "AWSSupport_20130415."

var errUnknownAction = errors.New("unknown action")

const defaultJanitorInterval = 10 * time.Minute

// Handler is the Echo HTTP handler for AWS Support operations.
type Handler struct {
	Backend       StorageBackend
	ops           map[string]service.JSONOpFunc
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}
}

// NewHandler creates a new Support handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// StartWorker starts the background janitor for attachment sets.
func (h *Handler) StartWorker(ctx context.Context) error {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		h.janitorCancel = cancel
		h.janitorDone = done

		go func() {
			defer close(done)
			// Run janitor every 10 minutes.
			mem.RunJanitor(runCtx, defaultJanitorInterval)
		}()
	}

	return nil
}

// Shutdown stops the background janitor.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.janitorCancel != nil {
		h.janitorCancel()
	}

	if h.janitorDone != nil {
		select {
		case <-h.janitorDone:
		case <-ctx.Done():
		}
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
)

// Reset clears the handler's backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "Support" }

// GetSupportedOperations returns the list of supported Support operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddAttachmentsToSet",
		"AddCommunicationToCase",
		"CreateCase",
		"DescribeAttachment",
		"DescribeCases",
		"DescribeCommunications",
		"DescribeCreateCaseOptions",
		"DescribeServices",
		"DescribeSeverityLevels",
		"DescribeSupportedLanguages",
		"DescribeTrustedAdvisorCheckRefreshStatuses",
		"DescribeTrustedAdvisorCheckResult",
		"DescribeTrustedAdvisorCheckSummaries",
		"DescribeTrustedAdvisorChecks",
		"RefreshTrustedAdvisorCheck",
		"ResolveCase",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "support" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Support instance handles.
func (h *Handler) ChaosRegions() []string { return []string{"us-east-1"} }

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
	CaseID       string `json:"caseId"`
	CheckID      string `json:"checkId"`
	AttachmentID string `json:"attachmentId"`
	Subject      string `json:"subject"`
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
	if req.CheckID != "" {
		return req.CheckID
	}
	if req.AttachmentID != "" {
		return req.AttachmentID
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

// buildOps constructs the dispatch map once at handler creation time.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCase":             service.WrapOp(h.handleCreateCase),
		"DescribeCases":          service.WrapOp(h.handleDescribeCases),
		"ResolveCase":            service.WrapOp(h.handleResolveCase),
		"AddCommunicationToCase": service.WrapOp(h.handleAddCommunicationToCase),
		"DescribeCommunications": service.WrapOp(h.handleDescribeCommunications),
		"DescribeTrustedAdvisorChecks": service.WrapOp(
			h.handleDescribeTrustedAdvisorChecks,
		),
		"AddAttachmentsToSet":       service.WrapOp(h.handleAddAttachmentsToSet),
		"DescribeAttachment":        service.WrapOp(h.handleDescribeAttachment),
		"DescribeCreateCaseOptions": service.WrapOp(h.handleDescribeCreateCaseOptions),
		"DescribeServices":          service.WrapOp(h.handleDescribeServices),
		"DescribeSeverityLevels":    service.WrapOp(h.handleDescribeSeverityLevels),
		"DescribeSupportedLanguages": service.WrapOp(
			h.handleDescribeSupportedLanguages,
		),
		"DescribeTrustedAdvisorCheckRefreshStatuses": service.WrapOp(
			h.handleDescribeTrustedAdvisorCheckRefreshStatuses,
		),
		"DescribeTrustedAdvisorCheckResult": service.WrapOp(
			h.handleDescribeTrustedAdvisorCheckResult,
		),
		"DescribeTrustedAdvisorCheckSummaries": service.WrapOp(
			h.handleDescribeTrustedAdvisorCheckSummaries,
		),
		"RefreshTrustedAdvisorCheck": service.WrapOp(h.handleRefreshTrustedAdvisorCheck),
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

// handleError writes an AWS JSON-1.1 error envelope: the "__type" field is
// what a real SDK client's error deserializer keys off (via
// resolveProtocolErrorType in aws-sdk-go-v2/service/support/deserializers.go)
// -- a bare "message" field with no "__type" is invisible to the SDK, which
// falls back to a generic "UnknownError" and can never produce the typed
// exception (e.g. *types.CaseIdNotFound) a caller's errors.As expects.
//
// Support is a JSON-RPC-style (awsjson1.1) protocol: per the service model
// (service-2.json) none of its exception shapes carry an httpStatusCode
// override, so every client-fault exception -- including the
// "*NotFound"-named ones -- uses the protocol default of HTTP 400, not 404.
// Only the fault:true shape (InternalServerError) defaults to 500.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, err error) error {
	errType, statusCode := resolveErrorType(err)

	if statusCode == http.StatusInternalServerError {
		logger.Load(ctx).ErrorContext(ctx, "Support internal error", "error", err, "action", action)
	}

	payload, marshalErr := json.Marshal(service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	return c.JSONBlob(statusCode, payload)
}

// resolveErrorType maps a backend error to the wire "__type" exception name
// and HTTP status a real AWS Support (awsjson1.1) response would carry.
func resolveErrorType(err error) (string, int) {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		return "CaseIdNotFound", http.StatusBadRequest
	case errors.Is(err, ErrAttachmentNotFound):
		return "AttachmentIdNotFound", http.StatusBadRequest
	case errors.Is(err, ErrAttachmentSetNotFound):
		return "AttachmentSetIdNotFound", http.StatusBadRequest
	case errors.Is(err, ErrAttachmentSetExpired):
		return "AttachmentSetExpired", http.StatusBadRequest
	case errors.Is(err, ErrAttachmentSetSizeLimitExceeded):
		return "AttachmentSetSizeLimitExceeded", http.StatusBadRequest
	case errors.Is(err, ErrAttachmentLimitExceeded):
		return "AttachmentLimitExceeded", http.StatusBadRequest
	case errors.Is(err, ErrCaseCreationLimitExceeded):
		return "CaseCreationLimitExceeded", http.StatusBadRequest
	case errors.Is(err, ErrDescribeAttachmentLimitExceeded):
		return "DescribeAttachmentLimitExceeded", http.StatusBadRequest
	case errors.Is(err, ErrValidation), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return "ValidationException", http.StatusBadRequest
	default:
		return "InternalServerError", http.StatusInternalServerError
	}
}

func parseFilterTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid timestamp", ErrValidation)
	}

	return parsed, nil
}

func nonZeroTimePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}

	return &value
}
