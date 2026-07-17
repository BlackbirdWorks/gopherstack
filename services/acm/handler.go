package acm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	acmMatchPriority = 81
	acmTargetPrefix  = "CertificateManager."
)

// Handler is the Echo HTTP handler for ACM operations.
type Handler struct {
	Backend       *InMemoryBackend
	tags          *store.Table[certTagsEntry]
	tagsMu        *lockmetrics.RWMutex
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}
}

// NewHandler creates a new ACM handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    store.New(certTagsKeyFn),
		tagsMu:  lockmetrics.New("acm.tags"),
	}
}

// StartWorker starts the background janitor for idempotency tokens.
func (h *Handler) StartWorker(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	h.janitorCancel = cancel
	h.janitorDone = done

	go func() {
		defer close(done)
		// Run janitor every hour.
		h.Backend.RunJanitor(runCtx, time.Hour)
	}()

	return nil
}

// Shutdown stops the background janitor and all in-flight certificate
// auto-validation timers so no goroutine outlives the service.
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

	h.Backend.Close()
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
)

// Name returns the service name.
func (h *Handler) Name() string { return "ACM" }

// GetSupportedOperations returns supported ACM operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"RequestCertificate",
		"DescribeCertificate",
		"ListCertificates",
		"DeleteCertificate",
		"ListTagsForCertificate",
		"AddTagsToCertificate",
		"RemoveTagsFromCertificate",
		"ImportCertificate",
		"RenewCertificate",
		"ExportCertificate",
		"GetCertificate",
		"GetAccountConfiguration",
		"PutAccountConfiguration",
		"ResendValidationEmail",
		"RevokeCertificate",
		"UpdateCertificateOptions",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "acm" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ACM instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches ACM JSON-protocol requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}
		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}
		target := r.Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, acmTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return acmMatchPriority }

// ExtractOperation extracts the ACM action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, acmTargetPrefix)
}

// ExtractResource returns the certificate ARN from the JSON body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var m map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &m); unmarshalErr != nil {
		return ""
	}
	var arn string
	if raw, ok := m["CertificateArn"]; ok {
		_ = json.Unmarshal(raw, &arn)
	}

	return arn
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)

		return service.HandleTarget(
			c, logger.Load(ctx),
			"ACM", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(_ context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(ctx, action, body)
			},
			h.handleError,
		)
	}
}

// dispatch routes the operation to the appropriate handler and marshals the response.
func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	resp, err := h.dispatchJSON(ctx, action, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(resp)
}

// handleError writes a standardized error response back to the client.
func (h *Handler) handleError(_ context.Context, c *echo.Context, action string, reqErr error) error {
	if errors.Is(reqErr, errUnknownACMAction) {
		return h.writeJSONError(c, http.StatusBadRequest, "InvalidAction",
			action+" is not a valid ACM action")
	}

	return h.handleOpError(c, action, reqErr)
}

// errUnknownACMAction is returned by dispatchJSON for unrecognised action names.
var errUnknownACMAction = errors.New("unknown ACM action")

// acmDispatchTable maps ACM action names to their JSON handler functions.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var acmDispatchTable = map[string]func(*Handler, context.Context, []byte) (any, error){
	"RequestCertificate":        (*Handler).jsonRequestCertificate,
	"DescribeCertificate":       (*Handler).jsonDescribeCertificate,
	"ListCertificates":          (*Handler).jsonListCertificates,
	"DeleteCertificate":         (*Handler).jsonDeleteCertificate,
	"ListTagsForCertificate":    (*Handler).jsonListTagsForCertificate,
	"AddTagsToCertificate":      (*Handler).jsonAddTagsToCertificate,
	"RemoveTagsFromCertificate": (*Handler).jsonRemoveTagsFromCertificate,
	"ImportCertificate":         (*Handler).jsonImportCertificate,
	"RenewCertificate":          (*Handler).jsonRenewCertificate,
	"ExportCertificate":         (*Handler).jsonExportCertificate,
	"GetCertificate":            (*Handler).jsonGetCertificate,
	"GetAccountConfiguration":   (*Handler).jsonGetAccountConfiguration,
	"PutAccountConfiguration":   (*Handler).jsonPutAccountConfiguration,
	"ResendValidationEmail":     (*Handler).jsonResendValidationEmail,
	"RevokeCertificate":         (*Handler).jsonRevokeCertificate,
	"UpdateCertificateOptions":  (*Handler).jsonUpdateCertificateOptions,
}

// dispatchJSON routes a JSON-protocol ACM action to the appropriate handler.
func (h *Handler) dispatchJSON(ctx context.Context, action string, body []byte) (any, error) {
	if fn, ok := acmDispatchTable[action]; ok {
		return fn(h, ctx, body)
	}

	return nil, errUnknownACMAction
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	var code string
	switch {
	case errors.Is(opErr, ErrCertNotFound):
		code = "ResourceNotFoundException"
	case errors.Is(opErr, ErrInvalidParameter):
		code = "ValidationException"
	case errors.Is(opErr, ErrNotEligible):
		code = "RequestInProgressException"
	case errors.Is(opErr, ErrRequestInProgress):
		code = "RequestInProgressException"
	case errors.Is(opErr, ErrInvalidState):
		code = "InvalidStateException"
	case errors.Is(opErr, ErrResourceInUse):
		code = "ResourceInUseException"
	case errors.Is(opErr, ErrAlreadyRevoked):
		code = "InvalidStateException"
	case errors.Is(opErr, ErrConflict):
		code = "ConflictException"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("ACM internal error", "error", opErr, "action", action)
	}

	return h.writeJSONError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeJSONError(c *echo.Context, statusCode int, code, message string) error {
	return c.JSON(statusCode, map[string]string{
		"__type":  code,
		"message": message,
	})
}

// Reset clears all handler tag state and delegates to the backend Reset.
func (h *Handler) Reset() {
	h.tagsMu.Lock("Reset")
	for _, entry := range h.tags.All() {
		entry.Tags.Close()
	}
	h.tags.Reset()
	h.tagsMu.Unlock()

	h.Backend.Reset()
}
