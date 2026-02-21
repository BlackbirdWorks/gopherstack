package sts

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var (
	// ErrMissingAction is returned when the Action field is absent from the request.
	ErrMissingAction = errors.New("action is required")

	// ErrInvalidAction is returned when the Action is not a supported STS operation.
	ErrInvalidAction = errors.New("invalid action")
)

const (
	stsMatchPriority = 90
	contentTypeForm  = "application/x-www-form-urlencoded"
	unknownOperation = "Unknown"
	invalidAction    = "InvalidAction"
)

// Handler is the Echo HTTP handler for STS operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
}

// NewHandler creates a new STS handler with the given backend.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  log,
	}
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "STS"
}

// GetSupportedOperations returns the list of supported STS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{"AssumeRole", "GetCallerIdentity"}
}

// RouteMatcher returns a matcher that identifies STS requests by Content-Type.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		ct := c.Request().Header.Get("Content-Type")

		return strings.Contains(ct, contentTypeForm)
	}
}

// MatchPriority returns the routing priority for the STS handler.
func (h *Handler) MatchPriority() int {
	return stsMatchPriority
}

// ExtractOperation reads the Action parameter from the request body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return unknownOperation
	}

	// Parse as form values manually to avoid consuming the body permanently.
	values := parseFormValues(body)
	action := values["Action"]

	if action == "" {
		return unknownOperation
	}

	return action
}

// ExtractResource returns the RoleArn for AssumeRole calls, empty otherwise.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	values := parseFormValues(body)

	return values["RoleArn"]
}

// Handler returns the Echo handler function for STS operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if c.Request().Method == http.MethodGet {
			return c.JSON(http.StatusOK, h.GetSupportedOperations())
		}

		if c.Request().Method != http.MethodPost {
			return c.String(http.StatusMethodNotAllowed, "Method not allowed")
		}

		response, err := h.dispatch(c.Request())
		if err != nil {
			return h.handleError(ctx, c, err)
		}

		log.DebugContext(ctx, "STS request completed")

		return writeXMLResponse(c, http.StatusOK, response)
	}
}

// dispatch parses the STS request and calls the appropriate backend method.
func (h *Handler) dispatch(r *http.Request) (any, error) {
	if err := r.ParseForm(); err != nil {
		return nil, fmt.Errorf("parse form: %w", err)
	}

	action := r.FormValue("Action")
	if action == "" {
		return nil, ErrMissingAction
	}

	switch action {
	case "AssumeRole":
		return h.dispatchAssumeRole(r)
	case "GetCallerIdentity":
		return h.Backend.GetCallerIdentity()
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidAction, action)
	}
}

// dispatchAssumeRole handles the AssumeRole action.
func (h *Handler) dispatchAssumeRole(r *http.Request) (*AssumeRoleResponse, error) {
	input := &AssumeRoleInput{
		RoleArn:         r.FormValue("RoleArn"),
		RoleSessionName: r.FormValue("RoleSessionName"),
		ExternalID:      r.FormValue("ExternalId"),
		Policy:          r.FormValue("Policy"),
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	return h.Backend.AssumeRole(input)
}

// handleError writes a standardised STS XML error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, reqErr error) error {
	log := logger.Load(ctx)

	code := "InternalFailure"
	httpStatus := http.StatusInternalServerError

	switch {
	case errors.Is(reqErr, ErrMissingRoleArn), errors.Is(reqErr, ErrMissingSessionName):
		code = "MissingParameter"
		httpStatus = http.StatusBadRequest
	case errors.Is(reqErr, ErrInvalidDuration):
		code = "ValidationError"
		httpStatus = http.StatusBadRequest
	case errors.Is(reqErr, ErrMissingAction), errors.Is(reqErr, ErrInvalidAction):
		code = invalidAction
		httpStatus = http.StatusBadRequest
	}

	if httpStatus == http.StatusInternalServerError {
		log.ErrorContext(ctx, "STS internal error", "error", reqErr)
	} else {
		log.WarnContext(ctx, "STS request error", "error", reqErr)
	}

	errResp := &ErrorResponse{
		Xmlns: STSNamespace,
		Error: ErrorDetail{
			Type:    "Sender",
			Code:    code,
			Message: reqErr.Error(),
		},
		RequestID: "00000000-0000-0000-0000-000000000000",
	}

	return writeXMLResponse(c, httpStatus, errResp)
}

// writeXMLResponse serialises payload to XML and writes it to the Echo response.
func writeXMLResponse(c *echo.Context, code int, payload any) error {
	var buf bytes.Buffer

	buf.WriteString(xml.Header)

	if err := xml.NewEncoder(&buf).Encode(payload); err != nil {
		return err
	}

	c.Response().Header().Set("Content-Type", "text/xml; charset=utf-8")

	return c.JSONBlob(code, buf.Bytes())
}

// parseFormValues parses URL-encoded form bytes into a simple key→value map.
func parseFormValues(body []byte) map[string]string {
	result := make(map[string]string)

	for pair := range strings.SplitSeq(string(body), "&") {
		if pair == "" {
			continue
		}

		kv := strings.SplitN(pair, "=", 2) //nolint:mnd // key=value split
		if len(kv) != 2 {                  //nolint:mnd // only key=value pairs are valid
			continue
		}

		key := decodeFormValue(kv[0])
		val := decodeFormValue(kv[1])
		result[key] = val
	}

	return result
}

// decodeFormValue performs basic URL percent-decoding for form field extraction.
func decodeFormValue(s string) string {
	// Replace + with space first, then do percent-decoding.
	s = strings.ReplaceAll(s, "+", " ")
	result := make([]byte, 0, len(s))

	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			var b byte
			_, err := fmt.Sscanf(s[i+1:i+3], "%02X", &b)
			if err == nil {
				result = append(result, b)
				i += 2

				continue
			}
		}

		result = append(result, s[i])
	}

	return string(result)
}
