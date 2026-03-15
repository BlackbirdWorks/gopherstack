package sts

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
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
	contentTypeForm  = "application/x-www-form-urlencoded"
	stsVersion       = "Version=2011-06-15"
	unknownOperation = "Unknown"
	invalidAction    = "InvalidAction"
	kvPairLen        = 2
)

// Handler is the Echo HTTP handler for STS operations.
type Handler struct {
	Backend StorageBackend
	janitor *Janitor
}

// NewHandler creates a new STS handler with the given backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
	}
}

// WithJanitor attaches a background janitor to the handler.
// The janitor periodically evicts expired sessions. interval=0 uses the default.
func (h *Handler) WithJanitor(interval time.Duration) *Handler {
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		h.janitor = NewJanitor(memBackend, interval)
	}

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "STS"
}

// GetSupportedOperations returns the list of supported STS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AssumeRole",
		"DecodeAuthorizationMessage",
		"GetAccessKeyInfo",
		"GetCallerIdentity",
		"GetSessionToken",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "sts" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this STS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a matcher that identifies STS requests by Content-Type and Version.
// Dashboard paths are excluded so that browser form submissions (Playwright tests)
// are not intercepted by the STS handler.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		if strings.HasPrefix(path, "/dashboard/") || path == "/dashboard" {
			return false
		}

		ct := c.Request().Header.Get("Content-Type")
		if !strings.Contains(ct, contentTypeForm) {
			return false
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return false
		}

		return strings.Contains(string(body), stsVersion)
	}
}

// MatchPriority returns the routing priority for the STS handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityFormEncoded
}

// ExtractOperation reads the Action parameter from the request body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
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
	body, err := httputils.ReadBody(c.Request())
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

		response, err := h.dispatch(ctx, c.Request())
		if err != nil {
			return h.handleError(ctx, c, err)
		}

		log.DebugContext(ctx, "STS request completed")

		return writeXMLResponse(c, http.StatusOK, response)
	}
}

// dispatch parses the STS request and calls the appropriate backend method.
func (h *Handler) dispatch(ctx context.Context, r *http.Request) (any, error) {
	log := logger.Load(ctx)

	if err := r.ParseForm(); err != nil {
		return nil, fmt.Errorf("parse form: %w", err)
	}

	action := r.FormValue("Action")
	if action == "" {
		return nil, ErrMissingAction
	}

	log.DebugContext(ctx, "STS request", "action", action)

	switch action {
	case "AssumeRole":
		return h.dispatchAssumeRole(r)
	case "GetCallerIdentity":
		return h.Backend.GetCallerIdentity(extractAccessKeyFromAuth(r))
	case "GetSessionToken":
		return h.dispatchGetSessionToken(r)
	case "GetAccessKeyInfo":
		return h.dispatchGetAccessKeyInfo(r)
	case "DecodeAuthorizationMessage":
		return h.dispatchDecodeAuthorizationMessage(r)
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
		SourceIdentity:  r.FormValue("SourceIdentity"),
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	// Parse session tags: Tags.member.N.Key / Tags.member.N.Value
	input.Tags = parseSessionTags(r)

	// Parse transitive tag keys: TransitiveTagKeys.member.N
	input.TransitiveTagKeys = parseTransitiveTagKeys(r)

	return h.Backend.AssumeRole(input)
}

// dispatchGetSessionToken handles the GetSessionToken action.
func (h *Handler) dispatchGetSessionToken(r *http.Request) (*GetSessionTokenResponse, error) {
	input := &GetSessionTokenInput{
		SerialNumber: r.FormValue("SerialNumber"),
		TokenCode:    r.FormValue("TokenCode"),
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	return h.Backend.GetSessionToken(input)
}

// dispatchGetAccessKeyInfo handles the GetAccessKeyInfo action.
func (h *Handler) dispatchGetAccessKeyInfo(r *http.Request) (*GetAccessKeyInfoResponse, error) {
	_ = r.FormValue("AccessKeyId") // consumed but not validated in mock

	callerIdentity, err := h.Backend.GetCallerIdentity("")
	if err != nil {
		return nil, err
	}

	return &GetAccessKeyInfoResponse{
		Xmlns: STSNamespace,
		GetAccessKeyInfoResult: GetAccessKeyInfoResult{
			Account: callerIdentity.GetCallerIdentityResult.Account,
		},
		ResponseMetadata: callerIdentity.ResponseMetadata,
	}, nil
}

// dispatchDecodeAuthorizationMessage handles the DecodeAuthorizationMessage action.
func (h *Handler) dispatchDecodeAuthorizationMessage(r *http.Request) (*DecodeAuthorizationMessageResponse, error) {
	encoded := r.FormValue("EncodedMessage")

	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// Try URL-safe base64 as fallback
		decoded, err = base64.URLEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid encoded message: %w", err)
		}
	}

	callerIdentity, ciErr := h.Backend.GetCallerIdentity("")
	if ciErr != nil {
		return nil, ciErr
	}

	return &DecodeAuthorizationMessageResponse{
		Xmlns: STSNamespace,
		DecodeAuthorizationMessageResult: DecodeAuthorizationMessageResult{
			DecodedMessage: string(decoded),
		},
		ResponseMetadata: callerIdentity.ResponseMetadata,
	}, nil
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
	case errors.Is(reqErr, ErrAccessDenied):
		code = "AccessDenied"
		httpStatus = http.StatusForbidden
	}

	if httpStatus == http.StatusInternalServerError {
		log.ErrorContext(ctx, "STS internal error", "error", reqErr)
	} else {
		log.WarnContext(ctx, "STS request error", "error", reqErr)
	}

	errType := "Sender"
	if httpStatus == http.StatusInternalServerError {
		errType = "Receiver"
	}

	errResp := &ErrorResponse{
		Xmlns: STSNamespace,
		Error: ErrorDetail{
			Type:    errType,
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

	return c.Blob(code, "text/xml; charset=utf-8", buf.Bytes())
}

// parseFormValues parses URL-encoded form bytes into a simple key→value map.
func parseFormValues(body []byte) map[string]string {
	result := make(map[string]string)

	for pair := range strings.SplitSeq(string(body), "&") {
		if pair == "" {
			continue
		}

		kv := strings.SplitN(pair, "=", kvPairLen)
		if len(kv) != kvPairLen {
			continue
		}

		key, _ := url.QueryUnescape(kv[0])
		val, _ := url.QueryUnescape(kv[1])
		result[key] = val
	}

	return result
}

// parseSessionTags reads Tags.member.N.Key / Tags.member.N.Value form fields and
// returns them as a []Tag slice. It supports up to MaxTagCount entries.
func parseSessionTags(r *http.Request) []Tag {
	var tags []Tag

	for i := 1; i <= MaxTagCount; i++ {
		key := r.FormValue(fmt.Sprintf("Tags.member.%d.Key", i))
		if key == "" {
			break
		}

		value := r.FormValue(fmt.Sprintf("Tags.member.%d.Value", i))
		tags = append(tags, Tag{Key: key, Value: value})
	}

	return tags
}

// parseTransitiveTagKeys reads TransitiveTagKeys.member.N form fields.
func parseTransitiveTagKeys(r *http.Request) []string {
	var keys []string

	for i := 1; i <= MaxTagCount; i++ {
		key := r.FormValue(fmt.Sprintf("TransitiveTagKeys.member.%d", i))
		if key == "" {
			break
		}

		keys = append(keys, key)
	}

	return keys
}

// extractAccessKeyFromAuth parses the SigV4 Authorization header and returns
// the access key ID (the portion before the first '/' in the Credential field).
// Returns an empty string if the header is absent or unparseable.
func extractAccessKeyFromAuth(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}

	_, after, ok := strings.Cut(auth, "Credential=")
	if !ok {
		return ""
	}

	// Strip any trailing comma/space that follows the Credential value.
	if commaIdx := strings.IndexAny(after, ", "); commaIdx != -1 {
		after = after[:commaIdx]
	}

	before, _, ok := strings.Cut(after, "/")
	if !ok {
		return after
	}

	return before
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}
