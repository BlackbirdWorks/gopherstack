package httputils

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// bodyReadCloser wraps a [bytes.Reader] to provide a seekable [io.ReadCloser]
// that also exposes the underlying bytes for direct access. This allows
// httputils.ReadBody to cache the body transparently in the Request.Body field.
type bodyReadCloser struct {
	*bytes.Reader

	body []byte
}

func (b *bodyReadCloser) Close() error { return nil }

// bodyReadErrCloser caches a body-read failure (e.g. an oversized body) so
// that a second ReadBody call on the same request returns the identical
// error instead of re-reading whatever is left of the now partially-drained
// underlying r.Body — which would silently succeed with a truncated body
// and no error, masking the original failure.
type bodyReadErrCloser struct {
	err error
}

func (b *bodyReadErrCloser) Read(_ []byte) (int, error) { return 0, b.err }
func (b *bodyReadErrCloser) Close() error               { return nil }

// MaxRequestBodyBytes caps every body read through ReadBody. AWS API request
// payloads top out at 6 MiB (Lambda synchronous invoke) or 5 GiB streamed
// (S3 PutObject, which uses its own streaming path and does not call ReadBody).
// 16 MiB leaves headroom for unusually large API requests while preventing
// unbounded memory growth from attacker-controlled bodies.
const MaxRequestBodyBytes int64 = 16 * 1024 * 1024

// ReadBody reads the request body and returns it as a byte slice.
// It handles cases where r.Body might be nil (e.g. in some test environments).
// It re-seeds the request body so it can be read multiple times and ensures
// the original request body is closed.
// It uses a custom ReadCloser to avoid redundant reads and allocations if
// called multiple times. The body is capped at MaxRequestBodyBytes; reads that
// exceed the cap return an `*http.MaxBytesError` (use [errors.As] to detect
// and translate to a 413 response). Because this helper has no access to the
// originating ResponseWriter, it cannot auto-write the 413 itself — callers
// must handle the size-cap error and respond appropriately.
func ReadBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}

	// Check if we've already cached the body in a custom readCloser
	if brc, ok := r.Body.(*bodyReadCloser); ok {
		// Rewind so that subsequent io.ReadAll(r.Body) calls also work.
		_, _ = brc.Reader.Seek(0, io.SeekStart)

		return brc.body, nil
	}

	// A prior call already hit a read failure on this request; return the
	// same error rather than reading whatever remains of the drained body.
	if erc, ok := r.Body.(*bodyReadErrCloser); ok {
		return nil, erc.err
	}

	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, MaxRequestBodyBytes))
	_ = r.Body.Close() // Ensure original body is closed
	if err != nil {
		r.Body = &bodyReadErrCloser{err: err}

		return nil, err
	}

	// Re-seed the body using our custom ReadCloser so subsequent calls to
	// ReadBody or io.ReadAll(r.Body) are efficient.
	r.Body = &bodyReadCloser{
		Reader: bytes.NewReader(body),
		body:   body,
	}

	return body, nil
}

// DrainBody reads and discards the request body.
// This is important for HTTP keep-alive, as the server needs to know
// the request body has been fully consumed before reusing the connection.
func DrainBody(r *http.Request) {
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
	}
}

// WriteJSON marshals the payload to JSON, sets standard headers, and writes the response.
// Sets Content-Type to "application/json" and Content-Length.
func WriteJSON(ctx context.Context, w http.ResponseWriter, code int, payload any) {
	log := logger.Load(ctx)

	body, err := json.Marshal(payload)
	if err != nil {
		log.ErrorContext(ctx, "failed to marshal JSON response", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)

		return
	}

	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)
	if _, wErr := w.Write(body); wErr != nil {
		log.ErrorContext(ctx, "failed to write JSON response", "error", wErr)
	}
}

// WriteXML writes an XML response with the given status code.
// The full body is buffered before writing it to the response.
func WriteXML(ctx context.Context, w http.ResponseWriter, code int, payload any) {
	log := logger.Load(ctx)

	var buf bytes.Buffer
	buf.WriteString(xml.Header)

	encoder := xml.NewEncoder(&buf)
	if err := encoder.Encode(payload); err != nil {
		log.ErrorContext(ctx, "failed to marshal XML response", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	if _, err := buf.WriteTo(w); err != nil {
		log.ErrorContext(ctx, "failed to write XML response", "error", err)
	}
}

// WriteDynamoDBResponse writes a DynamoDB-style JSON response with CRC32 checksum.
// Sets Content-Type to "application/x-amz-json-1.0" and X-Amz-Crc32.
func WriteDynamoDBResponse(ctx context.Context, w http.ResponseWriter, code int, payload any) {
	log := logger.Load(ctx)

	body, err := json.Marshal(payload)
	if err != nil {
		log.ErrorContext(ctx, "failed to marshal DynamoDB response", "error", err)
		http.Error(w,
			`{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"internal server error"}`,
			http.StatusInternalServerError)

		return
	}

	checksum := crc32.ChecksumIEEE(body)
	w.Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(checksum), 10))
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)
	if _, wErr := w.Write(body); wErr != nil {
		log.ErrorContext(ctx, "failed to write DynamoDB response", "error", wErr)
	}
}

// WriteError writes an error response with structured logging.
// Uses the logger from ctx to record the error with context.
// Drains the request body to ensure connection reuse.
func WriteError(ctx context.Context, w http.ResponseWriter, r *http.Request, err error, code int) {
	DrainBody(r)
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "request failed", "error", err, "code", code, "path", r.URL.Path)
	}
	http.Error(w, err.Error(), code)
}

// WriteS3ErrorResponse writes an S3-compatible XML error response.
// Drains the request body and writes the error as XML.
func WriteS3ErrorResponse(ctx context.Context, w http.ResponseWriter, r *http.Request, s3Err any, code int) {
	DrainBody(r)
	WriteXML(ctx, w, code, s3Err)
}

// EchoError is a helper for Echo handlers to write errors with proper logging.
func EchoError(ctx context.Context, c *echo.Context, code int, message string, err error) error {
	if err != nil {
		logger.Load(ctx).DebugContext(ctx, message, "error", err)
	}

	return c.String(code, message)
}

// ResponseWriter wraps [http.ResponseWriter] and tracks the HTTP status code.
// Use this when you need to inspect the status after WriteHeader is called.
type ResponseWriter struct {
	http.ResponseWriter

	statusCode int
}

// NewResponseWriter creates a ResponseWriter that wraps the given [http.ResponseWriter].
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

// WriteHeader writes the status code and delegates to the wrapped ResponseWriter.
func (w *ResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.ResponseWriter.WriteHeader(code)
}

// Write sets status to [http.StatusOK] if not already set, then delegates to wrapped ResponseWriter.
func (w *ResponseWriter) Write(b []byte) (int, error) {
	if w.statusCode == 0 {
		w.WriteHeader(http.StatusOK)
	}

	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}

	return w.ResponseWriter.Write(b)
}

// StatusCode returns the HTTP status code that was written.
func (w *ResponseWriter) StatusCode() int {
	return w.statusCode
}

// OperationKey is a type-safe context key for storing operation metadata.
type OperationKey struct{}

// operationData stores operation-related metadata in context.
type operationData struct {
	operation string
	resource  string
}

var operationCtxKey = OperationKey{} //nolint:gochecknoglobals // unexported context key used internally

// GetOperation retrieves the operation name from context, or "Unknown" if not set.
func GetOperation(ctx context.Context) string {
	if data, ok := ctx.Value(operationCtxKey).(*operationData); ok && data != nil {
		return data.operation
	}

	return "Unknown"
}

// SetOperation returns a new context with the operation name updated.
// This follows the idiomatic context pattern - immutable values.
func SetOperation(ctx context.Context, operation string) context.Context {
	return context.WithValue(ctx, operationCtxKey, &operationData{
		operation: operation,
		resource:  GetResource(ctx),
	})
}

// GetResource retrieves the resource identifier from context, or "" if not set.
func GetResource(ctx context.Context) string {
	if data, ok := ctx.Value(operationCtxKey).(*operationData); ok && data != nil {
		return data.resource
	}

	return ""
}

// SetResource returns a new context with the resource identifier updated.
// This follows the idiomatic context pattern - immutable values.
func SetResource(ctx context.Context, resource string) context.Context {
	return context.WithValue(ctx, operationCtxKey, &operationData{
		operation: GetOperation(ctx),
		resource:  resource,
	})
}

// SetOperationAndResource returns a new context with both operation and resource set.
// This is a convenience function to set both at once without intermediate contexts.
func SetOperationAndResource(ctx context.Context, operation, resource string) context.Context {
	return context.WithValue(ctx, operationCtxKey, &operationData{
		operation: operation,
		resource:  resource,
	})
}

// RequestIDMiddleware returns an Echo middleware that injects an x-amz-request-id
// header (a new UUID) into every HTTP response.
func RequestIDMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			c.Response().Header().Set("X-Amz-Request-Id", uuid.New().String())

			return next(c)
		}
	}
}

// expectedSigV4ScopeParts is the exact number of slash-separated parts in a valid SigV4 credential scope:
// AKID/date/region/service/aws4_request.
const (
	expectedSigV4ScopeParts = 5
	sigV4AccessKeyIndex     = 0
	sigV4DateIndex          = 1
	sigV4RegionIndex        = 2
	sigV4ServiceIndex       = 3
	sigV4TerminalIndex      = 4
	sigV4TerminalScope      = "aws4_request"
)

func parseValidSigV4Scope(raw string) []string {
	if idx := strings.IndexAny(raw, ", \t\r\n"); idx != -1 {
		raw = raw[:idx]
	}

	parts := strings.Split(raw, "/")
	if len(parts) != expectedSigV4ScopeParts {
		return nil
	}

	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			return nil
		}
	}

	if parts[sigV4TerminalIndex] != sigV4TerminalScope {
		return nil
	}

	return parts
}

func extractSigV4ScopeFromRequest(r *http.Request) []string {
	if r == nil {
		return nil
	}

	if auth := r.Header.Get("Authorization"); auth != "" {
		if _, raw, ok := strings.Cut(auth, "Credential="); ok {
			if scope := parseValidSigV4Scope(raw); scope != nil {
				return scope
			}
		}
	}

	if r.URL != nil {
		if cred := r.URL.Query().Get("X-Amz-Credential"); cred != "" {
			if scope := parseValidSigV4Scope(cred); scope != nil {
				return scope
			}
		}
	}

	return nil
}

// ExtractRegionFromRequest extracts the AWS region from an HTTP request.
// It checks the SigV4 Authorization header credential scope first, then query credential,
// then the X-Amz-Region header, then falls back to defaultRegion.
func ExtractRegionFromRequest(r *http.Request, defaultRegion string) string {
	if r != nil {
		if scope := extractSigV4ScopeFromRequest(r); scope != nil {
			return SanitizeHeaderString(scope[sigV4RegionIndex])
		}

		if region := r.Header.Get("X-Amz-Region"); region != "" {
			return SanitizeHeaderString(region)
		}
	}

	return SanitizeHeaderString(defaultRegion)
}

// ExtractServiceFromRequest extracts the AWS service name from the SigV4 Authorization
// header credential scope or X-Amz-Credential query parameter.
// Returns an empty string if the service name cannot be determined.
func ExtractServiceFromRequest(r *http.Request) string {
	if scope := extractSigV4ScopeFromRequest(r); scope != nil {
		return SanitizeHeaderString(scope[sigV4ServiceIndex])
	}

	return ""
}

func extractBareAccessKey(raw string) string {
	if idx := strings.IndexAny(raw, ", \t\r\n"); idx != -1 {
		raw = raw[:idx]
	}

	if !strings.Contains(raw, "/") && raw != "" {
		return SanitizeHeaderString(raw)
	}

	return ""
}

// ExtractAccessKeyFromRequest extracts the AWS access key ID from an HTTP request.
// It checks the SigV4 Authorization header credential scope first, then the
// X-Amz-Credential query parameter, and returns an empty string if none is found.
func ExtractAccessKeyFromRequest(r *http.Request) string {
	if scope := extractSigV4ScopeFromRequest(r); scope != nil {
		return SanitizeHeaderString(scope[sigV4AccessKeyIndex])
	}

	if r == nil {
		return ""
	}

	if auth := r.Header.Get("Authorization"); auth != "" {
		if _, raw, ok := strings.Cut(auth, "Credential="); ok {
			if key := extractBareAccessKey(raw); key != "" {
				return key
			}
		}
	}

	if r.URL != nil {
		if cred := r.URL.Query().Get("X-Amz-Credential"); cred != "" {
			if key := extractBareAccessKey(cred); key != "" {
				return key
			}
		}
	}

	return ""
}

// ExtractSecurityTokenFromRequest extracts the AWS session token from an HTTP request.
// It checks the X-Amz-Security-Token header first, then the X-Amz-Security-Token query parameter.
func ExtractSecurityTokenFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}

	if tok := r.Header.Get("X-Amz-Security-Token"); tok != "" {
		return SanitizeHeaderString(tok)
	}

	if r.URL != nil {
		if tok := r.URL.Query().Get("X-Amz-Security-Token"); tok != "" {
			return SanitizeHeaderString(tok)
		}
	}

	return ""
}

// TagsPathPrefix is the shared "/tags/{resourceArn}" prefix multiple
// services expose for TagResource/UntagResource/ListTagsForResource.
const TagsPathPrefix = "/tags/"

// MatchesTaggedResourceARN reports whether path is a "/tags/{resourceArn}"
// request whose ARN names serviceName (arn:{partition}:{serviceName}:...).
// Several services share the "/tags/" prefix; only the ARN's own service
// segment reliably disambiguates the true owner -- a bare prefix match
// steals every other service's tag requests too (see gopherstack-sokq).
func MatchesTaggedResourceARN(path, serviceName string) bool {
	after, ok := strings.CutPrefix(path, TagsPathPrefix)
	if !ok {
		return false
	}

	return strings.Contains(after, ":"+serviceName+":")
}

// ScopedPrefixMatch reports whether path has the given prefix AND the
// request's SigV4 signing scope, if present, permits serviceName to claim
// it: an unsigned request (no Authorization header, or none carrying a
// recognizable scope) still matches, but a request signed for a different,
// known service does not. Use this in a RouteMatcher instead of a bare
// strings.HasPrefix whenever the path shape is one another service's real
// wire API could also produce -- a bare prefix match steals that service's
// requests (see gopherstack-vpoh: iotdataplane's own "/connections/{id}"
// swallowed Outposts' GetConnection).
func ScopedPrefixMatch(r *http.Request, path, prefix, serviceName string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}

	scope := ExtractServiceFromRequest(r)

	return scope == "" || scope == serviceName
}

func isAllowedHeaderSpecial(c rune) bool {
	switch c {
	case '-', '_', '.', '+', '/', '=', ':', '~':
		return true
	default:
		return false
	}
}

func isAllowedHeaderChar(c rune) bool {
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
		return true
	}

	return isAllowedHeaderSpecial(c)
}

// SanitizeHeaderString removes control characters and unexpected characters while
// preserving alphanumeric, hyphens, underscores, periods, and base64/ARN characters (+, /, =, :, ~).
// This breaks the taint for static analysis tools like CodeQL which flag raw header values in logs.
func SanitizeHeaderString(s string) string {
	var b strings.Builder
	for _, c := range s {
		if isAllowedHeaderChar(c) {
			b.WriteRune(c)
		}
	}

	return b.String()
}
