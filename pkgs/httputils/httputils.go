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

// ReadBody reads the request body and returns it as a byte slice.
// It handles cases where r.Body might be nil (e.g. in some test environments).
// It re-seeds the request body so it can be read multiple times.
func ReadBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// Re-seed the body so it can be read again
	r.Body = io.NopCloser(bytes.NewReader(body))

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
		w.statusCode = http.StatusOK
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

// minSigV4CredentialParts is the minimum number of slash-separated parts in the
// SigV4 credential scope needed to safely read the region at index 2
// (AKID/date/region/...).
const minSigV4CredentialParts = 3

// sigV4ServiceIndex is the index of the service name in the credential scope parts.
const sigV4ServiceIndex = 3

// ExtractRegionFromRequest extracts the AWS region from an HTTP request.
// It checks the SigV4 Authorization header credential scope first, then the
// X-Amz-Region header, then falls back to defaultRegion.
func ExtractRegionFromRequest(r *http.Request, defaultRegion string) string {
	if auth := r.Header.Get("Authorization"); auth != "" && strings.Contains(auth, "Credential=") {
		parts := strings.Split(auth, "Credential=")
		if len(parts) > 1 {
			credParts := strings.Split(parts[1], "/")
			if len(credParts) >= minSigV4CredentialParts {
				return credParts[2]
			}
		}
	}

	if region := r.Header.Get("X-Amz-Region"); region != "" {
		return region
	}

	return defaultRegion
}

// ExtractServiceFromRequest extracts the AWS service name from the SigV4 Authorization
// header credential scope (AKID/date/region/service/aws4_request).
// Returns an empty string if the service name cannot be determined.
func ExtractServiceFromRequest(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" && strings.Contains(auth, "Credential=") {
		parts := strings.Split(auth, "Credential=")
		if len(parts) > 1 {
			credParts := strings.Split(parts[1], "/")
			if len(credParts) > sigV4ServiceIndex {
				return credParts[sigV4ServiceIndex]
			}
		}
	}

	return ""
}
