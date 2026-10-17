package logger

import (
	"html"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	maxErrorCodeLen    = 128
	maxErrorMessageLen = 1024
	sigV4ScopeParts    = 5
	sigV4ServiceIdx    = 3
	errorTypeHeader    = "X-Amzn-Errortype"
)

var (
	jsonTypeRe    = regexp.MustCompile(`"__type"\s*:\s*"([^"]*)"`)
	jsonCodeRe    = regexp.MustCompile(`"[Cc]ode"\s*:\s*"([^"]*)"`)
	jsonMessageRe = regexp.MustCompile(`"(?:[Mm]essage|errorMessage)"\s*:\s*("(?:[^"\\]|\\.)*")`)
	xmlCodeRe     = regexp.MustCompile(`<Code>([^<]*)</Code>`)
	xmlMessageRe  = regexp.MustCompile(`(?s)<Message>(.*?)</Message>`)
	sigV4CredRe   = regexp.MustCompile(`Credential=([^,\s]+)`)
)

// sensitiveQueryParams are lowercase presigned-URL parameters whose values are secrets.
var sensitiveQueryParams = map[string]struct{}{ //nolint:gochecknoglobals // immutable lookup table
	"x-amz-signature":      {},
	"x-amz-security-token": {},
	"x-amz-credential":     {},
	"signature":            {},
}

// errorBodyCapture keeps the first maxCapturedBodyBytes of an error response body.
// It is single-request scoped, so it needs no locking.
type errorBodyCapture struct {
	buf    []byte
	status int
}

// Write never short-writes, so io.MultiWriter always reaches the real writer.
func (c *errorBodyCapture) Write(b []byte) (int, error) {
	if c.status >= http.StatusBadRequest && len(c.buf) < maxCapturedBodyBytes {
		c.buf = append(c.buf, b[:min(len(b), maxCapturedBodyBytes-len(c.buf))]...)
	}

	return len(b), nil
}

// errorCaptureWriter tees error response bodies into an errorBodyCapture.
type errorCaptureWriter struct {
	http.ResponseWriter
	out io.Writer
	cap *errorBodyCapture
}

func (w *errorCaptureWriter) WriteHeader(code int) {
	w.cap.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *errorCaptureWriter) Write(b []byte) (int, error) {
	if w.cap.status == 0 {
		w.cap.status = http.StatusOK
	}

	return w.out.Write(b) //nolint:wrapcheck // pass-through writer
}

// Unwrap lets http.ResponseController and echo.UnwrapResponse reach the real writer.
func (w *errorCaptureWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// attachErrorCapture swaps the echo response's underlying writer for a capturing one.
// It returns nil when the response is not an echo.Response.
func attachErrorCapture(w http.ResponseWriter) *errorCaptureWriter {
	resp, err := echo.UnwrapResponse(w)
	if err != nil || resp == nil {
		return nil
	}

	c := &errorBodyCapture{}
	cw := &errorCaptureWriter{
		ResponseWriter: resp.ResponseWriter,
		out:            io.MultiWriter(resp.ResponseWriter, c),
		cap:            c,
	}
	resp.ResponseWriter = cw

	return cw
}

func resolveError(status int, err error, cw *errorCaptureWriter) (string, string) {
	if status < http.StatusBadRequest {
		return "", ""
	}

	var code, msg string

	if cw != nil {
		code, msg = ExtractErrorFields(cw.cap.buf)
		if code == "" {
			code = normalizeErrorCode(cw.Header().Get(errorTypeHeader))
		}
	}

	if msg == "" && err != nil {
		msg = truncate(err.Error(), maxErrorMessageLen)
	}

	return code, msg
}

// ExtractErrorFields pulls the AWS error code and message from a (possibly
// truncated) JSON or XML error body.
func ExtractErrorFields(body []byte) (string, string) {
	s := strings.TrimSpace(string(body))

	switch {
	case strings.HasPrefix(s, "{"):
		return extractJSONError(s)
	case strings.HasPrefix(s, "<"):
		return extractXMLError(s)
	default:
		return "", ""
	}
}

func extractJSONError(s string) (string, string) {
	var code string

	if m := jsonTypeRe.FindStringSubmatch(s); m != nil {
		code = m[1]
	} else if cm := jsonCodeRe.FindStringSubmatch(s); cm != nil {
		code = cm[1]
	}

	var msg string

	if m := jsonMessageRe.FindStringSubmatch(s); m != nil {
		if unq, err := strconv.Unquote(m[1]); err == nil {
			msg = unq
		} else {
			msg = strings.Trim(m[1], `"`)
		}
	}

	return normalizeErrorCode(code), truncate(msg, maxErrorMessageLen)
}

func extractXMLError(s string) (string, string) {
	var code, msg string

	if m := xmlCodeRe.FindStringSubmatch(s); m != nil {
		code = html.UnescapeString(m[1])
	}

	if m := xmlMessageRe.FindStringSubmatch(s); m != nil {
		msg = html.UnescapeString(m[1])
	}

	return normalizeErrorCode(code), truncate(msg, maxErrorMessageLen)
}

// normalizeErrorCode strips a Coral "namespace#" prefix and any ":detail" suffix.
func normalizeErrorCode(code string) string {
	code = strings.TrimSpace(code)
	if i := strings.LastIndex(code, "#"); i >= 0 {
		code = code[i+1:]
	}

	if i := strings.Index(code, ":"); i >= 0 {
		code = code[:i]
	}

	return truncate(code, maxErrorCodeLen)
}

func truncate(s string, limit int) string {
	if len(s) > limit {
		return s[:limit]
	}

	return s
}

// requestService returns the service the router dispatched to, falling back to
// the SigV4 credential-scope service.
func requestService(req *http.Request) string {
	if svc := ServiceTag(req.Context()); svc != "" {
		return svc
	}

	if m := sigV4CredRe.FindStringSubmatch(req.Header.Get("Authorization")); m != nil {
		return scopeService(m[1])
	}

	if cred := req.URL.Query().Get("X-Amz-Credential"); cred != "" {
		return scopeService(cred)
	}

	return ""
}

func scopeService(cred string) string {
	parts := strings.Split(cred, "/")
	if len(parts) != sigV4ScopeParts {
		return ""
	}

	return parts[sigV4ServiceIdx]
}

// redactQuery masks the values of secret presigned-URL parameters.
func redactQuery(raw string) string {
	if raw == "" {
		return ""
	}

	pairs := strings.Split(raw, "&")
	for i, p := range pairs {
		key, _, found := strings.Cut(p, "=")
		if !found {
			continue
		}

		if _, secret := sensitiveQueryParams[strings.ToLower(key)]; secret {
			pairs[i] = key + "=" + redactedHeaderValue
		}
	}

	return strings.Join(pairs, "&")
}
