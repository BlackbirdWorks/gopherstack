package apptrace

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const (
	errBodyCap       = 2048
	httpErrorStatus  = 400
	headerRequestID  = "X-Amz-Request-Id"
	headerErrorType  = "X-Amzn-Errortype"
	headerTarget     = "X-Amz-Target"
	headerEncoding   = "Content-Encoding"
	statusServerFail = http.StatusInternalServerError
)

// skipPrefixes are non-API paths that must not be traced (and would recurse).
func skipPath(p string) bool {
	for _, pre := range [...]string{"/dashboard", "/_gopherstack", "/_localstack", "/_aws", "/favicon.ico"} {
		if strings.HasPrefix(p, pre) {
			return true
		}
	}

	return false
}

// cappedBody tees up to cap bytes of what the handler actually reads.
type cappedBody struct {
	io.ReadCloser
	buf  bytes.Buffer
	cap  int
	read int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.read += int64(n)

	if room := b.cap - b.buf.Len(); room > 0 && n > 0 {
		b.buf.Write(p[:min(n, room)])
	}

	return n, err
}

// teeWriter records the status and a bounded copy of the response body.
type teeWriter struct {
	http.ResponseWriter
	buf    bytes.Buffer
	cap    int
	status int
	size   int64
}

func (w *teeWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}

	w.ResponseWriter.WriteHeader(code)
}

func (w *teeWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}

	n, err := w.ResponseWriter.Write(b)
	w.size += int64(n)

	if room := w.cap - w.buf.Len(); room > 0 && n > 0 {
		w.buf.Write(b[:min(n, room)])
	}

	return n, err
}

func (w *teeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Middleware traces every API request. Metadata is always cheap; headers and
// bodies are captured only while [Tracer.DetailEnabled].
func Middleware(t *Tracer) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			if !t.Enabled() || skipPath(req.URL.Path) {
				return next(c)
			}

			return t.trace(c, next)
		}
	}
}

func (t *Tracer) trace(c *echo.Context, next echo.HandlerFunc) error {
	req := c.Request()
	start := time.Now()

	id := c.Response().Header().Get(headerRequestID)
	if id == "" {
		id = uuid.NewString()
	}

	sp := &Span{
		ID: id, TraceID: id, Kind: KindRequest, Start: start,
		Method: req.Method, Path: req.URL.Path, tracer: t,
		RequestBytes: max(req.ContentLength, 0),
	}

	detail := t.detail.Load()

	var reqBody *cappedBody

	if detail && req.Body != nil && req.Body != http.NoBody {
		reqBody = &cappedBody{ReadCloser: req.Body, cap: t.maxBody}
		req.Body = reqBody
	}

	c.SetRequest(req.WithContext(WithSpan(req.Context(), sp)))

	var tee *teeWriter

	resp, ok := c.Response().(*echo.Response)
	if ok {
		bodyCap := errBodyCap
		if detail {
			bodyCap = t.maxBody
		}

		tee = &teeWriter{ResponseWriter: resp.ResponseWriter, cap: bodyCap}
		resp.ResponseWriter = tee
	}

	err := next(c)

	if ok {
		resp.ResponseWriter = tee.ResponseWriter
	}

	t.finish(c, sp, tee, reqBody, err, detail)

	return err
}

func (t *Tracer) finish(
	c *echo.Context, sp *Span, tee *teeWriter, reqBody *cappedBody, err error, detail bool,
) {
	req := c.Request()
	hdr := c.Response().Header()

	sp.LatencyMs = float64(time.Since(sp.Start)) / float64(time.Millisecond)

	var respBytes []byte

	if tee != nil {
		sp.Status, sp.ResponseBytes, respBytes = tee.status, tee.size, tee.buf.Bytes()
	}

	if sp.Status == 0 {
		sp.Status = http.StatusOK
	}

	if err != nil {
		sp.Status = statusFromError(err)
		sp.ErrorCode = errorCodeFromErr(err)
	}

	if md := awsmeta.Get(req.Context()); md != nil {
		sp.Region, sp.Account, sp.AccessKeyID = md.Region, md.Account, md.AccessKeyID

		if sp.Service == "" {
			sp.Service = md.Service
		}

		if md.Principal != nil {
			sp.Principal = md.Principal.Arn
		}
	}

	if sp.Service == "" {
		return
	}

	if sp.Principal == "" {
		sp.Principal = sp.AccessKeyID
	}

	if sp.Operation == "" {
		sp.Operation = fallbackOperation(req)
	}

	if sp.ErrorCode == "" && sp.Status >= httpErrorStatus {
		sp.ErrorCode = t.red.errorCode(hdr.Get(headerErrorType), respBytes)
	}

	if detail {
		sp.Detail = t.buildDetail(req, hdr, reqBody, respBytes, sp)
	}

	t.Record(sp)
}

func (t *Tracer) buildDetail(
	req *http.Request, respHdr http.Header, reqBody *cappedBody, respBody []byte, sp *Span,
) *Detail {
	d := &Detail{
		RequestHeaders:  t.red.headers(req.Header),
		ResponseHeaders: t.red.headers(respHdr),
		Query:           t.red.query(req.URL.RawQuery),
		ResponseBody: t.red.body(
			respBody, sp.ResponseBytes, respHdr.Get(headerEncoding) != "",
		),
		ResponseTrunc: sp.ResponseBytes > int64(len(respBody)),
	}

	if reqBody != nil {
		d.RequestBody = t.red.body(reqBody.buf.Bytes(), reqBody.read, req.Header.Get(headerEncoding) != "")
		d.RequestTrunc = reqBody.read > int64(reqBody.buf.Len())

		if sp.RequestBytes == 0 {
			sp.RequestBytes = reqBody.read
		}
	}

	return d
}

func fallbackOperation(req *http.Request) string {
	if tgt := req.Header.Get(headerTarget); tgt != "" {
		if _, op, found := strings.Cut(tgt, "."); found {
			return op
		}

		return tgt
	}

	if a := req.URL.Query().Get("Action"); a != "" {
		return a
	}

	return req.Method + " " + req.URL.Path
}

func statusFromError(err error) int {
	var sc echo.HTTPStatusCoder
	if errors.As(err, &sc) && sc.StatusCode() != 0 {
		return sc.StatusCode()
	}

	return statusServerFail
}

func errorCodeFromErr(err error) string {
	if he, ok := errors.AsType[*echo.HTTPError](err); ok {
		return http.StatusText(he.Code)
	}

	return "InternalFailure"
}

type errEnvelope struct {
	Type string `json:"__type"`
	Code string `json:"Code"`
	Low  string `json:"code"`
}

func (e *errEnvelope) pick() string {
	for _, v := range [...]string{e.Type, e.Code, e.Low} {
		if v != "" {
			return v
		}
	}

	return ""
}

// errorCode derives an AWS error code from the error-type header or the
// (possibly truncated) error body; "" when none is recognisable.
func (r *redactor) errorCode(header string, body []byte) string {
	if header != "" {
		return cleanCode(header)
	}

	b := bytes.TrimSpace(body)
	if len(b) == 0 {
		return ""
	}

	if b[0] == '{' {
		var env errEnvelope
		if json.Unmarshal(b, &env) == nil {
			return cleanCode(env.pick())
		}

		if m := r.jsonCode.FindSubmatch(b); m != nil {
			return cleanCode(string(m[1]))
		}

		return ""
	}

	if m := r.xmlCode.FindSubmatch(b); m != nil {
		return cleanCode(string(m[1]))
	}

	return ""
}

// cleanCode strips the "ns#" prefix and ":url" suffix AWS error types carry.
func cleanCode(s string) string {
	if i := strings.LastIndexByte(s, '#'); i >= 0 {
		s = s[i+1:]
	}

	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}

	return strings.TrimSpace(s)
}
