package azuresearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	maxBodyBytes  = 16 << 20
	headerAPIKey  = "api-key"
	mediaJSON     = "application/json; odata.metadata=minimal; odata.streaming=true; IEEE754Compatible=false; charset=utf-8"
	searchDomain  = ".search.windows.net"
	preferRepHdr  = "return=representation"
	defaultAPIVer = "2024-07-01"
)

var apiVersions = map[string]bool{ //nolint:gochecknoglobals // supported api-version set, read-only
	"2020-06-30": true, "2023-07-01-Preview": true, "2023-10-01-Preview": true, "2023-11-01": true,
	"2024-03-01-Preview": true, "2024-05-01-Preview": true, "2024-07-01": true, "2025-09-01": true,
}

type seg struct {
	name string
	arg  string
	has  bool
}

type request struct {
	c       *echo.Context
	r       *http.Request
	q       url.Values
	svc     string
	version string
	segs    []seg
	body    []byte
	role    keyRole
}

func parseSegments(p string) []seg {
	var out []seg

	for _, raw := range strings.Split(strings.Trim(p, "/"), "/") {
		if raw == "" {
			continue
		}

		if i := strings.Index(raw, "('"); i > 0 && strings.HasSuffix(raw, "')") {
			out = append(out, seg{name: raw[:i], arg: strings.ReplaceAll(raw[i+2:len(raw)-2], "''", "'"), has: true})

			continue
		}

		out = append(out, seg{name: raw})
	}

	return out
}

// pathTokens flattens `indexes('x')` into the two positional tokens "indexes", "x".
func (req *request) tokens() []string {
	var out []string

	for _, s := range req.segs {
		out = append(out, s.name)
		if s.has {
			out = append(out, s.arg)
		}
	}

	return out
}

func hostLabel(r *http.Request) string {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	if net.ParseIP(host) != nil {
		return ""
	}

	label, _, ok := strings.Cut(host, ".")
	if !ok {
		return ""
	}

	return label
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}

	if len(body) > maxBodyBytes {
		return nil, ErrTooLarge
	}

	return body, nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}

	return b
}

type apiErr struct {
	status int
	msg    string
}

func errorOf(err error) apiErr {
	table := []struct {
		err    error
		status int
	}{
		{ErrBadRequest, http.StatusBadRequest}, {ErrIndexNotFound, http.StatusNotFound},
		{ErrObjectNotFound, http.StatusNotFound}, {ErrDocumentNotFound, http.StatusNotFound},
		{ErrServiceNotFound, http.StatusNotFound}, {ErrPrecondition, http.StatusPreconditionFailed},
		{ErrUnauthorized, http.StatusUnauthorized}, {ErrForbidden, http.StatusForbidden},
		{ErrTooLarge, http.StatusRequestEntityTooLarge}, {ErrQuota, http.StatusForbidden},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return apiErr{status: e.status, msg: strings.TrimPrefix(err.Error(), e.err.Error()+": ")}
		}
	}

	return apiErr{status: http.StatusInternalServerError, msg: "An unexpected error occurred."}
}

func (h *Handler) fail(c *echo.Context, err error) error {
	e := errorOf(err)
	c.Response().Header().Set("Content-Type", mediaJSON)

	if e.status == http.StatusUnauthorized {
		c.Response().Header().Set("WWW-Authenticate", `Bearer realm="`+searchDomain[1:]+`"`)
	}

	return c.JSONBlob(e.status, mustJSON(map[string]any{"error": map[string]any{"code": "", "message": e.msg}}))
}

func (h *Handler) respond(req *request, status int, v any) error {
	hdr := req.c.Response().Header()
	hdr.Set("Content-Type", mediaJSON)
	hdr.Set("OData-Version", "4.0")
	hdr.Set("Preference-Applied", `odata.include-annotations="*"`)

	return req.c.JSONBlob(status, mustJSON(v))
}

func (h *Handler) respondEmpty(req *request, status int) error {
	return req.c.NoContent(status)
}

// Handler returns the echo handler of the HTTPS listener.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		c.Response().Header().Set("Server", "Microsoft-IIS/10.0")

		req, err := h.newRequest(c)
		if err != nil {
			return h.fail(c, err)
		}

		return h.dispatch(req)
	}
}

func (h *Handler) newRequest(c *echo.Context) (*request, error) {
	r := c.Request()

	body, err := readBody(r)
	if err != nil {
		return nil, err
	}

	req := &request{
		c: c, r: r, q: r.URL.Query(), segs: parseSegments(r.URL.Path), body: body,
		version: r.URL.Query().Get("api-version"),
	}

	if err = h.authenticate(req); err != nil {
		return nil, err
	}

	if req.version == "" {
		return nil, badf("Missing required parameter 'api-version'. Supported versions include %s.", defaultAPIVer)
	}

	if !apiVersions[req.version] {
		return nil, badf("Unsupported api-version '%s'. Supported versions include %s.", req.version, defaultAPIVer)
	}

	return req, nil
}

func (h *Handler) dispatch(req *request) error {
	toks := req.tokens()
	if len(toks) == 0 {
		return h.fail(req.c, notFound("The requested path was not found."))
	}

	var err error

	switch toks[0] {
	case "indexes":
		err = h.routeIndexes(req, toks[1:])
	case kindSynonymMaps, kindDataSources, kindIndexers, kindSkillsets:
		err = h.routeObjects(req, toks[0], toks[1:])
	case "servicestats":
		err = h.routeServiceStats(req)
	default:
		err = notFound("The requested path was not found.")
	}

	if err != nil {
		return h.fail(req.c, err)
	}

	return nil
}

func notFound(msg string) error { return fmt.Errorf("%w: %s", ErrObjectNotFound, msg) }

func methodNotAllowed() error { return badf("The HTTP method is not allowed for this resource.") }
