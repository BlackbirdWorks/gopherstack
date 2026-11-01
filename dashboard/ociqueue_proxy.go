package dashboard

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/ociqueue"
)

const (
	ociQueuePrefix    = ociProxyPrefix + "/queue"
	ociQueueAPIPrefix = "/20210201"
	ociQueueMaxBody   = 1 << 12
	ociQueueIDSegs    = 2
	ociQueueSubSegs   = 3
	ociQueueActSegs   = 4
	ociQueuePeekMax   = 20
	ociQueueKind      = "queues"
	ociQueueStats     = "stats"
	ociQueuePeek      = "peek"
	ociQueuePurge     = "purge"
)

// registerOCIQueueRoutes serves the Queue page by dispatching in-process to the emulator's handler without request
// signing. Only queue reads, stats and purge are reachable; messages are read through a side-effect-free peek and
// never carry receipts.
func (h *DashboardHandler) registerOCIQueueRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	ops := h.config.OCIQueueOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI Queue", next) }

	h.SubRouter.Add(http.MethodGet, ociQueuePrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociQueuePrefix+"/api")

		parts, ok := ociQueueParts(r.Method, raw)
		if !ok {
			return cloudJSONFail(c, http.StatusBadRequest, "InvalidParameter", "unsupported Queue proxy request")
		}

		if len(parts) == ociQueueSubSegs && parts[2] == ociQueuePeek {
			return ociQueueServePeek(c, ops.Backend, unescapedPath(parts[1]))
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(ociQueueAPIPrefix+raw), ociQueueAPIPrefix+raw
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, ociQueueMaxBody)

		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, fwd)

		return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id", "opc-work-request-id")
	})

	h.SubRouter.Add(http.MethodGet, ociQueuePrefix+"/api/*", serve)
	h.SubRouter.Add(http.MethodPost, ociQueuePrefix+"/api/*", serve)
}

// ociQueueParts validates a proxied path (below /api) against the allowed queue operations and returns its segments.
func ociQueueParts(method, raw string) ([]string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.Contains(dec, "/") {
			return nil, false
		}
	}

	if parts[0] != ociQueueKind {
		return nil, false
	}

	switch method {
	case http.MethodGet:
		switch len(parts) {
		case 1, ociQueueIDSegs:
			return parts, true
		case ociQueueSubSegs:
			return parts, parts[2] == ociQueueStats || parts[2] == ociQueuePeek
		}
	case http.MethodPost:
		return parts, len(parts) == ociQueueActSegs && parts[2] == ociActionsSeg && parts[3] == ociQueuePurge
	}

	return nil, false
}

func ociQueueServePeek(c *echo.Context, b *ociqueue.InMemoryBackend, id string) error {
	limit, err := strconv.Atoi(c.QueryParam("limit"))
	if err != nil || limit < 1 || limit > ociQueuePeekMax {
		limit = ociQueuePeekMax
	}

	msgs, err := b.Peek(id, c.QueryParam("dlq") == queryTrue, limit)
	if err != nil {
		if ae, ok := errors.AsType[*ociqueue.APIError](err); ok {
			return cloudJSONFail(c, ae.Status, ae.Code, ae.Message)
		}

		return cloudJSONFail(c, http.StatusInternalServerError, "InternalServerError", "peek failed")
	}

	return c.JSON(http.StatusOK, map[string]any{"messages": msgs})
}

// ociRelay copies the emulator's response and the listed headers.
func ociRelay(c *echo.Context, rec *httptest.ResponseRecorder, headers ...string) error {
	for _, k := range headers {
		if v := rec.Header().Get(k); v != "" {
			c.Response().Header().Set(k, v)
		}
	}

	return c.JSONBlob(rec.Code, rec.Body.Bytes())
}
