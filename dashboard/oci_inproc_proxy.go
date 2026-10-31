package dashboard

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ociInprocMaxBody = 1 << 14
	ociActionsSeg    = "actions"
	ociNamespaceKey  = "namespace"
)

// ociSplitPath splits a proxied path (below /api) into segments, rejecting empty, dot and encoded-slash segments.
func ociSplitPath(raw string) ([]string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.Contains(dec, "/") {
			return nil, false
		}
	}

	return parts, true
}

// ociDispatch forwards a request to the emulator under apiPath with body (nil keeps the caller's body, capped).
func ociDispatch(c *echo.Context, inner *echo.Echo, apiPath string, body []byte) error {
	r := c.Request()
	fwd := r.Clone(r.Context())
	fwd.URL.Path, fwd.URL.RawPath = unescapedPath(apiPath), apiPath
	fwd.RequestURI = ""

	if body == nil {
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, ociInprocMaxBody)
	} else {
		fwd.Body, fwd.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		fwd.Header = fwd.Header.Clone()
		fwd.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	inner.ServeHTTP(rec, fwd)

	return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id", "opc-work-request-id")
}

// ociDecodeBody decodes a capped JSON request body into v.
func ociDecodeBody(c *echo.Context, v any) bool {
	r := c.Request()

	return json.NewDecoder(http.MaxBytesReader(c.Response(), r.Body, ociInprocMaxBody)).Decode(v) == nil
}

// ociDecodeBodyN is ociDecodeBody with an explicit size cap.
func ociDecodeBodyN(c *echo.Context, v any, limit int64) bool {
	return json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, limit)).Decode(v) == nil
}

// ociInvalid answers 400 for a request outside the proxy's allow-list.
func ociInvalid(c *echo.Context, what string) error {
	return cloudJSONFail(c, http.StatusBadRequest, "InvalidParameter", what)
}
