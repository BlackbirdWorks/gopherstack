package appmesh

import (
	"net/http"
	"net/http/httptest"

	"github.com/labstack/echo/v5"
)

// NewInMemoryBackendForTest exposes NewInMemoryBackend for test packages.
func NewInMemoryBackendForTest(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackend(accountID, region)
}

// ExtractOperationForTest builds an echo context for the given method/path and
// returns the operation the handler resolves, for use by external test packages.
func ExtractOperationForTest(h *Handler, method, path string) string {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	return h.ExtractOperation(c)
}

// ExtractResourceForTest builds a GET echo context for path and returns the
// resource the handler resolves, for use by external test packages.
func ExtractResourceForTest(h *Handler, path string) string {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	return h.ExtractResource(c)
}

// RouteMatcherForTest builds a GET echo context for path and reports whether the
// handler's route matcher matches it, for use by external test packages.
func RouteMatcherForTest(h *Handler, path string) bool {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	return h.RouteMatcher()(c)
}

// NewProvider returns a new appmesh service Provider, for use by external test packages.
func NewProvider() *Provider {
	return &Provider{}
}
