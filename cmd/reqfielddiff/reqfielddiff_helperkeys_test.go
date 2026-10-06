package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyReaderHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     string
		missing []string
	}{
		{
			name: "echo context helper",
			src: `package fixture
type Handler struct{}
func pageSize(c *echo.Context) int { return len(c.QueryParam("max-keys")) }
func (h *Handler) handlePutThing(c *echo.Context) error { _ = pageSize(c); return nil }`,
		},
		{
			name: "helper one level deeper",
			src: `package fixture
type Handler struct{}
func pageSize(c *echo.Context) int { return len(c.Request().URL.Query().Get("max-keys")) }
func page(c *echo.Context) int { return pageSize(c) }
func (h *Handler) handlePutThing(c *echo.Context) error { _ = page(c); return nil }`,
		},
		{
			name: "url values helper",
			src: `package fixture
type Handler struct{}
func queryMax(q url.Values) string { return q.Get("max-keys") }
func (h *Handler) handlePutThing(r *http.Request) error {
	q := r.URL.Query()
	_ = queryMax(q)
	return nil
}`,
		},
		{
			name: "generic helper taking url values",
			src: `package fixture
type Handler struct{}
func pageParams(q url.Values) string { return q.Get("max-keys") }
func pageQuery[T any](q url.Values, list []T) []T { _ = pageParams(q); return list }
func (h *Handler) handlePutThing(c *echo.Context) error {
	_ = pageQuery(c.Request().URL.Query(), []string{})
	return nil
}`,
		},
		{
			name: "http request helper with package const",
			src: `package fixture
type Handler struct{}
const keyMax = "max-keys"
func readMax(r *http.Request) string { return r.FormValue(keyMax) }
func (h *Handler) handlePutThing(r *http.Request) error { _ = readMax(r); return nil }`,
		},
		{
			name: "method helper on the handler",
			src: `package fixture
type Handler struct{}
func (h *Handler) pageSize(c *echo.Context) int { return len(c.QueryParam("max-keys")) }
func (h *Handler) handlePutThing(c *echo.Context) error { _ = h.pageSize(c); return nil }`,
		},
		{
			name: "header helper over http header",
			src: `package fixture
type Handler struct{}
func sse(hdr http.Header) string { return hdr.Get("X-Amz-Server-Side-Encryption") }
func (h *Handler) handlePutThing(r *http.Request) error { _ = sse(r.Header); return nil }`,
			missing: []string{"MaxKeys"},
		},
		{
			name: "route table of a local struct type",
			src: `package fixture
type Handler struct{}
type dispatchFunc func(r *http.Request) error
type route struct {
	fn dispatchFunc
	op string
	method string
}
func queryMax(q url.Values) string { return q.Get("max-keys") }
func (h *Handler) dispatchPut(r *http.Request) error { _ = queryMax(r.URL.Query()); return nil }
func (h *Handler) routes() []route {
	return []route{{method: "PUT", op: "PutThing", fn: h.dispatchPut}}
}
func (h *Handler) PutThing() {}`,
		},
		{
			name: "switch case body over a shared dispatcher",
			src: `package fixture
type Handler struct{}
const opPutThing = "PutThing"
func pageSize(c *echo.Context) int { return len(c.QueryParam("max-keys")) }
func (h *Handler) dispatch(c *echo.Context, op string) (any, error) {
	switch op {
	case opPutThing:
		_ = pageSize(c)
		return nil, nil
	}
	return nil, nil
}`,
		},
		{
			name: "raw query string key parser",
			src: `package fixture
type Handler struct{}
func queryValue(query, key string) string { prefix := key + "="; _ = prefix; return "" }
func (h *Handler) handlePutThing(query string) error { _ = queryValue(query, "max-keys"); return nil }`,
		},
		{
			name: "helper without a request source is not a read",
			src: `package fixture
type Handler struct{}
type cache struct{}
func (c cache) Get(k string) string { return "" }
func lookup(m cache) string { return m.Get("max-keys") }
func (h *Handler) handlePutThing(c *echo.Context) error { _ = lookup(cache{}); return nil }`,
			missing: []string{"MaxKeys"},
		},
		{
			name: "dynamic key is not credited",
			src: `package fixture
type Handler struct{}
func read(c *echo.Context, k string) string { return c.QueryParam(k) }
func (h *Handler) handlePutThing(c *echo.Context) error { _ = read(c, "other"); return nil }`,
			missing: []string{"MaxKeys"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ops := []sdkOp{{Name: "PutThing", Fields: []sdkField{mustField("MaxKeys", "", false)}}}
			attachWireKeys(ops, loadSerializerFixture(t, "restxml.fixture"))

			res := parseSrc(t, tt.src).resolveOps(ops)["PutThing"]

			assert.ElementsMatch(t, tt.missing, missingNames(ops[0], res))
		})
	}
}
