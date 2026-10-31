package sdktest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Serve mounts regs behind the production service router on an httptest server and returns its URL.
func Serve(tb testing.TB, regs ...service.Registerable) string {
	tb.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	for _, r := range regs {
		if err := registry.Register(r); err != nil {
			tb.Fatalf("register %s: %v", r.Name(), err)
		}
	}

	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	tb.Cleanup(srv.Close)

	return srv.URL
}

// InMemoryEndpoint is the base endpoint to pair with the client from ServeInMemory.
const InMemoryEndpoint = "http://gopherstack.invalid"

type handlerTransport struct{ h http.Handler }

func (t handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, r)

	return rec.Result(), nil
}

// HandlerClient returns a client that calls h in-process (no socket); usable inside a
// testing/synctest bubble. Pair it with InMemoryEndpoint.
func HandlerClient(h http.Handler) *http.Client {
	return &http.Client{Transport: handlerTransport{h: h}}
}

// ServeInMemory is Serve without a socket: the returned client calls the router in-process, so
// it is usable inside a testing/synctest bubble. Pair it with InMemoryEndpoint.
func ServeInMemory(tb testing.TB, regs ...service.Registerable) *http.Client {
	tb.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	for _, r := range regs {
		if err := registry.Register(r); err != nil {
			tb.Fatalf("register %s: %v", r.Name(), err)
		}
	}

	e.Use(service.NewServiceRouter(registry).RouteHandler())

	return HandlerClient(e)
}
