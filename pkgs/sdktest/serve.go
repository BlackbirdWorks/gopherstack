package sdktest

import (
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
