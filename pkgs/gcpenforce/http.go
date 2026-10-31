package gcpenforce

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
)

// Middleware wraps next, answering requests that fail the Gate with the Google JSON error envelope. It returns
// next unchanged when ctx carries no Gate.
func Middleware(ctx context.Context, t Target, next http.Handler) http.Handler {
	g := GateFrom(ctx)
	if g == nil || exemptHost(t.Host) {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := checkRequest(g, t, r); err != nil {
			gcprest.WriteError(w, err)

			return
		}

		next.ServeHTTP(w, r)
	})
}

// EchoMiddleware is Middleware for an echo router; it is a pass-through when ctx carries no Gate.
func EchoMiddleware(ctx context.Context, t Target) echo.MiddlewareFunc {
	g := GateFrom(ctx)
	if g == nil || exemptHost(t.Host) {
		return func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if err := checkRequest(g, t, c.Request()); err != nil {
				gcprest.WriteError(c.Response(), err)

				return nil
			}

			return next(c)
		}
	}
}

func checkRequest(g *Gate, t Target, r *http.Request) error {
	if OperationsPath(r.URL.Path) {
		return nil
	}

	return g.Check(FromRequest(r, t.QueryOnly), t.Host)
}
