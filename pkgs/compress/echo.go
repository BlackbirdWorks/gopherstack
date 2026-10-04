package compress

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// EchoMiddleware adapts the Compressor to Echo. It swaps the writer inside the
// concrete *echo.Response so type assertions on c.Response() keep working.
func (c *Compressor) EchoMiddleware() func(echo.HandlerFunc) echo.HandlerFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ec *echo.Context) error {
			resp, ok := ec.Response().(*echo.Response)
			if !ok {
				return next(ec)
			}

			inner := resp.ResponseWriter

			rw, finish, served := c.Begin(inner, ec.Request())
			if served {
				resp.Committed, resp.Status = true, http.StatusOK

				return nil
			}

			if finish == nil {
				return next(ec)
			}

			resp.ResponseWriter = rw
			err := next(ec)
			resp.ResponseWriter = inner
			finish()

			return err
		}
	}
}
