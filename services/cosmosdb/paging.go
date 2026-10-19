package cosmosdb

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	headerMaxItemCount = "X-Ms-Max-Item-Count"
	headerContinuation = "X-Ms-Continuation"
	headerItemCount    = "X-Ms-Item-Count"
)

// pageRequest reads x-ms-max-item-count (absent or <= 0 means unbounded) and x-ms-continuation.
func pageRequest(r *http.Request) (string, int, error) {
	token := r.Header.Get(headerContinuation)
	if err := page.ValidateToken(token); err != nil {
		return "", 0, ErrInvalidContinuation
	}

	limit := 0

	if raw := r.Header.Get(headerMaxItemCount); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}

	return token, limit, nil
}

func paginate[T any](all []T, token string, limit int) page.Page[T] {
	return page.New(all, token, limit, max(len(all), 1))
}

func setPageHeaders(c *echo.Context, count int, next string) {
	hdr := c.Response().Header()
	hdr.Set(headerItemCount, strconv.Itoa(count))

	if next != "" {
		hdr.Set(headerContinuation, next)
	}
}

func (h *Handler) writeContinuationError(c *echo.Context) error {
	return h.writeError(c, http.StatusBadRequest, "BadRequest", "The continuation token provided is invalid.")
}
