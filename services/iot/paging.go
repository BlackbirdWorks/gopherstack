package iot

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
)

var errInvalidPageToken = errors.New("invalid pagination token")

// pageParams reads the page-size and token query keys; a malformed token is rejected.
func pageParams(c *echo.Context, sizeKey, tokenKey string) (int, int, error) {
	size := iotDefaultPageSize
	if n, err := strconv.Atoi(c.QueryParam(sizeKey)); err == nil && n > 0 && n < size {
		size = n
	}

	tok := c.QueryParam(tokenKey)
	if tok == "" {
		return size, 0, nil
	}

	start, err := strconv.Atoi(tok)
	if err != nil || start < 0 {
		return 0, 0, errInvalidPageToken
	}

	return size, start, nil
}

// respondPage writes one page of items under field, adding tokenField only when truncated.
func respondPage[T any](c *echo.Context, field, tokenField string, items []T, size, start int) error {
	page, next := paginateMaps(items, size, start)
	resp := map[string]any{field: page}
	if next != "" {
		resp[tokenField] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// respondListPage pages items using the standard maxResults/nextToken keys.
func respondListPage[T any](c *echo.Context, field string, items []T) error {
	size, start, err := pageParams(c, "maxResults", "nextToken")
	if err != nil {
		return respondInvalidPageToken(c, err)
	}

	return respondPage(c, field, "nextToken", items, size, start)
}

func respondInvalidPageToken(c *echo.Context, err error) error {
	return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
}

// parseIoTTimeQueryParam parses an RFC3339 (SDK wire) or epoch-seconds timestamp.
func parseIoTTimeQueryParam(c *echo.Context, name string) float64 {
	v := c.QueryParam(name)
	if v == "" {
		return 0
	}

	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return float64(t.Unix())
	}

	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}

	return f
}

// inTimeRange reports whether ts falls within [start, end]; a zero bound is open.
func inTimeRange(ts, start, end float64) bool {
	return (start == 0 || ts >= start) && (end == 0 || ts <= end)
}
