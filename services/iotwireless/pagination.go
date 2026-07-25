package iotwireless

import (
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultListPageSize is used when the request's maxResults query parameter
// is absent, zero, or non-numeric. AWS IoT Wireless does not document a
// single fixed default across every List* op; this value is a reasonable
// choice that keeps single-page responses common in tests while still
// exercising real cursor pagination for larger result sets.
const defaultListPageSize = 50

// paginateQuery applies cursor-based pagination to a fully materialized,
// deterministically sorted list, using the "maxResults"/"nextToken" query
// parameters that every IoT Wireless List* operation accepts (confirmed
// against aws-sdk-go-v2's REST-JSON serializers, which bind both as query
// string parameters on every List* input). Real AWS accepted-but-ignored
// pagination was a documented gap; this makes it real.
func paginateQuery[T any](c *echo.Context, all []T) ([]T, string) {
	limit := 0

	if raw := c.QueryParam("maxResults"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}

	pg := page.New(all, c.QueryParam("nextToken"), limit, defaultListPageSize)

	return pg.Data, pg.Next
}
