package cloudfront

import (
	"strconv"

	"github.com/labstack/echo/v5"
)

// paginateByMarkerID applies Marker/MaxItems pagination to items already sorted
// ascending by the key getID returns, matching the same Marker/MaxItems/IsTruncated/
// NextMarker scheme handleListDistributions and handleListAnycastIPLists already use
// (cloudfront@v1.67.4: every List op's Input has Marker *string, MaxItems *int32).
func paginateByMarkerID[T any](
	c *echo.Context,
	items []T,
	getID func(T) string,
) ([]T, int, bool, string) {
	marker := c.QueryParam("Marker")

	pageSize := maxItems
	if s := c.QueryParam("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < maxItems {
			pageSize = n
		}
	}

	if marker != "" {
		cut := 0
		for cut < len(items) && getID(items[cut]) <= marker {
			cut++
		}

		items = items[cut:]
	}

	isTruncated := len(items) > pageSize
	if isTruncated {
		items = items[:pageSize]
	}

	nextMarker := ""
	if isTruncated && len(items) > 0 {
		nextMarker = getID(items[len(items)-1])
	}

	return items, pageSize, isTruncated, nextMarker
}
