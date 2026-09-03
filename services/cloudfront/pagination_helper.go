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

	maxItemsReq := 0
	if s := c.QueryParam("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxItemsReq = n
		}
	}

	page, pageSize, isTruncated := paginateByMarkerValue(items, getID, marker, maxItemsReq)

	nextMarker := ""
	if isTruncated && len(page) > 0 {
		nextMarker = getID(page[len(page)-1])
	}

	return page, pageSize, isTruncated, nextMarker
}

// paginateByMarkerValue applies the same Marker/MaxItems page window as
// paginateByMarkerID, for handlers whose Marker/MaxItems travel in an XML
// request body rather than the query string (e.g. ListConnectionGroups,
// ListConnectionFunctions, ListDistributionTenants: cloudfront@v1.67.4
// serializers.go httpBindings functions for these ops return nil -- every
// field, including Marker/MaxItems, serializes into the XML document body).
func paginateByMarkerValue[T any](
	items []T,
	getID func(T) string,
	marker string,
	maxItemsReq int,
) ([]T, int, bool) {
	pageSize := maxItems
	if maxItemsReq > 0 && maxItemsReq < maxItems {
		pageSize = maxItemsReq
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

	return items, pageSize, isTruncated
}
