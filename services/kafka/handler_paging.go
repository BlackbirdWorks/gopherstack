package kafka

import "github.com/labstack/echo/v5"

// kafkaPage applies the request's maxResults/nextToken query members to all.
func kafkaPage[T any](c *echo.Context, all []T) ([]T, string) {
	offset := min(decodeKafkaPageToken(c.Request().URL.Query().Get("nextToken")), len(all))
	pageSize := kafkaPageSize(c)
	rest := all[offset:]

	if len(rest) <= pageSize {
		return rest, ""
	}

	return rest[:pageSize], encodeKafkaPageToken(offset + pageSize)
}
