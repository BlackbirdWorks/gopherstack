package sesv2

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/labstack/echo/v5"
)

const maxQueryPageSize = 1000

// validatePagingQuery rejects a NextToken this service could not have issued
// and a PageSize outside 0..1000.
func validatePagingQuery(c *echo.Context) error {
	q := c.Request().URL.Query()

	if tok := q.Get("NextToken"); tok != "" {
		if _, err := base64.StdEncoding.DecodeString(tok); err != nil {
			return fmt.Errorf("%w: NextToken is not valid", ErrInvalidInput)
		}
	}

	if v := q.Get("PageSize"); v != "" {
		if n, err := strconv.Atoi(v); err != nil || n < 0 || n > maxQueryPageSize {
			return fmt.Errorf("%w: PageSize must be between 0 and %d", ErrInvalidInput, maxQueryPageSize)
		}
	}

	return nil
}
