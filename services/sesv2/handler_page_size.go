package sesv2

import (
	"strconv"

	"github.com/labstack/echo/v5"
)

// queryPageSize reads the PageSize query member; 0 selects the backend default.
func queryPageSize(c *echo.Context) int {
	n, err := strconv.Atoi(c.QueryParam("PageSize"))
	if err != nil || n < 0 {
		return 0
	}

	return n
}
