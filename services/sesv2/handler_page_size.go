package sesv2

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func queryPageSize(c *echo.Context) int {
	n, err := strconv.Atoi(c.QueryParam("PageSize"))
	if err != nil || n < 0 {
		return 0
	}

	return n
}

type listParams struct {
	Filter    map[string]string
	NextToken string
	PageSize  int
}

// listRequestParams reads the list inputs from the JSON body of the POST
// form the SDK now emits, or from the query string of the legacy GET form.
func listRequestParams(c *echo.Context) listParams {
	if c.Request().Method != http.MethodPost {
		return listParams{NextToken: c.QueryParam(keyNextToken), PageSize: queryPageSize(c)}
	}

	var in struct {
		Filter    map[string]string `json:"Filter"`
		NextToken string            `json:"NextToken"`
		PageSize  int               `json:"PageSize"`
	}

	_ = json.NewDecoder(c.Request().Body).Decode(&in)

	return listParams{Filter: in.Filter, NextToken: in.NextToken, PageSize: max(in.PageSize, 0)}
}
