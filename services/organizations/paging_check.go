package organizations

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// checkPaging rejects a MaxResults outside 1-20 or a malformed NextToken with InvalidInputException.
func (h *Handler) checkPaging(c *echo.Context, maxResults int, nextToken string) (bool, error) {
	if maxResults != 0 && (maxResults < 1 || maxResults > maxPageSizeLimit) {
		return true, h.writeError(c, http.StatusBadRequest, "InvalidInputException",
			fmt.Sprintf("MaxResults must be between 1 and %d", maxPageSizeLimit))
	}

	if page.ValidateToken(nextToken) != nil {
		return true, h.writeError(c, http.StatusBadRequest, "InvalidInputException", "INVALID_PAGINATION_TOKEN")
	}

	return false, nil
}

const maxPageSizeLimit = 20
