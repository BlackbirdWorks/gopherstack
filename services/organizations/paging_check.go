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
		return true, h.writeErrorReason(c, http.StatusBadRequest, "InvalidInputException",
			fmt.Sprintf("MaxResults must be between 1 and %d", maxPageSizeLimit), "")
	}

	if page.ValidateToken(nextToken) != nil {
		return true, h.writeErrorReason(c, http.StatusBadRequest, "InvalidInputException",
			"The pagination token is not valid.", "INVALID_NEXT_TOKEN")
	}

	return false, nil
}

const maxPageSizeLimit = 20

func pageResponse[T any](
	h *Handler, c *echo.Context, items []T, token string, maxResults int, wrap func(page.Page[T]) any,
) error {
	if rejected, err := h.checkPaging(c, maxResults, token); rejected {
		return err
	}

	return c.JSON(http.StatusOK, wrap(page.New(items, token, maxResults, defaultMaxResults)))
}
