package cloudfront

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ifMatchFailure answers 412 PreconditionFailed when an If-Match header is present and not current.
// The header is optional on these operations in the SDK, so an absent one is accepted.
func ifMatchFailure(c *echo.Context, current, what string) (bool, error) {
	given := c.Request().Header.Get("If-Match")
	if given == "" || given == current {
		return false, nil
	}

	return true, xmlResp(c, http.StatusPreconditionFailed,
		cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current "+what+" ETag"))
}
