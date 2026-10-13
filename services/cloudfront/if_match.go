package cloudfront

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ifMatchRequiredFailure is ifMatchFailure for operations that demand the header: a missing
// If-Match is InvalidIfMatchVersion (400), a stale one PreconditionFailed (412).
func ifMatchRequiredFailure(c *echo.Context, current, what string) (bool, error) {
	if c.Request().Header.Get("If-Match") == "" {
		return true, xmlResp(c, http.StatusBadRequest,
			cfErrorXML("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the "+what+"."))
	}

	return ifMatchFailure(c, current, what)
}

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
