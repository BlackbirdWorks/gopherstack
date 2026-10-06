package cloudtrail

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const errInvalidNextToken = "InvalidNextTokenException"

// badPageToken reports a NextToken the page package could not have issued.
func badPageToken(token string) bool {
	return page.ValidateToken(token) != nil
}

// badOffsetToken is badPageToken for the plain-integer LookupEvents token.
func badOffsetToken(token string) bool {
	if token == "" {
		return false
	}

	n, err := strconv.Atoi(token)

	return err != nil || n < 0
}

func writeInvalidNextToken(c *echo.Context) error {
	return c.JSON(http.StatusBadRequest, errResp(errInvalidNextToken, "invalid NextToken"))
}
