package dashboard

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

const (
	cloudJSONMaxBody  = 1 << 20
	cloudStatusNotFnd = "NOT_FOUND"
	cloudStatusBadArg = "INVALID_ARGUMENT"
	cloudStatusNoSvc  = "UNAVAILABLE"
)

type cloudJSONErrorBody struct {
	Message string `json:"message"`
	Status  string `json:"status"`
	Code    int    `json:"code"`
}

// cloudJSONFail writes the Google-style error envelope the SPA's restCall client parses.
func cloudJSONFail(c *echo.Context, code int, status, msg string) error {
	return c.JSON(code, map[string]cloudJSONErrorBody{
		keyError: {Code: code, Status: status, Message: msg},
	})
}

func cloudUnavailable(c *echo.Context, name string) error {
	return cloudJSONFail(c, http.StatusServiceUnavailable, cloudStatusNoSvc, name+" is not enabled")
}

// cloudJSONDecode reads a size-bounded JSON body into v; on failure it has already replied.
func cloudJSONDecode(c *echo.Context, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, cloudJSONMaxBody))
	if err := dec.Decode(v); err != nil {
		_ = cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "invalid request body")

		return false
	}

	return true
}
