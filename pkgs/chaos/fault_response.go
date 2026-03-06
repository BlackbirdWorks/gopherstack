package chaos

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// faultResponse is the JSON body returned to the client when a fault fires.
type faultResponse struct {
	// Type is the error code (__type matches the AWS JSON error envelope convention).
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// respondWithFault writes the fault HTTP response and short-circuits the handler chain.
func respondWithFault(c *echo.Context, fe FaultError) error {
	statusCode := fe.StatusCode
	if statusCode <= 0 {
		statusCode = http.StatusServiceUnavailable
	}

	code := fe.Code
	if code == "" {
		code = "ServiceUnavailable"
	}

	body, err := json.Marshal(faultResponse{
		Type:    code,
		Message: "Fault injected by Gopherstack Chaos API",
	})
	if err != nil {
		return c.String(statusCode, code)
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(statusCode, body)
}
