package securityhub

import (
	"errors"
	"maps"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

const keyClientToken = "ClientToken"

// idemCreate replays a create carrying the same ClientToken and parameters; a token reused
// with other parameters is a ConflictException (modelled by each create's deserializer).
func idemCreate[T any](
	h *Handler, op string, body map[string]any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	token, _ := body[keyClientToken].(string)
	if token == "" {
		return create()
	}

	params := maps.Clone(body)
	delete(params, keyClientToken)

	res, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(params), idOf, get, create)

	return res, err
}

// createErrorResponse maps a create failure: a ClientToken parameter mismatch is a 409, the rest a 500.
func createErrorResponse(c *echo.Context, err error) error {
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return typedErrorResponse(
			c, http.StatusConflict, "ConflictException", "ClientToken already used with different parameters",
		)
	}

	return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
}
