package omics

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

func idemFingerprint(v any) string { return idempotency.Fingerprint(v) }

// idemCreate runs idempotency.Create, mapping a token reused with other parameters to ValidationException.
func idemCreate[T any](
	m *idempotency.Memo, op, token, fingerprint string,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	res, err := idempotency.Create(m, op, token, fingerprint, idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, fmt.Errorf("%w: token %q was already used with different parameters", ErrValidation, token)
	}

	return res, err
}

// idemCreated is idemCreate answered as a 201 JSON body, or the mapped error.
func idemCreated[T any](
	h *Handler, c *echo.Context, op, token, fingerprint string,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) error {
	res, err := idemCreate(h.idem, op, token, fingerprint, idOf, get, create)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, res)
}
