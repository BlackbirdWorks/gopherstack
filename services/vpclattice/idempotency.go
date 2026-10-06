package vpclattice

import (
	"errors"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

const keyClientToken = "clientToken"

// idemCreate replays a create carrying the same clientToken and parameters; a token reused
// with other parameters is a ConflictException (CreateService et al. document it).
func idemCreate[T any](
	h *Handler, op, scope string, body map[string]any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	token, _ := body[keyClientToken].(string)
	if token == "" {
		return create()
	}

	params := maps.Clone(body)
	delete(params, keyClientToken)

	res, err := idempotency.Create(h.idem, op, token, scope+"|"+idempotency.Fingerprint(params), idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, fmt.Errorf("%w: clientToken already used with different parameters", ErrAlreadyExists)
	}

	return res, err
}
