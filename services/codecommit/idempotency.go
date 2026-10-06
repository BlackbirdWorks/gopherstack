package codecommit

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// replayCreate replays a create made earlier with the same clientRequestToken and parameters.
func replayCreate[T any](
	h *Handler, op, token string, req any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	v, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(req), idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, fmt.Errorf("%w: %s", ErrIdempotencyMismatch, err.Error())
	}

	return v, err
}
