package dsql

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// ErrTokenReused is a clientToken reused with different parameters; the SDK declares only ConflictException for it.
var ErrTokenReused = awserr.New("client token was already used with different parameters", awserr.ErrConflict)

func mapIdemErr(err error) error {
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return fmt.Errorf("%w: %s", ErrTokenReused, err.Error())
	}

	return err
}

// idemCreate replays a create made earlier with the same clientToken and parameters.
func idemCreate[T any](
	h *Handler, op, token string, req any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	res, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(req), idOf, get, create)

	return res, mapIdemErr(err)
}

// idemReplay returns the response recorded for an earlier call with the same clientToken and parameters.
func idemReplay[T any](h *Handler, op, token string, req any, do func() (*T, error)) (*T, error) {
	res, err := idempotency.Replay(h.idem, op, token, idempotency.Fingerprint(req), do)

	return res, mapIdemErr(err)
}
