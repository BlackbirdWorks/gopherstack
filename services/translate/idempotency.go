package translate

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// replayCreate replays a create made earlier with the same ClientToken and parameters.
// mismatch is the modelled error for a reused token: only CreateParallelData models ConflictException.
func replayCreate[T any](
	h *Handler, op string, input map[string]any, mismatch error,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	token, _ := input["ClientToken"].(string)

	v, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(input), idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, fmt.Errorf("%w: %s", mismatch, err.Error())
	}

	return v, err
}
