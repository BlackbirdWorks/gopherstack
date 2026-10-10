package inspector2

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// replay returns the response recorded for (op, token), or runs do and records it; an empty token always runs do.
func (h *Handler) replay(op, token string, params any, do func() (map[string]any, error)) (map[string]any, error) {
	res, err := idempotency.Replay(h.idem, op, token, idempotency.Fingerprint(params), func() (*map[string]any, error) {
		out, doErr := do()
		if doErr != nil {
			return nil, doErr
		}

		return &out, nil
	})
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	if err != nil {
		return nil, err
	}

	return *res, nil
}
