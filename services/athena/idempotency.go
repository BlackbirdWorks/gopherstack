package athena

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// replayCreate replays a create made earlier with the same ClientRequestToken and parameters.
// A reused token with different parameters is an InvalidRequestException per the SDK docs.
func (h *Handler) replayCreate(
	op, token string, req any, exists func(string) error, create func() (string, error),
) (string, error) {
	id, err := idempotency.Create(
		h.idem, op, token, idempotency.Fingerprint(req),
		func(s *string) string { return *s },
		func(id string) (*string, error) { return &id, exists(id) },
		func() (*string, error) {
			id, err := create()
			if err != nil {
				return nil, err
			}

			return &id, nil
		},
	)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return "", fmt.Errorf("%w: %s", ErrValidation, err.Error())
	}

	if err != nil {
		return "", err
	}

	return *id, nil
}

// found adapts a Get method into the existence probe replayCreate needs.
func found[T any](get func(string) (*T, error)) func(string) error {
	return func(id string) error {
		_, err := get(id)

		return err
	}
}
