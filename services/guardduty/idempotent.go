package guardduty

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// createOnce replays the resource a clientToken already created, keyed by op and scope.
func (h *Handler) createOnce(
	op, scope, token string, req any, exists func(id string) error, create func() (string, error),
) (string, error) {
	id, err := idempotency.Create(
		h.idem, op+"|"+scope, token, idempotency.Fingerprint(req),
		func(s *string) string { return *s },
		func(id string) (*string, error) {
			if getErr := exists(id); getErr != nil {
				return nil, getErr
			}

			return &id, nil
		},
		func() (*string, error) {
			newID, createErr := create()
			if createErr != nil {
				return nil, createErr
			}

			return &newID, nil
		},
	)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return "", fmt.Errorf("%w: %w", ErrValidation, err)
	}

	if err != nil {
		return "", err
	}

	return *id, nil
}

func only[T any](_ T, err error) error { return err }
