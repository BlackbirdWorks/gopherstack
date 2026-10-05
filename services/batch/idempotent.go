package batch

import (
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

const (
	consumableResourceTokenTTL = 8 * time.Hour
	tokenMemoEntries           = 4096
)

func tokenMismatchAsValidation(err error) error {
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return fmt.Errorf("%w: %w", ErrValidation, err)
	}

	return err
}
