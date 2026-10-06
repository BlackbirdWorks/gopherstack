package lightsail

import (
	"fmt"
	"slices"
)

type sdkEnum[T any] interface {
	~string
	Values() []T
}

// checkEnum rejects v when it is non-empty and outside the SDK enum's Values().
func checkEnum[T sdkEnum[T]](field string, v T) error {
	if v == "" || slices.Contains(v.Values(), v) {
		return nil
	}

	return validationError(fmt.Sprintf("invalid %s %q", field, string(v)))
}

// firstErr returns the first non-nil error.
func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}
