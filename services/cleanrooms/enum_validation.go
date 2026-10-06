package cleanrooms

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

	return fmt.Errorf("%w: invalid %s %q", ErrValidation, field, string(v))
}

// checkEnumList applies checkEnum to every element of vs.
func checkEnumList[T sdkEnum[T]](field string, vs []string) error {
	for _, v := range vs {
		if err := checkEnum(field, T(v)); err != nil {
			return err
		}
	}

	return nil
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
