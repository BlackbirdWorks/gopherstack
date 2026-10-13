package macie2

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

const (
	minFilterNameLen = 3
	maxFilterNameLen = 64
	maxListNameLen   = 128
	maxDescLen       = 512
	maxAllowRegexLen = 512
)

type createdRef struct {
	Arn string
	ID  string
}

// ErrTokenReused is a clientToken reused with different parameters.
var ErrTokenReused = awserr.New("the client token was already used with different parameters", awserr.ErrConflict)

func idemCreate[T any](
	h *Handler, op, token string, req any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	res, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(req), idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, fmt.Errorf("%w: %s", ErrTokenReused, err.Error())
	}

	return res, err
}

func idemStatus(err error) int {
	switch {
	case errors.Is(err, awserr.ErrConflict):
		return http.StatusConflict
	case errors.Is(err, awserr.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, awserr.ErrInvalidParameter):
		return http.StatusBadRequest
	}

	return http.StatusInternalServerError
}

func validateAllowListInput(name, description string, criteria *AllowListCriteria) error {
	if name == "" || utf8.RuneCountInString(name) > maxListNameLen {
		return fmt.Errorf("%w: name must be 1 to %d characters", ErrValidation, maxListNameLen)
	}

	if utf8.RuneCountInString(description) > maxDescLen {
		return fmt.Errorf("%w: description can contain at most %d characters", ErrValidation, maxDescLen)
	}

	if criteria == nil {
		return fmt.Errorf("%w: criteria is required", ErrValidation)
	}

	return validateAllowListCriteria(criteria)
}

func validateAllowListCriteria(c *AllowListCriteria) error {
	if (c.Regex == nil) == (c.S3WordsList == nil) {
		return fmt.Errorf("%w: criteria must specify exactly one of regex or s3WordsList", ErrValidation)
	}

	if c.Regex == nil {
		return nil
	}

	if *c.Regex == "" || len(*c.Regex) > maxAllowRegexLen {
		return fmt.Errorf("%w: regex must be 1 to %d characters", ErrValidation, maxAllowRegexLen)
	}

	if _, err := regexp.Compile(*c.Regex); err != nil {
		return fmt.Errorf("%w: regex is invalid: %s", ErrValidation, err.Error())
	}

	return nil
}

func validFilterAction(a string) bool { return a == "ARCHIVE" || a == "NOOP" }

func validateFindingsFilterInput(name, description, action string) error {
	if n := utf8.RuneCountInString(name); n < minFilterNameLen || n > maxFilterNameLen {
		return fmt.Errorf("%w: name must be %d to %d characters", ErrValidation, minFilterNameLen, maxFilterNameLen)
	}

	if utf8.RuneCountInString(description) > maxDescLen {
		return fmt.Errorf("%w: description can contain at most %d characters", ErrValidation, maxDescLen)
	}

	if !validFilterAction(action) {
		return fmt.Errorf("%w: action must be ARCHIVE or NOOP", ErrValidation)
	}

	return nil
}
