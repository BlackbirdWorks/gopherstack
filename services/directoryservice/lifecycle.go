package directoryservice

import (
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// SetLifecycleDelay overrides the dwell of every simulated transition (directory
// Requested/Creating, restore, trust, snapshot, share and setting changes).
// Zero (the default) keeps the built-in sub-second timings.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.lifecycleDelay.Store(int64(d))
}

func (b *InMemoryBackend) delayOr(builtin time.Duration) time.Duration {
	if d := time.Duration(b.lifecycleDelay.Load()); d > 0 {
		return d
	}

	return builtin
}

var (
	directoryNamePattern = regexp.MustCompile(`^([a-zA-Z0-9]+[\.-])+([a-zA-Z0-9])+$`)
	shortNamePattern     = regexp.MustCompile(`^[^\\/:*?"<>|.]+[^\\/:*?"<>|]*$`)
)

const (
	minPasswordLength = 8
	maxPasswordLength = 64
	maxDirectoryName  = 255
	requiredSubnets   = 2
	minCharClasses    = 3
)

func validateDirectoryName(name string) error {
	if len(name) > maxDirectoryName || !directoryNamePattern.MatchString(name) {
		return wrapf(ErrInvalidParameter, "Name must be a fully qualified domain name such as corp.example.com.")
	}

	return nil
}

func validateShortName(name string) error {
	if name != "" && !shortNamePattern.MatchString(name) {
		return wrapf(ErrInvalidParameter, "ShortName contains invalid characters.")
	}

	return nil
}

func validateDirectoryPassword(pw string) error {
	if len(pw) < minPasswordLength || len(pw) > maxPasswordLength {
		return wrapf(ErrInvalidParameter, "Password must be between 8 and 64 characters.")
	}

	var lower, upper, digit, special bool

	for _, r := range pw {
		switch {
		case unicode.IsLower(r):
			lower = true
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsDigit(r):
			digit = true
		case !unicode.IsSpace(r):
			special = true
		}
	}

	classes := 0
	for _, ok := range []bool{lower, upper, digit, special} {
		if ok {
			classes++
		}
	}

	if classes < minCharClasses {
		return wrapf(
			ErrInvalidParameter,
			"Password must contain characters from at least three of: lowercase, uppercase, numbers, special characters.",
		)
	}

	return nil
}

func validateVpcSubnets(subnets []string) error {
	if len(subnets) != requiredSubnets || subnets[0] == subnets[1] {
		return wrapf(ErrInvalidParameter, "VpcSettings.SubnetIds must contain exactly two distinct subnets.")
	}

	return nil
}

// wireMessage returns err's text without a leading exception-name prefix, and a
// descriptive default when the error is just a bare sentinel.
func wireMessage(err error) string {
	msg := err.Error()

	if head, rest, ok := strings.Cut(msg, ": "); ok && isExceptionName(head) {
		return rest
	}

	if !isExceptionName(msg) {
		return msg
	}

	switch {
	case errors.Is(err, ErrDirectoryNotFound), errors.Is(err, ErrDirectoryNotFoundDDNE):
		return "Directory does not exist."
	case errors.Is(err, awserr.ErrNotFound):
		return "The specified entity does not exist."
	case errors.Is(err, awserr.ErrAlreadyExists):
		return "The specified entity already exists."
	case errors.Is(err, awserr.ErrInvalidParameter):
		return "One or more parameters are not valid."
	default:
		return "The request could not be completed."
	}
}

func isExceptionName(s string) bool {
	return strings.HasSuffix(s, "Exception") && !strings.ContainsAny(s, " :")
}
