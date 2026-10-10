package rekognition

import (
	"fmt"
	"regexp"
)

const (
	maxIdentifierLen  = 255
	maxUserIDLen      = 128
	maxClientTokenLen = 64
	maxPercent        = 100
)

var (
	nameRe        = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)
	userIDRe      = regexp.MustCompile(`^[a-zA-Z0-9_.\-:]+$`)
	clientTokenRe = regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)
)

func validateName(field, value string) error {
	if len(value) > maxIdentifierLen || !nameRe.MatchString(value) {
		return fmt.Errorf("%w: %s must match [a-zA-Z0-9_.\\-]+ and be at most %d characters",
			ErrValidation, field, maxIdentifierLen)
	}

	return nil
}

func validateUserLikeID(field, value string, maxLen int) error {
	if len(value) > maxLen || !userIDRe.MatchString(value) {
		return fmt.Errorf("%w: %s must match [a-zA-Z0-9_.\\-:]+ and be at most %d characters",
			ErrValidation, field, maxLen)
	}

	return nil
}

func validateClientToken(token string) error {
	if token != "" && (len(token) > maxClientTokenLen || !clientTokenRe.MatchString(token)) {
		return fmt.Errorf("%w: ClientRequestToken must match ^[a-zA-Z0-9-_]+$ and be at most %d characters",
			ErrValidation, maxClientTokenLen)
	}

	return nil
}

func validatePercent(field string, value float64) error {
	if value < 0 || value > maxPercent {
		return fmt.Errorf("%w: %s must be between 0 and %d", ErrValidation, field, maxPercent)
	}

	return nil
}
