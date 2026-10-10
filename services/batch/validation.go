package batch

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxRetryAttempts  = 10
	minTimeoutSeconds = 60
)

const paginationSecret = "batch-secret"

var resourceNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

func validateRetryStrategy(rs *RetryStrategy) error {
	if rs != nil && (rs.Attempts < 0 || rs.Attempts > maxRetryAttempts) {
		return fmt.Errorf("%w: retryStrategy.attempts must be between 1 and %d", ErrValidation, maxRetryAttempts)
	}

	return nil
}

func validateJobTimeout(t *JobTimeout) error {
	if t != nil && t.AttemptDurationSeconds != 0 && t.AttemptDurationSeconds < minTimeoutSeconds {
		return fmt.Errorf("%w: timeout.attemptDurationSeconds must be at least %d", ErrValidation, minTimeoutSeconds)
	}

	return nil
}

// checkNextToken rejects a non-empty nextToken this backend did not issue;
// issued tokens always encode an offset of at least 1.
func checkNextToken(body []byte) error {
	var in struct {
		NextToken *string `json:"nextToken"`
	}

	if json.Unmarshal(body, &in) == nil && in.NextToken != nil && *in.NextToken != "" &&
		page.DecodeHMACToken(*in.NextToken, paginationSecret) <= 0 {
		return fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	return nil
}
