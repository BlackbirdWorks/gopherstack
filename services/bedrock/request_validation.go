package bedrock

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxJobNameLen = 63

var (
	jobNamePattern = regexp.MustCompile(`^[a-zA-Z0-9](-*[a-zA-Z0-9+\-.])*$`)
	roleArnPattern = regexp.MustCompile(`^arn:aws(-[^:]+)?:iam::([0-9]{12})?:role/.+$`)
	s3UriPattern   = regexp.MustCompile(`^s3://[a-z0-9][.\-a-z0-9]{1,61}[a-z0-9](/.*)?$`)
)

func validateJobName(field, v string) error {
	if len(v) > maxJobNameLen || !jobNamePattern.MatchString(v) {
		return fmt.Errorf("%w: %s %q does not match pattern %s", ErrValidation, field, v, jobNamePattern.String())
	}

	return nil
}

func validateRoleArn(field, v string) error {
	if v != "" && !roleArnPattern.MatchString(v) {
		return fmt.Errorf("%w: %s %q does not match pattern %s", ErrValidation, field, v, roleArnPattern.String())
	}

	return nil
}

func validateS3Uri(field, v string) error {
	if v != "" && !s3UriPattern.MatchString(v) {
		return fmt.Errorf("%w: %s %q does not match pattern %s", ErrValidation, field, v, s3UriPattern.String())
	}

	return nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

const (
	minListPageSize = 1
	maxListPageSize = 1000
)

// validateListQuery rejects out-of-range maxResults and non-opaque nextToken.
func validateListQuery(q url.Values) error {
	if mr := q.Get("maxResults"); mr != "" {
		if n, err := strconv.Atoi(mr); err == nil && (n < minListPageSize || n > maxListPageSize) {
			return fmt.Errorf(
				"%w: maxResults must be between %d and %d",
				ErrValidation,
				minListPageSize,
				maxListPageSize,
			)
		}
	}

	if err := page.ValidateToken(q.Get("nextToken")); err != nil {
		return fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	return nil
}
