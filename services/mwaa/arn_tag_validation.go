package mwaa

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	maxTagKeyLength   = 128
	maxTagValueLength = 256
)

var (
	bucketArnPattern = regexp.MustCompile(`^arn:aws(-[a-z]+)*:s3:::[a-z0-9.\-]+$`)
	roleArnPattern   = regexp.MustCompile(`^arn:aws(-[a-z]+)*:iam::\d{12}:role/?[a-zA-Z_0-9+=,.@\-_/]+$`)
)

func validateARNField(field, value string, pattern *regexp.Regexp) error {
	if !pattern.MatchString(value) {
		return fmt.Errorf("%w: %s %q is not a valid ARN", ErrInvalidParameter, field, value)
	}

	return nil
}

func validateTagEntries(tags map[string]string) error {
	for k, v := range tags {
		switch {
		case k == "" || len(k) > maxTagKeyLength:
			return fmt.Errorf("%w: tag keys must be 1-%d characters", ErrInvalidParameter, maxTagKeyLength)
		case len(v) > maxTagValueLength:
			return fmt.Errorf("%w: tag values must be at most %d characters", ErrInvalidParameter, maxTagValueLength)
		case strings.HasPrefix(strings.ToLower(k), "aws:"):
			return fmt.Errorf("%w: tag key %q uses the reserved aws: prefix", ErrInvalidParameter, k)
		}
	}

	return nil
}
