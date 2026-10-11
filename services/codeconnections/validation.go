package codeconnections

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Limits from botocore codeconnections/2023-12-01/service-2.json shapes.
const (
	maxConnectionNameLen   = 32
	maxHostNameLen         = 64
	maxTagKeyLen           = 128
	maxTagValueLen         = 256
	maxTagsPerResource     = 200
	maxProviderEndpointLen = 512
)

// ErrTagLimitExceeded maps to LimitExceededException, the only modeled error for too many tags.
var ErrTagLimitExceeded = awserr.New("LimitExceededException", awserr.ErrInvalidParameter)

func validateResourceName(name, field string, maxLen int) error {
	if name == "" {
		return fmt.Errorf("%w: %s is required", ErrValidation, field)
	}

	if len(name) > maxLen {
		return fmt.Errorf("%w: %s must not exceed %d characters", ErrValidation, field, maxLen)
	}

	return nil
}

func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf("%w: cannot have more than %d tags", ErrTagLimitExceeded, maxTagsPerResource)
	}

	for k, v := range tags {
		if k == "" {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}

		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds %d characters", ErrValidation, k, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q exceeds %d characters", ErrValidation, k, maxTagValueLen)
		}
	}

	return nil
}
