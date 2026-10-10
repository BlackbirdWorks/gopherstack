package pipes

import (
	"fmt"
	"strings"
)

const (
	maxDescriptionLen = 512
	arnServiceFields  = 4
)

// arnService returns the service segment of an ARN, or "smk" for a self-managed Kafka source.
func arnService(s string) string {
	if strings.HasPrefix(s, "smk://") {
		return "smk"
	}

	parts := strings.SplitN(s, ":", arnServiceFields)
	if len(parts) < arnServiceFields || parts[0] != "arn" {
		return ""
	}

	return parts[2]
}

func validateTargetAndRole(target, role string) error {
	if arnService(target) == "" {
		return fmt.Errorf("%w: Target %q is not a valid ARN", ErrValidation, target)
	}

	if arnService(role) != "iam" {
		return fmt.Errorf("%w: RoleArn %q is not a valid IAM role ARN", ErrValidation, role)
	}

	return nil
}

func validateDescription(d string) error {
	if len(d) > maxDescriptionLen {
		return fmt.Errorf("%w: Description must be at most %d characters", ErrValidation, maxDescriptionLen)
	}

	return nil
}
