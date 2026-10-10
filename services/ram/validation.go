package ram

import (
	"fmt"
	"regexp"
	"strings"
)

var accountIDPattern = regexp.MustCompile(`^\d{12}$`)

func validateResourceOwner(owner string) error {
	if owner != "SELF" && owner != "OTHER-ACCOUNTS" {
		return fmt.Errorf("%w: resourceOwner must be SELF or OTHER-ACCOUNTS", ErrInvalidParameter)
	}

	return nil
}

func validateAssociationType(t string) error {
	if t != "PRINCIPAL" && t != "RESOURCE" {
		return fmt.Errorf("%w: associationType must be PRINCIPAL or RESOURCE", ErrInvalidParameter)
	}

	return nil
}

// validatePrincipals accepts a 12-digit account ID or an ARN, the only principal shapes RAM documents.
func validatePrincipals(principals []string) error {
	for _, p := range principals {
		if !accountIDPattern.MatchString(p) && !strings.HasPrefix(p, "arn:") {
			return fmt.Errorf("%w: principal %q is not an account ID or ARN", ErrInvalidParameter, p)
		}
	}

	return nil
}

func validateShareARN(arn string) error {
	if !strings.HasPrefix(arn, "arn:") {
		return fmt.Errorf("%w: %q is not a valid ARN", ErrMalformedArn, arn)
	}

	return nil
}
