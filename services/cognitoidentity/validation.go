package cognitoidentity

import (
	"fmt"
	"regexp"
)

const maxPoolNameLen = 128

var (
	poolNamePattern     = regexp.MustCompile(`^[\w\s+=,.@-]+$`)
	developerProviderRe = regexp.MustCompile(`^[\w.-]+$`)
)

func validatePoolNames(name, developerProviderName string) error {
	if len(name) > maxPoolNameLen || !poolNamePattern.MatchString(name) {
		return fmt.Errorf("%w: IdentityPoolName must be 1-%d characters matching [\\w\\s+=,.@-]+",
			ErrInvalidParameter, maxPoolNameLen)
	}

	if developerProviderName != "" &&
		(len(developerProviderName) > maxPoolNameLen || !developerProviderRe.MatchString(developerProviderName)) {
		return fmt.Errorf("%w: DeveloperProviderName must be 1-%d characters matching [\\w.-]+",
			ErrInvalidParameter, maxPoolNameLen)
	}

	return nil
}
