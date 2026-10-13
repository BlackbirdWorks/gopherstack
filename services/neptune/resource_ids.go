package neptune

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"regexp"
)

const (
	resourceIDLen   = 26
	resourceIDBytes = 17
)

var instanceClassPattern = regexp.MustCompile(
	`^db\.([a-z][a-z0-9-]*\.(micro|small|medium|large|xlarge|[0-9]+xlarge)|serverless)$`,
)

func newResourceID(prefix string) string {
	raw := make([]byte, resourceIDBytes)
	_, _ = rand.Read(raw)

	return prefix + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)[:resourceIDLen]
}

func validateInstanceNaming(id, class string) error {
	if err := validateNeptuneIdentifier(id, "DBInstanceIdentifier"); err != nil {
		return err
	}

	return validateInstanceClass(class)
}

func validateInstanceClass(class string) error {
	if class != "" && !instanceClassPattern.MatchString(class) {
		return fmt.Errorf("%w: DBInstanceClass %q is not a valid instance class", ErrInvalidParameter, class)
	}

	return nil
}
