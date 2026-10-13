package docdb

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	maxIdentifierLen = 63
	maxUsernameLen   = 63
)

var (
	identifierPattern     = regexp.MustCompile(`^[a-zA-Z](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$`)
	masterUsernamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)
	instanceClassPattern  = regexp.MustCompile(`^db\.[a-z][a-z0-9-]*\.(micro|small|medium|large|xlarge|[0-9]+xlarge)$`)
)

func validateIdentifier(field, id string) error {
	if len(id) > maxIdentifierLen || !identifierPattern.MatchString(id) || strings.Contains(id, "--") {
		return fmt.Errorf(
			"%w: %s %q is not valid: must be 1-%d letters, digits or hyphens, begin with a letter, "+
				"not end with a hyphen and not contain two consecutive hyphens",
			ErrInvalidParameter, field, id, maxIdentifierLen,
		)
	}

	return nil
}

func validateInstanceNaming(id, class, engine string) error {
	if id == "" {
		return fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if err := validateIdentifier("DBInstanceIdentifier", id); err != nil {
		return err
	}
	if err := validateInstanceClass(class); err != nil {
		return err
	}

	return validateDocDBEngine(engine)
}

func validateClusterNaming(masterUser, engine string) error {
	if err := validateMasterUsername(masterUser); err != nil {
		return err
	}

	return validateDocDBEngine(engine)
}

func validateMasterUsername(name string) error {
	if name == "" {
		return nil
	}
	if len(name) > maxUsernameLen || !masterUsernamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: MasterUsername %q is not valid: must be 1-%d letters, digits or underscores and begin with a letter",
			ErrInvalidParameter, name, maxUsernameLen,
		)
	}

	return nil
}

func validateInstanceClass(class string) error {
	if class != "" && !instanceClassPattern.MatchString(class) {
		return fmt.Errorf("%w: DBInstanceClass %q is not a valid instance class", ErrInvalidParameter, class)
	}

	return nil
}

func validateDocDBEngine(engine string) error {
	if engine != "" && engine != docDBEngine {
		return fmt.Errorf("%w: engine %q is not valid; must be %q", ErrInvalidParameter, engine, docDBEngine)
	}

	return nil
}
