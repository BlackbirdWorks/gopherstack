package memorydb

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	minUserPasswordLen = 16
	maxUserPasswordLen = 128
	maxUserPasswords   = 2
)

var (
	parameterGroupFamilyPattern = regexp.MustCompile(`^memorydb_(redis|valkey)[0-9]+$`)
	snapshotNamePattern         = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]*$`)
)

func validateUserPasswords(passwords []string) error {
	if len(passwords) > maxUserPasswords {
		return fmt.Errorf("at most %d passwords may be supplied: %w", maxUserPasswords, ErrValidation)
	}

	for _, p := range passwords {
		if len(p) < minUserPasswordLen || len(p) > maxUserPasswordLen {
			return fmt.Errorf(
				"passwords must be between %d and %d characters: %w",
				minUserPasswordLen, maxUserPasswordLen, ErrValidation,
			)
		}

		if strings.ContainsAny(p, "/\"@ ") {
			return fmt.Errorf(`passwords must not contain '/', '"', '@' or spaces: %w`, ErrValidation)
		}
	}

	return nil
}

func isAccessStringKeyword(tok string) bool {
	switch strings.ToLower(tok) {
	case "on", "off", "allkeys", "resetkeys", "allchannels", "resetchannels",
		"allcommands", "nocommands", "nopass", "reset", "resetpass":
		return true
	}

	return false
}

func hasAccessStringPrefix(tok string) bool {
	for _, pre := range []string{"~", "%R~", "%W~", "%RW~", "&", "+", "-", "(", ">", "<", "#", "!"} {
		if strings.HasPrefix(tok, pre) {
			return true
		}
	}

	return false
}

func validateAccessString(s string) error {
	for tok := range strings.FieldsSeq(s) {
		if !isAccessStringKeyword(tok) && !hasAccessStringPrefix(tok) {
			return fmt.Errorf("access string %q contains an unrecognized rule %q: %w", s, tok, ErrValidation)
		}
	}

	return nil
}

func validateParameterGroupFamily(family string) error {
	if !parameterGroupFamilyPattern.MatchString(family) {
		return fmt.Errorf("parameter group family %q is not supported: %w", family, ErrValidation)
	}

	return nil
}

func validateSnapshotName(name string) error {
	if !snapshotNamePattern.MatchString(name) || len(name) > maxResourceNameLen || strings.Contains(name, "--") {
		return fmt.Errorf(
			"snapshot name %q must contain only letters, digits and single hyphens and begin with a letter: %w",
			name, ErrValidation,
		)
	}

	return nil
}

func defaultParameterGroupName(engine, version string) string {
	major, _, _ := strings.Cut(version, ".")

	switch {
	case engine == engineValkey && major == "8":
		return "default.memorydb-valkey8"
	case engine == engineValkey:
		return "default.memorydb-valkey7"
	case major == "6":
		return "default.memorydb-redis6"
	default:
		return "default.memorydb-redis7"
	}
}
