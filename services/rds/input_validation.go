package rds

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	minMasterPasswordLen    = 8
	maxMasterPasswordLen    = 128
	maxMySQLPasswordLen     = 41
	maxOraclePasswordLen    = 30
	maxDBIdentifierLen      = 63
	masterPasswordForbidden = `/"@`
)

var (
	dbInstanceClassRegex = regexp.MustCompile(`^db\.(serverless|[a-z][a-z0-9-]*[0-9][a-z0-9-]*\.[a-z0-9]+)$`)
	dbClusterIDRegex     = regexp.MustCompile(`^[a-zA-Z](-?[a-zA-Z0-9])*$`)
)

func validateDBInstanceClass(class string) error {
	if class == "" || dbInstanceClassRegex.MatchString(class) {
		return nil
	}

	return fmt.Errorf("%w: invalid DB instance class: %s", ErrInvalidParameter, class)
}

func validateDBClusterIdentifier(id string) error {
	if id == "" || (len(id) <= maxDBIdentifierLen && dbClusterIDRegex.MatchString(id)) {
		return nil
	}

	return fmt.Errorf("%w: DBClusterIdentifier %q is invalid", ErrInvalidParameter, id)
}

func masterPasswordMaxLen(engine string) int {
	switch {
	case engine == engineMySQL, engine == engineMariaDB, engine == engineAuroraMySQL:
		return maxMySQLPasswordLen
	case strings.HasPrefix(engine, "oracle"):
		return maxOraclePasswordLen
	default:
		return maxMasterPasswordLen
	}
}

// validateMasterPassword applies the documented MasterUserPassword constraints; empty is allowed.
func validateMasterPassword(engine, password string) error {
	if password == "" {
		return nil
	}

	if len(password) < minMasterPasswordLen {
		return fmt.Errorf(
			"%w: the parameter MasterUserPassword is not a valid password because it is shorter than %d characters",
			ErrInvalidParameter, minMasterPasswordLen,
		)
	}

	if len(password) > masterPasswordMaxLen(engine) {
		return fmt.Errorf(
			"%w: the parameter MasterUserPassword is not a valid password because it is longer than %d characters",
			ErrInvalidParameter, masterPasswordMaxLen(engine),
		)
	}

	for _, r := range password {
		if r < ' ' || r > '~' || strings.ContainsRune(masterPasswordForbidden, r) {
			return fmt.Errorf(
				"%w: the parameter MasterUserPassword is not a valid password: only printable ASCII "+
					"characters besides '/', '\"', and '@' are allowed",
				ErrInvalidParameter,
			)
		}
	}

	return nil
}
