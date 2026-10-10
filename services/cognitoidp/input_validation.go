package cognitoidp

import (
	"encoding/json"
	"fmt"
	"regexp"
)

const (
	maxUserPoolIDLen = 55
	maxClientIDLen   = 128
)

var (
	userPoolIDRegex = regexp.MustCompile(`^[\w-]+_[0-9a-zA-Z]+$`)
	clientIDRegex   = regexp.MustCompile(`^[\w+]+$`)
)

type commonIdentifiers struct {
	UserPoolID *string `json:"UserPoolId"`
	ClientID   *string `json:"ClientId"`
}

// validateCommonIdentifiers applies the SDK's userPoolId/clientId pattern constraints before dispatch.
func validateCommonIdentifiers(body []byte) error {
	var ids commonIdentifiers
	if err := json.Unmarshal(body, &ids); err != nil {
		return nil //nolint:nilerr // malformed bodies are reported by the op's own decoder
	}

	if ids.UserPoolID != nil && *ids.UserPoolID != "" &&
		(len(*ids.UserPoolID) > maxUserPoolIDLen || !userPoolIDRegex.MatchString(*ids.UserPoolID)) {
		return fmt.Errorf(
			"%w: 1 validation error detected: Value '%s' at 'userPoolId' failed to satisfy constraint: "+
				`Member must satisfy regular expression pattern: [\w-]+_[0-9a-zA-Z]+`,
			ErrInvalidParameter, *ids.UserPoolID,
		)
	}

	if ids.ClientID != nil && *ids.ClientID != "" &&
		(len(*ids.ClientID) > maxClientIDLen || !clientIDRegex.MatchString(*ids.ClientID)) {
		return fmt.Errorf(
			"%w: 1 validation error detected: Value '%s' at 'clientId' failed to satisfy constraint: "+
				`Member must satisfy regular expression pattern: [\w+]+`,
			ErrInvalidParameter, *ids.ClientID,
		)
	}

	return nil
}
