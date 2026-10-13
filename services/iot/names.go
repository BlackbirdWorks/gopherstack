package iot

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

const maxEntityNameLen = 128

var (
	entityNameRe = regexp.MustCompile(`^[a-zA-Z0-9:_-]+$`)
	policyNameRe = regexp.MustCompile(`^[\w+=,.@-]+$`)
)

// ErrMalformedPolicy is returned when a policy document is not valid JSON.
var ErrMalformedPolicy = errors.New("MalformedPolicyException")

func validateEntityName(kind, name string) error {
	if len(name) > maxEntityNameLen || !entityNameRe.MatchString(name) {
		return fmt.Errorf("%w: %s %q must match [a-zA-Z0-9:_-]+ and be at most %d characters",
			ErrValidation, kind, name, maxEntityNameLen)
	}

	return nil
}

func validatePolicyName(name string) error {
	if len(name) > maxEntityNameLen || !policyNameRe.MatchString(name) {
		return fmt.Errorf("%w: policy name %q must match [\\w+=,.@-]+ and be at most %d characters",
			ErrValidation, name, maxEntityNameLen)
	}

	return nil
}

func validatePolicyDocument(doc string) error {
	var v map[string]any
	if err := json.Unmarshal([]byte(doc), &v); err != nil {
		return fmt.Errorf("%w: policy document is not valid JSON", ErrMalformedPolicy)
	}

	return nil
}
