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

func malformedPolicy(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformedPolicy, fmt.Sprintf(format, args...))
}

// validatePolicyDocument checks the document is JSON with a Version and well-formed Statement entries.
func validatePolicyDocument(doc string) error {
	var v map[string]json.RawMessage
	if err := json.Unmarshal([]byte(doc), &v); err != nil {
		return malformedPolicy("policy document is not valid JSON")
	}

	var version string
	if raw, ok := v["Version"]; !ok || json.Unmarshal(raw, &version) != nil || version == "" {
		return malformedPolicy("policy document must have a string Version")
	}

	raw, ok := v["Statement"]
	if !ok {
		return malformedPolicy("policy document must have a Statement")
	}

	var statements []map[string]json.RawMessage

	if err := json.Unmarshal(raw, &statements); err != nil {
		var single map[string]json.RawMessage
		if json.Unmarshal(raw, &single) != nil {
			return malformedPolicy("Statement must be an object or an array of objects")
		}

		statements = []map[string]json.RawMessage{single}
	}

	for _, st := range statements {
		if err := validatePolicyStatement(st); err != nil {
			return err
		}
	}

	return nil
}

func validatePolicyStatement(st map[string]json.RawMessage) error {
	var effect string
	if raw, ok := st["Effect"]; !ok || json.Unmarshal(raw, &effect) != nil || (effect != "Allow" && effect != "Deny") {
		return malformedPolicy("statement Effect must be Allow or Deny")
	}

	for _, pair := range [][2]string{{"Action", "NotAction"}, {"Resource", "NotResource"}} {
		if !stringOrList(st[pair[0]]) && !stringOrList(st[pair[1]]) {
			return malformedPolicy("statement must have %s as a string or list of strings", pair[0])
		}
	}

	return nil
}

func stringOrList(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}

	var one string
	if json.Unmarshal(raw, &one) == nil {
		return one != ""
	}

	var many []string

	return json.Unmarshal(raw, &many) == nil && len(many) > 0
}
