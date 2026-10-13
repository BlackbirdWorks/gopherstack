package personalize

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	maxNameLen             = 63
	maxPageResults         = 100
	maxTokenLen            = 1500
	maxRoleArnLen          = 256
	maxFilterExpressionLen = 2500
	pageTokenPrefix        = "pz1:"
)

var roleArnPattern = regexp.MustCompile(`^arn:[a-z\d-]+:iam::\d{12}:role/?[a-zA-Z_0-9+=,.@\-_/]+$`)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9\-_]*$`)

func validateName(field, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: %s is required", ErrValidation, field)
	case len(name) > maxNameLen:
		return fmt.Errorf("%w: %s must be at most %d characters", ErrValidation, field, maxNameLen)
	case !namePattern.MatchString(name):
		return fmt.Errorf(
			"%w: %s %q must start with a letter or digit and contain only letters, digits, hyphens and underscores",
			ErrValidation, field, name,
		)
	}

	return nil
}

func validateSchemaDocument(schema string) error {
	if schema == "" {
		return fmt.Errorf("%w: schema is required", ErrValidation)
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(schema), &doc); err != nil {
		return fmt.Errorf("%w: schema is not valid JSON", ErrValidation)
	}

	if typ, _ := doc["type"].(string); typ != "record" {
		return fmt.Errorf("%w: schema must be an Avro record", ErrValidation)
	}

	return nil
}

func encodePageToken(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(pageTokenPrefix + key))
}

func decodePageToken(token string) (string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || !strings.HasPrefix(string(raw), pageTokenPrefix) {
		return "", false
	}

	return strings.TrimPrefix(string(raw), pageTokenPrefix), true
}

// validatePageInput rejects out-of-range maxResults and tokens this backend never issued.
func validatePageInput(input map[string]any) error {
	if v, ok := input["maxResults"].(float64); ok && (v < 1 || v > maxPageResults) {
		return fmt.Errorf("%w: maxResults must be between 1 and %d", ErrValidation, maxPageResults)
	}

	token, ok := input["nextToken"].(string)
	if !ok || token == "" {
		return nil
	}

	if _, valid := decodePageToken(token); !valid || len(token) > maxTokenLen {
		return fmt.Errorf("%w: nextToken is not valid", ErrInvalidNextToken)
	}

	return nil
}

func validateRoleArn(roleArn string) error {
	if roleArn == "" {
		return fmt.Errorf("%w: roleArn is required", ErrValidation)
	}

	if len(roleArn) > maxRoleArnLen || !roleArnPattern.MatchString(roleArn) {
		return fmt.Errorf("%w: roleArn %q is not a valid IAM role ARN", ErrValidation, roleArn)
	}

	return nil
}

func validateFilterExpression(expr string) error {
	trimmed := strings.TrimSpace(expr)

	switch {
	case trimmed == "":
		return fmt.Errorf("%w: filterExpression is required", ErrValidation)
	case len(expr) > maxFilterExpressionLen:
		return fmt.Errorf("%w: filterExpression must be at most %d characters", ErrValidation, maxFilterExpressionLen)
	case !strings.HasPrefix(trimmed, "INCLUDE") && !strings.HasPrefix(trimmed, "EXCLUDE"):
		return fmt.Errorf("%w: filterExpression must start with INCLUDE or EXCLUDE", ErrValidation)
	}

	return nil
}

// errorMessage drops the redundant "<Code>: " prefix wrapped sentinels carry.
func errorMessage(errType string, err error) string {
	msg := strings.TrimPrefix(err.Error(), errType+": ")

	for _, sentinel := range []error{ErrNotFound, ErrAlreadyExists, ErrValidation, ErrInUse, ErrInvalidNextToken} {
		if errors.Is(err, sentinel) {
			msg = strings.TrimPrefix(msg, sentinel.Error()+": ")
		}
	}

	return msg
}
