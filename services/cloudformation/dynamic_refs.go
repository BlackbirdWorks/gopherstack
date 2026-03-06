package cloudformation

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// dynamicRefPattern matches CloudFormation dynamic reference strings:
// {{resolve:service-name:reference-key}} or {{resolve:service-name:reference-key:extra...}}.
var dynamicRefPattern = regexp.MustCompile(`\{\{resolve:([^:}]+):([^}]+)\}\}`)

// ErrDynamicRefFailed is returned when a dynamic reference cannot be resolved.
var ErrDynamicRefFailed = errors.New("dynamic reference resolution failed")

const (
	// splitTwo splits a string into at most 2 parts on ":".
	splitTwo = 2
	// smMaxParts is the maximum number of colon-separated parts in a secretsmanager reference.
	// Format: secret-id:SecretString:json-key:version-stage:version-id (5 parts).
	smMaxParts = 5
	// smJSONKeyParts is the minimum number of parts required for the JSON key to be present.
	// Parts: [0]=secret-id, [1]=SecretString, [2]=json-key, ...
	smJSONKeyParts = 3
	// smJSONKeyIndex is the index of the optional JSON key within secretsmanager parts.
	smJSONKeyIndex = 2
)

// DynamicRefResolver is the interface for resolving CloudFormation dynamic references.
type DynamicRefResolver interface {
	// ResolveSSMParameter retrieves an SSM plain-text or StringList parameter value.
	ResolveSSMParameter(name string) (string, error)
	// ResolveSSMSecureParameter retrieves an SSM SecureString parameter with decryption.
	ResolveSSMSecureParameter(name string) (string, error)
	// ResolveSecret retrieves a Secrets Manager secret value.
	// jsonKey may be empty; if non-empty the secret is parsed as JSON and the key is extracted.
	ResolveSecret(secretID, jsonKey string) (string, error)
}

// resolveDynamicRef resolves a single `{{resolve:...}}` string using the provided resolver.
// If the string is not a dynamic reference it is returned unchanged.
func resolveDynamicRef(s string, resolver DynamicRefResolver) (string, error) {
	if !strings.Contains(s, "{{resolve:") {
		return s, nil
	}

	match := dynamicRefPattern.FindStringSubmatch(s)
	if match == nil {
		return s, nil
	}

	service := match[1]
	rest := match[2]

	var resolved string

	var err error

	switch service {
	case "ssm":
		// Format: {{resolve:ssm:parameter-name}} or {{resolve:ssm:parameter-name:version}}
		name := strings.SplitN(rest, ":", splitTwo)[0]
		resolved, err = resolver.ResolveSSMParameter(name)
	case "ssm-secure":
		// Format: {{resolve:ssm-secure:parameter-name}} or {{resolve:ssm-secure:parameter-name:version}}
		name := strings.SplitN(rest, ":", splitTwo)[0]
		resolved, err = resolver.ResolveSSMSecureParameter(name)
	case "secretsmanager":
		// Format: {{resolve:secretsmanager:secret-id}}
		//      or {{resolve:secretsmanager:secret-id:SecretString:json-key:version-stage:version-id}}
		parts := strings.SplitN(rest, ":", smMaxParts)
		secretID := parts[0]

		jsonKey := ""
		if len(parts) >= smJSONKeyParts {
			jsonKey = parts[smJSONKeyIndex]
		}

		resolved, err = resolver.ResolveSecret(secretID, jsonKey)
	default:
		return "", fmt.Errorf("%w: unsupported service %q in reference %q", ErrDynamicRefFailed, service, match[0])
	}

	if err != nil {
		return "", fmt.Errorf("%w: %s: %w", ErrDynamicRefFailed, match[0], err)
	}

	// Replace the matched reference within the full string (supports embedded refs).
	return strings.ReplaceAll(s, match[0], resolved), nil
}

// resolveDynamicRefsInValue recursively walks a value tree and replaces any
// dynamic references in string leaves using the provided resolver.
// The value is modified in place for maps and slices; a new value is returned for strings.
func resolveDynamicRefsInValue(v any, resolver DynamicRefResolver) (any, error) {
	switch val := v.(type) {
	case string:
		return resolveDynamicRef(val, resolver)
	case map[string]any:
		for k, child := range val {
			resolved, err := resolveDynamicRefsInValue(child, resolver)
			if err != nil {
				return nil, err
			}

			val[k] = resolved
		}

		return val, nil
	case []any:
		for i, item := range val {
			resolved, err := resolveDynamicRefsInValue(item, resolver)
			if err != nil {
				return nil, err
			}

			val[i] = resolved
		}

		return val, nil
	default:
		return v, nil
	}
}

// ResolveDynamicRefsInTemplate walks all resource properties in tmpl and replaces any
// {{resolve:ssm:...}} or {{resolve:secretsmanager:...}} references with their resolved values.
// Returns a descriptive error (wrapping ErrDynamicRefFailed) if any reference cannot be resolved.
// If resolver is nil the function is a no-op.
func ResolveDynamicRefsInTemplate(tmpl *Template, resolver DynamicRefResolver) error {
	if resolver == nil {
		return nil
	}

	for logicalID, res := range tmpl.Resources {
		if len(res.Properties) == 0 {
			continue
		}

		resolved, err := resolveDynamicRefsInValue(res.Properties, resolver)
		if err != nil {
			return fmt.Errorf("resource %s: %w", logicalID, err)
		}

		if props, ok := resolved.(map[string]any); ok {
			res.Properties = props
			tmpl.Resources[logicalID] = res
		}
	}

	return nil
}

// resolveJSONKey attempts to parse secretValue as a JSON object and returns the value at key.
// If parsing fails or the key is absent, an error is returned.
func resolveJSONKey(secretValue, key string) (string, error) {
	var obj map[string]any
	if err := json.Unmarshal([]byte(secretValue), &obj); err != nil {
		return "", fmt.Errorf("%w: secret is not valid JSON: %w", ErrDynamicRefFailed, err)
	}

	v, ok := obj[key]
	if !ok {
		return "", fmt.Errorf("%w: key %q not found in secret JSON", ErrDynamicRefFailed, key)
	}

	return fmt.Sprintf("%v", v), nil
}
