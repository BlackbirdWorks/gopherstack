package cloudformation

import (
	"context"
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

// errJSONKeyNotFound is returned when a JSON key is absent from a secret value.
var errJSONKeyNotFound = errors.New("json key not found in secret")

const (
	// smMaxParts is the maximum number of colon-separated parts in a secretsmanager reference.
	// Format: secret-id:SecretString:json-key:version-stage:version-id (5 parts).
	smMaxParts = 5
	// smJSONKeyParts is the minimum number of parts required for the JSON key to be present.
	// Parts: [0]=secret-id, [1]=SecretString, [2]=json-key, ...
	smJSONKeyParts = 3
	// smJSONKeyIndex is the index of the optional JSON key within secretsmanager parts.
	smJSONKeyIndex = 2
	// maxDynamicRefIterations caps the resolution loop to prevent runaway cycles.
	maxDynamicRefIterations = 100
)

// DynamicRefResolver is the interface for resolving CloudFormation dynamic references.
type DynamicRefResolver interface {
	// ResolveSSMParameter retrieves an SSM plain-text or StringList parameter value.
	ResolveSSMParameter(ctx context.Context, name string) (string, error)
	// ResolveSSMSecureParameter retrieves an SSM SecureString parameter with decryption.
	ResolveSSMSecureParameter(ctx context.Context, name string) (string, error)
	// ResolveSecret retrieves a Secrets Manager secret value.
	// jsonKey may be empty; if non-empty the secret is parsed as JSON and the key is extracted.
	ResolveSecret(ctx context.Context, secretID, jsonKey string) (string, error)
}

// resolveDynamicRef resolves all `{{resolve:...}}` occurrences within the string
// using the provided resolver. If the string contains no dynamic references it is
// returned unchanged.
func resolveDynamicRef(ctx context.Context, s string, resolver DynamicRefResolver) (string, error) {
	if !strings.Contains(s, "{{resolve:") {
		return s, nil
	}

	for i := range maxDynamicRefIterations {
		// FindStringSubmatchIndex returns the byte offsets of the full match and
		// capture groups so we can replace the exact occurrence.
		loc := dynamicRefPattern.FindStringSubmatchIndex(s)
		if loc == nil {
			return s, nil
		}

		_ = i // iteration counter used only for the loop bound

		fullStart, fullEnd := loc[0], loc[1]
		service := s[loc[2]:loc[3]]
		rest := s[loc[4]:loc[5]]

		var resolved string

		var err error

		switch service {
		case "ssm":
			// Format: {{resolve:ssm:parameter-name}} or {{resolve:ssm:parameter-name:version}}
			name, _, _ := strings.Cut(rest, ":")
			resolved, err = resolver.ResolveSSMParameter(ctx, name)
		case "ssm-secure":
			// Format: {{resolve:ssm-secure:parameter-name}} or {{resolve:ssm-secure:parameter-name:version}}
			name, _, _ := strings.Cut(rest, ":")
			resolved, err = resolver.ResolveSSMSecureParameter(ctx, name)
		case "secretsmanager":
			// Format: {{resolve:secretsmanager:secret-id}}
			//      or {{resolve:secretsmanager:secret-id:SecretString:json-key:version-stage:version-id}}
			parts := strings.SplitN(rest, ":", smMaxParts)
			secretID := parts[0]

			jsonKey := ""
			if len(parts) >= smJSONKeyParts && parts[1] == "SecretString" {
				jsonKey = parts[smJSONKeyIndex]
			}

			resolved, err = resolver.ResolveSecret(ctx, secretID, jsonKey)
		default:
			ref := s[fullStart:fullEnd]

			return "", fmt.Errorf("%w: unsupported service %q in reference %q", ErrDynamicRefFailed, service, ref)
		}

		if err != nil {
			return "", fmt.Errorf("%w: %s: %w", ErrDynamicRefFailed, s[fullStart:fullEnd], err)
		}

		// Replace only the exact matched bytes to avoid touching any other
		// occurrence of the same pattern elsewhere in the string.
		s = s[:fullStart] + resolved + s[fullEnd:]
	}

	// The loop body resolves one reference per iteration. After exhausting the
	// iteration budget, only fail if references actually remain — a value with
	// exactly maxDynamicRefIterations references is fully resolved and must not
	// be reported as an error (off-by-one guard).
	if dynamicRefPattern.MatchString(s) {
		return "", fmt.Errorf(
			"%w: too many dynamic references in a single value (limit %d)",
			ErrDynamicRefFailed,
			maxDynamicRefIterations,
		)
	}

	return s, nil
}

// resolveDynamicRefsInValue recursively walks a value tree and replaces any
// dynamic references in string leaves using the provided resolver.
// The value is modified in place for maps and slices; a new value is returned for strings.
func resolveDynamicRefsInValue(ctx context.Context, v any, resolver DynamicRefResolver) (any, error) {
	switch val := v.(type) {
	case string:
		return resolveDynamicRef(ctx, val, resolver)
	case map[string]any:
		for k, child := range val {
			resolved, err := resolveDynamicRefsInValue(ctx, child, resolver)
			if err != nil {
				return nil, err
			}

			val[k] = resolved
		}

		return val, nil
	case []any:
		for i, item := range val {
			resolved, err := resolveDynamicRefsInValue(ctx, item, resolver)
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
func ResolveDynamicRefsInTemplate(ctx context.Context, tmpl *Template, resolver DynamicRefResolver) error {
	for logicalID, res := range tmpl.Resources {
		if len(res.Properties) == 0 {
			continue
		}

		if resolver == nil {
			return nil
		}

		resolved, err := resolveDynamicRefsInValue(ctx, res.Properties, resolver)
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
		return "", fmt.Errorf("secret is not valid JSON: %w", err)
	}

	v, ok := obj[key]
	if !ok {
		return "", fmt.Errorf("%w: key %q not found in secret JSON", errJSONKeyNotFound, key)
	}

	return fmt.Sprintf("%v", v), nil
}
