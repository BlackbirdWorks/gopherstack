package eventbridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// inputPathsMapKeyRe validates InputPathsMap variable names per AWS spec.
var inputPathsMapKeyRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// applyInputPath extracts a value from the event envelope using a simple JSONPath expression.
// Returns the JSON-serialized extracted value, or an empty string if extraction fails.
func applyInputPath(path string, envelope map[string]any) string {
	val := jsonPathExtract(path, envelope)
	if val == nil {
		return ""
	}

	b, _ := json.Marshal(val)

	return string(b)
}

// validateInputTransformer checks that all InputPathsMap keys satisfy the
// AWS constraint ([A-Za-z0-9_]+). Returns an error if any key is invalid.
func validateInputTransformer(t *InputTransformer) error {
	for key := range t.InputPathsMap {
		if !inputPathsMapKeyRe.MatchString(key) {
			return fmt.Errorf(
				"%w: InputPathsMap key %q must match [A-Za-z0-9_]+",
				ErrInvalidParameter,
				key,
			)
		}
	}

	return nil
}

// applyInputTransformer applies InputPathsMap variable extraction and InputTemplate substitution.
// Variables defined in InputPathsMap are extracted from the envelope and substituted into
// InputTemplate using <variableName> syntax.
//
// Substitution is context-aware so the output is valid JSON: a placeholder that
// sits inside a JSON string literal (e.g. "<var>") receives the escaped scalar
// content of the value, while a placeholder in value position (e.g. {"k":<var>})
// receives the full JSON encoding of the value (adding quotes for strings). This
// matches AWS EventBridge, which produces valid JSON for both forms rather than
// splicing raw, unquoted strings into value positions.
func applyInputTransformer(t *InputTransformer, envelope map[string]any) string {
	vars := make(map[string]any, len(t.InputPathsMap))
	for varName, path := range t.InputPathsMap {
		vars[varName] = jsonPathExtract(path, envelope)
	}

	return substituteInputTemplate(t.InputTemplate, vars)
}

// substituteInputTemplate scans template byte-by-byte, tracking whether the
// cursor is inside a JSON string literal, and replaces every <name> placeholder
// whose name is present in vars using the appropriate rendering for its context.
func substituteInputTemplate(template string, vars map[string]any) string {
	var out strings.Builder
	out.Grow(len(template))

	inString := false
	escaped := false

	for i := 0; i < len(template); {
		c := template[i]

		if c == '<' && !escaped {
			if name, adv, ok := matchTemplatePlaceholder(template, i, vars); ok {
				if inString {
					out.WriteString(inputTransformerStringValue(vars[name]))
				} else {
					out.WriteString(inputTransformerValue(vars[name]))
				}
				i += adv

				continue
			}
		}

		out.WriteByte(c)
		switch {
		case escaped:
			escaped = false
		case inString && c == '\\':
			escaped = true
		case c == '"':
			inString = !inString
		}
		i++
	}

	return out.String()
}

// matchTemplatePlaceholder reports whether template[start:] begins with a
// <name> placeholder whose name is a key in vars. It returns the name and the
// number of bytes the placeholder spans (including the angle brackets).
func matchTemplatePlaceholder(template string, start int, vars map[string]any) (string, int, bool) {
	j := start + 1
	for j < len(template) {
		ch := template[j]
		if ch == '>' {
			name := template[start+1 : j]
			if name == "" {
				return "", 0, false
			}
			if _, ok := vars[name]; !ok {
				return "", 0, false
			}

			return name, j - start + 1, true
		}
		if !isPlaceholderNameByte(ch) {
			return "", 0, false
		}
		j++
	}

	return "", 0, false
}

func isPlaceholderNameByte(ch byte) bool {
	return ch >= 'A' && ch <= 'Z' ||
		ch >= 'a' && ch <= 'z' ||
		ch >= '0' && ch <= '9' ||
		ch == '_'
}

// inputTransformerStringValue renders v for a placeholder that sits inside a
// JSON string literal: the escaped scalar content, with no surrounding quotes.
// A missing value (nil) renders as the empty string.
func inputTransformerStringValue(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return jsonEscapeInner(val)
	default:
		encoded := marshalNoHTMLEscape(val)

		return jsonEscapeInner(string(encoded))
	}
}

// inputTransformerValue renders v for a placeholder in JSON value position: the
// full JSON encoding (strings gain surrounding quotes, objects/arrays/numbers
// are spliced verbatim). A missing value (nil) renders as an empty JSON string.
func inputTransformerValue(v any) string {
	if v == nil {
		return `""`
	}

	return string(marshalNoHTMLEscape(v))
}

// jsonEscapeInner returns the JSON-escaped form of s without the surrounding
// quotation marks, suitable for splicing into an existing string literal.
func jsonEscapeInner(s string) string {
	encoded := string(marshalNoHTMLEscape(s))
	encoded = strings.TrimPrefix(encoded, `"`)
	encoded = strings.TrimSuffix(encoded, `"`)

	return encoded
}

// marshalNoHTMLEscape JSON-encodes v without escaping <, >, and & so the output
// mirrors AWS, which does not HTML-escape input-transformer substitutions.
func marshalNoHTMLEscape(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil
	}

	return bytes.TrimRight(buf.Bytes(), "\n")
}

// jsonPathExtract resolves a JSONPath expression against the given event envelope.
// Supports dot-notation ($.source, $.detail.key) and array indexing ($.detail.items[0]).
// Returns nil if the path cannot be resolved.
func jsonPathExtract(path string, data map[string]any) any {
	if path == "$" || path == "" {
		return data
	}

	if !strings.HasPrefix(path, "$.") {
		return nil
	}

	parts := splitJSONPathParts(path[2:])
	var current any = data

	for _, part := range parts {
		// Check for array index suffix, e.g. "items[0]".
		key, idx, hasIndex := parseArrayIndex(part)

		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}

		val, ok := m[key]
		if !ok {
			return nil
		}

		if !hasIndex {
			current = val

			continue
		}

		arr, ok := val.([]any)
		if !ok || idx < 0 || idx >= len(arr) {
			return nil
		}

		current = arr[idx]
	}

	return current
}

// splitJSONPathParts splits a dot-separated path into segments while preserving
// array index brackets as part of the segment (e.g. "items[0].name" → ["items[0]", "name"]).
func splitJSONPathParts(path string) []string {
	return strings.Split(path, ".")
}

const decimalBase = 10

// parseArrayIndex parses a path segment like "items[0]" into key="items", idx=0, hasIndex=true.
// Returns hasIndex=false when no bracket expression is found.
func parseArrayIndex(segment string) (string, int, bool) {
	open := strings.LastIndex(segment, "[")
	if open < 0 {
		return segment, 0, false
	}

	closeIdx := strings.Index(segment[open:], "]")
	if closeIdx < 0 {
		return segment, 0, false
	}

	indexStr := segment[open+1 : open+closeIdx]
	n := 0

	for _, ch := range indexStr {
		if ch < '0' || ch > '9' {
			return segment, 0, false
		}

		n = n*decimalBase + int(ch-'0')
	}

	return segment[:open], n, true
}
