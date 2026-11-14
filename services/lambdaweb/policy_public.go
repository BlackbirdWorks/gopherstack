package lambdaweb

import (
	"encoding/json/v2"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// trustedPolicyKeys are the condition keys that make a wildcard-principal policy non-public
// (lambda web-functions-permissions.html, "Use a trusted condition key").
var trustedPolicyKeys = map[string]bool{ //nolint:gochecknoglobals // immutable lookup
	"aws:principalaccount": true, "aws:principalarn": true, "aws:principalorgid": true,
	"aws:principalorgpaths": true, "aws:sourceaccount": true, "aws:sourcearn": true,
	"aws:sourceorgid": true, "aws:sourceorgpaths": true, "aws:sourcevpc": true, "aws:sourcevpcarn": true,
	"aws:sourcevpce": true, "aws:vpceaccount": true, "aws:vpceorgid": true, "aws:vpceorgpaths": true,
	"aws:ec2instancesourcevpc": true, "aws:userid": true, "aws:principalservicename": true,
	"aws:principalservicenameslist": true, "aws:principalisawsservice": true, "lambda:sourcefunctionarn": true,
}

var arnPolicyKeys = map[string]bool{ //nolint:gochecknoglobals // immutable lookup
	"aws:principalarn": true, "aws:sourcearn": true, "aws:sourcevpcarn": true, "lambda:sourcefunctionarn": true,
}

const arnLeadingFields = 5

// validateNotPublic rejects an Allow statement that grants access to an unspecified set of principals.
func validateNotPublic(policy string) error {
	var doc struct {
		Statement any `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(policy), &doc, jsonx.V1()); err != nil {
		return validationError("The resource policy must be a valid JSON policy document with a Statement.")
	}

	var stmts []any

	switch s := doc.Statement.(type) {
	case []any:
		stmts = s
	case map[string]any:
		stmts = []any{s}
	}

	for _, raw := range stmts {
		st, ok := raw.(map[string]any)
		if ok && statementIsPublic(st) {
			return validationError("Public resource-based policies are not allowed on a web function.")
		}
	}

	return nil
}

func statementIsPublic(st map[string]any) bool {
	if effect, _ := st["Effect"].(string); effect != "Allow" {
		return false
	}

	if _, notPrincipal := st["NotPrincipal"]; notPrincipal {
		return !restrictedByCondition(st["Condition"])
	}

	return principalIsOpen(st["Principal"]) && !restrictedByCondition(st["Condition"])
}

func principalIsOpen(p any) bool {
	switch v := p.(type) {
	case string:
		return openValue(v)
	case map[string]any:
		for kind, vals := range v {
			if kind == "Service" || kind == "CanonicalUser" {
				continue
			}

			if kind == "Federated" {
				return true
			}

			if anyOpen(vals) {
				return true
			}
		}
	}

	return false
}

func anyOpen(v any) bool {
	switch x := v.(type) {
	case string:
		return openValue(x)
	case []any:
		return slices.ContainsFunc(x, anyOpen)
	}

	return false
}

func openValue(s string) bool { return strings.ContainsAny(s, "*?") || strings.Contains(s, "${") }

func restrictedByCondition(cond any) bool {
	ops, ok := cond.(map[string]any)
	if !ok {
		return false
	}

	for op, keys := range ops {
		kv, isMap := keys.(map[string]any)
		if !isMap || !positiveOperator(op) {
			continue
		}

		for key, vals := range kv {
			if trustedPolicyKeys[strings.ToLower(key)] && fixedValues(strings.ToLower(key), vals) {
				return true
			}
		}
	}

	return false
}

func positiveOperator(op string) bool {
	if strings.HasPrefix(op, "ForAllValues:") || strings.HasSuffix(op, "IfExists") {
		return false
	}

	switch strings.TrimPrefix(op, "ForAnyValue:") {
	case "StringEquals", "StringEqualsIgnoreCase", "StringLike", "ArnEquals", "ArnLike", "Bool":
		return true
	}

	return false
}

func fixedValues(key string, vals any) bool {
	list, ok := vals.([]any)
	if !ok {
		list = []any{vals}
	}

	if len(list) == 0 {
		return false
	}

	for _, v := range list {
		if !fixedValue(key, v) {
			return false
		}
	}

	return true
}

func fixedValue(key string, v any) bool {
	if key == "aws:principalisawsservice" {
		b, isBool := v.(bool)
		s, isStr := v.(string)

		return (isBool && b) || (isStr && strings.EqualFold(s, "true"))
	}

	s, ok := v.(string)
	if !ok || s == "" {
		return false
	}

	if arnPolicyKeys[key] {
		fields := strings.SplitN(s, ":", arnLeadingFields+1)
		for i := 0; i < len(fields) && i < arnLeadingFields; i++ {
			if openValue(fields[i]) {
				return false
			}
		}

		return len(fields) > arnLeadingFields || !openValue(s)
	}

	return !openValue(s)
}
