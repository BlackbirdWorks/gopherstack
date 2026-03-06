package iam

import "strings"

// SubstituteVariables replaces IAM policy variables in a policy document string
// with values from the provided ConditionContext.
//
// Supported variables:
//   - ${aws:username}      → IAM user name
//   - ${aws:userid}        → IAM user ID
//   - ${aws:sourceip}      → caller source IP
//
// Unknown variables are left unchanged to avoid masking policy mistakes.
func SubstituteVariables(doc string, ctx ConditionContext) string {
	if !strings.Contains(doc, "${") {
		return doc
	}

	replacements := buildVariableReplacements(ctx)

	var result strings.Builder

	result.Grow(len(doc))

	i := 0
	for i < len(doc) {
		start := strings.Index(doc[i:], "${")
		if start < 0 {
			result.WriteString(doc[i:])

			break
		}

		result.WriteString(doc[i : i+start])
		i += start

		end := strings.Index(doc[i:], "}")
		if end < 0 {
			// Unclosed variable reference — write as-is.
			result.WriteString(doc[i:])

			break
		}

		varName := doc[i : i+end+1] // e.g. "${aws:username}"
		inner := strings.ToLower(doc[i+2 : i+end])

		if replacement, ok := replacements[inner]; ok {
			result.WriteString(replacement)
		} else {
			result.WriteString(varName)
		}

		i += end + 1
	}

	return result.String()
}

// buildVariableReplacements returns a map of lower-case variable name → value.
func buildVariableReplacements(ctx ConditionContext) map[string]string {
	m := map[string]string{
		"aws:username": ctx.Username,
		"aws:userid":   ctx.UserID,
		"aws:sourceip": ctx.SourceIP,
	}

	// Merge any extra context values as potential policy variables.
	for k, v := range ctx.Extra {
		m[strings.ToLower(k)] = v
	}

	return m
}
