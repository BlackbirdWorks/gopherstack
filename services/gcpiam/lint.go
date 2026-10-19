package gcpiam

import (
	"strings"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
)

const (
	maxConditionOperators = 12
	maxExpressionLen      = 2048
	unitComplexity        = "lintValidationUnits/ConditionComplexityCheck"
	unitSyntax            = "lintValidationUnits/ConditionSyntaxCheck"
)

// LintPolicy checks a condition expression: non-empty, balanced brackets/quotes, size and operator limits.
func (b *InMemoryBackend) LintPolicy(req *adminpb.LintPolicyRequest) (*adminpb.LintPolicyResponse, error) {
	if req.GetFullResourceName() == "" {
		return nil, errInvalid("full_resource_name must be specified.")
	}

	cond := req.GetCondition()
	if cond == nil {
		return nil, errInvalid("A condition must be specified as the lint object.")
	}

	var results []*adminpb.LintResult

	add := func(unit string, sev adminpb.LintResult_Severity, offset int, msg string) {
		results = append(results, &adminpb.LintResult{
			Level:              adminpb.LintResult_CONDITION,
			ValidationUnitName: unit,
			Severity:           sev,
			FieldName:          "condition.expression",
			LocationOffset:     int32(offset), //nolint:gosec // bounded by maxExpressionLen check below
			DebugMessage:       msg,
		})
	}

	expr := cond.GetExpression()

	switch {
	case strings.TrimSpace(expr) == "":
		add(unitSyntax, adminpb.LintResult_ERROR, 0, "The condition expression must not be empty.")
	case len(expr) > maxExpressionLen:
		add(unitComplexity, adminpb.LintResult_ERROR, maxExpressionLen,
			"The condition expression exceeds the maximum length of 2048 characters.")
	default:
		if off, msg := syntaxProblem(expr); msg != "" {
			add(unitSyntax, adminpb.LintResult_ERROR, off, msg)
		}

		if n := strings.Count(expr, "&&") + strings.Count(expr, "||"); n > maxConditionOperators {
			add(unitComplexity, adminpb.LintResult_ERROR, 0, "The condition uses more than 12 logical operators.")
		}
	}

	return &adminpb.LintPolicyResponse{LintResults: results}, nil
}

// syntaxProblem reports the first unbalanced bracket or unterminated string literal.
func syntaxProblem(expr string) (int, string) {
	var stack []rune

	var quote rune

	for i, r := range expr {
		if quote != 0 {
			if r == quote {
				quote = 0
			}

			continue
		}

		switch r {
		case '\'', '"':
			quote = r
		case '(', '[', '{':
			stack = append(stack, r)
		case ')', ']', '}':
			if len(stack) == 0 || !matches(stack[len(stack)-1], r) {
				return i, "Unbalanced '" + string(r) + "' in the condition expression."
			}

			stack = stack[:len(stack)-1]
		}
	}

	if quote != 0 {
		return len(expr), "Unterminated string literal in the condition expression."
	}

	if len(stack) > 0 {
		return len(expr), "Unclosed '" + string(stack[len(stack)-1]) + "' in the condition expression."
	}

	return 0, ""
}

func matches(open, closing rune) bool {
	return (open == '(' && closing == ')') || (open == '[' && closing == ']') || (open == '{' && closing == '}')
}
