package gcpiam

import (
	"strings"
	"sync"

	"cel.dev/cel-go/cel"
	"cloud.google.com/go/iam/admin/apiv1/adminpb"
)

const (
	maxConditionOperators = 12
	maxExpressionLen      = 2048
	unitComplexity        = "lintValidationUnits/ConditionComplexityCheck"
	unitSyntax            = "lintValidationUnits/ConditionSyntaxCheck"
)

// LintPolicy checks a condition expression: non-empty, CEL syntax, size and operator limits.
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

//nolint:gochecknoglobals // lazy: eager CEL setup costs memory
var celParseEnv = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv()
})

// syntaxProblem parses expr as CEL and reports the first syntax error with its byte offset.
func syntaxProblem(expr string) (int, string) {
	env, err := celParseEnv()
	if err != nil {
		return 0, ""
	}

	_, iss := env.Parse(expr)
	if iss == nil || iss.Err() == nil {
		return 0, ""
	}

	first := iss.Errors()[0]

	return offsetOf(expr, first.Location.Line(), first.Location.Column()), first.Message
}

// offsetOf converts a 1-based line and 0-based column into a byte offset.
func offsetOf(expr string, line, col int) int {
	off := 0

	for l := 1; l < line; l++ {
		nl := strings.IndexByte(expr[off:], '\n')
		if nl < 0 {
			return len(expr)
		}

		off += nl + 1
	}

	return min(off+col, len(expr))
}
