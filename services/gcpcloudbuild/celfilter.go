package gcpcloudbuild

import (
	"errors"
	"fmt"
	"regexp"

	"cel.dev/cel-go/cel"
)

const (
	maxFilterLen    = 4096
	maxFilterCost   = 100_000
	maxFilterDepth  = 64
	maxFilterVarLen = 256
)

var (
	errFilterSyntax  = errors.New("unsupported filter")
	filterVarNameRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	errFilterNotBool = errors.New("filter must evaluate to a bool")
)

// celExpr is a parsed trigger filter; it is type-checked against the substitution variables when evaluated.
type celExpr struct {
	ast *cel.Ast
}

func newFilterEnv(opts ...cel.EnvOption) (*cel.Env, error) {
	base := []cel.EnvOption{
		cel.ParserExpressionSizeLimit(maxFilterLen),
		cel.ParserRecursionLimit(maxFilterDepth),
	}

	env, err := cel.NewEnv(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("build CEL environment: %w", err)
	}

	return env, nil
}

// compileFilter parses a CEL trigger filter over substitution variables, which are strings.
func compileFilter(src string) (celExpr, error) {
	if len(src) > maxFilterLen {
		return celExpr{}, fmt.Errorf("filter is longer than %d bytes: %w", maxFilterLen, errFilterSyntax)
	}

	env, err := newFilterEnv()
	if err != nil {
		return celExpr{}, err
	}

	ast, iss := env.Parse(src)
	if iss.Err() != nil {
		return celExpr{}, fmt.Errorf("%w: %w", iss.Err(), errFilterSyntax)
	}

	return celExpr{ast: ast}, nil
}

// evalFilter reports whether the filter accepts vars.
func evalFilter(e celExpr, vars map[string]string) (bool, error) {
	opts := make([]cel.EnvOption, 0, len(vars))

	for name := range vars {
		if len(name) <= maxFilterVarLen && filterVarNameRe.MatchString(name) {
			opts = append(opts, cel.Variable(name, cel.StringType))
		}
	}

	env, err := newFilterEnv(opts...)
	if err != nil {
		return false, err
	}

	checked, iss := env.Check(e.ast)
	if iss.Err() != nil {
		return false, fmt.Errorf("%w: %w", iss.Err(), errFilterSyntax)
	}

	prg, err := env.Program(checked, cel.CostLimit(maxFilterCost))
	if err != nil {
		return false, fmt.Errorf("%w: %w", err, errFilterSyntax)
	}

	activation := make(map[string]any, len(vars))
	for k, v := range vars {
		activation[k] = v
	}

	out, _, err := prg.Eval(activation)
	if err != nil {
		return false, fmt.Errorf("evaluate filter: %w", err)
	}

	ok, isBool := out.Value().(bool)
	if !isBool {
		return false, errFilterNotBool
	}

	return ok, nil
}
