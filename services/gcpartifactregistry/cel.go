package gcpartifactregistry

import (
	"errors"
	"fmt"
	"time"

	"cel.dev/cel-go/cel"
	"google.golang.org/genproto/googleapis/type/expr"
)

var errNoCEL = errors.New("CEL environment unavailable")

// ruleResource is what a rule condition sees as `resource`.
type ruleResource struct {
	repository string
	pkg        string
	version    string
	tags       []string
}

// newRuleEnv declares the variables rule conditions may use: request.time and resource.{repository,package,
// version,tags}. The variable set is the emulator's choice; Google does not publish it (unverified).
func newRuleEnv() (*cel.Env, error) {
	env, err := cel.NewEnv(
		cel.Variable("request", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("resource", cel.MapType(cel.StringType, cel.DynType)),
	)
	if err != nil {
		return nil, fmt.Errorf("build CEL environment: %w", err)
	}

	return env, nil
}

// compileCondition checks that the expression parses, type-checks and yields a bool.
func (b *InMemoryBackend) compileCondition(src string) (cel.Program, error) {
	if b.celEnv == nil {
		return nil, errInternal(errNoCEL)
	}

	ast, iss := b.celEnv.Compile(src)
	if iss.Err() != nil {
		return nil, errInvalid("rule.condition is not a valid CEL expression: %v", iss.Err())
	}

	if t := ast.OutputType(); t != cel.BoolType {
		return nil, errInvalid("rule.condition must evaluate to a bool, not %s.", t)
	}

	prg, err := b.celEnv.Program(ast)
	if err != nil {
		return nil, errInvalid("rule.condition cannot be evaluated: %v", err)
	}

	return prg, nil
}

func (b *InMemoryBackend) validateCondition(c *expr.Expr) error {
	if c == nil || c.GetExpression() == "" {
		return nil
	}

	_, err := b.compileCondition(c.GetExpression())

	return err
}

// conditionHolds evaluates a rule condition; an absent condition always holds and an unevaluable one never does.
func (b *InMemoryBackend) conditionHolds(c *expr.Expr, res ruleResource, now time.Time) bool {
	if c == nil || c.GetExpression() == "" {
		return true
	}

	prg, err := b.compileCondition(c.GetExpression())
	if err != nil {
		return false
	}

	tags := make([]any, 0, len(res.tags))
	for _, t := range res.tags {
		tags = append(tags, t)
	}

	out, _, err := prg.Eval(map[string]any{
		"request": map[string]any{"time": now.UTC()},
		"resource": map[string]any{
			"repository": res.repository, "package": res.pkg, "version": res.version, "tags": tags,
		},
	})
	if err != nil {
		return false
	}

	v, ok := out.Value().(bool)

	return ok && v
}
