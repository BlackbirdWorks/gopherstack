package gcpartifactregistry

import (
	"errors"
	"fmt"
	"time"

	"cel.dev/cel-go/cel"
	"google.golang.org/genproto/googleapis/type/expr"
)

const celVersion = "version"

var errNoCEL = errors.New("CEL environment unavailable")

// ruleResource is the artifact a download rule condition is evaluated for.
type ruleResource struct {
	annotations map[string]string
	repository  string
	pkg         string
	version     string
	tags        []string
}

// newRuleEnv declares the variables rule conditions may use: pkg.id, pkg.version.{id,tag,annotations} as
// documented on docs.cloud.google.com/artifact-registry/docs/restrict-artifact-downloads, plus request.time and
// resource.{repository,package,version,tags}, which are the emulator's own.
func newRuleEnv() (*cel.Env, error) {
	env, err := cel.NewEnv(
		cel.Variable("pkg", cel.MapType(cel.StringType, cel.DynType)),
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

	annotations := make(map[string]any, len(res.annotations))
	for k, v := range res.annotations {
		annotations[k] = v
	}

	tag := ""
	if len(res.tags) > 0 {
		tag = res.tags[0]
	}

	out, _, err := prg.Eval(map[string]any{
		"pkg": map[string]any{
			"id":       res.pkg,
			celVersion: map[string]any{"id": res.version, "tag": tag, "annotations": annotations},
		},
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
