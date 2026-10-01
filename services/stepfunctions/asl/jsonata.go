package asl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/recolabs/gnata"
)

const (
	queryLangJSONPath = "JSONPath"
	queryLangJSONata  = "JSONata"

	errCodeStatesQueryEvaluation = "States.QueryEvaluationError"
)

var (
	errJSONataEval       = errors.New("jsonata evaluation failed")
	errJSONataEvalBanned = errors.New("$eval is not available in Step Functions; use $parse")

	evalCallRe = regexp.MustCompile(`\$eval\b`)
)

// jsonataEvalTimeout is the AWS per-expression limit ("Handling expression errors").
const jsonataEvalTimeout = time.Second

// jsonataExpr is a compiled, goroutine-safe expression.
type jsonataExpr struct {
	expr *gnata.Expression
}

func compileJSONata(src string) (*jsonataExpr, error) {
	if evalCallRe.MatchString(src) {
		return nil, errJSONataEvalBanned
	}

	expr, err := gnata.Compile(src, gnata.WithTimeout(jsonataEvalTimeout))
	if err != nil {
		return nil, err
	}

	return &jsonataExpr{expr: expr}, nil
}

// wrappedJSONata returns the expression inside "{% ... %}" (no surrounding
// whitespace allowed, per the AWS "Writing JSONata expressions" rules).
func wrappedJSONata(s string) (string, bool) {
	if len(s) >= len("{%%}") && strings.HasPrefix(s, "{%") && strings.HasSuffix(s, "%}") {
		return s[2 : len(s)-2], true
	}

	return "", false
}

// malformedJSONata reports a string that opens or closes a "{% %}" wrapper
// without doing both exactly (leading/trailing spaces included).
func malformedJSONata(s string) bool {
	t := strings.TrimSpace(s)
	if !strings.HasPrefix(t, "{%") && !strings.HasSuffix(t, "%}") {
		return false
	}

	_, ok := wrappedJSONata(s)

	return !ok
}

func queryEvalError(err error) error {
	return &FailError{ErrCode: errCodeStatesQueryEvaluation, Cause: err.Error()}
}

// eval runs the expression with the given variable bindings ($states and
// workflow variables). An undefined result is an error: JSON cannot hold it.
func (x *jsonataExpr) eval(bind map[string]any, input any) (any, error) {
	out, err := x.expr.EvalWithCustomEnvironmentAndVars(context.Background(), input, sfnJSONataEnv(), bind)
	if err != nil {
		return nil, queryEvalError(err)
	}

	if out == nil {
		return nil, queryEvalError(fmt.Errorf("%w: expression returned no value", errJSONataEval))
	}

	return normalizeJSONata(gnata.NormalizeValue(out))
}

func normalizeJSONata(v any) (any, error) {
	switch v.(type) {
	case nil, bool, string, float64:
		return v, nil
	}

	b, err := json.Marshal(v)
	if err != nil {
		return nil, queryEvalError(fmt.Errorf("%w: result is not valid JSON: %w", errJSONataEval, err))
	}

	var out any
	if err = json.Unmarshal(b, &out); err != nil {
		return nil, queryEvalError(err)
	}

	return out, nil
}

// jxScope is the evaluation scope for one JSONata state entry: variables as
// of state entry plus the reserved $states object.
type jxScope struct {
	st     *jxState
	bind   map[string]any
	states map[string]any
}

func (e *Executor) newJXScope(st *jxState, stateName string, input any) *jxScope {
	vars := e.visibleVars()
	bind := make(map[string]any, len(vars)+1)

	maps.Copy(bind, vars)

	ctx := e.buildContextObject()
	ctx["State"] = map[string]any{"Name": stateName}
	states := map[string]any{"input": input, "context": ctx}
	bind["states"] = states

	return &jxScope{st: st, bind: bind, states: states}
}

func (s *jxScope) input() any { return s.states["input"] }

func (s *jxScope) evalExpr(src string) (any, error) {
	x := s.st.exprs[src]
	if x == nil {
		var err error
		if x, err = compileJSONata(src); err != nil {
			return nil, queryEvalError(err)
		}
	}

	return x.eval(s.bind, s.input())
}

// evalValue evaluates every "{% %}" string in a JSON value (object, array or
// scalar); other values pass through unchanged.
func (s *jxScope) evalValue(v any) (any, error) {
	switch t := v.(type) {
	case string:
		if src, ok := wrappedJSONata(t); ok {
			return s.evalExpr(src)
		}

		return t, nil
	case map[string]any:
		out := make(map[string]any, len(t))

		for k, val := range t {
			r, err := s.evalValue(val)
			if err != nil {
				return nil, err
			}

			out[k] = r
		}

		return out, nil
	case []any:
		out := make([]any, len(t))

		for i, val := range t {
			r, err := s.evalValue(val)
			if err != nil {
				return nil, err
			}

			out[i] = r
		}

		return out, nil
	default:
		return v, nil
	}
}

func (s *jxScope) evalAssign(assign map[string]any) (map[string]any, error) {
	if len(assign) == 0 {
		return nil, nil //nolint:nilnil // no assignments
	}

	out := make(map[string]any, len(assign))

	for name, v := range assign {
		r, err := s.evalValue(v)
		if err != nil {
			return nil, err
		}

		out[name] = r
	}

	return out, nil
}

func (s *jxScope) evalNumber(src string) (float64, error) {
	inner, ok := wrappedJSONata(src)
	if !ok {
		return 0, queryEvalError(fmt.Errorf("%w: %q is not a JSONata expression", errJSONataEval, src))
	}

	v, err := s.evalExpr(inner)
	if err != nil {
		return 0, err
	}

	f, ok := v.(float64)
	if !ok {
		return 0, queryEvalError(fmt.Errorf("%w: expected a number, got %T", errJSONataEval, v))
	}

	return f, nil
}

// withMapItem returns a copy of the scope whose context carries Map.Item.
func (s *jxScope) withMapItem(idx int, item any) *jxScope {
	ctx := map[string]any{}
	if cur, ok := s.states["context"].(map[string]any); ok {
		maps.Copy(ctx, cur)
	}

	ctx["Map"] = mapItemContext(idx, item)

	states := maps.Clone(s.states)
	states["context"] = ctx

	bind := maps.Clone(s.bind)
	bind["states"] = states

	return &jxScope{st: s.st, bind: bind, states: states}
}
