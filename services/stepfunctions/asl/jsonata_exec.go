package asl

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// runJSONataState runs one JSONata-mode state (Arguments, body, then the
// parallel Assign and Output evaluation).
func (e *Executor) runJSONataState(
	ctx context.Context,
	executionARN, name string,
	state *State,
	input any,
) (string, any, error) {
	scope := e.newJXScope(state.jx, name, input)
	prev := e.jx
	e.jx = scope

	defer func() {
		e.jx = prev
		e.jxNums = nil
	}()

	switch state.Type {
	case stateTypePass, stateTypeSucceed:
		return e.jxFinish(state, scope, state.Next, input, nil)
	case stateTypeFail:
		return e.jxFail(state, scope)
	case stateTypeWait:
		return e.jxWait(ctx, state, scope)
	case stateTypeChoice:
		return e.jxChoice(state, scope)
	case stateTypeTask, stateTypeParallel, StateTypeMap:
		return e.jxCompound(ctx, executionARN, name, state, scope)
	default:
		return "", nil, fmt.Errorf("%w: %q in state %q", ErrUnsupportedStateType, state.Type, name)
	}
}

// jxFinish evaluates Assign and Output against the same state-entry scope
// ("Assign and Output steps occur in parallel"), then applies the assignments.
func (e *Executor) jxFinish(
	state *State,
	scope *jxScope,
	next string,
	defaultOut any,
	extraAssign map[string]any,
) (string, any, error) {
	assign, err := scope.evalAssign(state.assignVals)
	if err != nil {
		return "", nil, err
	}

	out := defaultOut
	if scope.st.hasOut {
		if out, err = scope.evalValue(scope.st.output); err != nil {
			return "", nil, err
		}
	}

	e.setVars(assign)
	e.setVars(extraAssign)

	return next, out, nil
}

func (s *jxScope) evalString(v string) (string, error) {
	if _, ok := wrappedJSONata(v); !ok {
		return v, nil
	}

	r, err := s.evalValue(v)
	if err != nil {
		return "", err
	}

	str, ok := r.(string)
	if !ok {
		return "", queryEvalError(fmt.Errorf("%w: expected a string, got %T", errJSONataEval, r))
	}

	return str, nil
}

func (e *Executor) jxFail(state *State, scope *jxScope) (string, any, error) {
	code, err := scope.evalString(state.Error)
	if err != nil {
		return "", nil, err
	}

	cause, err := scope.evalString(state.Cause)
	if err != nil {
		return "", nil, err
	}

	return "", nil, &FailError{ErrCode: code, Cause: cause}
}

func (e *Executor) jxWait(ctx context.Context, state *State, scope *jxScope) (string, any, error) {
	dur, err := e.jxWaitDuration(state, scope)
	if err != nil {
		return "", nil, err
	}

	if dur > 0 {
		if err = e.waitForDuration(ctx, dur); err != nil {
			return "", nil, err
		}
	}

	return e.jxFinish(state, scope, state.Next, scope.input(), nil)
}

func (e *Executor) jxWaitDuration(state *State, scope *jxScope) (time.Duration, error) {
	if src, ok := state.numExprs[fieldSeconds]; ok {
		secs, err := scope.evalNumber(src)
		if err != nil {
			return 0, err
		}

		if secs < 0 {
			return 0, queryEvalError(fmt.Errorf("%w: Seconds must not be negative", errJSONataEval))
		}

		return time.Duration(secs * float64(time.Second)), nil
	}

	ts, err := scope.evalString(state.Timestamp)
	if err != nil {
		return 0, err
	}

	if ts != "" {
		return resolveTimestampDuration(ts)
	}

	return time.Duration(state.Seconds) * time.Second, nil
}

func (e *Executor) jxChoice(state *State, scope *jxScope) (string, any, error) {
	for i := range state.Choices {
		rule := &state.Choices[i]

		src, _ := wrappedJSONata(rule.Condition)

		v, err := scope.evalExpr(src)
		if err != nil {
			return "", nil, err
		}

		matched, ok := v.(bool)
		if !ok {
			return "", nil, queryEvalError(fmt.Errorf("%w: Condition must be a boolean, got %T", errJSONataEval, v))
		}

		if !matched {
			continue
		}

		if rule.Next == "" {
			return "", nil, ErrChoiceNoNext
		}

		ruleAssign, err := scope.evalAssign(rule.assignVals)
		if err != nil {
			return "", nil, err
		}

		return e.jxFinish(state, scope, rule.Next, scope.input(), ruleAssign)
	}

	if state.Default != "" {
		return e.jxFinish(state, scope, state.Default, scope.input(), nil)
	}

	return "", nil, &FailError{
		ErrCode: ErrChoiceNoMatch.Error(),
		Cause:   "No choice matched and no Default was provided",
	}
}

// jxCompound runs Task, Parallel and Map; Retry and Catch live in the
// executeX bodies, evaluation failures before them are routed to Catch here.
func (e *Executor) jxCompound(
	ctx context.Context,
	executionARN, name string,
	state *State,
	scope *jxScope,
) (string, any, error) {
	next, out, err := e.jxCompoundBody(ctx, executionARN, name, state, scope)
	if err == nil {
		return next, out, nil
	}

	if _, isFail := errors.AsType[*FailError](err); !isFail || e.caught {
		return "", nil, err
	}

	if cnext, cout, matched, cerr := e.checkCatchers(executionARN, name, state, scope.input(), err); matched {
		return cnext, cout, cerr
	}

	return "", nil, err
}

func (e *Executor) jxCompoundBody(
	ctx context.Context,
	executionARN, name string,
	state *State,
	scope *jxScope,
) (string, any, error) {
	args := scope.input()

	if scope.st.hasArgs {
		var err error
		if args, err = scope.evalValue(scope.st.args); err != nil {
			return "", nil, err
		}
	}

	if err := e.jxResolveNumbers(state, scope); err != nil {
		return "", nil, err
	}

	var (
		next   string
		result any
		err    error
	)

	switch state.Type {
	case stateTypeTask:
		next, result, err = e.executeTask(ctx, executionARN, name, state, scope.input(), args)
	case stateTypeParallel:
		next, result, err = e.executeParallel(ctx, executionARN, name, state, args)
	default:
		next, result, err = e.executeMap(ctx, executionARN, name, state, scope.input(), scope.input())
	}

	if err != nil || e.caught {
		return next, result, err
	}

	scope.states["result"] = result

	return e.jxFinish(state, scope, next, result, nil)
}

func (e *Executor) jxResolveNumbers(state *State, scope *jxScope) error {
	for field, src := range state.numExprs {
		if field == fieldSeconds {
			continue
		}

		f, err := scope.evalNumber(src)
		if err != nil {
			return err
		}

		if f < 0 {
			return queryEvalError(fmt.Errorf("%w: %s must not be negative", errJSONataEval, field))
		}

		if e.jxNums == nil {
			e.jxNums = map[string]int{}
		}

		e.jxNums[field] = int(f)
	}

	return nil
}

func (e *Executor) jxCatchOutput(catcher *Catcher, errorResult map[string]any) (any, error) {
	scope := e.jx
	scope.states["errorOutput"] = errorResult

	assign, err := scope.evalAssign(catcher.assignVals)
	if err != nil {
		return nil, err
	}

	var out any = errorResult
	if len(catcher.Output) > 0 {
		if out, err = scope.evalValue(catcher.outputVal); err != nil {
			return nil, err
		}
	}

	e.setVars(assign)

	return out, nil
}

// jxMapItems resolves a Map's Items field (array literal or expression).
func (e *Executor) jxMapItems(state *State) ([]any, error) {
	if !state.jx.hasItems {
		return nil, queryEvalError(fmt.Errorf("%w: Map state requires Items", errJSONataEval))
	}

	v, err := e.jx.evalValue(state.jx.items)
	if err != nil {
		return nil, err
	}

	items, ok := v.([]any)
	if !ok {
		return nil, queryEvalError(fmt.Errorf("%w: Items must evaluate to an array, got %T", errJSONataEval, v))
	}

	return items, nil
}

// jxItemSelector evaluates ItemSelector per item with $states.context.Map.Item set.
func (e *Executor) jxItemSelector(state *State, items []any) ([]any, error) {
	out := make([]any, len(items))

	for i, item := range items {
		sub := e.jx.withMapItem(i, item)

		v, err := sub.evalValue(state.jx.itemSel)
		if err != nil {
			return nil, err
		}

		out[i] = v
	}

	return out, nil
}
