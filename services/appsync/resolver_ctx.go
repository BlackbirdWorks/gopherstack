package appsync

import "errors"

const keyHeaders = "headers"

// callCtx is the per-resolver-invocation state shared by the before/after
// mappings and every pipeline function: stash, identity, info and the sink
// for $util.appendError.
type callCtx struct {
	stash      map[string]any
	identity   any
	info       map[string]any
	request    map[string]any
	sink       func(graphqlError)
	headerSink func(name, value string)
	util       *utilEnv
}

func (c *callCtx) stashMap() map[string]any {
	if c.stash == nil {
		c.stash = map[string]any{}
	}

	return c.stash
}

func (c *callCtx) utilEnv() *utilEnv {
	if c.util == nil {
		c.util = &utilEnv{headerSink: c.headerSink, appendErr: func(e *resolverError) {
			if c.sink != nil {
				c.sink(e.graphql())
			}
		}}

		if f, ok := c.info["fieldName"].(string); ok {
			c.util.field = f
			c.util.typeName, _ = c.info["parentTypeName"].(string)
		}
	}

	return c.util
}

// ctxMap builds the AppSync context object. result is included only when
// withResult is set (response mappings).
func (c *callCtx) ctxMap(args map[string]any, prev, result, source any, withResult bool) map[string]any {
	if args == nil {
		args = map[string]any{}
	}

	m := map[string]any{
		jsCtxKeyArguments: args,
		"args":            args,
		"identity":        c.identity,
		jsCtxKeySource:    source,
		"stash":           c.stashMap(),
		jsCtxKeyPrev:      map[string]any{jsCtxKeyResult: prev},
		"info":            c.info,
		"request":         c.request,
	}

	if withResult {
		m[jsCtxKeyResult] = result
	}

	if c.info == nil {
		m["info"] = map[string]any{}
	}

	if c.request == nil {
		m["request"] = map[string]any{keyHeaders: map[string]any{}}
	}

	return m
}

func (mu mappingUnit) call() *callCtx {
	if mu.Call == nil {
		return &callCtx{}
	}

	return mu.Call
}

func handlerMissing(err error) bool { return errors.Is(err, errJSNoHandler) }

// evalRequestMapping evaluates mu's request mapping (Code's request handler or
// a VTL request template). ok is false when none is configured. A #return /
// runtime.earlyReturn surfaces as an *earlyReturn error.
func evalRequestMapping(mu mappingUnit, args map[string]any, prevResult any) (any, bool, error) {
	cc := mu.call()

	switch {
	case mu.Code != "":
		out, err := runJS(mu.Code, jsHandlerRequest, cc.ctxMap(args, prevResult, nil, mu.Source, false), cc.utilEnv())
		if handlerMissing(err) {
			return nil, false, nil
		}

		if err != nil {
			return nil, true, err
		}

		if out.Early {
			return nil, true, &earlyReturnError{value: out.Value}
		}

		return out.Value, true, nil
	case mu.RequestTemplate != "":
		out, err := runVTL(mu.RequestTemplate, cc.ctxMap(args, prevResult, nil, mu.Source, false), cc.utilEnv())
		if err != nil {
			return nil, true, err
		}

		if out.Returned {
			return nil, true, &earlyReturnError{value: out.Value}
		}

		return vtlOutputValue(out), true, nil
	default:
		return nil, false, nil
	}
}

// evalResponseMapping evaluates mu's response mapping against the data
// source result, returning result unchanged when none is configured.
func evalResponseMapping(mu mappingUnit, args map[string]any, result any) (any, error) {
	cc := mu.call()

	var prev any
	if mu.PipelineAfter {
		prev = result
	}

	switch {
	case mu.Code != "":
		out, err := runJS(mu.Code, jsHandlerResponse, cc.ctxMap(args, prev, result, mu.Source, true), cc.utilEnv())
		if handlerMissing(err) {
			return result, nil
		}

		if err != nil {
			return nil, err
		}

		return out.Value, nil
	case mu.ResponseTemplate != "":
		out, err := runVTL(mu.ResponseTemplate, cc.ctxMap(args, prev, result, mu.Source, true), cc.utilEnv())
		if err != nil {
			return nil, err
		}

		return vtlOutputValue(out), nil
	default:
		return result, nil
	}
}

// evalEarly resolves an early return into the step result.
func evalEarly(err error) (any, bool) {
	if e, ok := earlyFrom(err); ok {
		return e.value, true
	}

	return nil, false
}
