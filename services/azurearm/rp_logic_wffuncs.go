package azurearm

import "strings"

func (rs *logicRunState) lookupAction(env *logicEnv, name string) (*logicActionRec, bool) {
	rs.p.mu.RLock("lookupAction")
	defer rs.p.mu.RUnlock()

	for e := env; e != nil; e = e.parent {
		if r, ok := e.local[name]; ok {
			return r, true
		}
	}

	return nil, false
}

func (rs *logicRunState) actionOutputs(env *logicEnv, name string) (any, error) {
	rec, ok := rs.lookupAction(env, name)
	if !ok {
		return nil, exprErr("the action '%s' has not run or does not exist in this scope", name)
	}

	rs.p.mu.RLock("actionOutputs")
	defer rs.p.mu.RUnlock()

	if !rec.Outputs.Set {
		return nil, exprErr("the action '%s' has no outputs (status %s)", name, rec.Status)
	}

	return rec.Outputs.Value, nil
}

func bodyOf(out any) any {
	if m, ok := out.(map[string]any); ok {
		if b, has := m[logicKBody]; has {
			return b
		}
	}

	return out
}

func (c *logicEvalCtx) frame(name string) (*logicFrame, bool) {
	for e := c.env; e != nil; e = e.parent {
		if e.frame != nil && (name == "" || strings.EqualFold(e.frame.name, name)) {
			return e.frame, true
		}
	}

	return nil, false
}

func addLogicWorkflowFuncs(m map[string]logicFunc) {
	for name, f := range workflowFuncs() {
		m[name] = ctxLogic(name, f)
	}
}

type wfFunc struct {
	f func(c *logicEvalCtx, a []any) (any, error)
	n int
}

// ctxLogic wraps a function that needs the running workflow: arguments are evaluated first.
func ctxLogic(name string, w wfFunc) logicFunc {
	return func(c *logicEvalCtx, args []*logicExpr) (any, error) {
		if len(args) != w.n {
			return nil, exprErr("function '%s' expects %d arguments", name, w.n)
		}

		vals := make([]any, len(args))

		for i, n := range args {
			v, err := c.eval(n)
			if err != nil {
				return nil, err
			}

			vals[i] = v
		}

		if c.rs == nil {
			return nil, exprErr("the function '%s' can only be used while a workflow runs", name)
		}

		return w.f(c, vals)
	}
}

func workflowFuncs() map[string]wfFunc {
	return map[string]wfFunc{
		"outputs": {n: 1, f: func(c *logicEvalCtx, a []any) (any, error) {
			return c.rs.actionOutputs(c.env, logicString(a[0]))
		}},
		logicKBody: {n: 1, f: func(c *logicEvalCtx, a []any) (any, error) {
			out, err := c.rs.actionOutputs(c.env, logicString(a[0]))
			if err != nil {
				return nil, err
			}

			return bodyOf(out), nil
		}},
		logicKActions: {n: 1, f: wfActions},
		"triggeroutputs": {
			n: 0,
			f: func(c *logicEvalCtx, _ []any) (any, error) { return c.rs.trigger["outputs"], nil },
		},
		"triggerbody": {n: 0, f: func(c *logicEvalCtx, _ []any) (any, error) {
			out, _ := objectOf(c.rs.trigger["outputs"])

			return out[logicKBody], nil
		}},
		"trigger":    {n: 0, f: func(c *logicEvalCtx, _ []any) (any, error) { return c.rs.trigger, nil }},
		"variables":  {n: 1, f: wfVariables},
		"parameters": {n: 1, f: wfParameters},
		"items": {n: 1, f: func(c *logicEvalCtx, a []any) (any, error) {
			f, ok := c.frame(logicString(a[0]))
			if !ok {
				return nil, exprErr("the loop '%s' is not running in this scope", logicString(a[0]))
			}

			return f.item, nil
		}},
		"item": {n: 0, f: func(c *logicEvalCtx, _ []any) (any, error) {
			f, ok := c.frame("")
			if !ok {
				return nil, exprErr("item() can only be used inside a repeating action")
			}

			return f.item, nil
		}},
		logicKWorkflow: {n: 0, f: func(c *logicEvalCtx, _ []any) (any, error) { return c.rs.info, nil }},
	}
}

func wfActions(c *logicEvalCtx, a []any) (any, error) {
	name := logicString(a[0])

	rec, ok := c.rs.lookupAction(c.env, name)
	if !ok {
		return nil, exprErr("the action '%s' has not run or does not exist in this scope", name)
	}

	c.rs.p.mu.RLock(logicKActions)
	defer c.rs.p.mu.RUnlock()

	out := map[string]any{
		fieldName:        name,
		logicKStatus:     rec.Status,
		logicKStartTime:  logicTime(rec.Start),
		logicKTrackingID: rec.Tracking,
	}

	if !rec.End.IsZero() {
		out[logicKEndTime] = logicTime(rec.End)
	}

	if rec.Code != "" {
		out[logicKCode] = rec.Code
	}

	if rec.Inputs.Set {
		out["inputs"] = rec.Inputs.Value
	}

	if rec.Outputs.Set {
		o := rec.Outputs.Value
		if m, isMap := objectOf(o); !isMap || m[logicKBody] == nil {
			o = map[string]any{logicKBody: o}
		}

		out["outputs"] = o
	}

	return out, nil
}

func wfVariables(c *logicEvalCtx, a []any) (any, error) {
	name := logicString(a[0])

	c.rs.p.mu.RLock("variables")
	defer c.rs.p.mu.RUnlock()

	v, ok := c.rs.vars[name]
	if !ok {
		return nil, exprErr("the variable '%s' has not been declared", name)
	}

	return v.value, nil
}

func wfParameters(c *logicEvalCtx, a []any) (any, error) {
	name := logicString(a[0])

	v, ok := c.rs.params[name]
	if !ok {
		return nil, exprErr("the workflow parameter '%s' is not defined", name)
	}

	return v, nil
}
