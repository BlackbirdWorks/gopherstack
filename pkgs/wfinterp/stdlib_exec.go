package wfinterp

import (
	"context"
	"sync/atomic"
	"time"
)

const (
	// TagExecutionError is raised when an execution started by experimental.executions fails.
	TagExecutionError = "ExecutionError"

	defaultExecWait = 1800.0
	maxMapExecs     = 1000
)

// ExecutionRequest is one execution started by experimental.executions.run or map.
type ExecutionRequest struct {
	Argument any
	Workflow string
	Location string
	Project  string
}

// ExecutionError is the error a host returns when an execution it ran finished unsuccessfully.
func ExecutionError(msg string) error { return rtErr(TagExecutionError, "%s", msg) }

func (e *Engine) addExecutions() {
	e.regBlocking("experimental.executions.run",
		[]string{"workflow_id", "location", "project_id", "argument"}, 1, fnExecRun)
	e.regBlocking("experimental.executions.map",
		[]string{"workflow_id", "arguments", paramTimeout, "location", "project_id"}, arity2, fnExecMap)
}

func execTarget(fn string, id, loc, proj any) (ExecutionRequest, error) {
	wf, err := argString(fn, "workflow_id", id)
	if err != nil {
		return ExecutionRequest{}, err
	}

	req := ExecutionRequest{Workflow: wf}

	if loc != nil {
		if req.Location, err = argString(fn, "location", loc); err != nil {
			return req, err
		}
	}

	if proj != nil {
		if req.Project, err = argString(fn, "project_id", proj); err != nil {
			return req, err
		}
	}

	return req, nil
}

func execArgument(fn string, v any) error {
	switch v.(type) {
	case nil, int64, float64, bool, string, []any, map[string]any:
		return nil
	default:
		return typeErr("%s: argument must be an integer, float, bool, string, list or map, not %s", fn, typeName(v))
	}
}

func fnExecRun(c *callCtx, a []any) (any, error) {
	const fn = "experimental.executions.run"

	req, err := execTarget(fn, a[0], a[1], a[2])
	if err != nil {
		return nil, err
	}

	if err = execArgument(fn, a[3]); err != nil {
		return nil, err
	}

	req.Argument = a[3]

	out, err := runExecutions(c, []ExecutionRequest{req}, defaultExecWait)
	if err != nil {
		return nil, err
	}

	return out[0], nil
}

func fnExecMap(c *callCtx, a []any) (any, error) {
	const fn = "experimental.executions.map"

	base, err := execTarget(fn, a[0], a[3], a[4])
	if err != nil {
		return nil, err
	}

	args, err := argList(fn, "arguments", a[1])
	if err != nil {
		return nil, err
	}

	if len(args) > maxMapExecs {
		return nil, rtErr(TagResourceLimitError, "%s: at most %d arguments are allowed", fn, maxMapExecs)
	}

	wait := defaultExecWait

	if a[2] != nil {
		if wait, err = argNumber(fn, paramTimeout, a[2]); err != nil {
			return nil, err
		}
	}

	if !(wait > 0) || wait > maxSleep {
		return nil, valueErr("%s: timeout must be between 0 and %d seconds", fn, maxSleep)
	}

	reqs := make([]ExecutionRequest, len(args))

	for i, arg := range args {
		if err = execArgument(fn, arg); err != nil {
			return nil, err
		}

		reqs[i] = base
		reqs[i].Argument = arg
	}

	out, err := runExecutions(c, reqs, wait)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func runExecutions(c *callCtx, reqs []ExecutionRequest, wait float64) ([]any, error) {
	h := c.host()
	if h.RunExecutions == nil {
		return nil, rtErr(TagOperationError, "starting executions is not available")
	}

	tctx, cancel := context.WithCancel(c.ctx)

	var timedOut atomic.Bool

	done := make(chan struct{})

	go func() {
		defer close(done)

		if c.clock().Sleep(tctx, time.Duration(wait*secondsF)) == nil {
			timedOut.Store(true)
			cancel()
		}
	}()

	out, err := h.RunExecutions(tctx, reqs)

	cancel()
	<-done

	switch {
	case err == nil:
		return out, nil
	case timedOut.Load():
		return nil, rtErr(TagTimeoutError, "timed out waiting for the executions to finish")
	default:
		return nil, err
	}
}
