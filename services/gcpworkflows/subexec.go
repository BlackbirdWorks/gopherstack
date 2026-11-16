package gcpworkflows

import (
	"context"
	"sync"

	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

// maxExecutionNesting bounds executions that start executions; the nesting limit is an emulator restriction.
const maxExecutionNesting = 20

type depthKey struct{}

// runExecutions starts one execution per request, waits for all of them and returns the results in order.
func (b *InMemoryBackend) runExecutions(
	ctx context.Context, info runInfo, reqs []wfinterp.ExecutionRequest,
) ([]any, error) {
	depth, _ := ctx.Value(depthKey{}).(int)
	if depth+1 > maxExecutionNesting {
		return nil, wfinterp.ExecutionError("executions started from executions are nested too deeply")
	}

	names := make([]string, 0, len(reqs))

	for _, rq := range reqs {
		name, err := b.startChild(info, rq, depth+1)
		if err != nil {
			b.cancelAll(names)

			return nil, err
		}

		names = append(names, name)
	}

	results := make([]any, len(names))
	errs := make([]error, len(names))

	var wg sync.WaitGroup

	for i, n := range names {
		wg.Go(func() { results[i], errs[i] = b.awaitChild(ctx, n) })
	}

	wg.Wait()

	if ctx.Err() != nil {
		b.cancelAll(names)

		return nil, ctx.Err()
	}

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

func (b *InMemoryBackend) startChild(info runInfo, rq wfinterp.ExecutionRequest, depth int) (string, error) {
	project, location := info.project, info.location

	if rq.Project != "" {
		project = rq.Project
	}

	if rq.Location != "" {
		location = rq.Location
	}

	arg := ""

	if rq.Argument != nil {
		raw, err := wfinterp.ToJSON(rq.Argument)
		if err != nil {
			return "", wfinterp.ExecutionError("the argument cannot be encoded as JSON: " + err.Error())
		}

		arg = string(raw)
	}

	ex, err := b.createExecution(&executionspb.CreateExecutionRequest{
		Parent:    workflowName(project, location, rq.Workflow),
		Execution: &executionspb.Execution{Argument: arg},
	}, depth)
	if err != nil {
		return "", wfinterp.ExecutionError(err.Error())
	}

	return ex.GetName(), nil
}

func (b *InMemoryBackend) cancelAll(names []string) {
	for _, n := range names {
		_, _ = b.CancelExecution(&executionspb.CancelExecutionRequest{Name: n})
	}
}

func (b *InMemoryBackend) awaitChild(ctx context.Context, name string) (any, error) {
	b.mu.RLock("awaitChild")
	rec, ok := b.execs[name]

	var done chan struct{}
	if ok {
		done = rec.done
	}

	b.mu.RUnlock()

	if !ok {
		return nil, wfinterp.ExecutionError("execution " + name + " no longer exists")
	}

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	b.mu.RLock("awaitChild")
	rec, ok = b.execs[name]

	var pb *executionspb.Execution
	if ok {
		pb = rec.pb
	}

	var state executionspb.Execution_State
	var result, msg string

	if pb != nil {
		state, result, msg = pb.GetState(), pb.GetResult(), pb.GetError().GetContext()
	}

	b.mu.RUnlock()

	if state != executionspb.Execution_SUCCEEDED {
		return nil, wfinterp.ExecutionError("execution " + name + " finished " + state.String() + ": " + msg)
	}

	if result == "" {
		return nil, nil //nolint:nilnil // a workflow without a result yields null
	}

	v, err := wfinterp.ParseJSON([]byte(result))
	if err != nil {
		return nil, wfinterp.ExecutionError(err.Error())
	}

	return v, nil
}
