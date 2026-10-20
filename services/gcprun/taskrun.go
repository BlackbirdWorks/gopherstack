package gcprun

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errNoExitStatus = errors.New("the container runtime cannot report exit status")

type attempt struct {
	err      error
	exitCode int
	timedOut bool
}

func taskSpec(ex *runpb.Execution, t *runpb.Task, attemptNo int) container.Spec {
	c := t.GetContainers()[0]
	env := append(envList(c),
		"CLOUD_RUN_JOB="+lastSegment(ex.GetJob()),
		"CLOUD_RUN_EXECUTION="+lastSegment(ex.GetName()),
		"CLOUD_RUN_TASK_INDEX="+strconv.Itoa(int(t.GetIndex())),
		"CLOUD_RUN_TASK_ATTEMPT="+strconv.Itoa(attemptNo),
		"CLOUD_RUN_TASK_COUNT="+strconv.Itoa(int(ex.GetTaskCount())),
	)

	return container.Spec{
		Image:      c.GetImage(),
		Name:       "gopherstack-run-" + lastSegment(t.GetName()) + "-" + strconv.Itoa(attemptNo),
		Env:        env,
		Entrypoint: c.GetCommand(),
		Cmd:        c.GetArgs(),
	}
}

// runAttempt runs one container to completion within the task timeout.
func (b *InMemoryBackend) runAttempt(ctx context.Context, runner Runner, spec container.Spec, t *runpb.Task) attempt {
	actx, cancel := context.WithTimeout(ctx, t.GetTimeout().AsDuration())
	defer cancel()

	id, err := runner.CreateAndStart(actx, spec)
	if err != nil {
		return attempt{
			err:      fmt.Errorf("start container: %w", err),
			timedOut: errors.Is(actx.Err(), context.DeadlineExceeded),
		}
	}

	defer b.stopContainer(runner, id)

	waiter, ok := runner.(container.ExitWaiter)
	if !ok {
		return attempt{err: errNoExitStatus}
	}

	code, err := waiter.WaitExit(actx, id)
	if err != nil {
		return attempt{err: err, timedOut: errors.Is(actx.Err(), context.DeadlineExceeded) && ctx.Err() == nil}
	}

	return attempt{exitCode: code}
}

func (b *InMemoryBackend) runTask(ctx context.Context, execName, taskName string) {
	runner, rerr := b.runner()
	if rerr != nil {
		b.settleTask(taskName, stateFailed, rerr.Error(), 0)

		return
	}

	for n := 0; ; n++ {
		ex, t, ok := b.beginAttempt(execName, taskName, n)
		if !ok || ctx.Err() != nil {
			b.settleTask(taskName, stateCancelled, "Task was cancelled.", 0)

			return
		}

		res := b.runAttempt(ctx, runner, taskSpec(ex, t, n), t)

		if ctx.Err() != nil {
			b.settleTask(taskName, stateCancelled, "Task was cancelled.", 0)

			return
		}

		msg := attemptMessage(res)
		if msg == "" {
			b.settleTask(taskName, stateSucceeded, "", 0)

			return
		}

		b.recordAttempt(taskName, msg, res.exitCode)

		if n >= int(t.GetMaxRetries()) || errors.Is(res.err, errNoExitStatus) {
			b.settleTask(taskName, stateFailed, msg, res.exitCode)

			return
		}
	}
}

func attemptMessage(a attempt) string {
	switch {
	case a.timedOut:
		return "Task timed out."
	case a.err != nil:
		return a.err.Error()
	case a.exitCode != 0:
		return fmt.Sprintf("Container called exit(%d).", a.exitCode)
	}

	return ""
}

// beginAttempt marks the task running and returns snapshots of its execution and task.
func (b *InMemoryBackend) beginAttempt(execName, taskName string, n int) (*runpb.Execution, *runpb.Task, bool) {
	b.mu.Lock("beginAttempt")
	defer b.mu.Unlock()

	ex, t := b.executions[execName], b.tasks[taskName]
	if ex == nil || t == nil {
		return nil, nil, false
	}

	at := b.ts()
	t.state = stateRunning
	t.pb.Retried = clamp32(n)
	t.pb.UpdateTime = at

	if t.pb.GetStartTime() == nil {
		t.pb.StartTime = at
	}

	t.pb.Conditions = []*runpb.Condition{
		newCondition("Completed", runpb.Condition_CONDITION_RECONCILING, "Task is running.", at),
	}

	if ex.pb.GetStartTime() == nil {
		ex.pb.StartTime = at
	}

	b.recountLocked(ex)

	return clone(ex.pb), clone(t.pb), true
}

func (b *InMemoryBackend) recordAttempt(taskName, msg string, exitCode int) {
	b.mu.Lock("recordAttempt")
	defer b.mu.Unlock()

	if t := b.tasks[taskName]; t != nil {
		t.pb.LastAttemptResult = &runpb.TaskAttemptResult{
			Status:   &status.Status{Code: int32(codes.Unknown), Message: msg},
			ExitCode: clamp32(exitCode),
		}
	}
}

func (b *InMemoryBackend) settleTask(taskName string, state taskState, msg string, exitCode int) {
	b.mu.Lock("settleTask")
	defer b.mu.Unlock()

	t := b.tasks[taskName]
	if t == nil || t.state.terminal() {
		return
	}

	b.finishTaskLocked(t, state, msg, exitCode)

	if ex := b.executions[t.pb.GetExecution()]; ex != nil {
		b.recountLocked(ex)
	}
}

func (b *InMemoryBackend) finishTaskLocked(t *taskRec, state taskState, msg string, exitCode int) {
	at, pb := b.ts(), t.pb
	t.state = state
	pb.CompletionTime, pb.UpdateTime, pb.Reconciling = at, at, false

	cs := runpb.Condition_CONDITION_SUCCEEDED
	if state != stateSucceeded {
		cs = runpb.Condition_CONDITION_FAILED
		pb.LastAttemptResult = &runpb.TaskAttemptResult{
			Status: &status.Status{Code: int32(codes.Unknown), Message: msg}, ExitCode: clamp32(exitCode),
		}
	} else {
		pb.LastAttemptResult = &runpb.TaskAttemptResult{Status: &status.Status{}}
	}

	pb.Conditions = []*runpb.Condition{newCondition("Completed", cs, msg, at)}
}

func clamp32(n int) int32 {
	return int32(max(math.MinInt32, min(n, math.MaxInt32)))
}
