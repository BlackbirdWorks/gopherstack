package budgets

import (
	"context"
	"errors"
	"slices"
)

var errNoExecutor = errors.New("no action executor is configured")

// ExecuteBudgetAction drives an action through approve, retry, reverse or reset.
func (b *InMemoryBackend) ExecuteBudgetAction(
	ctx context.Context, account, name, id, execType string,
) error {
	if err := validateAccount(account); err != nil {
		return err
	}

	b.mu.Lock("ExecuteBudgetAction")

	r, err := b.actionLocked(account, name, id)
	if err != nil {
		b.mu.Unlock()

		return err
	}

	st := r.Action.Status

	if inProgress(st) {
		b.mu.Unlock()

		return fail(ErrResourceLocked, "action %s is in progress", id)
	}

	switch execType {
	case "APPROVE_BUDGET_ACTION":
		err = requireStatus(execType, st, statusPending)
	case "RETRY_BUDGET_ACTION":
		err = requireStatus(execType, st, statusExecFailure)
	case "REVERSE_BUDGET_ACTION":
		err = requireStatus(execType, st, statusExecSuccess, statusReverseFailure)
	case "RESET_BUDGET_ACTION":
		err = requireStatus(execType, st, statusPending, statusExecFailure, statusReverseSuccess,
			statusReverseFailure, statusResetFailure)
	default:
		err = fail(ErrInvalidParameter, "ExecutionType %q is not valid", execType)
	}

	if err != nil {
		b.mu.Unlock()

		return err
	}

	if execType == "RESET_BUDGET_ACTION" {
		r.Action.Status = statusStandby
		b.recordLocked(r, eventExecute, statusResetInProgress, "Reset started")
		b.recordLocked(r, eventExecute, statusStandby, "Action reset to standby")
		b.mu.Unlock()

		return nil
	}

	reverse := execType == "REVERSE_BUDGET_ACTION"
	key := r.Key
	act, exec := b.beginRunLocked(r, reverse, eventExecute)
	b.mu.Unlock()

	b.finishRun(ctx, account, key, act, exec, reverse, eventExecute)

	return nil
}

func requireStatus(execType, got string, allowed ...string) error {
	if slices.Contains(allowed, got) {
		return nil
	}

	return fail(ErrInvalidParameter, "%s is not valid for an action in status %s", execType, got)
}

// beginRunLocked marks the action in progress and returns what the run needs.
func (b *InMemoryBackend) beginRunLocked(r *ActionRecord, reverse bool, event string) (Action, ActionExecutor) {
	st, msg := statusExecInProgress, "Execution started"
	if reverse {
		st, msg = statusReverseInProgress, "Reversal started"
	}

	r.Action.Status = st
	b.recordLocked(r, event, st, msg)

	return *deepCopy(&r.Action), b.executor
}

// finishRun runs the executor outside the lock, then records the outcome.
func (b *InMemoryBackend) finishRun(
	ctx context.Context, account, key string, act Action, exec ActionExecutor, reverse bool, event string,
) {
	runErr := errNoExecutor
	if exec != nil {
		runErr = exec.Run(ctx, account, act, reverse)
	}

	okSt, failSt := statusExecSuccess, statusExecFailure
	okMsg := "Action executed successfully"

	if reverse {
		okSt, failSt = statusReverseSuccess, statusReverseFailure
		okMsg = "Action reversed successfully"
	}

	b.mu.Lock("finishRun")
	defer b.mu.Unlock()

	r, found := b.actions.Get(key)
	if !found {
		return
	}

	if runErr != nil {
		r.Action.Status = failSt
		b.recordLocked(r, event, failSt, runErr.Error())

		return
	}

	r.Action.Status = okSt
	b.recordLocked(r, event, okSt, okMsg)
}
