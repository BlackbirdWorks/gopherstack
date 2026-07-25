package codepipeline

import (
	"time"

	"github.com/google/uuid"
)

// findStage returns a pointer into p.Declaration.Stages for the stage named
// stageName, or nil if no such stage exists.
func findStage(p *Pipeline, stageName string) *Stage {
	for i := range p.Declaration.Stages {
		if p.Declaration.Stages[i].Name == stageName {
			return &p.Declaration.Stages[i]
		}
	}

	return nil
}

// findAction returns a pointer into stage.Actions for the action named
// actionName, or nil if no such action exists.
func findAction(stage *Stage, actionName string) *Action {
	for i := range stage.Actions {
		if stage.Actions[i].Name == actionName {
			return &stage.Actions[i]
		}
	}

	return nil
}

// runPipelineActions advances exec from wherever its existing action-execution
// records leave off, executing every action in declaration order,
// synchronously and instantaneously, with one exception: the first
// unresolved Approval-category action gates the run. It is recorded
// InProgress with a freshly generated approval token and processing stops
// there -- mirroring the transient wait a real AWS client observes while a
// reviewer decides, via PutApprovalResult (approvals.go). RetryStageExecution
// and RollbackStage (pipeline_state.go) also call this after resetting the
// action-execution records they mutate, so a resumed run picks up exactly
// where the reset left it.
//
// exec.Status is left at statusInProgress if a gate is hit, statusFailed if
// a previously-recorded Failed action is encountered (a rejected approval
// that was never retried -- the stage is broken and processing does not
// continue past it, matching real AWS's stage-scoped failure semantics), or
// statusSucceeded once every action in the pipeline has succeeded. Callers
// must hold b.mu.Lock.
func (b *InMemoryBackend) runPipelineActions(region string, p *Pipeline, exec *PipelineExecution) {
	actionExecs := b.actionExecutionsStore(region)
	byKey := indexActionExecutions(actionExecs[p.Declaration.Name], exec.PipelineExecutionID)

	for _, stage := range p.Declaration.Stages {
		for _, action := range stage.Actions {
			resolved, done := resolvedActionStatus(byKey, stage.Name, action.Name)
			if done {
				switch resolved {
				case statusSucceeded:
					// Already recorded, move on to the next action.
					continue
				case statusInProgress:
					exec.Status = statusInProgress
				default:
					// statusFailed (rejected approval) or
					// statusActionAbandoned (stopped while pending): the
					// stage is broken and processing does not continue
					// past it without an explicit RetryStageExecution.
					exec.Status = statusFailed
				}

				return
			}

			ae := b.runOneAction(region, p.Declaration.Name, exec.PipelineExecutionID, stage.Name, action)

			if ae.Status == statusInProgress {
				exec.Status = statusInProgress

				return
			}
		}
	}

	exec.Status = statusSucceeded
}

// indexActionExecutions builds a "stageName/actionName" lookup of the action
// executions already recorded for executionID, so runPipelineActions can
// resume from wherever a prior pass (or a retry/rollback reset) left off.
func indexActionExecutions(all []*ActionExecution, executionID string) map[string]*ActionExecution {
	byKey := make(map[string]*ActionExecution, len(all))

	for _, ae := range all {
		if ae.PipelineExecutionID == executionID {
			byKey[ae.StageName+"/"+ae.ActionName] = ae
		}
	}

	return byKey
}

// resolvedActionStatus reports the status of an already-recorded action
// execution for stageName/actionName, if one exists.
func resolvedActionStatus(byKey map[string]*ActionExecution, stageName, actionName string) (string, bool) {
	ae, ok := byKey[stageName+"/"+actionName]
	if !ok {
		return "", false
	}

	return ae.Status, true
}

// runOneAction records and executes a single action: Approval-category
// actions gate the run (InProgress + a fresh token); every other action
// completes immediately (Succeeded). Callers must hold b.mu.Lock.
func (b *InMemoryBackend) runOneAction(
	region, pipelineName, executionID, stageName string,
	action Action,
) *ActionExecution {
	now := time.Now().UTC()

	ae := &ActionExecution{
		PipelineExecutionID: executionID,
		ActionExecutionID:   uuid.NewString(),
		StageName:           stageName,
		ActionName:          action.Name,
		Status:              statusSucceeded,
		StartTime:           now,
		LastUpdateTime:      now,
	}

	if action.ActionTypeID.Category == actionCategoryApproval {
		ae.Status = statusInProgress
		ae.Token = uuid.NewString()
	}

	store := b.actionExecutionsStore(region)
	store[pipelineName] = append(store[pipelineName], ae)

	return ae
}
