package codepipeline

import (
	"slices"
	"time"
)

const (
	keyCategory         = "category"
	keyOwner            = "owner"
	keyProvider         = "provider"
	keyVersion          = "version"
	keyStartTime        = "startTime"
	keyLastUpdateTime   = "lastUpdateTime"
	keyLastStatusChange = "lastStatusChange"
	keyStageName        = "stageName"
	keyLatestExecution  = "latestExecution"
	keyCode             = "code"
	keyName             = "name"
	keyMessage          = "message"
)

func epoch(t time.Time) float64 { return float64(t.Unix()) }

func ruleErrorDetails(rr *RuleRun) map[string]any {
	if rr.ErrorCode == "" {
		return nil
	}

	return map[string]any{keyCode: rr.ErrorCode, keyMessage: rr.Summary}
}

// ruleExecutionDetail renders types.RuleExecutionDetail for ListRuleExecutions.
func ruleExecutionDetail(run *ConditionRun, rr *RuleRun) map[string]any {
	input := map[string]any{
		"ruleTypeId": map[string]any{
			keyCategory: rr.RuleTypeID.Category,
			keyOwner:    rr.RuleTypeID.Owner,
			keyProvider: rr.RuleTypeID.Provider,
			keyVersion:  rr.RuleTypeID.Version,
		},
	}

	if len(rr.Configuration) > 0 {
		input["configuration"] = rr.Configuration
		input["resolvedConfiguration"] = rr.ResolvedConfiguration
	}

	if rr.RoleArn != "" {
		input["roleArn"] = rr.RoleArn
	}

	if rr.Region != "" {
		input["region"] = rr.Region
	}

	out := map[string]any{
		keyPipelineExecutionID: run.PipelineExecutionID,
		"pipelineVersion":      rr.PipelineVersion,
		"ruleExecutionId":      rr.RuleExecutionID,
		"ruleName":             rr.RuleName,
		keyStageName:           run.StageName,
		keyStartTime:           epoch(rr.StartTime),
		keyLastUpdateTime:      epoch(rr.LastUpdateTime),
		keyStatus:              rr.Status,
		"input":                input,
	}

	if rr.UpdatedBy != "" {
		out["updatedBy"] = rr.UpdatedBy
	}

	if details := ruleErrorDetails(rr); details != nil {
		out["output"] = map[string]any{"executionResult": map[string]any{"errorDetails": details}}
	}

	return out
}

// stageConditionStates renders the stage's BeforeEntry/OnSuccess/OnFailure ConditionState members from the
// most recent runs, keyed by their StageState wire names.
func (b *InMemoryBackend) stageConditionStates(region, pipelineName, stageName string) map[string]any {
	runs := b.conditionRunsStoreRO(region)[pipelineName]
	out := make(map[string]any)

	keys := map[string]string{
		conditionTypeBeforeEntry: "beforeEntryConditionState",
		conditionTypeOnSuccess:   "onSuccessConditionState",
		conditionTypeOnFailure:   "onFailureConditionState",
	}

	for condType, key := range keys {
		for _, run := range slices.Backward(runs) {
			if run.StageName == stageName && run.ConditionType == condType {
				out[key] = conditionStateWire(run)

				break
			}
		}
	}

	if latest := latestStageRetry(runs, stageName); latest {
		out["retryStageMetadata"] = map[string]any{
			"autoStageRetryAttempt": 1,
			"latestRetryTrigger":    retryTriggerAutomated,
		}
	}

	return out
}

func latestStageRetry(runs []*ConditionRun, stageName string) bool {
	for _, run := range slices.Backward(runs) {
		if run.StageName == stageName && run.ConditionType == conditionTypeOnFailure {
			return run.AutoRetried
		}
	}

	return false
}

func conditionStateWire(run *ConditionRun) map[string]any {
	states := make([]map[string]any, 0, len(run.Conditions))

	for _, ce := range run.Conditions {
		ruleStates := make([]map[string]any, 0, len(ce.Rules))

		for _, rr := range ce.Rules {
			latest := map[string]any{
				"ruleExecutionId":   rr.RuleExecutionID,
				keyLastStatusChange: epoch(rr.LastUpdateTime),
				keyStatus:           rr.Status,
			}

			if rr.Summary != "" {
				latest["summary"] = rr.Summary
			}

			if rr.UpdatedBy != "" {
				latest["lastUpdatedBy"] = rr.UpdatedBy
			}

			if details := ruleErrorDetails(rr); details != nil {
				latest["errorDetails"] = details
			}

			ruleStates = append(ruleStates, map[string]any{"ruleName": rr.RuleName, keyLatestExecution: latest})
		}

		execution := map[string]any{keyLastStatusChange: epoch(ce.LastStatusChange), keyStatus: ce.Status}
		if ce.Summary != "" {
			execution["summary"] = ce.Summary
		}

		states = append(states, map[string]any{keyLatestExecution: execution, "ruleStates": ruleStates})
	}

	latest := map[string]any{keyStatus: run.Status}
	if run.Summary != "" {
		latest["summary"] = run.Summary
	}

	return map[string]any{keyLatestExecution: latest, "conditionStates": states}
}
