package codepipeline

import (
	"context"
	"fmt"
	"slices"
)

// ListRuleExecutions returns the recorded rule executions of a pipeline, most recent first, optionally
// narrowed to one pipeline execution.
func (b *InMemoryBackend) ListRuleExecutions(
	ctx context.Context, pipelineName, pipelineExecutionID string,
) ([]map[string]any, error) {
	b.mu.RLock("ListRuleExecutions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	if !b.pipelines.Has(regionKey(region, pipelineName)) {
		return nil, b.pipelineNotFound(pipelineName)
	}

	execs := b.executionsStoreRO(region)[pipelineName]
	if pipelineExecutionID != "" && findExecution(execs, pipelineExecutionID) == nil {
		return nil, fmt.Errorf("%w: pipeline %q execution %q", ErrExecutionNotFound, pipelineName, pipelineExecutionID)
	}

	runs := b.conditionRunsStoreRO(region)[pipelineName]
	out := make([]map[string]any, 0)

	for _, run := range slices.Backward(runs) {
		if pipelineExecutionID != "" && run.PipelineExecutionID != pipelineExecutionID {
			continue
		}

		for _, ce := range run.Conditions {
			for _, rr := range slices.Backward(ce.Rules) {
				out = append(out, ruleExecutionDetail(run, rr))
			}
		}
	}

	return out, nil
}

// ListRuleTypes returns the AWS-managed CodePipeline rule types. These mirror
// the built-in condition rule providers AWS exposes.
func (b *InMemoryBackend) ListRuleTypes() []map[string]any {
	providers := []string{"Deployment", "LambdaInvoke", "CloudWatchAlarm", "VariableCheck"}

	out := make([]map[string]any, 0, len(providers))

	for _, provider := range providers {
		out = append(out, map[string]any{
			"id": map[string]any{
				"category": "Rule",
				"owner":    ruleOwnerAWS,
				"provider": provider,
				"version":  "1",
			},
		})
	}

	return out
}
