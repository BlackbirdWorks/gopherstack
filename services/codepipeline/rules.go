package codepipeline

import "context"

// ListRuleExecutions returns rule executions for a pipeline. The emulator does
// not run condition rules, so this returns an empty (but valid) list for a known
// pipeline and ErrNotFound otherwise.
func (b *InMemoryBackend) ListRuleExecutions(ctx context.Context, pipelineName string) ([]map[string]any, error) {
	b.mu.RLock("ListRuleExecutions")
	defer b.mu.RUnlock()

	if !b.pipelines.Has(regionKey(getRegion(ctx, b.region), pipelineName)) {
		return nil, ErrNotFound
	}

	return []map[string]any{}, nil
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
