package bedrock

import "strconv"

// AppendFoundationModelsForTest appends additional foundation models to the backend.
// This is only used in tests to populate beyond the default seeded models.
func (b *InMemoryBackend) AppendFoundationModelsForTest(models []*FoundationModelSummary) {
	b.mu.Lock("AppendFoundationModelsForTest")
	defer b.mu.Unlock()
	b.foundationModels = append(b.foundationModels, models...)
}

// AddBuildWorkflowForTest adds a build workflow to the backend for testing.
func (b *InMemoryBackend) AddBuildWorkflowForTest(policyARN string) *AutomatedReasoningPolicyBuildWorkflow {
	b.mu.Lock("AddBuildWorkflowForTest")
	defer b.mu.Unlock()

	b.arpWorkflowCounter++
	id := "bw-" + strconv.Itoa(b.arpWorkflowCounter)

	wf := &AutomatedReasoningPolicyBuildWorkflow{
		BuildWorkflowID: id,
		PolicyArn:       policyARN,
		Status:          "Running",
	}
	b.arpBuildWorkflows[id] = wf

	return wf
}
